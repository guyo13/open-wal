# Kickoff C — `open-wal-replica` RM3: catch-up / resume (§9)

> **Status: DRAFT, not yet dispatched.** It waits on two things:
> 1. PR #53 (RM0–RM2) merged to `main`;
> 2. **the owner's decision on the §16 resumable WAL reader position** (WAL v7.2,
>    designer-recommended *before* RM3). If it lands first, the dispatched kickoff adds one
>    scope item ("chunks after the first in a session use `Wal::reader_at`") and T5's bound
>    drops the segment-prefix term. Everything else below is unchanged.
>
> **How this draft was hardened.** Three adversarial critique rounds (28 + 27 + 32 findings).
> Round 1 was re-verified against the RM2 code, with zero findings refuted. The main ones per
> round:
> - **Round 1:** an attach that races session teardown; an R1/ceiling mismatch; a mid-stream
>   `RESEED_REQUIRED` that the real `Receiver` rejects; vacuous T1/T5/T6/T8.
> - **Round 2:** the reader cap stranding requests; T8 opening no reader; T12 contradicting
>   its own rule; L8 unable to drive `shipper.rs`.
> - **Round 3:**
>   - the `poll(&wal)` signature breaks RM2 tests the kickoff protected;
>   - the writer stall is per **record** (measured: 1.34 s per 32 MiB segment of empty
>     records);
>   - the shipped `abandon` must never give up;
>   - there was no observable for a pending request;
>   - stickiness must not cover transient I/O.
>
> The spec (§9–§9.3, §15.7, §16) and this kickoff are the corrected contract.

You are implementing **RM3** of `open-wal-replica`.
- A replica that (re)connects behind the live ring is served the missing records **from the
  primary's log**, in bounded chunks on the writer thread. It is then cut over to the ring.
- A replica that overflows the ring falls back to catch-up **inside its session**.
- This replaces RM2's `hold()` and RM2's close-and-re-HELLO on overflow.

Everything you need from the WAL is on `main`. **Do not touch `open-wal`'s `src/`.**

## Read first — in this order, before any code
1. `crates/open-wal-replica/CLAUDE.md`. Its RM2 status block lists what was deferred to you.
2. `docs/replica_design_v1.md`:
   - **§9, §9.1, §9.2, §9.3** in full: the build contract, normative down to the answer
     variants, the slot state machine and its exported sequences.
   - Then §5, §6 (the `RESEED_REQUIRED` row), §8.2, §8.3, §15.2 P5, §15.3, §15.7.4–15.7.5,
     §16.
3. The RM2 review threads on PR #53, for the bug classes this crate has already hit.
4. The WAL API you drive:
   - `Wal::reader_from`: it borrows `&Wal`, starts at the containing segment's base, and
     makes two `pread`s per record;
   - `Reader::next`: it ends with **`None`** on an invalid record or an LSN break;
   - `oldest_lsn`, `durable_lsn`, `last_lsn`;
   - `commit()` on an empty batch returns `Ok(durable_lsn)`;
   - a WAL seeded at `s` has `last_lsn = s − 1`.

## Scope (the spec is authoritative; this is the checklist)
1. **Slot module (§9.3).**
   - It builds under `--cfg loom`.
   - States `IDLE → PENDING → TAKEN → DONE → IDLE`. The answer and the buffer live in
     `crate::sync::UnsafeCell`, and the slot holds the network thread's waker.
   - Exports `post`, `answer(f)` (stores `DONE`, then unparks), `await_answer` and
     `abandon(consumer)`:
     - `abandon` tries CAS `PENDING → IDLE`; an `IDLE` slot means detach and return;
       otherwise it waits for `DONE`;
     - neither `await_answer` nor `abandon` has an exit before `DONE` except shutdown.
   - `Producer::attach` is called only inside `answer`'s closure. `AlreadyAttached` means
     end the session loudly and `debug_assert!`.
   - Add a `#[doc(hidden)]` `Shipper::request_pending(id)`, read-only.
2. **Join decision (§9 steps 1–3) in `Shipper::poll(&mut self, wal: &Wal<O>) -> Result<()>`.**
   - A join never reads: Serve{Attached | Wait | NeedChunk}, Reseed, or Reject.
   - Step 2 is the **only** rejection. Delete RM2's `Ahead ⇒ JOIN_AHEAD ⇒ Rejected`.
   - `primary_oldest = wal.oldest_lsn()` at decision time.
   - `d ≥ ring::MAX_LSN` ⇒ `ERR(Protocol)` on the network thread.
   - `on_commit` is release + wake only.
   - Joins are answered before any reader is opened in a pass.
3. **Chunked catch-up (§9.1).**
   - Ceiling = **`Producer::released()`**, the clamped value.
   - Answers: Attached / Wait / Deferred / Chunk{records, attached?} / BelowOldest /
     ReadFailed / Terminal.
   - The chunk is bounded by `catchup_chunk_bytes`, counting `payload + 32` per record, and
     by `catchup_chunk_records`. It always takes at least one record.
   - The reader cap is `catchup_replicas_per_poll`. The pass starts at a rotating index and
     always runs to completion; errors are returned after the pass.
   - `Shipper::new` rejects a zero cap and zero records.
   - Defaults: 16 MiB / 16 384 / 1.
   - **ReadFailed:**
     - `None` or an LSN mismatch ⇒ sticky, **data-level only**, replacing answer 3 only;
     - an I/O `Err` ⇒ fails that answer, not sticky;
     - `poll` returns `Err(PrimaryLogUnreadable)` only for a pass that answered ReadFailed.
   - `checked_next()` everywhere.
4. **Cutover and in-session fall-back (§9.2).**
   - One sending-side check function sits on every encoded record. A mismatch ⇒
     `ERR(Contiguity)` + close, and the record is never sent.
   - `resume()` must return the answered cursor.
   - `NeedsCatchUp` ⇒ detach and post `Chunk{next}`, staying in the same session.
   - BelowOldest ⇒ close with a FIN and reconnect at once.
   - **Wait re-posts when `released ≥ cursor`.** That is a condition re-checked before every
     park, not an event.
   - **Heartbeat** on one session-level timer across all post-`SERVING` waits. Nothing is
     sent before `SERVING`.
5. **R4.** Publish `sent` before **every** write. The ACK reader runs from `SERVING` onward.
   Set `acked = d` on acceptance.
6. **Status.** `Connecting → Joining → CatchingUp ⇄ Streaming`, plus `Rejected`,
   `NeedsReseed` and `Failed`. Add `catching_up()`.
7. **LSN domain (§9.2).**
   - `Shipper::new`: `checked_next` first, then `last_lsn ≥ MAX_LSN` ⇒ `Err`.
   - `Shipper::capture` pre-checks `lsn > MAX_LSN` and goes terminal **without** calling
     the ring.
   - Ring fast-path check: defence in depth, and the only `ring.rs` change.
8. **Delete `hold()` and `SessionEnd::FellBehind`'s reconnect.**
9. **Docs.**
   - Crate `CLAUDE.md`: status, the mutation table, and the integrator obligations
     (cadence; §8.2 reopen).
   - Spec §15.7.8: as-built notes.

## Edits allowed to existing RM2 tests (exactly these; the PR diff must show nothing else)
1. **Mechanical API migration** in `tests/shipper.rs`: `sh.poll()` → `sh.poll(&wal).unwrap()`.
   No other change to those tests.
2. **Cadence.** In `acks_during_a_blocked_batch_write_keep_the_session`,
   `two_replicas_stream_independently` and `primary_restart_resumes_from_replica_hello`,
   insert `sh.poll(&wal).unwrap()` after each `on_commit`. Joins now happen only in `poll`,
   and these loops otherwise starve the receiver past `idle_timeout`. No assertion changes.
3. **`assert_no_inband_errors`** also fails on
   `Remote{code: Contiguity | Protocol | WireCrc | Poisoned}`.
4. **Rewrite `stalled_replica_overflows_without_blocking_the_writer`.** It asserts the
   reconnect-on-overflow, `NeedsCatchUp` and `hold` behaviour that RM3 deletes.
   - **Keep:** worst `capture` / `on_commit` < 50 ms, and **poll < 50 ms for polls that open
     no reader**.
   - **Bound reading polls** by the §9.1 formula, or shrink the geometry (e.g. 1 MiB
     segments, 64 KiB chunks) and state the per-poll budget.
   - **Keep:** the dense-prefix check.
   - **Replace** the reconnect, hold and status assertions with in-session
     `CatchingUp → Streaming` and convergence.

Any other existing test that fails on a correct RM3 build: stop and report it.

## Shared test infrastructure (independent oracles)
- **Forwarding proxy** in `tests/common`, on loopback between `Shipper` and the real
  `Receiver`. It:
  - forwards raw bytes and tee-parses them (a parse error fails the test);
  - propagates half-closes and counts sessions;
  - per session, checks `SERVING.from == HELLO.d + 1`, dense `RECORD`s, no `ERR` either way,
    and every `ACK ≤ DurableTap` at forwarding time;
  - records the first P→R frame and `primary_oldest`;
  - can pause forwarding, and can wait for a forwarded ACK.
- **`rchar` / `syscr` probe.** The writer thread's `/proc/thread-self/io` deltas around a
  `poll`, minus the measured self-cost. Linux-only.
- **`request_pending(id)`** is for reachability only. Shipper counters are never an oracle.

## Anti-vacuity rule
- **"A reader ran"** is proven by a structural precondition, or by `rchar > 0` for that
  poll. The structural precondition is that the ring cannot hold the cursor: the `Shipper`
  was created after the prefix, or an overflow was observed. The wire proves only that
  records were shipped. Ring-served and log-served frames are byte-identical.
- **Ceiling rows** need `cursor ≤ released`, and a chunk budget with room for **at least one
  record above the ceiling**. Otherwise the budget, not the ceiling, ends the chunk.
- **Failure rows** (ReadFailed) accept `rchar > 0` plus the returned `Err` as proof that a
  reader ran.

## Tests — each shown failing on a deliberately broken build, then reverted
Record every row (mutation → test → failure message → reverted) in the PR description and in
the crate `CLAUDE.md`. **A test that passes with its mutation applied is too weak: fix the
test, never the mutation.** Rows marked *(liveness)* have no crate mutation that can fail
them; say so in the PR.

| # | Test | Mutation(s) that must fail it |
|---|---|---|
| T1 | **Overflow → in-session catch-up → cutover.** Real `Receiver` behind the proxy; small segments; concurrent appends/commits. Exactly one overflow, induced by the proxy pause, with the ring sized so no other overflow can happen. After the first chunk answer, wait for a forwarded ACK ≥ that chunk's first LSN before the next `poll`. **Oracles:** receiver results `len() == 1`; no in-band `Err`/`Remote`; proxy density, no ERR, ACK ≤ tap; byte-identical, LSN-mirrored, `acked == durable`. **Reachability:** catch-up spans ≥ 2 polls, and `CatchingUp → Streaming` is seen while the writer is committing. | attach at `cursor ± 1`; advance past an un-copied record; catch-up writes that do not publish `sent` |
| T2 | **P5 + boundary table.** Proptest: a checkpoint past a lagging replica ⇒ the next HELLO's first frame is `RESEED_REQUIRED{wal.oldest_lsn()}`. Table via a fake replica: primary seeded at 1000 and checkpointed so `P = oldest > 1001`; `d ∈ {P−2, P−1, P}`, each with `d+1` in the ring and not. Expected: `P−2` ⇒ `RESEED_REQUIRED{P}` and never `SERVING`; `P−1` ⇒ `SERVING{P, P}` then dense; `P` ⇒ `SERVING{P+1, P}`. | `d+1 <= oldest`; `d+2 < oldest`; `primary_oldest` = 0; `primary_oldest` cached at `Shipper::new` |
| T3 | **Step 2 boundaries**, judged on raw frames. (i) `d == durable` ⇒ `SERVING{d+1}`, then exactly `d+1..d+N`. (ii) `d == durable+1` ⇒ `ERR(Contiguity)` + close; ≥ 2 more commits + polls ⇒ zero `RECORD`s. (iii) `d == ring::MAX_LSN` ⇒ `ERR(Protocol)`. (iv) `heartbeat = 10 s`. `poll` between `commit()` and `on_commit()` with HELLO `d == wal.durable_lsn() > released` ⇒ `SERVING{d+1}`. After `on_commit`, keep appending/committing and polling every ≤ 10 ms; the first `RECORD` arrives within 1 s. Run (i) and (ii) with `d+1` in and out of the ring. | `d+1 > durable`; `d > durable+1`; restore `Ahead ⇒ reject`; refuse only `d > MAX_LSN`; Wait re-posts on heartbeat only |
| T4 | **R7 restart with two lagging replicas.** They converge via catch-up; T1 oracles. | catch-up never starts |
| T5 | **Writer stall (Linux).** `segment_size = 8 MiB`, `C = 1 MiB`, lag ≥ 64 MiB, `d` mid-segment. Per reading `poll`: `rchar ≤ segment_size + C + max framed + 4 KiB` **and** `syscr ≤ 2·(segment records + catchup_chunk_records) + slack`. Variants: 4 KiB records; **0-byte payloads** (the record cap binds). Companions, each `rchar ≈ 0`: the poll that answers the **join**; a poll with nothing pending; every poll in a ≥ 2-heartbeat window where a **chunk request** waits (`released < cursor ≤ durable`, no `on_commit`). Record in the PR that "≤ C + one record" fails a correct build. | unchunked; no record cap (0-byte variant); join reads its first chunk inline; Wait falls through to answer 3 |
| T6 | **Mid-catch-up checkpoint.** Tiny segments; chunk = a few records; the test thread drives `poll` k times. `c−1` is the last LSN shipped. Then checkpoint far past `c`. Preconditions: `oldest > c`, and `released + 1 − oldest > ring capacity + one chunk`. **Oracles:** session 1 = `SERVING{d+1}`, dense `d+1..=c−1`, EOF, no `RECORD ≥ c`, no ERR. Session 2's first P→R frame is `RESEED_REQUIRED{wal.oldest_lsn()}`. Receiver results: `[0] = Ok(())`, `[1] = Err(ReseedRequired{P})`, and **every later entry also** `Err(ReseedRequired{P})` (the shipper keeps reconnecting). Replica log exactly `d+1..=c−1`. | swallow the below-oldest error and jump to `oldest` |
| T7 | **R4 detection power.** T1 and T4 sample `acked` before the tap after every poll/commit/wait iteration; the proxy checks every ACK at forwarding time. | the strong-form ack-before-commit mutation must fail **T1 and T4 themselves** |
| T8a | **Ceiling = `released` (per-PR, structural).** Commit `1..=R` **before** `Shipper::new`, so the ring starts at `R+1`. Fake replica with HELLO `d < R`; the lag plus one more record fits one chunk. Commit `R+1..` **without** `on_commit`, then `poll` until the replica has `RECORD == R`. On Linux, assert `rchar > 0` for that poll. Nothing `> R`. Then `on_commit`. | ceiling = `wal.durable_lsn()` |
| T8b | **Poisoned primary (Linux, LD_PRELOAD EIO shim, `#[ignore]` + script + paths-filtered CI).** Same structural setup as T8a. Arm the one-shot shim only while no replica WAL can sync. Failing batch: **K=0** (`durable` stays `R`) or **K=2** (split: `R+1..=S` synced). WAL preconditions: injection count ≥ 1; `durable < last`; `reader_from(durable+1)` yields ≥ 1. `poll` the poisoned handle. **Positive control:** exactly `d+1..=R` received; `rchar > 0`. **Oracle:** nothing `> R`. **§8.2 extension** *(liveness)*: reopen in-process, `on_commit(wal.commit()?)`, `poll` ⇒ exactly the recovered prefix, byte-identical; then the stream stays dense. | read to the reader's end / `last_lsn` (K=0); ceiling = `wal.durable_lsn()` (K=2) |
| T9 | **Unreadable committed record.** Replica C is **streaming**. After open, flip a payload byte of a record in a **sealed** segment below `oldest_valid`. Replica A's catch-up ⇒ `poll` returns `Err(PrimaryLogUnreadable{lsn})`; A `Failed`; nothing `≥ lsn` on A's wire. Then wait for `request_pending(B)`, where B's cursor is above `lsn` in the same segment: the measured `poll` returns `Err`, with `rchar <` one record. C's proxy session count stays 1, and C keeps receiving new commits. **Non-sticky variant:** an I/O `Err` on one answer (e.g. the segment file made unreadable, then restored) fails that answer, and the next request reads normally. | `None` as end of chunk; per-cursor stickiness; rescan per request; ReadFailed ends all sessions; I/O errors made sticky |
| T10 | **Cadence under load.** Tight commit loop, `poll(&wal)` **only** after each `on_commit`. A fresh real `Receiver` reaches `SERVING` within `idle_timeout` and converges. | `poll` decides joins only when no release happened since its previous call |
| T11 | **Teardown liveness** *(liveness)*. Drop the socket mid-catch-up across several polls and reconnect. The next session streams; the proxy counts exactly the induced sessions; no `AlreadyAttached`. (The ordering rows belong to L8, L8b and the slot unit test.) | the session never re-posts after a teardown |
| T12 | **LSN domain.** Seed `MAX_LSN + 1` ⇒ `Shipper::new` `Err`. Seed `MAX_LSN` ⇒ `Ok`; `MAX_LSN` is shipped; the next capture ⇒ terminal (`ERR(Protocol)`, `poll` ⇒ `Err(LsnDomainExhausted)`, nothing `> MAX_LSN`). A replica whose chunk request is in flight at that moment gets **Terminal**, with no read and no panic. `u64::MAX` ⇒ `LsnExhausted`. **Ring unit tests** at `2^56 − 1` and `2^56` (`catch_unwind` + `is_err() == cfg!(debug_assertions)`; nothing written; no emit). | `last_lsn > MAX_LSN`; no `Shipper::capture` pre-check; drop the ring fast-path check (unit tests); a raw-`w` ceiling (the in-flight variant panics) |
| T13 | **Reader cap and round-robin.** Two fake replicas, both lagging far beyond a chunk, cap 1; both fake replicas drain promptly. Each round: wait until `request_pending` holds for **both**, `poll` once, then assert exactly one received records (`rchar` ≤ one answer's bound). Over k rounds they alternate. | Deferred not re-posted; cap ignored; no round-robin |
| T14 | **Sending-side check (unit).** The one check function: it accepts `lsn == next` and returns `checked_next`; it rejects ±1 and anything else. Its wiring is a review item. | delete the check |
| T15 | **Heartbeat rules.** Real `Receiver` behind the proxy, `idle_timeout = 3 × heartbeat`. (a) Withhold polls for 2 × heartbeat after the HELLO ⇒ the first P→R frame is `SERVING`, with no ERR. (b) With a post-`SERVING` chunk request pending (`request_pending`), withhold polls for 5 × heartbeat ⇒ HEARTBEATs seen and session count stays 1. (c) Same for a Wait loop. | heartbeat before `SERVING`; no heartbeat while awaiting an answer; a per-wait (not session-level) timer |
| T16 | **`poll` finishes its pass.** T9 corrupt-segment setup, with A's first chunk request and B's join both pending (`request_pending`), in both id orders; one `poll` ⇒ B gets `SERVING` within 1 s, while A's fake replica stays silent. | return the first error immediately |
| T17 | **At least one record.** `catchup_chunk_bytes = 64 B`, 1 KiB payloads, lag outside the ring ⇒ converges, one record per chunk. Plus a `Shipper::new` unit test rejecting a zero cap and zero records. | drop "at least one"; accept a zero cap |
| U1 | **Slot unit test (std, no hooks).** Thread W runs `answer` with an `f` that signals, then sleeps 300 ms, then attaches. Once signalled, thread N calls `abandon` ⇒ it returns only after `f` finishes; cursor `PARKED`; slot `IDLE`. Also `abandon` on an `IDLE` slot returns at once. | `abandon` gives up after a bounded wait; no `IDLE` case (hangs) |

**Interop.** T1, T4, T6, T10, T11 and T15 run against the real `Receiver`; this replaces the
`FakeReplica`-only `hold` test.

**Loom (§15.7.4–15.7.5):**
- Add **L7** (bound 4), **L7b** (exhaustive), **L8** and **L8b**. Each includes the specified
  finish step and drives the crate's ring and slot module (`post` / `answer` /
  `await_answer` / `abandon`, with the waker registered), never a copy.
- Rows: the measured L7/L7b shapes; L8's four teardown rows; L8b's `await_answer` `Relaxed`
  row.
- Re-run **all nine existing rows** after the `ring.rs` change.
- If a model or bound does not reproduce, report it with numbers. Do not weaken it.

## Guardrails
- **Do not touch `open-wal`'s `src/`.** A WAL change is the owner's §16 decision.
- The only `ring.rs` change allowed is the fast-path bound check.
- Never `reader_from` in `on_commit`, on a network thread, or for a join, Wait, Deferred,
  Terminal or sticky-ReadFailed answer.
- No checkpoint observer, no writer-side gating, no `truncate_after`, no HELLO wire change,
  and no test-only fault hooks in shipped code. `request_pending` is a read-only accessor.
- Edit existing tests only as listed above.
- Every oracle must be independent of the code under test.

## Before pushing, run
- `cargo test -p open-wal-replica`, three times.
- `cargo clippy -p open-wal-replica --all-targets -- -D warnings`, normal and `--cfg loom`.
- `cargo fmt --all -- --check`.
- `cargo +1.85.0 check -p open-wal-replica --all-targets --locked`.
- `RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=target/loom cargo test -p open-wal-replica --test loom_ring --release`.
- T8b via its script, under the shim.
- Root `cargo test`. The root `src/` diff must be empty.

## Definition of done
- §9–§9.3 are implemented as written.
- `hold()` and the overflow reconnect are deleted.
- T1–T17 and U1 are green, each mutation shown failing; *(liveness)* rows are recorded as
  such.
- L7, L7b, L8 and L8b are added, with their rows shown failing.
- The nine existing rows have been re-verified.
- RM4 (re-seed) is **not** part of this kickoff.
