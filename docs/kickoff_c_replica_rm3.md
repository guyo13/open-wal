# Kickoff C — `open-wal-replica` RM3: catch-up / resume (§9)

> **Status: DRAFT, not yet dispatched.** Dispatch it only after PR #53 (RM0–RM2) is merged to
> `main`. Branch off a `main` that contains #53.
>
> **How this draft was hardened.**
> - **Round 1:** an adversarial critique of the first draft produced 28 findings. All were
>   re-verified against the RM2 code; none was refuted.
> - **Round 2:** a critique of the revision produced 27 more, the main ones found
>   independently by two reviewers:
>   - the per-poll reader cap could strand a request;
>   - T8a/T8b opened no reader in their natural setup;
>   - T12 contradicted its own rule;
>   - teardown races cannot be hit deterministically from an integration test.
>
> The spec (§9–§9.3) and this kickoff are the corrected contract.

You are implementing **RM3** of `open-wal-replica`.
- A replica that (re)connects behind the live ring is served the missing records **from the
  primary's log**, in bounded chunks on the writer thread. It is then cut over to the ring.
- A replica that overflows the ring falls back to catch-up **inside its session**.
- This replaces RM2's `hold()` and RM2's close-and-re-HELLO on overflow.

Everything you need from the WAL is already on `main`: v7's `Wal::oldest_lsn()` and
containing-segment `reader_from`, and v7.1's `checked_next`. **Do not touch `open-wal`'s
`src/`.**

## Read first — in this order, before any code
1. `crates/open-wal-replica/CLAUDE.md`. Its RM2 status block records what was deferred to
   you: `hold()`, the real-`Receiver` interop test, and the ring `MAX_LSN` vs seed issue.
2. `docs/replica_design_v1.md`:
   - **§9, §9.1, §9.2 and §9.3** in full. They are the build contract, normative down to the
     answer variants, the slot state machine and the exported slot sequences.
   - Then §5 (R1/R3/R4/R6/R7), §6 (the `RESEED_REQUIRED` row), §8.2 (in-process reopen),
     §8.3, §15.2 P5, §15.3, §15.7.4–15.7.5 (L7, L7b, L8 and their rows), §16.
3. The RM2 review threads on PR #53. They show the bug classes this crate has already hit:
   - self-reported oracles (twice);
   - an R4 bound published after the write;
   - a silent primary wedging the receiver;
   - stale `acked` after a wipe.
4. The WAL API you drive:
   - `Wal::reader_from`: it borrows `&Wal`; a read below `oldest_lsn` returns
     `WalError::ContiguityViolation`, and it starts at the containing segment's base;
   - `Reader::next`: it ends with **`None`** on an invalid record or an LSN break;
   - `oldest_lsn`, `durable_lsn`, `last_lsn`;
   - `commit()` on an empty batch returns `Ok(durable_lsn)`;
   - a WAL seeded at `s` has `last_lsn = s − 1`.

## Scope (the spec is authoritative; this is the checklist)

1. **Request-slot module (§9.3).**
   - States `IDLE → PENDING → TAKEN → DONE → IDLE`, with a cancel CAS `PENDING → IDLE`.
   - The answer and the buffer live in `crate::sync::UnsafeCell`.
   - It exports `post`, `answer(f)`, `abandon(consumer)` and `await_answer`; `shipper.rs`
     calls them verbatim.
   - The module builds under `--cfg loom`.
   - `Producer::attach` is called **only** inside an `answer` closure.
   - `AlreadyAttached` is an internal bug: end the session loudly and `debug_assert!`. It
     never maps to `JOIN_BEHIND`.
2. **Join decision (§9 steps 1–3) in `Shipper::poll(&mut self, wal: &Wal<O>) -> Result<()>`.**
   - A join answer **never opens a reader**. It answers Serve with Attached, Wait or
     NeedChunk; Reseed; or Reject.
   - Step 2 (`d > wal.durable_lsn()` ⇒ `ERR(Contiguity)`) is the **only** rejection.
     **Delete RM2's `Ahead ⇒ JOIN_AHEAD ⇒ Rejected`.**
   - `SERVING.primary_oldest = wal.oldest_lsn()`, read at decision time.
   - A HELLO with `d ≥ ring::MAX_LSN` is refused on the network thread with
     `ERR(Protocol)`.
   - `on_commit` becomes release + wake only. Rewrite the module docs with the normative
     cadence (§9.1).
3. **Chunked catch-up (§9.1).**
   - Answers: Attached / Wait / Deferred / Chunk{records, attached?} / BelowOldest /
     ReadFailed.
   - Read only while `lsn ≤ released`. **Wait and Deferred open no reader.**
   - The chunk budget counts **`payload.len() + 32` per record**.
   - Readers per `poll` are capped at `catchup_replicas_per_poll`. The pass starts at a
     rotating index, and `poll` always finishes its pass, returning errors afterwards.
   - New `ShipperConfig` knobs, with documented defaults: `catchup_chunk_bytes` (16 MiB) and
     `catchup_replicas_per_poll` (1).
   - `None`, `Err` or an unexpected LSN at or below the ceiling ⇒ ReadFailed:
     - `poll` returns `Err(ReplError::PrimaryLogUnreadable{lsn})`;
     - catch-up is **disabled shipper-wide (sticky)**, with no rescan;
     - streaming replicas are unaffected;
     - status `Failed`.
   - Every advance uses `checked_next()`.
4. **Cutover and in-session fall-back (§9.2).**
   - The network thread keeps `next` and passes every record it encodes, catch-up or ring,
     through **one** check function. A mismatch ⇒ `ERR(Contiguity)` + close, and that record
     is never sent.
   - `resume()` must return the answered cursor.
   - `NeedsCatchUp` mid-stream ⇒ detach and post a chunk request at `next`, staying in the
     same session.
   - BelowOldest ⇒ close with a FIN and reconnect immediately.
5. **R4.** Publish `sent` before **every** write, catch-up included. The ACK reader runs from
   `SERVING` onward. Set `acked = d` on session acceptance.
6. **Status.** `Connecting → Joining → CatchingUp ⇄ Streaming`, plus `Rejected`,
   `NeedsReseed` and `Failed`. Add `catching_up() -> usize`.
7. **LSN domain (§9.2, pulled forward from RM4).**
   - `Shipper::new`: first `checked_next` (`LsnExhausted`, unchanged), then `Err` if
     `wal.last_lsn() ≥ ring::MAX_LSN`.
   - `Shipper::capture` pre-checks `lsn > MAX_LSN` and goes terminal **without** calling the
     ring.
   - `ring.rs` capture fast path: `lsn != next || lsn > MAX_LSN` goes to the cold path. This
     is defence in depth and the **only** `ring.rs` change.
8. **Delete `hold()` and the overflow reconnect (`SessionEnd::FellBehind`).**
9. **Docs.**
   - Crate `CLAUDE.md`: RM3 status, the mutation table, and the integrator obligations
     (cadence; `on_commit(wal.commit()?)` after an in-process reopen, §8.2).
   - Spec §15.7.8: as-built notes.

## RM2 tests whose asserted behaviour RM3 deletes (rewrite these and only these)
- `tests/shipper.rs::stalled_replica_overflows_without_blocking_the_writer`. It asserts a
  reconnect after overflow, a second `NeedsCatchUp`, `ReplicaStatus::NeedsCatchUp`, and a
  `hold` heartbeat.
  - **Keep:** the writer-never-blocks assertion (worst `capture`/`on_commit` < 50 ms with a
    stalled replica). **Extend** it to `poll(&wal)`.
  - **Keep:** the dense-prefix check.
  - **Replace** the reconnect, hold and status assertions with in-session
    `CatchingUp → Streaming` and convergence.
- Any other existing test that fails on a correct RM3 build: stop and report it. Do not edit
  it. "Never weaken an existing test" still governs everything else.

## Shared test infrastructure (independent oracles — required)
- **Forwarding proxy** in `tests/common`, on loopback between `Shipper` and the real
  `Receiver`. It:
  - forwards raw bytes and tee-parses them (a parse error fails the test);
  - propagates half-closes and counts sessions;
  - per session, checks `SERVING.from == HELLO.d + 1`, dense `RECORD`s, no `ERR` in either
    direction, and every `ACK ≤ DurableTap` at forwarding time;
  - records `SERVING.primary_oldest` and the first P→R frame;
  - can pause forwarding, and can be awaited until it has forwarded a given ACK.
- **`assert_no_inband_errors`** also fails on
  `Remote{code: Contiguity | Protocol | WireCrc | Poisoned}`.
- **`rchar` probe.** A helper reads the writer thread's `/proc/thread-self/io` `rchar`
  around a `poll` and subtracts the measured self-cost. Linux-only.
- **No self-reported oracle.** Shipper counters are allowed only as reachability checks.

## Tests — each must be shown failing on a deliberately broken build, then reverted
Record every row (mutation → test → failure message → reverted) in the PR description and in
the crate `CLAUDE.md`. **A test that passes with its mutation applied is too weak: fix the
test, never the mutation.**

**Anti-vacuity rule.**
- Every row that targets the catch-up copy loop MUST assert, against the wire, that the poll
  under test actually opened a reader and shipped records.
- Every row that targets the ceiling MUST arrange `cursor ≤ released` with a lag no larger
  than one chunk, so that the ceiling, not the budget, ends the chunk.

| # | Test | Mutation(s) that must fail it |
|---|---|---|
| T1 | **Overflow → in-session catch-up → cutover.** Real `Receiver` behind the proxy; real primary WAL with small segments; concurrent appends/commits. Induce exactly one overflow with the proxy pause; size the ring so no other overflow can happen. After the first chunk answer, wait until the proxy has forwarded an ACK ≥ that chunk's first LSN before the next `poll`. **Oracles:** receiver results `len() == 1`; no in-band `Err`/`Remote`; proxy density, no ERR, ACK ≤ tap; converged byte-identical, LSN-mirrored, `acked == durable`. **Reachability:** catch-up spans ≥ 2 polls, and `CatchingUp → Streaming` is seen while the writer is still committing. | attach at `cursor + 1`; attach at `cursor − 1`; advance the cursor past a record not copied; catch-up writes that do not publish `sent` |
| T2 | **P5 + boundary table.** Proptest: a checkpoint past a lagging replica ⇒ the next HELLO's first frame is `RESEED_REQUIRED{wal.oldest_lsn()}`. Deterministic table via a fake replica: primary seeded at 1000 and checkpointed so `P = oldest > 1001`; `d ∈ {P−2, P−1, P}`, each with `d+1` in the ring and not. `P−2` ⇒ `RESEED_REQUIRED{P}`, never `SERVING`. `P−1` ⇒ `SERVING{P, primary_oldest = P}`, then dense `P..`. `P` ⇒ `SERVING{P+1, P}`. | `d + 1 <= oldest`; `d + 2 < oldest`; `primary_oldest` = 0; `primary_oldest` cached at `Shipper::new` |
| T3 | **Step 2 boundaries**, judged on raw frames. (i) `d == durable` ⇒ `SERVING{d+1}`, then exactly `d+1..d+N`. (ii) `d == durable+1` ⇒ `ERR(Contiguity)` + close, then ≥ 2 more commits + polls and zero `RECORD`s. (iii) `d == ring::MAX_LSN` ⇒ `ERR(Protocol)`. (iv) with `heartbeat = 10 s`: `poll` between `commit()` and `on_commit()`, HELLO `d == wal.durable_lsn() > released` ⇒ `SERVING{d+1}`; after `on_commit` + `poll`, the first `RECORD` arrives within 1 s. Run (i) and (ii) with `d+1` in and out of the ring. | `d + 1 > durable` (the 2026-10-27 spec bug); `d > durable + 1`; restore `Ahead ⇒ reject`; Wait re-posts on heartbeat only |
| T4 | **R7 primary restart with two lagging replicas.** Both converge via catch-up; proxy oracles as in T1. | catch-up never starts (`JOIN_BEHIND` holds) |
| T5 | **Writer stall bound (Linux, `rchar`).** `segment_size = 8 MiB`, `C = 1 MiB`, lag ≥ 64 MiB, HELLO `d` mid-segment. Per `poll`, `rchar ≤ segment_size + C + max framed record + 4 KiB`. Variants: 4 KiB records, and **0-byte payloads**. Companions: a `poll` with nothing pending reads ≈ 0; a **Wait** answer whose cursor sits ≥ 1 MiB into its segment reads ≈ 0. Record in the PR that "≤ C + one record" fails a correct build (the old, false bound). | unchunked catch-up; the budget counts payload bytes only (fails the 0-byte variant); Wait falls through to answer 3 |
| T6 | **Mid-catch-up checkpoint.** Tiny segments; chunk = a few records; the test thread drives `poll` k times. Let `c−1` be the last LSN shipped, so `c − 1 ≤ d + k·(chunk+1)`. Then `checkpoint` far past `c`. Preconditions, asserted on the test thread: `oldest > c`, and `released + 1 − oldest > ring capacity + one chunk`, so a jump cannot cut over and mask the bug. **Oracles:** session 1 is `SERVING{d+1}`, dense `d+1..=c−1`, EOF, no `RECORD ≥ c`, no ERR. Session 2's first P→R frame is `RESEED_REQUIRED{wal.oldest_lsn()}`. Receiver results are `Ok(())` then `Err(ReseedRequired{P})` only. The replica log is exactly `d+1..=c−1`. | swallow the below-oldest error and jump to `oldest` |
| T7 | **R4 detection power.** T1 and T4 sample `acked` before the tap after every poll/commit/wait iteration (P3 discipline), and the proxy checks every ACK at forwarding time. | the strong-form ack-before-commit mutation must fail **T1 and T4 themselves** |
| T8a | **Ceiling = `released` (per-PR, no fault injection).** Fake replica, lag ≤ one chunk, `cursor ≤ released`. Commit **without** `on_commit`, then `poll` repeatedly until the replica has received a `RECORD == released` (positive control: a reader ran and the ceiling ended it). Assert nothing `> released`. Only then `on_commit`. | ceiling = `wal.durable_lsn()` |
| T8b | **Never-ahead on a poisoned primary (Linux, LD_PRELOAD EIO shim, `#[ignore]` + script + paths-filtered CI, like `fsync_fault_gate`).** Commit and `on_commit` a prefix `1..=R`, then create the `Shipper` (its ring starts at `R+1`). Fake replica with HELLO `d < R`, lag ≤ one chunk. Arm the one-shot shim only while no replica WAL can sync. Failing batch: **K=0** (one segment; `durable` stays `R`, `R+1..` unsynced in the file), or **K=2** (a split: `R+1..=S` synced, `S > R`, the rest failed). Preconditions against the WAL: injection count ≥ 1; `durable < last`; `reader_from(durable+1)` yields ≥ 1. Then `poll` the poisoned handle. **Positive control:** the replica received exactly `d+1..=R`. **Oracle:** nothing `> R`. **Extension (§8.2):** reopen in-process, call `on_commit(wal.commit()?)`, and `poll` with no new appends. The replica receives exactly the recovered prefix, byte-identical. Then append/capture/commit, and the stream stays dense. | read to the reader's end / `last_lsn` (K=0); ceiling = `wal.durable_lsn()` (K=2 ships `R+1..=S`); skip the release when `on_commit`'s `w` comes from an empty commit (the extension stalls) |
| T9 | **Unreadable committed record.** After open, flip a payload byte of a record in a **sealed** segment, below `oldest_valid`. Replica A's catch-up ⇒ `poll` returns `Err(PrimaryLogUnreadable{lsn})`; status `Failed`; nothing `≥ lsn` on A's wire. Then wait until replica B's chunk request is `PENDING` (B's cursor is in the same segment, above `lsn`). The measured `poll` returns the same `Err`, which proves it answered, and its `rchar` is < one record. | treat `None` as end of chunk; per-cursor (not shipper-wide) stickiness; rescan on every request |
| T10 | **Cadence under load.** The primary commits in a tight loop and calls `poll(&wal)` **only** after each `on_commit`. A fresh real `Receiver` reaches `SERVING` within its `idle_timeout` and converges. | `poll` decides joins only when no release happened since its previous call |
| T11 | **Teardown liveness.** Drop the replica socket mid-catch-up across several polls and reconnect. The next session reaches Streaming; the proxy counts exactly the induced sessions; no `AlreadyAttached`. (The teardown **ordering** rows belong to L8, which hits `TAKEN` deterministically. An integration test cannot.) | the session never re-posts after a teardown (the replica stays `Joining`) |
| T12 | **LSN domain, both sides of the boundary.** Seed `MAX_LSN + 1` ⇒ `Shipper::new` `Err`. Seed `MAX_LSN` ⇒ `Ok`; the record `MAX_LSN` is shipped; the next capture ⇒ terminal (`ERR(Protocol)` to the replica, `poll` ⇒ `Err(LsnDomainExhausted)`, nothing `> MAX_LSN` on the wire). A WAL at `u64::MAX` still gets `LsnExhausted`. **Ring unit tests** at `2^56 − 1` and `2^56` in the `catch_unwind` + `is_err() == cfg!(debug_assertions)` style: nothing written, no consumer emits that LSN. | `last_lsn > MAX_LSN` instead of `≥`; no `Shipper::capture` pre-check (debug panic or a record > MAX_LSN); drop the ring fast-path check (ring unit tests) |
| T13 | **Reader cap and round-robin.** Two fake replicas, both lagging far beyond one chunk, `catchup_replicas_per_poll = 1`. Wait until both have posted chunk requests, then `poll` once: exactly one gets a Chunk, the other Deferred, and that poll's `rchar` ≤ one answer's bound. `poll` again with no other network activity: the deferred one is answered first. Over k polls both progress. | Deferred not re-posted (the second replica never served); cap ignored (both served in one poll, or `rchar` ≈ 2× bound); no round-robin (always the lowest id first) |
| T14 | **Sending-side check (unit).** The single check function that every encoded record passes through. It accepts `lsn == next` (returning `checked_next`) and rejects ±1 and anything else. Its integration wiring is a review item: a correct build never produces a mismatch, and no fault hook is added. | delete the check |

**Interop.** T1, T4, T6, T10 and T11 run against the real `Receiver`; this replaces the
`FakeReplica`-only `hold` test.

**Loom (§15.7.4–15.7.5):**
- Add **L7** (bound 4), **L7b** (exhaustive) and **L8**. They drive the crate's ring and the
  crate's slot module (`post` / `answer` / `abandon`), never a copy.
- Show every RM3 row failing its model. Measured by the reviewer:
  - L7 fails on "drop generation bump" (from bound 2) and "Relaxed pin" (bound 4);
  - L7b fails on the four `attach`/`resume` ordering mutations (`spurious NeedsCatchUp{5}`).
- L8 owns the four teardown rows: no cancel CAS; no wait for `DONE`; `DONE` before attach;
  answer written after `DONE`.
- Re-run **all nine existing rows** after the `ring.rs` change.
- If a model or bound does not reproduce, report it with the numbers. Do not weaken the
  model.

## Guardrails
- **Do not touch `open-wal`'s `src/`.** If catch-up seems to need a WAL change, stop and
  flag it (§16).
- The only `ring.rs` change allowed is the §9.2 fast-path bound check.
- Never `reader_from` in `on_commit`, on a network thread, or for a join, Wait or Deferred
  answer.
- No checkpoint observer, no writer-side gating, no `truncate_after`, no HELLO wire change,
  and no test-only fault hooks in shipped code.
- Never weaken an existing test, loom model or mutation row, except the one RM2 test named
  above.
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
- T1–T14 are green, each with its mutation(s) shown failing.
- L7, L7b and L8 are added, with their rows shown failing.
- The nine existing rows have been re-verified.
- RM4 (re-seed) is **not** part of this kickoff.
