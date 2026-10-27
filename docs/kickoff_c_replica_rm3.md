# Kickoff C — `open-wal-replica` RM3: catch-up / resume (§9)

> **Status: DRAFT, not yet dispatched.** Dispatch it only after PR #53 (RM0–RM2) is merged to
> `main`. Branch off a `main` that contains #53.
>
> This revision follows an adversarial critique of the first draft: 28 findings, every one
> re-verified against the RM2 code (none refuted). The first draft's chunk-end attach raced
> session teardown; its catch-up ceiling disagreed with R1; its mid-stream `RESEED_REQUIRED`
> was rejected by the real `Receiver`; and four of its tests passed with their mutations
> applied. The spec (§9.1–§9.3) and this kickoff are the corrected contract.

You are implementing **RM3** of `open-wal-replica`. A replica that (re)connects behind the
live ring is served the missing records **from the primary's log**, in bounded chunks on the
writer thread, then cut over to the ring. A replica that overflows the ring falls back to
catch-up **inside its session**. This replaces RM2's `hold()` and RM2's
close-and-re-HELLO on overflow.

Everything you need from the WAL is already on `main`: v7's `Wal::oldest_lsn()` and
containing-segment `reader_from`, and v7.1's `checked_next`. **Do not touch `open-wal`'s
`src/`.**

## Read first — in this order, before any code
1. `crates/open-wal-replica/CLAUDE.md`. Its RM2 status block records what was deferred to
   you: `hold()`, the real-`Receiver` interop test, and the ring `MAX_LSN` vs seed issue.
2. `docs/replica_design_v1.md`:
   - **§9, §9.1, §9.2 and §9.3** in full. They are the build contract, and they are
     normative down to the answer variants and the slot state machine.
   - Then §5 (R1/R3/R4/R6/R7), §6 (the `RESEED_REQUIRED` row), §8.2 (in-process reopen),
     §8.3, §15.2 P5, §15.3, §15.7.4–15.7.5 (L7, L7b, L8 and their mutation rows), §16.
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
   - `commit()` on an empty batch returns `Ok(durable_lsn)`.

## Scope (the spec section is authoritative; this is the checklist)

1. **Handshake (§9.3).**
   - Add a request-slot module written against the crate's loom-swappable atomics, with
     states `IDLE → PENDING → TAKEN → DONE → IDLE` and a cancel CAS `PENDING → IDLE`.
   - The network thread never ends a session with a request outstanding.
   - The writer calls `Producer::attach` **only** while answering a taken request.
   - `AlreadyAttached` is an internal bug: end the session loudly and `debug_assert!`. It
     never maps to `JOIN_BEHIND`.
2. **Join decision (§9 steps 1–3) in `Shipper::poll(&mut self, wal: &Wal<O>) -> Result<()>`.**
   - Step 2 (`d > wal.durable_lsn()` ⇒ `ERR(Contiguity)`) is the **only** rejection.
     **Delete RM2's `Ahead ⇒ JOIN_AHEAD ⇒ Rejected` mapping.** A ring `Ahead` means wait.
   - Fill `SERVING.primary_oldest = wal.oldest_lsn()`, read at decision time (not cached).
   - A HELLO with `d ≥ ring::MAX_LSN` is refused on the network thread with
     `ERR(Protocol)`.
   - `on_commit` becomes release + wake only. It no longer calls `poll`. Rewrite the module
     docs: RM2's "poll when idle / also done inside on_commit" text is now wrong. State the
     normative cadence from §9.1.
3. **Chunked catch-up (§9.1).**
   - Answers: Attached / Wait / Chunk{records, attached?} / BelowOldest / ReadFailed.
   - Read only while `lsn ≤ released`. **Wait opens no reader.**
   - Opened readers per `poll` are capped at `catchup_replicas_per_poll` (default 1,
     round-robin).
   - New `ShipperConfig` knobs, with documented defaults: `catchup_chunk_bytes` (16 MiB) and
     `catchup_replicas_per_poll` (1).
   - `None`, `Err` or an unexpected LSN at or below the ceiling ⇒ `ReadFailed{lsn}`:
     - **sticky**, no rescan;
     - `poll` returns `Err(ReplError::PrimaryLogUnreadable{lsn})`;
     - status `Failed`.
   - Every advance uses `checked_next()`.
4. **Cutover and in-session fall-back (§9.2).**
   - The network thread keeps `next` and checks every record it encodes. A mismatch ⇒
     `ERR(Contiguity)` + close, and that record is never sent.
   - `resume()` must return the answered cursor.
   - `NeedsCatchUp` mid-stream ⇒ detach and post a chunk request at `next`, staying in the
     same session. Delete `SessionEnd::FellBehind`'s reconnect.
   - BelowOldest ⇒ close with a FIN and reconnect immediately (step 1 then answers
     `RESEED_REQUIRED`).
5. **R4.** Publish `sent` before **every** write, catch-up included. The ACK reader runs from
   `SERVING` through catch-up and streaming. Set `acked = d` on session acceptance (RM2).
6. **Status.** `Connecting → Joining → CatchingUp ⇄ Streaming`, plus `Rejected`,
   `NeedsReseed` and `Failed`. `CatchingUp` replaces `NeedsCatchUp`. Add
   `catching_up() -> usize`.
7. **LSN domain (§9.2, pulled forward from RM4).**
   - `Shipper::new` returns `Err` when `wal.last_lsn() ≥ ring::MAX_LSN`.
   - `ring.rs` capture fast path: `lsn != next || lsn > MAX_LSN` goes to the cold path. This
     is the **only** `ring.rs` change, and it must leave all existing loom rows red-on-mutation.
   - Crossing at runtime makes the shipper terminal:
     - `ERR(Protocol, "LSN domain exhausted")` to every session;
     - status `Failed`;
     - `poll` returns `Err(ReplError::LsnDomainExhausted)`.
8. **Delete `hold()`.** The `JOIN_BEHIND` path becomes catch-up.
9. **Docs.**
   - Crate `CLAUDE.md`: RM3 status, the mutation table, and the integrator obligations
     (cadence; `on_commit(wal.commit()?)` after an in-process reopen, per §8.2).
   - Spec §15.7.8: as-built notes for RM3.

## Shared test infrastructure (independent oracles — required)
- **Forwarding proxy** in `tests/common`. It sits on loopback between `Shipper` and the real
  `Receiver` (`ShipperConfig.replicas` takes any `SocketAddr`). It:
  - forwards raw bytes and tee-parses them; any parse error fails the test;
  - propagates half-closes both ways and counts sessions;
  - per session, checks `SERVING.from == HELLO.d + 1`, dense `RECORD` LSNs, no `ERR` frame
    in either direction, and every `ACK ≤ DurableTap` at forwarding time;
  - records `SERVING.primary_oldest` and the first P→R frame;
  - can **pause forwarding**, to induce an overflow deterministically.
- **`assert_no_inband_errors` is strengthened.** It also fails on
  `Remote{code: Contiguity | Protocol | WireCrc | Poisoned}`. Today `Remote` maps to `None`
  and slips through.
- **No test may use a value the shipper or receiver reports about itself as its oracle.**
  Counters (`catch_up_events`, sending-side violations) are allowed only as **reachability**
  checks.

## Tests — each must be shown failing on a deliberately broken build, then reverted
Record every row (mutation → test → failure message → reverted) in the PR description and in
the crate `CLAUDE.md`. **A test that passes with its mutation applied is too weak: fix the
test, never the mutation.**

| # | Test | Mutation(s) that must fail it |
|---|---|---|
| T1 | **Overflow → in-session catch-up → cutover.** Real `Receiver` behind the proxy; real primary WAL with small segments; concurrent appends/commits; catch-up spans ≥ 2 polls. Induce exactly one overflow with the proxy pause; size the ring so no other overflow can happen. **Oracles:** receiver results `len() == 1` (any session end fails); no in-band `Err`/`Remote`; proxy density, no ERR, ACK ≤ tap; converged byte-identical, LSN-mirrored, `acked == durable`. **Reachability:** `CatchingUp → Streaming` observed while the writer is still committing. | attach at `cursor + 1`; attach at `cursor − 1` (at the single attach site); advance the cursor past a record not copied into the chunk; catch-up writes that do not publish `sent` |
| T2 | **P5 + boundary table.** Proptest: a checkpoint past a lagging replica ⇒ the next HELLO's first frame is `RESEED_REQUIRED{wal.oldest_lsn()}`. Deterministic table via a fake replica: primary seeded at 1000 and checkpointed so `P = oldest > 1001`; `d ∈ {P−2, P−1, P}`, each with `d+1` in the ring and not. Expected: `P−2` ⇒ `RESEED_REQUIRED{P}` and never `SERVING`; `P−1` ⇒ `SERVING{P, primary_oldest = P}` then dense `P..`; `P` ⇒ `SERVING{P+1, P}`. | `d + 1 <= oldest`; `d + 2 < oldest`; `primary_oldest` left at 0; `primary_oldest` cached at `Shipper::new` |
| T3 | **Step 2 boundaries**, judged on raw frames: (i) `d == durable` ⇒ `SERVING{d+1}` then exactly `d+1..d+N` after N commits; (ii) `d == durable+1` ⇒ `ERR(Contiguity)` + close, then ≥ 2 more commits + polls ⇒ zero `RECORD`s to it; (iii) `d == ring::MAX_LSN` ⇒ `ERR(Protocol)`, never `SERVING`; (iv) `poll` between `commit()` and `on_commit()` with HELLO `d == wal.durable_lsn() > released` ⇒ `SERVING{d+1}`, and records arrive after `on_commit`. Run (i) and (ii) with `d+1` in and out of the ring. | `d + 1 > durable` (the 2026-10-27 spec bug); `d > durable + 1`; restore `Ahead ⇒ reject` |
| T4 | **R7 primary restart with lagging replicas.** Two replicas behind the restarted primary's ring converge via catch-up; proxy oracles as in T1. | catch-up never starts (the `JOIN_BEHIND` arm holds) |
| T5 | **Writer stall bound (Linux-only).** `segment_size = 8 MiB`, `catchup_chunk_bytes = 1 MiB`, 4 KiB records, lag ≥ 64 MiB, HELLO `d` mid-segment. Around each `poll`, take the writer thread's `/proc/thread-self/io` `rchar` delta minus the measured self-cost. Assert `≤ segment_size + C + max framed record + 4 KiB`. Companions: a `poll` with nothing pending, and a Wait answer, both read ≈ 0. Record in the PR that "≤ C + one record" on `rchar` **fails** a correct build (that was the old, false bound). | unchunked catch-up (whole range in one call); Wait opens a reader |
| T6 | **Mid-catch-up checkpoint.** Tiny segments; chunk = a few records; the test thread drives `poll` k times, then `checkpoint` far past `cursor ≤ d+1+k·(chunk+1)`. Preconditions asserted on the test thread: `oldest > bound`, and `released + 1 − oldest > ring capacity + one chunk`, so a jump cannot cut over and mask the bug. **Oracles:** session 1 = `SERVING{d+1}`, dense `d+1..=k`, EOF, no `RECORD > k`, no ERR; session 2's first P→R frame is `RESEED_REQUIRED{wal.oldest_lsn()}`; receiver results are `Ok(())` then `Err(ReseedRequired{P})` only; the replica log is exactly `d+1..=k`, byte-identical. | swallow the below-oldest error and jump to `oldest` |
| T7 | **R4 detection power in the integration tests.** T1 and T4 sample `acked` before the tap after every poll/commit/wait iteration (P3 discipline), and the proxy checks every ACK at forwarding time. | the strong-form ack-before-commit mutation (ACK `last_lsn` before `commit()` and publish `watermarks.durable` early) must fail **T1 and T4 themselves**, not only P1/P3 |
| T8a | **Never-ahead of `released` (per-PR, no fault injection).** A replica in catch-up; `poll(&wal)` between `commit()` and `on_commit()`. Oracle: no `RECORD` on the wire `>` the test's own last value passed to `on_commit`. | ceiling = `wal.durable_lsn()` |
| T8b | **Never-ahead on a poisoned primary (Linux, LD_PRELOAD EIO shim, `#[ignore]` + script + paths-filtered CI, like `fsync_fault_gate`).** Force the catch-up path (`d+1` outside the ring). Arm the one-shot shim only while no replica WAL can `fdatasync` (use a fake replica). Variants: K=0 (single segment) and **K=2** (split batch: `durable = 1`, record 2 readable). **Anti-vacuity preconditions, asserted against the WAL:** injection count ≥ 1, `wal.durable_lsn() < wal.last_lsn()`, `reader_from(durable+1)` yields ≥ 1 record. Then `poll(&wal)` on the poisoned handle. Oracle: nothing `> released` on the wire. | read to the reader's end / `last_lsn`; ceiling = `wal.durable_lsn()` (K=2 ships record 1) |
| T9 | **Unreadable committed record.** After open, flip a payload byte of a record in a **sealed** segment, below `oldest_valid`. `poll` returns `Err(PrimaryLogUnreadable{lsn})`; status `Failed`; nothing `≥ lsn` on the wire; the next `poll` for that replica reads ≈ 0 bytes (`rchar`). | treat `None` as end of chunk (the replica stays `CatchingUp` and the assert times out); rescan on every poll |
| T10 | **Cadence under load.** The primary commits in a tight loop and calls `poll(&wal)` **only** after each `on_commit`. A fresh real `Receiver` reaches `SERVING` within its `idle_timeout` and converges. | `poll` decides joins only when no release happened since its previous call |
| T11 | **Teardown never leaves a stale attach.** Drop the replica socket mid-catch-up across several polls, both while a request is `PENDING` and while it is `TAKEN` (drive `poll` from the test thread); reconnect. Streaming is reached in the next session; the proxy counts exactly the induced sessions; `debug_assert` never sees `AlreadyAttached`. | the network thread ends the session without the cancel CAS / without waiting for `DONE` |
| T12 | **LSN domain.** `Shipper::new` on a WAL seeded at `ring::MAX_LSN` ⇒ `Err`. A WAL seeded at `MAX_LSN − 3` that crosses the bound at runtime ⇒ terminal: `ERR(Protocol)` to the replica, `poll` ⇒ `Err(LsnDomainExhausted)`, nothing `> MAX_LSN` on the wire. Unit tests at `2^56 − 1` and `2^56`. | drop the `lsn > MAX_LSN` fast-path check |

**Interop.** T1/T4/T6/T10/T11 run against the real `Receiver`; this replaces the
`FakeReplica`-only `hold` test.

**Loom (§15.7.4–15.7.5):**
- Add **L7** (bound 4), **L7b** (exhaustive) and **L8** exactly as specified. They drive the
  crate's ring and the crate's request-slot module, never a copy.
- Show every RM3 mutation row failing its model. The verified shapes are:
  - L7 fails on "drop generation bump" (from bound 2) and on "Relaxed pin" (bound 4);
  - L7b fails on the four `attach`/`resume` ordering mutations (`spurious NeedsCatchUp{5}`).
- Re-run **all nine existing rows** after the `ring.rs` fast-path change: each must still be
  green on the baseline and red on its mutation.
- If a model or bound does not reproduce, report it with the numbers. Do not weaken the model.

## Guardrails
- **Do not touch `open-wal`'s `src/`.** If catch-up seems to need a WAL change (for example a
  resumable reader position), stop and flag it. It is a §16 owner decision.
- The only `ring.rs` change allowed is the §9.2 fast-path bound check.
- Never `reader_from` in `on_commit`, on a network thread, or for a Wait answer.
- No checkpoint observer, no writer-side gating, no `truncate_after`, no HELLO wire change.
- Never weaken an existing test, loom model or mutation row.
- Every oracle must be independent of the code under test.

## Before pushing, run
- `cargo test -p open-wal-replica`, three times, to shake out timing.
- `cargo clippy -p open-wal-replica --all-targets -- -D warnings`, normal and `--cfg loom`.
- `cargo fmt --all -- --check`.
- `cargo +1.85.0 check -p open-wal-replica --all-targets --locked`.
- `RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=target/loom cargo test -p open-wal-replica --test loom_ring --release`.
- T8b via its script, under the shim.
- Root `cargo test`. The root `src/` diff must be empty.

## Definition of done
- §9–§9.3 are implemented as written.
- `hold()` and the overflow reconnect are deleted.
- T1–T12 are green, each with its mutation(s) shown failing.
- L7, L7b and L8 are added, with their rows shown failing.
- The nine existing rows have been re-verified after the `ring.rs` change.
- RM4 (re-seed) is **not** part of this kickoff.
