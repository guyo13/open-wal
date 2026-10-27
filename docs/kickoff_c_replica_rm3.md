# Kickoff C — `open-wal-replica` RM3: catch-up / resume (§9)

> **Status: DRAFT, not yet dispatched.** Dispatch only after PR #53 (RM0–RM2) is merged to
> `main` and this kickoff has been reviewed. Branch off the `main` that contains #53.

You are implementing **RM3** of `open-wal-replica`. A replica that reconnects behind the
live ring is served the missing records **from the primary's log**, then cut over to the
ring. This replaces RM2's `hold()` placeholder. Everything RM3 needs from the WAL is already
on `main`: v7's `Wal::oldest_lsn()` and containing-segment `reader_from`, and v7.1's
`checked_next`. **Do not touch `open-wal`'s `src/`.**

## Read first — in this order, before any code
1. `crates/open-wal-replica/CLAUDE.md`. Its RM2 status block records what was deferred to
   you: `hold()` and the real-`Receiver` interop test.
2. `docs/replica_design_v1.md`:
   - **§9**, the build contract for this milestone. Read the whole of §9.1 and §9.2 (bounded,
     chunked catch-up and the cutover rule) and the **corrected step 2** (`d > durable_lsn`,
     *not* `d + 1 > durable_lsn`).
   - §5 R1/R3/R5/R6/R7.
   - §8.3 overflow.
   - §11 retention.
   - §15.2 P5 and §15.3.
3. The RM2 review threads on PR #53. They show the bug classes this crate has already hit:
   - a self-reported oracle (L2, P3/R4);
   - an R4 bound published after the write (`sent`);
   - a silent primary wedging the receiver;
   - stale `acked` after a wipe.
4. The WAL API you will drive: `Wal::reader_from` (borrows `&Wal`; below `oldest_lsn` ⇒
   `WalError::ContiguityViolation`), `Wal::oldest_lsn`, `Wal::durable_lsn`, and
   `Reader::next`.

## Scope

### 1. Join decision (§9 steps 1–2, writer thread)
The decision is made on the writer thread when the replica's HELLO reports `durable_lsn = d`:

| Case | Response |
|---|---|
| `d.checked_next()` is `None` | Serve nothing: `ERR(Contiguity)` if `d > durable`, else nothing to serve. Never wrap or saturate. |
| `d + 1 < wal.oldest_lsn()` | `RESEED_REQUIRED{oldest}` (R6). |
| `d > wal.durable_lsn()` | `ERR(Contiguity)`: former primary or a bug (§12.2). |
| otherwise | `SERVING{from = d+1, primary_oldest = wal.oldest_lsn()}`; fill in `primary_oldest` (RM2 sent 0). |

- `d == durable` is a **caught-up** replica. It MUST be served, as an empty catch-up followed
  by an immediate cutover.
- If `d + 1` is still in the ring, keep RM2's fast path (attach directly). Otherwise go to
  catch-up.

### 2. Chunked catch-up (§9.1)
- Catch-up runs only inside a writer-thread call that receives `&Wal`, e.g.
  `Shipper::poll(&mut self, wal: &Wal<O>)`. Changing `poll`'s signature is fine; the crate is
  unpublished.
- **`on_commit` stays release + wake: O(1) per replica, no reads.**
- Per call and per lagging replica, serve at most one chunk:
  - reopen `reader_from(cursor)`;
  - read records with `lsn ≤ wal.durable_lsn()` (re-read each call) up to `catchup_chunk_bytes`;
  - enqueue them on a **bounded** per-replica queue with a non-blocking send (`try_send`); a
    full queue ends the chunk;
  - advance `cursor`.
- A below-oldest error mid-catch-up ⇒ `RESEED_REQUIRED` after the already-queued records.
- Any other read error ⇒ end the session. Never skip.
- New `ShipperConfig` knobs: `catchup_chunk_bytes` and `catchup_queue_records`, with
  documented defaults.

### 3. Cutover (§9.2)
- At the end of a chunk, if `cursor ≥ producer.oldest_valid()` and `cursor ≤ released + 1`,
  call `attach(id, cursor)` and end catch-up.
- The network thread sends every queued catch-up record, **then** resumes the ring consumer.
- It **asserts** that the first ring record is `last_catchup_lsn + 1` (or `d + 1`). A
  violation closes the session loudly.
- If the ring evicts the cutover point before the first drain (`NeedsCatchUp`), the session
  ends and the replica re-HELLOs (R7).
- Document the ring-sizing constraint from §9.2: `ring_bytes / slot_bytes ≥` the largest
  commit batch.

### 4. Delete `hold()`
- The `JOIN_BEHIND` path becomes catch-up.
- `ReplicaStatus` gains a catch-up state (e.g. `CatchingUp`). `NeedsCatchUp` remains the
  transient overflow signal.

## Tests — each must be shown failing on a deliberately broken build, then reverted
Record every row (mutation → test → failure message → reverted) in the PR description and the
crate `CLAUDE.md`. **A test that passes with its mutation applied is too weak; fix the test.**

| # | Test | Mutation it must catch |
|---|---|---|
| T1 | **§15.3 overflow → catch-up → live cutover**, real `Receiver`, real primary WAL with rolls (small segments) and concurrent appends/commits while catch-up is in progress across several `poll` calls. Replica converges byte-identical, LSN-mirrored, and `acked == durable`. | cutover attaches at `cursor + 1` (gap) **and** at `cursor − 1` (duplicate) — both must fail on the sending-side assertion or on the receiver's R3 check |
| T2 | **P5 proptest:** a primary checkpoints past a lagging replica ⇒ the replica receives `RESEED_REQUIRED{oldest}` (assert the frame, not just "an error"), never a silent skip. A replica *within* retention catches up exactly. | serve from `oldest_lsn` when `d + 1 < oldest` (silent skip) |
| T3 | **Caught-up replica is served:** HELLO with `d == durable` ⇒ `SERVING{d+1}` and live streaming. HELLO with `d > durable` ⇒ `ERR(Contiguity)`. | step 2 reverted to `d + 1 > durable` (the spec bug fixed 2026-10-27) |
| T4 | **R7 primary restart with lagging replicas:** a primary restarts while replicas are behind its new ring; they converge via catch-up. This previously stalled in `hold()`. | catch-up never started (e.g. the `JOIN_BEHIND` arm still holds) |
| T5 | **Writer stall is bounded:** a replica lagging by ≥ 64 MiB; each `poll` serves ≤ `catchup_chunk_bytes` (+ one record) and returns. Assert per-call bytes read and a generous wall bound. Then the replica converges. | an unchunked catch-up (copy the whole range in one call) |
| T6 | **Mid-catch-up checkpoint:** `checkpoint` past the cursor between two chunks ⇒ `RESEED_REQUIRED` after the queued records, with no skipped LSN on the wire. | swallow the reader error and jump to `oldest_lsn` |
| T7 | **R4 independent oracle stays on:** every ACK in T1–T4 ≤ the replica's `DurableTap` at receipt. | (re-use RM2's "ack before commit" row; it must still fail) |
| T8 | **Never-ahead during catch-up (R1):** at every sample, `replica.last ≤ primary.durable` (RM2's P3 sampling discipline). | catch-up reads to `last_lsn` / the reader's end instead of `durable_lsn` — note: on a healthy WAL the file holds only committed records, so this mutation may be **unobservable without fault injection**. If it does not fail, say so explicitly in the PR and leave the row as "RM7 (LazyFS)"; do **not** claim it. |

**Loom (RM3 makes mid-stream attach the normal path):** add **L7 — late attach**.
- Setup:
  - the producer captures and releases 1..=2 with the consumer **detached**;
  - the writer then `attach`es it at 2;
  - the producer keeps capturing and releasing 3..=4 on a 2-slot ring, so eviction can hit
    the cutover point, while the consumer `resume`s and drains.
- Assert:
  - no race;
  - every emitted record is genuine and dense from 2;
  - the consumer either streams 2..=4 or reports `NeedsCatchUp` with a dense prefix.
- Show that the existing "drop the generation bump" and "Relaxed pin" mutations also fail L7.
- If they don't, explain why in the PR rather than weakening the model.
- Keep all 9 existing mutation rows green-on-baseline, red-on-mutation.

## Guardrails
- **Do not touch `open-wal`'s `src/`.** If catch-up seems to need a WAL change, stop and flag it.
- **Never `reader_from` per commit or on the hot path.** Catch-up is chunked and runs in
  `poll` only.
- No checkpoint observer, no writer-side gating, no `truncate_after`.
- Never weaken an existing test, loom model or mutation row.
- Every oracle must be independent of the code under test. Do not assert against a value the
  shipper or receiver reports about itself (RM2 lesson, twice).

## Before pushing, run
- `cargo test -p open-wal-replica`, three times to shake out timing.
- `cargo clippy -p open-wal-replica --all-targets -- -D warnings`, normal and `--cfg loom`.
- `cargo fmt --all -- --check`.
- `cargo +1.85.0 check -p open-wal-replica --all-targets --locked`.
- `RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=target/loom cargo test -p open-wal-replica --test loom_ring --release`.
- Root `cargo test`; the root `src/` diff must be empty.

## Definition of done
- §9 implemented exactly as written, including the corrected step 2 and §9.1/§9.2.
- `hold()` deleted.
- T1–T7 green, each with its mutation shown failing. T8 either green with its mutation shown
  failing, or honestly deferred to RM7.
- L7 added, with the mutations shown failing.
- The interop test against the real `Receiver` replaces the `FakeReplica`-only `hold` test.
- RM4 (re-seed) is **not** part of this kickoff.
