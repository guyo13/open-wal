# CLAUDE.md — `open-wal-replica` (crates/open-wal-replica)

Operating guide for this crate. Read fully before writing code. The build contract is
`docs/replica_design_v1.md` (re-read **§4.1–4.2, §5, §8, §9, §12.2, §15.5, §15.7** before
touching the shipper, receiver, catch-up, or promotion). The WAL's own `CLAUDE.md` at the repo
root still applies to anything that touches `open-wal`; this file is the replica-specific
summary of what must never be violated.

---

## What this is

Async **master/slave** replication on top of `open-wal`: one static primary, N replicas, no
leader election, no sharding. It gives **data redundancy** — every record the primary durably
committed eventually exists byte-identical in each replica's own durable WAL — with **manual**
promotion. It is **not** availability redundancy, and the async lag window is **lost on primary
failure** (a deliberate product decision; the sync barrier is a designed surface, §13, not gated).

The crate sits **on top** of the WAL: no WAL invariant weakened, no on-disk format change, no
destructive operation. The one property that makes v1 tractable: **a replica only ever appends
records ≤ the primary's `durable_lsn`, in order, so it is always a dense prefix of the primary
and never needs to truncate.** Every design rule below exists to preserve that.

Depends on open-wal **spec v7** (`WalConfig::initial_lsn`, `Wal::oldest_lsn()`, containing-segment
`reader_from`). RM0–RM2 need none of it; **RM3+ waits for v7 to land.**

---

## Prime directives

1. **Implement to the contract.** §5 (R1–R9) is normative. Nothing may weaken it. If a change
   would, stop and flag it.
2. **Tests are co-equal with code.** Each milestone lists its §15 tests; a milestone is not done
   until they pass. **Loom L1–L6 + all falsifiability mutations gate RM2 (do not start RM3 before).**
3. **When uncertain, ask — don't guess.** §16 lists what is deferred and why. A silent wrong
   guess here is a silent divergence bug.
4. **Never weaken a test to make it pass.** A failing fault-injection or loom test means the
   implementation is wrong, not the test.

---

## Invariants (R1–R9) — preserve all of these in every change

- **R1 Never ahead.** A replica's durable set ⊆ the primary's durable set. The shipper MUST NOT
  send any record with `lsn > w` where `w` is the latest `commit() → Ok(w)`. A captured record
  whose commit has not returned is **not shippable** (the §15.1 visibility gap applied to the ring).
- **R2 LSN mirroring.** Same LSN on primary and replica for every record (via `initial_lsn` +
  R3). After `wal.append(payload)` on the replica, the returned LSN MUST equal the shipped LSN.
- **R3 Stream contiguity — hard check.** Receiver verifies `lsn == replica_wal.last_lsn() + 1`
  **before** `append`. Any other value ⇒ do NOT append, `ERR(Contiguity)`, close, re-HELLO.
  Never skip, never best-effort.
- **R4 Ack honesty.** Replica acks **only** its own `durable_lsn` after `commit() → Ok`. Never
  `last_lsn`. An ack means "durable on this replica."
- **R5 Prefix property.** Every replica is a dense prefix of the primary; promoting the highest
  replica makes all others prefixes of it ⇒ catch-up without truncation.
- **R6 Gap is fatal at catch-up.** `replica.durable_lsn + 1 < primary.oldest_lsn()` ⇒
  `RESEED_REQUIRED`. Never resume from anywhere else, never silent skip.
- **R7 Stateless primary.** No persisted shipping position. Resume from each replica's HELLO.
- **R8 Replica is durability-first.** Real `open-wal`, real commit discipline, same poison rules.
- **R9 Byte fidelity.** Payloads byte-identical; every `RECORD` frame carries CRC-32C over
  `(lsn ‖ payload)`; mismatch ⇒ `ERR(WireCrc)`, no append, reconnect.

### Non-guarantees (do not assume these)
- No loss-free failover; the lag window is lost. No automatic promotion.
- **A former primary MUST be re-seeded, never resumed.** It may hold durable records beyond the
  new primary (the only divergence v1 admits); there is no suffix-truncate. The primary rejects a
  HELLO whose `durable_lsn > own durable_lsn` (§9 step 2, §12.2).
- Ring overflow never loses anything durable — it only forces a catch-up from the log.

---

## Hard rules / footguns (these are the silent-divergence and latency traps)

- **Capture-at-append, release-at-commit. NEVER read-after-commit via `reader_from`.** `Wal` is
  `!Sync` (no other thread can read it) and `reader_from` rescans from a segment start — a
  per-commit `reader_from` rescans the retained log every commit (passes a 10-record test, dies
  in production). Records are memcpy'd into a preallocated ring at `append`; `on_commit(w)` only
  marks entries `≤ w` shippable and wakes the network thread. `reader_from` is for **rare
  catch-up only** (§9), on the writer thread.
- **`on_commit` sends nothing. The writer thread never blocks.** No socket I/O, no lock
  contention, no unbounded allocation on the `capture`/`on_commit` path. Network I/O lives on a
  separate thread **per replica** (§4.2). A slow replica fills its own ring cursor, then overflows
  ⇒ `NeedsCatchUp`; it never stalls `append`/`commit` or other replicas.
- **Release only to a commit-returned `w`.** Never `last_lsn`, never on `commit() → Err`
  (handle is poisoned; captured-but-unreleased entries are discarded; anything durable from a
  split batch is recovered by the replica's catch-up from the log).
- **`DurabilityObserver` is NOT required** for v1 (direct `on_commit(w)` is equivalent). Don't
  add observer plumbing you don't need.
- **The ring is a real concurrent structure — build it loom-ready from day one, not retrofitted:**
  `#[cfg(loom)]`-swappable `AtomicU64`/`UnsafeCell`/`Arc`/`Condvar`/`thread` (§15.7.1); slot
  contents in **`UnsafeCell`, not `Mutex`** (a Mutex serializes away the interleavings loom must
  explore — §15.7.2); `released` is an `AtomicU64` with **Release store / Acquire load**;
  per-consumer cursors published with Release, producer takes the **min** over all cursors with
  Acquire before reusing a slot; per-slot **generation tag** bumped on eviction. The loom tests
  (§15.7.4) pin this structure — **read §15.7 before designing the ring.** The loom test MUST
  import the crate's ring type; a hand-written model proves nothing.
- **Receiver: CRC first, then contiguity, then append, then assert returned LSN.** Never append
  on a CRC failure. Never ack `last_lsn`. Replica `WalConfig` `segment_size`/`max_record_size`
  MUST match the primary's.
- **Catch-up cutover has no gap and no duplicate** — assert at the ring/live boundary (§9 step 3).
- **No checkpoint observer, no writer-side gating, no WAL changes** beyond v7's three. Checkpoint
  coordination is the integrator's `checkpoint(up_to)` consulting `min_replica_acked_lsn()` (§11).
- **Re-seed loop guard:** the primary MUST NOT checkpoint past an in-flight re-seed's snapshot
  LSN (§10 step 5, §11).
- **Wire decoder is attacker-facing:** bounded frame lengths, no OOB, no unbounded alloc — fuzz it (RM8).

---

## Workspace

`open-wal` is the **root package**; this crate is a workspace member at
`crates/open-wal-replica`. Existing root CI/scripts are unchanged and test only `open-wal`. Run
this crate with `-p open-wal-replica` (or `--workspace`). `fuzz/` is excluded (own workspace).
Depend on the WAL via `open-wal = { path = "../..", version = "0.2" }`.

---

## Milestone order (gates are mandatory)

| M | Scope | Done when |
|---|---|---|
| **RM0** | workspace migration (root pkg + member, `exclude = ["fuzz"]`, **all existing gates unchanged & green**), crate skeleton, wire codec | §15.1 |
| **RM1** | `Receiver`: own WAL, R3 hard check, group commit, R4 ack | §15.1, §15.2 P1–P2 |
| **RM2** | `Shipper`: ring capture/release, per-replica net threads, overflow ⇒ `NeedsCatchUp`, R1 as Release/Acquire watermark | §15.2 P3–P4, §15.3 overflow, **§15.7 loom L1–L6 + all 7 mutations — GATE** |
| **RM3** | catch-up (§9) *(needs WAL v7)* | §15.3, §15.2 P5 |
| **RM4** | re-seed traits + `initial_lsn` cold start + reseed checkpoint guard | §15.3 |
| **RM5** | checkpoint policy helper, promotion runbook, former-primary rejection | §15.3 |
| **RM6** | model/oracle harness | §15.4 |
| **RM7** | fault injection: LazyFS replica-never-ahead headline + negative control, split-batch, SIGKILL matrices, wire corruption, partition | §15.5 |
| **RM8** | decoder fuzz, soak (primary + 2 replicas), CI | §15.6 |

### The RM2 gate (do not skip)
Do not start RM3 until loom L1–L6 pass **and** each of the seven §15.7.5 mutations is
demonstrated to make its model fail, then reverted. If a mutation does not fail its model, the
model is too weak — fix the model, do not ship it. Loom proves the in-memory handoff only; the
durability half of R1 is proven by the RM7 LazyFS headline (§15.7.6) — never conflate them.

---

## Environment & tooling

- Rust stable, edition 2024, MSRV 1.85 (same as the WAL). Deps minimal: `open-wal`, `crc32c`;
  `loom` under `[target.'cfg(loom)'.dependencies]` (NOT dev-deps — the lib itself is built with
  `--cfg loom` as a regular dependency of `tests/loom_ring.rs`); `proptest`, `tempfile`,
  `arbitrary`/`cargo-fuzz` for tests.
- Always before declaring done: `cargo test -p open-wal-replica`,
  `cargo clippy -p open-wal-replica --all-targets -- -D warnings`, `cargo fmt --check`,
  `RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=target/loom cargo test -p open-wal-replica --test
  loom_ring --release`, and the **root**
  `cargo test` to prove `open-wal` is untouched.
- Fault injection (RM7) needs the same LazyFS/FUSE Linux environment as the WAL's §14.4. If
  unavailable, say so and leave the gate open — never fake or skip it.

---

## Working style

Vertical slices per milestone; failing test first where practical; reference R-invariants in
commit messages (e.g. "RM2: ring release watermark, R1 / loom L1"). If the design doc is wrong or
underspecified, flag it and propose a fix — do not silently diverge.

## Project status (keep updated)
- **RM0 — DONE.** Workspace migration: root `Cargo.toml` gains `[workspace] members =
  ["crates/open-wal-replica"], exclude = ["fuzz"]`; `open-wal` stays the root package and the
  only default member, so every existing workflow/script invocation is byte-identical (verified:
  root `cargo test` 137 passed and builds no replica code; root `clippy --all-targets`; MSRV
  `cargo +1.85.0 check --all-targets --locked`; `cargo package --list` ships no `crates/` file;
  `fuzz/` still resolves as its own workspace). `Cargo.lock` gained entries only (no existing
  version moved). The root `lint` job's `cargo fmt --all -- --check` already covers members.
  `publish.yml` now names `-p open-wal` for package+publish. New CI jobs (not edits to the
  open-wal ones): `replica — test + clippy`, `replica — MSRV (1.85)`. Crate skeleton:
  `ReplError` (§14, non-panicking, `#[non_exhaustive]`; adds `Remote{code,msg}` for a peer `ERR`),
  `wire` codec (§6): `len:u32` counts type+body; pure, allocation-free `decode_frame` (bounded
  `len` rejected from the prefix alone, unknown type / wrong fixed size / oversize payload /
  unknown ERR code / non-UTF-8 msg ⇒ `Protocol`, RECORD CRC mismatch ⇒ `WireCrc`), plus a
  bounded streaming `FrameReader` (keeps partial bytes across a read timeout). §15.1 codec tests
  incl. every-bit-flip ⇒ `WireCrc` and an arbitrary-bytes proptest. `loom` is a
  `[target.'cfg(loom)'.dependencies]` entry (NOT dev-deps: the lib itself is compiled with
  `--cfg loom` as a regular dependency of `tests/loom_ring.rs`, where dev-deps are invisible).
- **RM1 — DONE.** `Receiver<O: DurabilityObserver = NullObserver>` (§7): own WAL
  (`open`/`open_with` — the observer variant serves §7.1 replica-hosted consumers, added after
  PR #51; v7's `options().seed(..)` cold start is RM4), sends `HELLO{durable}`, accepts
  `SERVING{from}` only if `from == last+1`, per RECORD: CRC (decoder) → **R3 hard check before
  append** → append → R2 returned-LSN check; group commit on `batch_records`/`batch_interval`
  (socket read timeout = the interval clock); `ACK{w}` with the value `commit` returned (R4).
  On any error: commit+ACK what was validly appended, send `ERR(code)`, close (next HELLO then
  has `durable == last`). Commit `Err` ⇒ `ERR(Poisoned)` and the receiver refuses all later
  connections (reopen required). Tests (`tests/receiver.rs`, fake primary over loopback TCP):
  P1 mirroring proptest (op-scripts on a 4 KiB-segment primary ⇒ rolls/splits; byte-identical,
  same LSNs, ACKs survive reopen), P2 fault proptest (gap/dup/reorder/foreign/stale/bad-CRC ⇒
  never appended, right `ERR`, reconnect converges), SERVING mismatch, batch_interval, heartbeat,
  reseed, protocol/oversize, §7.1 observer-sees-only-committed. **Falsifiability shown:**
  deleting the R3 check makes P2 fail (shrinks to `n=2, Gap`), then reverted. **Not tested
  here:** the commit-failure ⇒ `ERR(Poisoned)` path (needs fault injection — §15.3 replica-poison
  scenario, RM7).
- **RM2 — DONE; the loom gate PASSES (L1–L6 green, all 7 mutations demonstrated to fail).**
  `ring` (pub, SPMC, §8/§15.7): `Producer::capture` (memcpy into a preallocated slot; no I/O, no
  syscall, no steady-state alloc; oversized payload grows its buffer once), `release(w)` (one
  `Release` store + unpark per consumer, sends nothing, clamped to the last capture),
  `Consumer::drain` (one `Acquire` load of `released` per pass, never emits past it), per-consumer
  `Release` cursors with an `Acquire` **min** before in-place reuse, per-buffer generation tag +
  reader pin (`tag<<8|readers`), eviction = tombstone CAS + spare buffer if pinned (never writes a
  pinned buffer, never blocks, never drops the new capture) ⇒ `NeedsCatchUp`; writer-thread
  `attach` (Ahead ⇒ §9 step 2 refusal, Behind ⇒ catch-up). Discontinuities are conservative
  (re-capture after a failed commit tombstones the discarded tail — never resurrected; a
  capture ≤ released writes nothing). All primitives via `src/sync.rs` (`cfg(loom)` swap).
  `Shipper` (`#[cfg(not(loom))]`): `capture`/`on_commit`/`poll` on the writer thread; one network
  thread per replica (+ an ack-reader) — connect/backoff, HELLO, join via the writer thread at
  `durable+1` (R7), SERVING, RECORD stream, heartbeats, `ACK` ⇒ `replica_acked[id]` (an ack beyond
  what was sent ends the connection), overflow ⇒ close + re-HELLO ⇒ rejoin if still in the ring
  else `NeedsCatchUp` (held; RM3 serves it). `min_replica_acked_lsn` (never-acked counts as 0),
  `replica_acked_lsn`, `replica_status`. **Tests:** ring unit tests; `tests/shipper.rs` — P3
  never-ahead proptest (real WAL + shipper + receiver; `replica.last ≤ primary.durable`,
  `acked ≤ replica.durable` at every step; converge byte-identical) **+ its negative control**
  (release-on-capture misuse is detected by the same check), stalled-replica overflow (socket
  really fills: ~34k records shipped, then `NeedsCatchUp` mid-stream and again on reconnect;
  shipped prefix dense/no-dup/byte-identical; worst `capture` ≈ 0.2–3 ms, `on_commit` < 0.3 ms
  vs a 30 s io_timeout), former-primary rejection, version mismatch, R7 primary restart;
  `tests/ring_overflow.rs` — P4 proptest (tiny ring + simulated log catch-up: stream exactly
  `1..=N`, no dup at any cutover; 273 `NeedsCatchUp` on a 2-slot ring). Disabling the consumer's
  generation check makes P4 fail (shown, reverted).
  **loom (`tests/loom_ring.rs`, imports the crate's ring):** L1 visibility, L2 never-ahead, L3
  slot reuse, L4 eviction, L5 no lost wakeup, L6a/L6b SPMC. Bounds: L1/L2/L3/L5 exhaustive;
  L4/L6a preemption bound 3; L6b bound 2 per-PR (bound 3 run once here: 1.45M iterations,
  669 s, green). **Loom found a real bug**: `Relaxed` pins let the producer see a consumer's new
  pin without its earlier unpin ⇒ "spare always free" invariant panicked (L4) ⇒ pins are now
  `Release`. **Mutation results (each applied to `src/ring.rs`, run, reverted):**
  | Mutation | Model | Result |
  |---|---|---|
  | `released.store` Release→Relaxed | L1 | FAIL — stale slot read ⇒ "NeedsCatchUp without overflow" |
  | `released.load` Acquire→Relaxed | L1 | FAIL — same |
  | remove min-cursor check before in-place reuse | L3 | FAIL — loom `Causality violation` (UnsafeCell) |
  | free on ANY cursor (max) instead of min | L6b | FAIL — loom `Causality violation` |
  | drop generation bump (tombstone) on eviction | L4 | FAIL — loom `Causality violation` |
  | drop `unpark` in `release` | L5 | FAIL — loom deadlock (consumer parked forever) |
  | re-read `released` mid-drain, send to new value | L2 | FAIL — "emitted N past its pass watermark" |
  Model-strength fixes made to get there (recorded in design §15.7.8): L1's first version did
  not catch the two L1 mutations because loom's `unpark` synchronizes the target immediately —
  non-L5 models now register no waker and do bounded drain passes (no spinning: a spin made the
  max-branch limit, not a real catch, fail M4). Spec refinements flagged in §15.7.8 (pinning
  instead of overwrite-then-detect for L4; park/unpark instead of Condvar for L5).
- **Current milestone:** RM3 (catch-up) — BLOCKED on WAL v7 (`options().seed(..)`,
  `oldest_lsn()`, containing-segment `reader_from`).
- **RM2 loom gate:** PASSED (see RM2 entry).
- **WAL v7 dependency (RM3+):** pending
