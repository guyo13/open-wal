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
  `loom` under `[target.'cfg(loom)'.dev-dependencies]`; `proptest`, `tempfile`, `arbitrary`/
  `cargo-fuzz` for tests.
- Always before declaring done: `cargo test -p open-wal-replica`,
  `cargo clippy -p open-wal-replica --all-targets -- -D warnings`, `cargo fmt --check`,
  `RUSTFLAGS="--cfg loom" cargo test -p open-wal-replica --test loom_ring`, and the **root**
  `cargo test` to prove `open-wal` is untouched.
- Fault injection (RM7) needs the same LazyFS/FUSE Linux environment as the WAL's §14.4. If
  unavailable, say so and leave the gate open — never fake or skip it.

---

## Working style

Vertical slices per milestone; failing test first where practical; reference R-invariants in
commit messages (e.g. "RM2: ring release watermark, R1 / loom L1"). If the design doc is wrong or
underspecified, flag it and propose a fix — do not silently diverge.

## Project status (keep updated)
- **Current milestone:** RM0 — not started
- **RM2 loom gate:** NOT yet passed
- **WAL v7 dependency (RM3+):** pending
