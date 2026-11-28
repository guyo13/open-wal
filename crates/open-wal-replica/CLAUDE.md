# CLAUDE.md — `open-wal-replica` (crates/open-wal-replica)

Operating guide for this crate. Read fully before writing code. The build contract is
`docs/replica_design_v1.md` (re-read **§4.1–4.2, §5, §8, §9 (incl. §9.1–§9.3), §10 (incl.
§10.1–§10.2), §11, §12.2, §15.5, §15.7** before touching the shipper, receiver, catch-up, re-seed,
or promotion). The WAL's own `CLAUDE.md` at the repo
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

Depends on open-wal **spec v7** — **landed** (#52 v7, #54 v7.1): the cold-start seed is the
`Wal::options().seed(..)` builder (not a `WalConfig` field), plus `Wal::oldest_lsn()` and
containing-segment `reader_from`; v7.1 adds `LsnExhausted` — **sequencing uses
`Lsn::checked_next()`, never the saturating `next()`**. RM3 also uses **v7.2** (#56):
`Reader::position()` / `Wal::reader_at(pos)`, so only the first chunk of a catch-up episode pays
the containing-segment prefix scan. Tracker: issue #47.

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
- **R2 LSN mirroring.** Same LSN on primary and replica for every record (via `Wal::options().seed(..)` +
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
- **`on_commit` sends nothing and decides nothing. The writer thread never blocks.** No socket
  I/O, no lock contention, no unbounded allocation on the `capture`/`on_commit` path. Network I/O
  lives on a separate thread **per replica** (§4.2). A slow replica fills its own ring cursor,
  then overflows ⇒ it falls back to catch-up **inside its session** (`CatchingUp`, §9.2); it never
  stalls `append`/`commit` or other replicas.
- **Catch-up (RM3, §9–§9.3) — the rules that keep it from becoming a divergence or a stall:**
  - **The serve ceiling is `Producer::released()`** — the clamped watermark. Never
    `wal.durable_lsn()` (it is ahead between `commit()` and `on_commit()`, and after a failed
    split commit), never the raw `on_commit` argument, never the reader's end. A poisoned primary
    handle may still be polled; the ceiling is what keeps its unsynced-but-readable tail off the
    wire (T8b).
  - **`reader_from` / `reader_at` run only inside `Shipper::poll(&wal)`, only to answer a chunk
    request (answer 3).** Never in `on_commit`, never on a network thread, never for a join, Wait,
    Deferred, Terminal or sticky-ReadFailed answer. A join never reads.
  - **`Producer::attach` is called only inside `Slot::answer`'s closure** (`Writer::decide`). The
    network thread always detaches before it posts. Nothing else attaches, so a session teardown
    can never race an attach.
  - **`Slot::await_answer` and `Slot::abandon` never give up before `DONE`** (shutdown aside). No
    timeout, no retry bound — that is exactly the stale-attach / buffer race the handshake exists
    to prevent (U1, L8).
  - **`None` from `Reader::next` below the ceiling is an unreadable committed record**, never an
    empty chunk ⇒ ReadFailed, sticky (data-level only; an I/O `Err` fails one answer and is not
    sticky). A reader started by `reader_at` never causes a failure by itself: discard and retry
    once with `reader_from`.
  - **One sending-side check (`send_check`) sits on every encoded record**, chunk or ring; `sent`
    is published before every write (R4).
  - **`checked_next()` everywhere**; the ring's LSN domain (`ring::MAX_LSN`) is enforced in
    `Shipper::new`, in `Shipper::capture` (before the ring is called) and on the ring's fast path.
  - The writer keeps **no per-session state**: the reader position travels in the handshake.
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
- **Catch-up cutover has no gap and no duplicate** — `resume()` must return the answered cursor,
  and every record passes the sending-side check (§9.2).
- **No checkpoint observer, no writer-side gating, no WAL changes** beyond v7's three. Checkpoint
  coordination is the integrator's `checkpoint(up_to)` consulting `retention_floor()` (§11).
- **Re-seed (RM4, §10) — the crate owns two things and no more.** It never produces, transports
  or installs a snapshot (no snapshot trait, stream type or frame).
  - **Replica: `Receiver::open_seeded` is stateless.** Keep the log iff `durable_lsn ≥ s`;
    otherwise drop the WAL (and its `LOCK`) **first**, then rename the directory to
    `<dir>.stale.<k>`, fsync the parent, create `dir`, fsync the parent, open with the seed. Both
    opens carry the seed (that is what makes every crash state converge). **Never delete, never
    overwrite a `.stale.<k>`, never open one, never set aside on an open error, no probe open**
    (the observer factory is called exactly once per WAL open). The crate never calls the
    observer.
  - **Primary: the re-seed loop guard is the hold** (`begin_reseed`, §10.2) plus the integrator
    bounding every checkpoint by `retention_floor()`. The hold is released only when a join with
    `d ≥ s` is answered Serve, and the writer thread stores `acked[id] = d` **before** it clears
    `needs_reseed` and the hold (no window in which neither protects `s + 1`).
  - **The hold and `needs_reseed` are writer-thread state only** (`Reseed` in `shipper.rs`,
    updated inside `Slot::answer`'s closure). The `acked[id]` store at Serve is the one shared
    write. Not persisted (R7).
  - `checked_next()` for `s + 1`, as everywhere.
- **Wire decoder is attacker-facing:** bounded frame lengths, no OOB, no unbounded alloc — fuzz it (RM8).

---

## Integrator obligations (NORMATIVE — §9.1 cadence, §8.2)

- **Call `shipper.poll(&wal)` after every `on_commit`, and at least once per `heartbeat` when no
  commits occur.** Joins and catch-up chunks are answered only there. The join decision is
  therefore bounded by one commit or one heartbeat, and that bound must stay well under the
  receiver's `idle_timeout`: nothing the receiver accepts can be sent before `SERVING`. A `poll`
  with nothing pending is one atomic swap.
- **While `catching_up() > 0`, poll more often.** Catch-up advances about one chunk per opened
  reader per `poll` (`catchup_replicas_per_poll`, default 1).
- **Writer stall per reading `poll`:** at most one containing-segment prefix scan (first chunk of
  an episode only) plus one chunk. With small payloads, size segments by record count (~16 k
  records per segment keeps that one scan near 16 ms) — §9.1 "Residual stall".
- **Ring sizing (liveness only):** `ring_bytes / slot_bytes` ≥ the largest commit batch plus what
  is captured while one chunk is on the wire; an undersized ring only degrades to serving from
  the log.
- **After an in-process WAL reopen (§8.2):** before the next `append`, call
  `shipper.on_commit(wal.commit()?)` — the empty commit returns `Ok(durable_lsn)` and releases
  what a failed split commit left durable. (A `Shipper` built after a process restart needs
  nothing extra.)
- **`poll` returning `Err(PrimaryLogUnreadable{lsn})`** means a committed record could not be read
  (a WAL D5-class emergency for data-level failures); catch-up reads stay disabled until the
  `Shipper` is recreated. **`Err(LsnDomainExhausted)`** means the shipper is terminal.
- **Checkpoint policy (§11):** `up_to = min(latest durable snapshot LSN, retention_floor() −
  margin)`. `retention_floor()` is the minimum over replicas of: its re-seed hold if it has one;
  nothing if it needs a re-seed and has no hold; otherwise its acked LSN (0 until its first HELLO
  since this `Shipper` started). `None` = no replica constrains. Checkpointing past it gets some
  replica `RESEED_REQUIRED` (loud, R6). `min_replica_acked_lsn()` is unchanged and knows nothing
  about re-seeds.
- **Re-seed, primary side (§10.2) — call order (MUST):** once a snapshot LSN `s` is chosen for
  replica `id`, call `shipper.begin_reseed(&wal, id, s)` on the writer thread **before the next
  `checkpoint`** (it fails with `InvalidSnapshotLsn` if `s > durable_lsn` or `s + 1 <
  oldest_lsn`; an existing durable snapshot is a valid `s`). The hold releases itself when that
  replica's `HELLO{d ≥ s}` is served; `end_reseed(id)` abandons it — bound how long a hold may pin
  the log. Holds are not persisted: after a primary restart, re-register a re-seed still in
  flight. On promotion, discard every snapshot not yet installed and served and start over
  against the new primary (§10, §12.1).
- **Re-seed, replica side (§10.1) — in this order:**
  1. `Receiver::run` returns `Err(ReseedRequired{oldest})` (or the replica is new). Drop the
     receiver.
  2. Obtain a snapshot with LSN `s` and install it into application state **durably, including
     `s` itself**. Only then:
  3. `Receiver::open_seeded(dir, cfg, Some(s), observer_factory)`, then `run`. The next `HELLO`
     carries `d = s`; the primary serves from `s + 1`.
  4. On every later start pass the same `installed` value, read back from durable state (`None`
     if never re-seeded). `installed` is the re-seed LSN, never a restore point.
- **The replica's `dir` MUST be dedicated to the WAL** (the set-aside renames the whole
  directory): keep the `installed` record and snapshot files elsewhere. Not a mount point; its
  parent must exist and be writable. The set-aside directory `<dir>.stale.<k>`
  (`SeedReport.set_aside`) is never deleted by the crate — removing it is the operator's call. A
  former primary (§12.2) is not covered: wipe it by hand before the install.
- **A consumer on a seeded replica (§7.1)** reads its starting watermark from
  `Receiver::durable_lsn()` after the open (`s` after a seeded cold start): the observer is not
  called at open.

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
| **RM3** | catch-up / resume (§9, §9.1–§9.3) *(WAL v7 + v7.2)*: request slot, join decision in `poll(&wal)`, chunked catch-up under the `released` ceiling, cutover, in-session overflow fall-back, LSN domain | §15.3, §15.2 P5, kickoff C T1–T18 + U1, **§15.7 L7/L7b/L8/L8b + all mutation rows** |
| **RM4** | re-seed (§10): `Receiver::open_seeded` (stale-log set-aside + `options().seed(..)` cold start) and the primary's re-seed hold (`begin_reseed` / `end_reseed` / `replica_needs_reseed` / `retention_floor`) | §15.3 re-seed, kickoff E E1–E10 + all mutation rows |
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
- **RM3 extras:** `scripts/replica/poisoned-primary.sh` runs T8b
  (`tests/poisoned_primary.rs`, `#[ignore]`) under the `LD_PRELOAD` EIO shim
  (`tests/fault/eio_preload.c`, shared with the WAL's H3 gate); CI: `replica-poisoned.yml`
  (paths-filtered, blocking). The writer-stall tests (T5 and friends) read
  `/proc/thread-self/io` and print a loud skip without it. **The replica's test builds compile
  `open-wal` with its `fuzzing` feature** (dev-dependency; `fuzzing::reader_pos` for T18(b) and
  loom L8b) — the non-test build of `open-wal`, and so the shipped library, stays without it.
- **RM4 extra:** `scripts/replica/reseed-dirops.sh` runs E9: it straces the two `#[ignore]`d
  `e9_target_*` tests of `tests/reseed.rs` and checks the order of `open_seeded`'s directory
  syscalls (needs `strace` + `python3`; exits 77 — not a pass — without strace). CI: the
  `replica — §10.1 directory ops (strace, E9)` job in `ci.yml`, per-PR, blocking.
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
  slot reuse, L4 eviction, L5 no lost wakeup, L6a/L6b SPMC; **L2b** never-emits-unreleased
  (added at review). Bounds: L1/L2/L2b/L3/L5 exhaustive; **L4 bound 4** (raised at review — the
  shallowest bound catching the `Relaxed`-pin regression, ~5 s); L6a bound 3; L6b bound 2 per-PR
  (bound 3: 1.45M iterations, 669 s, green — now the scheduled `replica-loom-deep.yml` job). **Loom found a real bug**: `Relaxed` pins let the producer see a consumer's new
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
  | *(review)* pin CAS `Release`→`Relaxed` (loom-found bug regressed) | L4 @ bound 4 | FAIL — "ring invariant: a spare buffer is always free" |
  | *(review)* release-on-capture inside the ring (`released.store(lsn)` in `capture`, private watermark untouched) | L2b | FAIL — "R1: emitted a captured-but-unreleased record" |
  Model-strength fixes made to get there (recorded in design §15.7.8): L1's first version did
  not catch the two L1 mutations because loom's `unpark` synchronizes the target immediately —
  non-L5 models now register no waker and do bounded drain passes (no spinning: a spin made the
  max-branch limit, not a real catch, fail M4). Spec refinements flagged in §15.7.8 (pinning
  instead of overwrite-then-detect for L4; park/unpark instead of Condvar for L5).
- **RM2 review follow-up (2026-10-27, designer review on #53).** Merged `main` (v7 + v7.1, clean).
  Required 1: L4 raised to preemption bound 4 so the `Relaxed`-pin regression fails per-PR.
  Required 2: new L2b (unreleased tail never emitted, checked against the producer's log, not
  `Drain.watermark`) + the release-on-capture-inside-the-ring mutation. All **nine** mutations
  re-run at the per-PR bounds — all FAIL their model, `ring.rs` restored. Non-blocking: sequencing
  `next()` → `checked_next()` in `receiver.rs`/`shipper.rs` with a dedicated
  `ReplError::LsnExhausted` (not poison; `Receiver::run` treats it as fatal) + tests; `hold()`
  heartbeats now carry the primary's `released` (§6), via a reporting-only
  `Consumer::released()`; L6b bound 3 moved to a scheduled job. Note 3 (proptest case counts)
  needed no change: `proptest!` runs every config through `contextualize_config`, so
  `PROPTEST_CASES` already overrides `cases: N` (verified: P4 0.10 s → 7.5 s at 2000). Notes 2
  (`MAX_LSN` vs `seed`, RM4) and 7 (0.3.0 bump must also bump this crate's `open-wal`
  requirement) are tracked, not done here.
- **RM2 review round 2 (2026-10-27, crate-wide review on #53; loom gate CONFIRMED by the
  designer at `b72a2fd` — all 9 mutations re-derived independently).** Five fixes, each with a
  regression test shown **failing on the unfixed code**, then passing:
  - **R1 (major) honest ACKs killed sessions:** `stream_records` published `sent` only after
    `write_all` returned, so a replica ACKing the front of a batch bigger than the socket buffers
    tripped `ack_reader`'s `durable > sent` check. Fix: store `sent` **before** the write (every
    receivable byte is already in `out`, so the R4 bound stays sound). Test
    `acks_during_a_blocked_batch_write_keep_the_session` (512 × 60 KB records, `send_batch` 256,
    real `Receiver` at `batch_records=1`): old ordering **144 sessions** ⇒ FAIL; fixed: 1 session,
    acked == durable, byte-identical.
  - **R2 (major) R4 had no independent oracle:** P3 compared acks with `ReplicaWatermarks.durable`,
    which the receiver writes itself. Tests now open the replica via `open_with(DurableTap)` — a
    `DurabilityObserver` the WAL fires inside `commit` after the sync — and assert every ACK is
    `≤` the tap at receipt (P1 via `FakePrimary::await_ack_tapped`; P3 via `ReplicaServer.tap`).
    **Mutation row (receiver, not ring):** `commit_and_ack` sends `ACK{last_lsn}` BEFORE
    `wal.commit()` ⇒ P1 FAIL "R4: ACK 4 received while the replica WAL is durable only to 0", P3
    FAIL (self-report check fired first). **Strong form** (also publish `watermarks.durable` early,
    so the self-report agrees with the lie) ⇒ P3 FAIL on the tap alone: "R4: acked 5 > what the
    replica WAL certified durable (0)". Reverted (file byte-compared).
  - **R3 (major) a silent primary wedged the replica:** `ReceiverConfig::new` now defaults
    `idle_timeout = Some(5 s)` (≥ 3× the 500 ms shipper heartbeat); `None` is documented as
    disabling dead-primary detection. Tests `default_idle_timeout_detects_a_dead_primary` +
    `half_open_primary_does_not_wedge_the_replica` (primary #1 acks then goes silent with its
    socket open; the restarted primary's connection must be served): with `None` the second HELLO
    never arrives ⇒ FAIL (20 s read timeout); fixed: served after ~5 s, resumes at durable 1.
  - **R4 (minor) stale `acked` after wipe/re-seed:** on an accepted (not `JOIN_AHEAD`) HELLO the
    shipper now `store`s the HELLO's `durable_lsn` into `acked` (doc comments updated). Test
    `acked_drops_when_a_wiped_replica_says_hello`: old ⇒ `acked` stays 5 ⇒ FAIL; fixed ⇒ drops to 0.
  - **R5 (minor) a poisoning commit could send no ERR:** `send_err` now sends `ERR(Poisoned)`
    whenever the receiver is poisoned. Finding while writing the test: a mid-stream `Io` commit
    failure was already reported as `Poisoned` (the end-of-session commit retry returns
    `WalError::Poisoned`); the real hole is a session ending on an in-band fault (contiguity) whose
    end-of-session commit then fails with `Io` — that error replaced the pending one and nothing
    was sent. Test `poisoning_end_of_session_commit_still_sends_err_poisoned` (planted
    next-segment file ⇒ the roll's `O_EXCL` create fails): old ⇒ `rest = []` ⇒ FAIL; fixed ⇒
    `ERR(Poisoned)`. (`io_commit_failure_is_reported_as_err_poisoned` covers the batch-full path.)
  - **Recommended, done:** reconnect backoff resets once a session reached Streaming, even if it
    ends in `Err`; new `two_replicas_stream_independently` (each replica byte-identical, own acks,
    each replica WAL's tap == w).
  - **Deferred to RM3 (not fixed here):** `hold()` sends HEARTBEAT before SERVING; the real
    `Receiver` answers that with `ERR(Protocol)`, so a held replica reconnect-loops. RM3's §9
    catch-up replaces `hold()` — add a real-`Receiver` interop test then.
  - Still tracked: ring `MAX_LSN` vs `seed` (RM4); the 0.3.0 commit must bump this crate's
    `open-wal` requirement to `"0.3"`.
  `ring.rs` untouched this round (all 9 loom mutation rows stand); loom gate 8/8 green; replica
  suite 58/58 (×3 runs); root `cargo test` 174/0.
- **RM3 — LANDED on a PR branch (2026-11-27; kickoff `docs/kickoff_c_replica_rm3.md`, contract
  `docs/replica_design_v1.md` §9–§9.3). Awaiting designer review + owner merge; RM4 not started.**
  `open-wal`'s `src/` is untouched; the only non-test `ring.rs` change is the fast-path bound
  check (`lsn != next || lsn > MAX_LSN` ⇒ cold path).
  - **`src/slot.rs` (new; built under `--cfg loom`)** — the §9.3 request slot: `IDLE → PENDING →
    TAKEN → DONE → IDLE`; request, answer, chunk buffer and waker in `crate::sync::UnsafeCell`;
    exports `post` / `answer(f)` / `await_answer` / `abandon(consumer)`, called verbatim by
    `shipper.rs`. `Request::{Join, Chunk{cursor, pos}}`; `Answer::{Reseed, Reject,
    Serve{Attached|Wait|NeedChunk}, Attached, Wait, Deferred, Chunk{next, Attached|More(pos)},
    BelowOldest, ReadFailed, Terminal, Internal}`.
  - **`src/shipper.rs`** — `poll(&mut self, &Wal<O>) -> Result<()>` answers joins first (§9
    steps 1–3; step 2 `d > wal.durable_lsn()` is the ONLY rejection; `Ahead` ⇒ Wait) and then
    chunk requests (§9.1): attach, else Wait above `released`, else sticky ReadFailed, else
    Deferred past `catchup_replicas_per_poll`, else one reader (`reader_at(pos)` iff
    `pos.next_lsn() == cursor`, falling back to `reader_from`) copying while `lsn ≤ released`,
    `bytes < catchup_chunk_bytes` (payload + 32 per record) and `records <
    catchup_chunk_records`, always ≥ 1 record. The pass starts at a rotating replica, answers a
    replica at most once, always completes, and returns errors after it. Network thread: one
    served-session state machine (ring / post+await / wait), `send_check` on every record, `sent`
    before every write, one session-level heartbeat timer, nothing before `SERVING`, FIN on
    BelowOldest. Status `Connecting → Joining → CatchingUp ⇄ Streaming` + `Rejected` /
    `NeedsReseed` / `Failed`; `catching_up()`, `request_pending(id)`, `position_fallbacks()`.
    `on_commit` is release + wake only. **`hold()` and `SessionEnd::FellBehind` are deleted.**
    New config `catchup_chunk_bytes` (16 MiB) / `catchup_chunk_records` (16 384) /
    `catchup_replicas_per_poll` (1); new errors `PrimaryLogUnreadable{lsn}`,
    `LsnDomainExhausted`.
  - **Edits to existing RM2 tests — exactly the kickoff's five:** `sh.poll()` →
    `sh.poll(&wal).unwrap()`; a `poll` after each `on_commit` in the three named tests;
    `assert_no_inband_errors` also fails on `Remote{Contiguity|Protocol|WireCrc|Poisoned}`;
    `stalled_replica_overflows_without_blocking_the_writer` rewritten (1 MiB segments, 64 KiB /
    512-record chunks; non-reading poll < 50 ms, reading poll < 500 ms; measured: worst capture
    0.26 ms, on_commit 0.06 ms, non-reading poll 1.2 ms, 334 reading polls with worst 12.7 ms,
    one catch-up episode, one connection); `capture_at_or_below_released_writes_nothing`
    inspects the slot (shown failing on a build that proceeds with the write).
  - **Tests (all green; replica suite at the first push: 96 + 2 shim-only = 39 unit + 27 catch-up
    + 17 receiver + 2 overflow + 11 shipper; 98 + 2 after the review follow-up's T6b):** `tests/catchup.rs` T1–T6, T8a, T9–T13, T15–T17;
    `src/shipper.rs` tests T14, T18; `src/slot.rs` tests U1; `src/ring.rs` tests T12;
    `tests/poisoned_primary.rs` T8b (K=0: durable 12 / last 22, 6 records exposed above durable;
    K=2: durable 18 / last 22, 4 exposed; exactly `4..=12` served in both; §8.2 reopen then
    dense). Shared oracles in `tests/common`: the forwarding proxy, the `rchar`/`syscr` probe,
    the fake replica. **T5 measured (debug build):** 4 KiB records — join poll `rchar` 0; first
    reading poll 6 291 240 B (prefix 5 240 640 B); 66 later reading polls, max 1 054 740 B /
    513 `syscr`. 0-byte records — first poll 5 439 480 B / 453 290 `syscr`; 357 later polls, max
    196 632 B / 16 386 `syscr` (= 2·8192 + 2). **"≤ C + one record" fails a correct build:**
    C + one framed 4 KiB record = 1 052 696 < 1 054 740 (`reader_at` validates the first record
    and `Reader::next` reads it again), hence the spec's `C + 2·max framed`.
  - **Mutation table (each applied, the named test run and shown failing, reverted; `--release`
    reruns where a debug assertion fires first, to show the oracle itself):**
  | # | Mutation (applied, run, reverted) | Test | Failure observed |
  |---|---|---|---|
  | T1 | chunk-end `attach` at `next − 1` | T1 | proxy oracle: `ERR(Contiguity) from the primary` (the network thread's `resume() == answered cursor` check) |
  | T1 | chunk-end `attach` at `next + 1` | T1 | proxy oracle violation (same) |
  | T1 | advance past an un-copied record (`copy` skips a `chunk_push`) | T1 | proxy oracle: `ERR(Contiguity)` from the sending-side check; the record after the hole is never sent |
  | T1 | catch-up writes do not publish `sent` | T1 | `overflow must fall back in-session` (proxy session count 2: an honest ACK > `sent` killed the session) |
  | T2 | step 1 is `d+1 <= oldest` | T2 table + P5 | `in_ring=true d=P-1 (P=1135)`: got `RESEED_REQUIRED` |
  | T2 | step 1 is `d+2 < oldest` | T2 table | `in_ring=true d=P-2`: got `SERVING` |
  | T2 | `SERVING.primary_oldest = 0` | T2 table + P5 | `SERVING{from, oldest}` mismatch |
  | T2 | `primary_oldest` cached at `Shipper::new` | T2 table + P5 | `in_ring=true d=P-2`: served instead of `RESEED_REQUIRED` |
  | T3 | step 2 is `d+1 > durable` | T3 (i), (iv) | `(i) … d == durable must be served` |
  | T3 | step 2 is `d > durable+1` | T3 (ii) | `expected ERR(Contiguity), got Serving { from: 27, .. }` |
  | T3 | restore `Ahead ⇒ reject` | T3 (iv) | `durable-but-unreleased must be served, not rejected` |
  | T3 | refuse only `d > MAX_LSN` | T3 (iii) | `refused for the wrong reason: internal error deciding the join` (the writer's own defensive check; the test judges the frame's message) |
  | T3 | Wait re-posts on heartbeat only | T3 (iv) | `no RECORD within 1 s of the release` |
  | T4 | catch-up never starts (answer 3 ⇒ Wait) | T4 | `timed out waiting for both replicas to ack` |
  | T5 | unchunked | T5 4 KiB | `first reading poll read 75495040 B` |
  | T5 | no record cap | T5 0-byte | `a later reading poll made 65538 read syscalls (bound: 2·8192 + slack)` |
  | T5 | join reads its first chunk inline | T5 4 KiB | `the poll that answers the join must read nothing (rchar: 6291240)` |
  | T5 | Wait falls through to answer 3 | T5 companions | debug: `assertion failed: cursor <= released`; `--release`: `a chunk request above released must wait without opening a reader (rchar: 3608)` |
  | T5 | position ignored (always `reader_from`) | T5 4 KiB | `a later reading poll read 8507820 B (bound: C + 2·max framed + 4 KiB)` |
  | T5 | position not returned in the Chunk answer | T5 4 KiB | same, `8507820 B` |
  | T6 | swallow below-oldest and jump to `oldest` | T6 | proxy oracle: `ERR(Contiguity) from the primary` in session 1 |
  | T7 | receiver ACKs `last_lsn` **before** `commit()` and publishes its self-report early (strong form) | **T1** | `R4: acked 88 > what the replica WAL certified durable (80)` |
  | T7 | same | **T4** | `R4: acked 16 > what the replica WAL certified durable (8)` |
  | T8a | ceiling = `wal.durable_lsn()` | T8a | debug: `catch-up read past the released ceiling`; `--release`: `R1: nothing above the released watermark 40 may be served, got [Record(41, ..` |
  | T8b | K=0: read to the reader's end (ceiling = `last_lsn`, `None` ends the chunk) | T8b K=0 | debug: `catch-up read past the released ceiling`; with ceiling = `last_lsn` alone: `PrimaryLogUnreadable { lsn: 19 }` |
  | T8b | K=2: ceiling = `wal.durable_lsn()` | T8b K=2 | debug: same assert; `--release`: `K=2: R1 — nothing above the released watermark 12 may be served (durable 18, last 22), got Record(13, ..` |
  | T9 | `None` is the end of the chunk | T9 | `poll must return PrimaryLogUnreadable{77}, got Ok(())` |
  | T9 | per-cursor stickiness | T9 | `the sticky failure must be reported for B's request too: Err(PrimaryLogUnreadable { lsn: Lsn(80) })` (B's request was re-read) |
  | T9 | rescan per request (no stickiness) | T9 | same |
  | T9 | ReadFailed ends all sessions | T9 | `timed out waiting for a pending request` (B refused; C's session torn down) |
  | T9 | I/O errors made sticky | T9 non-sticky | `PrimaryLogUnreadable { lsn: Lsn(71) }` on the request after the file came back |
  | T10 | `poll` decides joins only when no release happened since its previous call | T10 | `the replica never reached SERVING under load` |
  | T11 | *(liveness)* the session never re-posts after a teardown | T11 | `timed out waiting for the reconnect` |
  | T12 | `Shipper::new` refuses only `last_lsn > MAX_LSN` | T12 | `matches!(Shipper::new(..), Err(LsnDomainExhausted))` failed |
  | T12 | no `Shipper::capture` pre-check | T12 | debug: ring `capture(2^56−1) at or below released`; `--release`: `matches!(poll, Err(LsnDomainExhausted))` failed |
  | T12 | drop the ring fast-path check | ring unit tests | debug: `r.is_err() == cfg!(debug_assertions)`; `--release`: `next must not move past the domain` |
  | T12 | a raw-`w` ceiling | `t12_the_ceiling_is_the_clamped_released_watermark` | debug: `catch-up read past the released ceiling`; `--release`: `timed out waiting for the cutover`. **The in-flight variant passes with this mutation** (see findings) |
  | T13 | Deferred not re-posted | T13 | `timed out waiting for a pending request` |
  | T13 | cap ignored | T13 | `round 0: exactly one replica must get one chunk, got [10, 10]` |
  | T13 | no round-robin | T13 | `the replicas must alternate: [0, 0, 0, 0, 0, 0, 0, 0]` |
  | T14 | delete the sending-side check | T14 | `send_check(5, 4) must be Contiguity, got Ok(Lsn(5))` |
  | T15 | heartbeat before `SERVING` | T15a | `nothing may be sent before SERVING` |
  | T15 | no heartbeat while awaiting an answer | T15b | `expected heartbeats while awaiting an answer, saw 0` |
  | T15 | a per-wait (not session-level) timer | T15c | `expected ≥ 3 heartbeats across the wait loop, saw []` |
  | T16 | return the first error immediately | T16 (chunk/chunk shape) | `b_is_join=false a_first=true: B's chunk request must be answered by the same poll`. **The specified join shape passes with this mutation** (see findings) |
  | T17 | drop "at least one" | T17 (budget 0) | `budget 0: exactly one record per chunk (at lsn 1)` |
  | T17 | accept a zero cap / zero records | T17 unit | `matches!(Shipper::new(..), Err(Protocol(_)))` failed |
  | T18 | (a) use `pos` without the `next_lsn == cursor` check | T18a | `a stale position must not end the session: got BelowOldest` |
  | T18 | (b) no `reader_from` retry | T18b | `no ReadFailed from a reader_at reader: got ReadFailed { lsn: Lsn(8) }` |
  | U1 | `abandon` gives up after a bounded wait while `TAKEN` | U1 | `abandon returned while the writer was still inside answer` |
  | U1 | `abandon` has no `IDLE` case | U1 | `abandon on an IDLE slot must return (it hung): Timeout` |
  | ring | `capture(≤ released)` proceeds with the write (the formerly vacuous test) | `capture_at_or_below_released_writes_nothing` | debug: `r.is_err() == cfg!(debug_assertions)`; `--release`: `a released record was overwritten` |
  - **Loom (`tests/loom_ring.rs`): L7 (bound 4), L7b (exhaustive), L8, L8b added; 12/12 green
    (~40 s). All nine RM2 rows re-run after the `ring.rs` change, plus the RM3 rows:**
  | Mutation | Model | Result |
  |---|---|---|
  | `released.store` Release→Relaxed | L1 | FAIL — "NeedsCatchUp without overflow" |
  | `released.load` Acquire→Relaxed | L1 | FAIL — same |
  | remove the min-cursor check before in-place reuse | L3 | FAIL — loom `Causality violation` |
  | free on ANY cursor (max) instead of min | L6b | FAIL — loom `Causality violation` |
  | drop the generation bump (tombstone) on eviction | L4 | FAIL — loom `Causality violation` |
  | drop `unpark` in `release` | L5 | FAIL — loom deadlock |
  | re-read `released` mid-drain | L2 | FAIL — "emitted N past its pass watermark" |
  | pin CAS `Release`→`Relaxed` | L4 @ bound 4 | FAIL — "ring invariant: a spare buffer is always free" |
  | release-on-capture inside the ring | L2b | FAIL — "R1: emitted a captured-but-unreleased record" |
  | *(RM3)* drop the generation bump on eviction | L7 | FAIL — loom `Causality violation` |
  | *(RM3)* pin CAS `Release`→`Relaxed` | L7 @ bound 4 | FAIL — "ring invariant: a spare buffer is always free" |
  | *(RM3)* `attach` does not clear the catch-up flag | L7b | FAIL — "spurious NeedsCatchUp on a caught-up cutover" |
  | *(RM3)* `attach` stores the cursor before clearing the flag | L7b | FAIL — same |
  | *(RM3)* `attach`'s cursor store `Release`→`Relaxed` | L7b | FAIL — same |
  | *(RM3)* `resume`'s cursor load `Acquire`→`Relaxed` | L7b | FAIL — same |
  | *(RM3)* `abandon` uses a plain store instead of the CAS | L8 | FAIL — "the slot must be IDLE after abandon" / "stale attach" |
  | *(RM3)* writer stores `DONE` before it attaches | L8 | FAIL — loom `Causality violation` |
  | *(RM3)* `abandon` does not wait for `DONE` when the CAS fails | L8 | FAIL — loom `Causality violation` |
  | *(RM3)* writer writes the answer after storing `DONE` | L8 | FAIL — loom `Causality violation` (only with the scheduling point, see findings) |
  | *(RM3)* `await_answer`'s `DONE` load `Acquire`→`Relaxed` | L8b | FAIL — loom `Causality violation` (only with the scheduling point) |
  | *(RM3)* `post` stores `PENDING` before it writes the request | L8b | FAIL — loom `Causality violation` |
  - **Findings flagged to the designer (implemented as described; none weakens R1–R9):**
    1. **loom could not see two §15.7.5 rows** ("writer writes the answer after `DONE`" ⇒ L8,
       "`await_answer`'s `DONE` load `Relaxed`" ⇒ L8b): loom's `unpark` is not a scheduling point
       and joins causality eagerly, so no modelled execution ran the network thread between the
       `DONE` store and the unpark. Fix (model side, mutations untouched): a `cfg(loom)`-only
       `loom::thread::yield_now()` between them in `Slot::answer` (compiled to nothing
       otherwise). A `Relaxed` load there is a branch point too but loom's partial-order
       reduction does not reorder it against the network thread's load — the `Relaxed` row still
       passed with it. With the yield both rows fail.
    2. **`await_answer` takes an `idle` callback** (called before each park, with the detached
       consumer for `released()`): it is what runs the session-level heartbeat timer while a
       request is outstanding, and on a dead socket it returns `Idle::Abandon`, making
       `await_answer` run `abandon` itself. So there is still no exit with a request outstanding
       except shutdown, and a dead session can take its `PENDING` request back.
    3. **T16 as specified cannot fail under its mutation**: with joins answered first, B's join
       is answered before A's failing read whatever `poll` does afterwards. Added the
       chunk/chunk shape (both requests pending, one `poll`, both sessions must get their answer
       within 1 s), which does fail; the specified shape is kept.
    4. **T12 "raw-`w` ceiling": the in-flight variant does not panic here** — a terminal shipper
       answers Terminal before any ceiling is computed. The row is falsified by an added test
       (`t12_the_ceiling_is_the_clamped_released_watermark`: a record committed but not captured,
       so the raw `w` is above the clamp, exactly as at the crossing).
    5. **A `poll` pass answers each replica at most once.** Otherwise the chunk request a
       network thread posts right after its join is answered could be read in the same pass, and
       "the poll that answers the join reads nothing" (T5) was racy.
    6. **Joins-first needs the request kind without reading the request cell**: the slot keeps
       a `join` hint (`AtomicBool`, written before `PENDING`), used for ordering only.
    7. **ReadFailed / Internal on the wire** (the spec says only "its session ends"): as built,
       `ERR(Poisoned, "primary log unreadable at lsn N")`, then close; the replica reconnects
       under backoff and gets the same answer without a read.
    8. **T18 is writer-level** (`src/shipper.rs` tests): the request is posted to the slot by
       hand, because the network thread never forges or misplaces a position. (a)'s "session
       count stays 1, no FIN" is "the answer is Chunk, not BelowOldest".
    9. **T15(c) uses a fake replica**: a real `Receiver` cannot be durable above the primary's
       `released` for five heartbeats.
    10. **BelowOldest closes with a half-close** (`shutdown(Write)`) and waits for the replica's
        EOF (bounded by the ack reader's timeout), so the receiver sees a clean end
        (`results[0] == Ok(())` in T6) rather than a reset racing its final ACK.
    11. `acked` is set to the HELLO's `d` on `Serve` **and** on `Reseed` (RM2 review R4: every
        HELLO that is not "ahead"); the spec's §9.2 wording says "accepted".
    12. `Rejected` / `NeedsReseed` / `Failed` stay visible until the replica's next `HELLO`
        (RM2 reset to `Connecting` at once, which made them racy to observe).
    13. `ring.rs` doc comments still describe RM2 (`AttachError::Ahead` "§9 step 2 refusal",
        "resume from the log — RM3"): not edited, under the kickoff's ring guardrail.
    14. Debug builds hit `debug_assert!`s first on several ceiling mutations (T5 Wait
        fall-through, T8a, T8b, T12); the `--release` reruns in the table show the wire oracles.
    15. **T1's "exactly one overflow" needs test-side sequencing, not only ring sizing.** The
        cutover attaches wherever a chunk happens to end — possibly on the ring's very oldest
        record, which the next capture evicts (a second, legitimate, in-session fall-back,
        §9.2). No ring size removes that. T1 therefore lets the network thread act on each
        poll's answer before the writer appends again (it still appends and commits every
        iteration up to and including the cutover poll). Before this, 1 run in ~12 saw two
        episodes (all oracles still held).
    16. **Pre-existing flake in an RM2 test, NOT fixed (outside the allowed edits):**
        `p3_replica_is_never_ahead_and_converges` asserts `Streaming` after waiting for
        `acked == w`; with zero records (`ops = [Commit]`, the shrunk case) `acked == w == 0`
        holds before the replica has connected. Measured **2 failures in 25 runs on this
        branch and 2 in 25 on unmodified `main` (`a0c47bb`)**. Proposed fix for the designer:
        wait for `Streaming` as well. No `shipper.proptest-regressions` file is committed (it
        would pin the failing case).
- **RM3 review follow-up (2026-11-27, designer review of #59 at `fb67914`: no correctness defect;
  four required changes + a `main` merge, all landed on the same branch).**
  1. **T1 was flaky in CI** (`exactly one overflow … left: 2`): it asserted a shipper counter as
     an exact value. Now `episodes >= 1`; the in-session oracle stays the proxy's session count
     (exactly 1). **T1 run 50 times: 50 passed.**
  2. **A peer could hang `Shipper::shutdown`**: after BelowOldest the primary half-closed and
     waited for the ack reader, whose read timeout restarts on every frame. The post-FIN drain
     now has an `io_timeout` deadline, then `shutdown(Both)`; it also ends on shutdown. New test
     **T6b** (fake replica ACKing every 20 ms after the FIN, two shapes): the next HELLO gets
     `RESEED_REQUIRED` (measured 407 ms with `io_timeout` 400 ms) and `shutdown()` returns
     (0.6 ms). Mutations: remove the deadline ⇒ `no new session within 3.4s: the peer's ACKs
     pinned the old one`; no bounded drain at all (the code as reviewed) ⇒ that and
     `Shipper::shutdown did not return within 3.4s`.
  3. **P3 fixed** (edit to the RM2 test authorized in the review): it also waits for `Streaming`.
     With `ops = [Commit]` pinned: 0 failures in 6 runs (was 6 of 6). No regressions file
     committed. Finding 16 above is thereby closed.
  4. **`slot` is no longer public**: `pub` only under `cfg(loom)`, `pub(crate)` otherwise;
     `Slot::is_idle` is `cfg(any(test, loom))`. L8/L8b still drive the shipped sequences; all six
     L8/L8b rows and both U1 rows re-run and fail.
  - Also: the network thread drops its chunk buffer at the cutover (one chunk-sized buffer per
    replica, as §9.1 states); stale RM2 doc comments in `ring.rs` updated (comments only).
  - Gates after the follow-up: replica suite 98/98 in three runs of three (+2 shim-only),
    clippy normal and `--cfg loom`, fmt, MSRV 1.85, loom 12/12, T8b 2/2, root 191/0, root `src/`
    diff empty.
- **RM4 — LANDED on a PR branch (2026-11-28; kickoff `docs/kickoff_e_replica_rm4.md`, contract
  `docs/replica_design_v1.md` §10–§10.2). Awaiting designer review + owner merge.**
  `open-wal`'s `src/`, `ring.rs`, `slot.rs` and the wire protocol are untouched; no existing test
  was edited (`tests/common` gained helpers only; `src/shipper.rs`'s test module gained one test).
  - **`src/receiver.rs`** — `Receiver::open_seeded(dir, cfg, installed, observer: impl FnMut() ->
    O) -> Result<(Receiver<O>, SeedReport)>` and `SeedReport { set_aside }`. `None` ⇒ exactly
    `open_with(dir, cfg, observer())`. `Some(s)`: seed `s.checked_next()`; a missing `dir` is
    created (one level) and its parent fsynced; open with the seed; keep iff `durable_lsn ≥ s`;
    else drop the WAL, `rename(dir, <dir>.stale.<k>)` (smallest unused `k`), fsync the parent,
    create `dir`, fsync the parent, open with the seed again. An open error is returned unchanged.
    Module doc no longer says the seeded open "is RM4".
  - **`src/shipper.rs`** — `begin_reseed`, `end_reseed`, `replica_needs_reseed`,
    `retention_floor`. State is `Reseed { holds, needs_reseed }` on `Shipper` (writer thread),
    updated by `Reseed::on_answer` inside `Slot::answer`'s closure, right after `Writer::decide`
    returns and before the slot goes `DONE`: Reseed answer ⇒ `needs_reseed = true`; Serve answer ⇒
    `acked[id] = d`, then `needs_reseed = false`, then the hold if `d ≥ s`.
  - **`src/error.rs`** — `InvalidSnapshotLsn{s, oldest, durable}`, `UnknownReplica{id}`;
    `SnapshotFailed` removed.
  - **Tests (all green; replica suite 116 = 40 unit + 29 catch-up + 17 receiver + 17 re-seed + 2
    overflow + 11 shipper; + 2 shim-only + 2 strace targets):** `tests/reseed.rs` E1 (+ variants
    A, B), E2, E3 (11 on-disk states × 2 calls), E4, E5, E6 (validation table, floor table, R7,
    release, handover), E7 (+ negative control), E8, E9 targets, E10;
    `shipper::tests::e6_on_answer_release_rules`; `scripts/replica/reseed-dirops.sh` (E9,
    **executed**: on the head merged with #62, set-aside run 16 relevant syscalls / 5 parent
    fsyncs, missing-directory run 6 / 2; before that merge 13 / 2 and 5 / 1). New shared helpers in `tests/common`: `SeededServer` (a real `Receiver` opened with
    `open_seeded`, running `Receiver::run`, on a shared listener so a re-seeded receiver comes up
    on the same address; it seeds the shared `DurableTap` with the opened WAL's `durable_lsn()`),
    `stale_dirs`, `stale_path`.
  - **Mutation table (each applied in a separate worktree, the named test run and shown failing,
    reverted; the worktree was checked clean after every row). The whole table was run twice:
    before and after merging `main` at `e729934` (#62, which changed `Wal::open`'s directory
    creation). All 27 rows fail on both.**
  | # | Mutation (applied, run, reverted) | Test | Failure observed |
  |---|---|---|---|
  | E1 | seed = `s` | `e1_reseed_end_to_end` | `the new log starts at s + 1` (left 240, right 241) |
  | E1 | seed = `s + 2` | `e1_reseed_end_to_end` | `the new log starts at s + 1` (left 242, right 241) |
  | E1 | keep the stale log (no set-aside) | `e1_reseed_end_to_end` | `the re-seeded replica never converged: its run ended with Err(ReseedRequired { oldest: Lsn(232) })` |
  | E1 | delete instead of rename | `e1_reseed_end_to_end` | set-aside assertion: `left: []`, `right: [".../replica.stale.0"]` |
  | E2 | keep iff `durable > s` (`≤` instead of `<`) | `e2_keep_set_aside_boundary_table` | `d == s: the log's records must survive` (left `[]`) |
  | E2 | compare against `s + 1` | `e2_keep_set_aside_boundary_table` | `d == s: the log's records must survive` |
  | E2 | set aside on `None` | `e2_keep_set_aside_boundary_table` | `None: the log's records must survive` |
  | E2 | a probe open with a throwaway observer | `e2_keep_set_aside_boundary_table` | `d < s: two opens, two observers` (left 3, right 2) |
  | E3 | the first open is seed-less | `e3_every_crash_state_converges` | `(b) dir absent, .stale.0 present, call 1: .stale.* directories` (left 2, right 1) |
  | E3 | the re-open after the set-aside is seed-less | `e3_every_crash_state_converges` | `(a) old log intact, call 1: durable_lsn 0 < s` |
  | E4 | always `.stale.0` | `e4_set_aside_never_clobbers` | `open_seeded: Io(Os { code: 39, kind: DirectoryNotEmpty, .. })` |
  | E5 | set aside on open error | `e5_an_unrecoverable_log_is_never_set_aside` | `an unrecoverable log was opened (or replaced)` |
  | E6 | drop the `durable` check | `e6_begin_reseed_validation_table` | `s = 301: expected InvalidSnapshotLsn, got Ok(())` |
  | E6 | drop the `oldest` check | `e6_begin_reseed_validation_table` | `s = 99: expected InvalidSnapshotLsn, got Ok(())` |
  | E6 | clear the hold on any join | `e6_hold_release` | `a served join with d < s keeps the hold` (left 150, right 200) |
  | E6 | clear the hold on session end (in `poll`: a served replica whose status is `Connecting`) | `e6_hold_release` | `a session end keeps the hold` (left 150, right 200) |
  | E6 | never clear the hold | `e6_hold_release` | `Serve with d >= s releases A's hold (and only A's)` (left 200, right 210) |
  | E6 | no writer-side `acked` store | `e6_handover_has_no_unprotected_window` | `the writer must store acked = d when it answers Serve` (left 1000, right 500) — **30 of 30 runs here; by construction this row races with the network thread's own store** |
  | E6 | same | `shipper::tests::e6_on_answer_release_rules` | `the writer stores acked = d on Serve` (left 1000, right 500) — deterministic |
  | E6 | `needs_reseed` never cleared | `e6_retention_floor_table` | `a served replica counts again, at its HELLO value` (left `None`, right `Some(300)`) |
  | E6 | a needs-reseed replica is not excluded | `e6_retention_floor_table` | `a replica that needs a re-seed and has no hold is excluded` (left `Some(0)`, right `Some(295)`) |
  | E6 | *(extra)* release at `d > s` instead of `d ≥ s` | `e6_hold_release` | `d == s released the hold` (left 220, right 230) |
  | E7 | *(extra)* `retention_floor` ignores holds | `e7_the_hold_prevents_the_reseed_loop` | `the hold must keep s + 1 = 241 (oldest is 265)` |
  | E9 | no parent fsync after the rename | `reseed-dirops.sh` | `FAIL (set_aside): no fsync of the parent directory between the rename and the mkdir` |
  | E9 | no parent fsync after the mkdir | `reseed-dirops.sh` | `FAIL (set_aside)` and `FAIL (missing_dir)`: `no fsync of the parent directory between the mkdir and the first segment file` |
  | E9 | rename before the WAL is dropped | `reseed-dirops.sh` | `FAIL (set_aside): the WAL's LOCK descriptor is still open at the rename (2 opened, 1 closed before it)` |
  | E10 | `open_seeded` calls `on_durable(s)` itself | `e10_observer_on_a_seeded_replica` | `no observer call at open` (left `[40]`, right `[]`) |
  E7's own row is its negative control (same code, a policy that ignores holds): the session with
  `HELLO.d == s` is answered `RESEED_REQUIRED`, after `oldest_lsn > s + 1` was asserted. E8 has no
  mutation row (kickoff).
  - **Findings flagged to the designer (implemented as described; none weakens R1–R9):**
    1. **Spec typo:** §10.1 "Integrator procedure" step 3 writes `open_seeded(dir, cfg, observer,
       Some(s))`; the signature (and the kickoff) is `(dir, cfg, installed, observer)`. Built to
       the signature.
    2. **The re-seed state is not inside `Writer::join`.** The RM3 in-source tests build a
       `Writer` literal and call `decide` directly, so a new `Writer` field or `decide` parameter
       would edit existing tests. It lives in `Reseed` on `Shipper` and is updated inside the same
       `Slot::answer` closure, after `decide` returns and before `DONE` — the same thread, the
       same point in the handshake, before `poll` returns.
    3. **A missing `dir` is created one level deep** (`create_dir`, then one parent fsync): a
       missing *parent* is `ReplError::Io`, unlike `open` / `open_with`, whose `create_dir_all`
       builds the whole path. A multi-level create would leave ancestors' entries un-fsynced.
    4. **The handover row cannot be made deterministic from outside** (its mutation "reads 1000
       or races", as the kickoff says): 30 of 30 runs failed here. Added the writer-level unit
       test above as the deterministic falsifier.
    5. **"Clear on session end" has no natural mutation site** — the writer never learns that a
       session ended. The row's mutation is synthesized in `poll` (status `Connecting` and
       `acked ≠ 0`).
    6. **"Exactly one session after the re-seed" is asserted as "exactly one *answered* session
       with `HELLO.d ≥ s`".** While the receiver is down between `run` returning and
       `open_seeded`, the shipper's reconnect sits in the shared listener's backlog; if it timed
       out there (`io_timeout`), the proxy would log a session in which the primary said nothing.
       Such a session is ignored; any answered one counts.
    7. **`SeededServer` seeds the tap with the opened WAL's `durable_lsn()`** (equal to `s` after
       a seeded cold start) rather than the literal `s`, so the same helper serves the unseeded
       phase of a test.
    8. **`SeedReport.set_aside` is `None` when a call finishes an interrupted set-aside** (crash
       state "`dir` absent, `.stale.0` present"): that call moved nothing. The stale directory is
       then only discoverable by name.
    9. **A hold's term is `s` even while that replica is being served from `d < s`** (the spec's
       table). The floor is then above that replica's cursor, so the policy may checkpoint under
       it and it gets a BelowOldest FIN and `RESEED_REQUIRED` — which is the re-seed already in
       flight. Stated, not changed.
    10. **#62 (WAL §8.4.1) merged while this was in progress and makes part of §10.1 redundant.**
        `Wal::open` now creates a missing directory durably itself (parent fsynced before
        anything is put inside) and fsyncs the parent once more after a cold start. §10.1's
        stated reason for the explicit create ("the WAL's own `create_dir_all` does not fsync the
        parent") no longer holds. `open_seeded` still does what §10.1 and the kickoff say (its
        own `mkdir` + parent fsync, in both the missing-directory and the set-aside path); the
        WAL's creation step then finds the directory and does nothing. Harmless; the designer may
        want to simplify §10.1 (the missing-directory branch could be left to the WAL; the
        post-rename parent fsync must stay). E9's three mutation rows still fail after the merge.
    11. **Pre-existing, not touched:** `cargo doc` with `-D warnings` fails on a private
        intra-doc link in `ring.rs:30` (ring guardrail).
  - Gates: replica suite 116/116 ×3 (+2 shim-only, +2 strace targets), clippy normal and
    `--cfg loom`, fmt, MSRV 1.85, loom 12/12, T8b 2/2, E9 script, root 202/0 (191 before the
    #62 merge), `git diff origin/main -- src tests benches fuzz` empty. `main` was merged in
    (`e729934`), not rebased.
- **RM4 CI follow-up (2026-11-28, #63; edit authorized by the designer, limited to
  `tests/ring_overflow.rs`).** The first CI run was red on the RM2 test
  `tiny_ring_marks_needs_catch_up_repeatedly` (`expected repeated NeedsCatchUp, got 1`): its
  producer was unpaced, so a consumer thread scheduled after the producer's 5,000 captures is
  evicted exactly once. Pre-existing: under `taskset -c 0` it failed 20 of 30 runs on this branch
  and 19 of 30 on unmodified `main` (`e729934`). Fix: `run` takes `gates` (P4 passes `&[]`, so it
  is unchanged); at each gate LSN the producer waits until the consumer is back in the ring at
  that cursor, and the consumer, on emitting the gate record, holds until the producer is more
  than a ring's worth ahead — one eviction per gate on any schedule, still decided and reported
  by the ring. Waits are bounded (30 s) and fail loudly. Assertions kept (`emitted == 1..=5000`,
  `catch_ups >= 2`). After: 50 of 50 under `taskset -c 0` (25–30 events), 50 of 50 unpinned.
  Mutations on `ring.rs` (worktree, reverted): `attach` does not clear the catch-up flag ⇒ FAIL
  `gate 1000: the consumer never caught up to 999 (at 997)`; dropping the eviction's flag store
  ⇒ **passes** (414 events: the consumer still detects the eviction from the slot itself), as it
  did before the edit — recorded, not claimed.
- **Current milestone:** RM4 — implemented, PR open, **awaiting designer review + owner merge**.
  RM5 is NOT started.
- **RM2 loom gate:** confirmed by the designer at `b72a2fd`; the nine rows were re-run for RM3
  after the `ring.rs` fast-path change (table above).
- **WAL dependency:** v7 (#52), v7.1 (#54), v7.2 (#56) all on `main`.
- Still tracked: the 0.3.0 commit must bump this crate's `open-wal` requirement to `"0.3"`.
