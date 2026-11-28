# Kickoff E — `open-wal-replica` RM4: re-seed (§10)

> **Status: READY (2026-11-28).** The owner approved the §10 revision: the snapshot traits are
> replaced by `Receiver::open_seeded` and the primary's re-seed hold, and snapshot transport
> is **not** part of the crate (it is application-specific).
>
> **Critique pass 1** (independent, against the code at `2705cf0`, with experiments on the
> real WAL) confirmed the stateless keep / set-aside rule in every reachable crash state, and
> found 2 blockers and 5 majors, all fixed here and in §10–§12:
> - the observer is consumed by the first open, so the set-aside path could not open twice ⇒
>   an observer factory;
> - the WAL never calls the observer at open, so a seeded replica's tap starts at 0 and the
>   proxy oracle would report false R4 violations ⇒ tests seed the tap; E10 rewritten;
> - a window where neither the hold nor `acked` protected `s + 1` ⇒ the writer stores `acked`
>   before it clears the hold;
> - E7 failed on a correct build and its control was vacuous ⇒ `retention_floor()` and a
>   precise exclusion rule;
> - a new replica once opened without a seed has an empty base-1 log, which is set aside;
> - `dir` created without a parent fsync;
> - a snapshot in flight across a promotion must be discarded.

You are implementing **RM4** of `open-wal-replica`.
- A replica that is answered `RESEED_REQUIRED`, or that is new on a primary whose log no
  longer starts at 1, is restarted from an application snapshot at LSN `s`: its stale log is
  set aside and a new log is cold-started at `s + 1`.
- The primary gains a **re-seed hold**, so its checkpoint policy cannot delete `s + 1` while
  the re-seed is in flight.
- The crate does **not** produce, transport or install snapshots. That is the integrator's.

Everything you need from the WAL is on `main`. **Do not touch `open-wal`'s `src/`.**

## Read first — in this order, before any code
1. `crates/open-wal-replica/CLAUDE.md`.
2. `docs/replica_design_v1.md`: **§10, §10.1, §10.2** in full (the build contract), then §5
   (R2, R5, R6, R7), §7, §9 steps 1–3, §9.2 "Retention", §11, §11.1, §12.2, §15.3.
3. The WAL API you drive: `Wal::options().seed(..).observer(..).open(..)` (the seed is
   ignored when the directory holds a log; `Lsn(0)` is `InvalidConfig`), `durable_lsn`,
   `oldest_lsn`, WAL spec §8.4 (incomplete highest segment is discarded), and the directory
   `LOCK` (released when the `Wal` is dropped).
4. `src/receiver.rs` (`open_with`, `run`) and, in `src/shipper.rs`, `Writer::decide` /
   `join` (where a join is answered Serve) and `min_replica_acked_lsn`.

## Scope (the spec is authoritative; this is the checklist)
1. **`Receiver::open_seeded(dir, cfg, installed, observer: impl FnMut() -> O)
   -> Result<(Receiver<O>, SeedReport)>`** (§10.1).
   - `None` behaves exactly as `open_with(dir, cfg, observer())`. `open` / `open_with` keep
     their signatures.
   - `Some(s)`: `seed = s.checked_next()`; if `dir` does not exist, create it and fsync its
     parent; open with the seed and `observer()`; keep the log iff `durable_lsn ≥ s`;
     otherwise drop the WAL, set the directory aside, open again with a second `observer()`.
   - `observer()` is called exactly once per WAL open. There is no probe open.
   - The crate does not call the observer itself.
   - An open error is returned unchanged; nothing is set aside.
   - Set-aside: `rename(dir, <dir>.stale.<k>)`, smallest unused `k ≥ 0`; fsync the parent;
     create `dir`; fsync the parent; open. Never delete, never overwrite an existing
     `.stale.<k>`, never open a set-aside directory. A bare relative `dir` has parent `.`.
   - `SeedReport { set_aside: Option<PathBuf> }`.
2. **Re-seed hold on `Shipper`** (§10.2): `begin_reseed(&mut self, wal, id, s)`,
   `end_reseed(&mut self, id)`, `replica_needs_reseed(&self, id)`, `retention_floor(&self)`.
   - Validation order: known `id` (else `UnknownReplica{id}`); `s ≤ wal.durable_lsn()`;
     `s + 1 ≥ wal.oldest_lsn()` with `checked_next` (else
     `InvalidSnapshotLsn{s, oldest, durable}`). Nothing is recorded on failure.
   - `needs_reseed[id]`: set when a join from `id` is answered `RESEED_REQUIRED`, cleared
     when one is answered Serve.
   - `retention_floor()`: the per-replica terms of the §10.2 table, minimum over replicas.
   - Release: when a join from `id` with `d == s` (review of #63: was `d ≥ s`) is answered Serve, the writer thread stores
     `acked[id] = d`, then clears `needs_reseed[id]`, then clears the hold, all before `poll`
     returns. (A Serve answer always stores `acked[id] = d` on the writer thread, hold or
     not; the network thread's later store stays.)
   - The hold and `needs_reseed` are writer-thread state only. The `acked[id]` store on the
     existing atomic is the one permitted shared write.
3. **`ReplError::InvalidSnapshotLsn{s, oldest, durable}` and `UnknownReplica{id}`**; remove
   the never-constructed `SnapshotFailed` (the enum is `#[non_exhaustive]`, pre-1.0).
4. **Docs.** Crate `CLAUDE.md`: a status entry; the integrator obligations for §10.1 steps
   1–4, the dedicated-directory rule, the §10.2 call order and `retention_floor()` in the
   checkpoint policy; the milestone table's RM4 row ("re-seed traits" is gone); the stale
   "§10 step 5" reference. Crate-level rustdoc; `receiver.rs`'s module doc (it still says the
   seeded open "is RM4").

## Edits allowed to existing tests
None to test bodies. If an existing test needs an edit, stop and flag it.

**Additive helpers in `tests/common` are allowed and needed:** `ReplicaServer` / `Proxied`
hard-code `Receiver::open_with`, so add `open_seeded` variants beside them, leaving the
existing ones and their callers untouched. A re-seeded replica's `DurableTap` MUST be seeded
by the test (`fetch_max(s)`) before the proxy starts: the WAL does not call the observer at
open, and a seeded receiver answers heartbeats with `durable_lsn = s`, which the proxy's R4
check would otherwise report against a tap of 0. Tests that need `Receiver::run`'s
stop-on-`ReseedRequired` behaviour must use `run`, not a helper that loops
`serve_connection`.

## Anti-vacuity rule
Each test below states what it observes that would differ on a broken build, and each
mutation row must be applied, the named test shown failing with its message, and reverted.
A test that passes with its mutation applied is too weak: fix the test, never the mutation.
A shipper-reported counter or status is reachability evidence, never the oracle.

## Tests — each shown failing on its mutation, then reverted
Use the existing shared oracles in `tests/common` (the forwarding proxy, the fake replica,
the `DurableTap` observer). New file `tests/reseed.rs` unless stated.

- **E1 — re-seed end to end (real `Shipper`, real `Receiver::run`, proxy between them).**
  - Setup: small segments; the replica receives a prefix, disconnects; the primary appends,
    rolls and checkpoints past it.
  - Flow: `run` returns `ReseedRequired{oldest}` with `oldest == wal.oldest_lsn()`; the test
    calls `begin_reseed(s)` with `s = wal.durable_lsn()` at that moment, keeps appending,
    then `open_seeded(.., Some(s), ..)` and `run`.
  - Assert, from the replica's own WAL: `oldest_lsn == s + 1`; the records
    `s + 1..=primary durable` are byte-identical to what the test appended. From the proxy:
    the session after the re-seed has `HELLO.d == s` and `SERVING.from == s + 1`; no session
    with `HELLO.d ≥ s` contains `RESEED_REQUIRED`; no `ERR`. From disk:
    `SeedReport.set_aside` is `<dir>.stale.0`, and opening it with a plain `Wal::open` yields
    the old log with its old `durable_lsn`, byte-identical.
  - Variant A, a new replica opened directly with `Some(s)` on a missing directory: same
    flow, `set_aside == None`.
  - Variant B, a new replica that was first opened with `None` (so it holds an empty log at
    base 1) and answered `RESEED_REQUIRED`: `set_aside == Some(<dir>.stale.0)`, and that
    directory opens as an empty log with `oldest_lsn == 1`.
  - Mutations: seed = `s` ⇒ fails; seed = `s + 2` ⇒ fails; keep the stale log (no set-aside)
    ⇒ `RESEED_REQUIRED` again at the old `d` and the replica never converges; delete instead
    of rename ⇒ the set-aside assertion fails.
- **E2 — the keep / set-aside boundary (table test).** For a log with durable `d` and
  `installed`: `d < s` ⇒ set aside; `d == s` ⇒ kept; `d > s` ⇒ kept; `None` ⇒ kept; missing
  or empty directory with `Some(s)` ⇒ seeded, nothing set aside; empty with `None` ⇒ starts
  at 1; an empty base-1 log with `Some(s)` ⇒ set aside.
  - "Kept" is asserted from data: the pre-existing records are still readable and no
    `.stale.*` directory exists. "Seeded" is asserted from `oldest_lsn` / `durable_lsn` and
    from the first appended record's LSN.
  - The `d == s` row needs a log that genuinely ends at `s` with records in it (not a fresh
    seeded log), so that a wrong comparison destroys something observable.
  - The observer factory's call count is asserted per row: 1 when kept or seeded, 2 when set
    aside.
  - Mutations: `≤` instead of `<` ⇒ the `d == s` row fails; `<` against `s + 1` ⇒ the same
    row fails; set aside on `None` ⇒ fails; a probe open (factory called twice on a kept log)
    ⇒ the call-count assertion fails.
- **E3 — every §10.1 crash state converges.** Build each on-disk state by hand, then call
  `open_seeded(.., Some(s), ..)` twice in a row:
  (a) old log intact; (b) `dir` absent with `<dir>.stale.0` present; (c) `dir` empty;
  (d) `dir` holding only `LOCK`; (e) `dir` holding a zero-length segment file, three cases:
  named for base `s + 1`, for base 1, for an unrelated base; (f) `dir` holding a full-size
  all-zero segment file (a crash after pre-allocation); (g) the completed seeded log with
  records after `s`; (h) as (g), after the replica checkpointed (`oldest_lsn > s + 1`);
  (i) an empty base-1 log.
  - Assert after each call: `durable_lsn ≥ s`; `oldest_lsn == s + 1` (for (g) and (h):
    unchanged from before the call, records still there); and the number of `.stale.*`
    directories (1 for (a), (b), (i); 0 otherwise) is the same after the second call as after
    the first.
  - Mutations: the **first** open is seed-less ⇒ (b)–(f) come up with an empty base-1 log and
    a spurious set-aside; the **re-open after the set-aside** is seed-less ⇒ (a) and (i)
    start at LSN 1.
- **E4 — set-aside never clobbers.** With `<dir>.stale.0` and `.stale.1` already present
  (each holding a marker file), a set-aside uses `.stale.2` and both markers survive.
  - Mutation: always `.stale.0` ⇒ fails (rename error or lost marker, either is a failure).
- **E5 — an unrecoverable log is never set aside.** Corrupt the old log so `Wal::open` fails
  (mid-log corruption, WAL D5). `open_seeded(.., Some(s), ..)` returns that `WalError`, the
  directory's segment files are byte-for-byte unchanged, no `.stale.*` exists.
  - Mutation: set aside on open error ⇒ fails.
- **E6 — `begin_reseed` validation, the hold lifecycle and `retention_floor` (real WAL).**
  - Validation table: unknown `id` ⇒ `UnknownReplica`; `s = durable + 1` ⇒ Err;
    `s = durable` ⇒ Ok; `s + 1 = oldest − 1` ⇒ Err; `s + 1 = oldest` ⇒ Ok. After every `Err`,
    `retention_floor()` is unchanged.
  - Floor table, two replicas: both streaming ⇒ min of their `acked`; one answered
    `RESEED_REQUIRED`, no hold ⇒ the other's `acked` (and `replica_needs_reseed == Some(true)`
    as soon as that `poll` returns); that one with hold `s` ⇒ `min(s, other acked)`; both
    excluded ⇒ `None`; a second `begin_reseed` for the same replica replaces its hold;
    `end_reseed` clears only that replica's hold; a fresh `Shipper` on the reopened WAL has no
    hold (R7) and re-registering `s` fails with `InvalidSnapshotLsn` once a checkpoint passed
    it.
  - Release, with a fake replica: a join with `d < s` that is served does not clear the
    hold; a `RESEED_REQUIRED` answer does not; a rejected join does not; a session end does
    not; a join with `d > s` that is served does not (review of #63); a join with `d == s`
    answered Serve clears that replica's hold and no other.
  - **Handover, no sleep:** give a replica a stale-high `acked` (it acked 1000 in an earlier
    session), `begin_reseed(s = 500)` on a WAL whose `oldest ≤ 501`, let the fake replica say
    `HELLO{500}`, wait for `request_pending`, call `poll` once. Immediately after `poll`
    returns, with no wait: `replica_acked_lsn(id) == 500` and `retention_floor() == 500`.
  - Mutations: drop the `durable` check; drop the `oldest` check; clear on any join; clear on
    session end; never clear; no writer-side `acked` store (the handover row reads 1000 or
    races); `needs_reseed` never cleared (the floor stays `None` after Serve); a needs-reseed
    replica not excluded (the floor is its old `d`). Each fails a named row.
- **E7 — the hold prevents the loop (end to end), with its negative control.**
  - The test's checkpoint policy is `up_to = min(wal.durable_lsn(), retention_floor())`,
    i.e. a policy that would otherwise checkpoint everything, called after every roll. The
    primary appends and rolls at least five segments while the re-seed is in flight, and at
    least five more while the re-seeded replica catches up; it checkpoints throughout both
    phases.
  - Assert: `wal.oldest_lsn() ≤ s + 1` until the replica is served; from the proxy, no
    session with `HELLO.d ≥ s` contains `RESEED_REQUIRED` and there is exactly one session
    after the re-seed (no BelowOldest FIN during catch-up); the replica converges
    byte-identical.
  - **Negative control (same code; the policy's floor is computed by the test from
    `replica_needs_reseed` and `replica_acked_lsn`, ignoring holds):** the session with
    `HELLO.d == s` is answered `RESEED_REQUIRED`. Anti-vacuity: assert
    `wal.oldest_lsn() > s + 1` before the replica reconnects.
- **E8 — a snapshot ahead of the primary is never served.** A replica opened with
  `installed = Some(s)` where `s > primary durable` (no `begin_reseed`, simulating a provider
  bug) is answered `ERR(Contiguity)` and receives no record. The proxy records a primary
  `ERR` as a violation, so this test reads the session log's errors directly instead of
  asserting a clean run. And `begin_reseed` for that `s` is `InvalidSnapshotLsn`. (No
  mutation row: §9 step 2 is covered by RM3's T3; this pins the re-seed entry to it. It is a
  tripwire only: see §10 on promotion.)
- **E9 — the directory operations are durable and ordered (Linux, `strace`).** Modelled on
  `scripts/m8/dirfsync-presence.sh`: trace a set-aside run and a missing-directory run of a
  small test binary or test, and assert from the trace: the WAL's `LOCK` descriptor is closed
  before the `rename`; an `fsync` of the parent directory follows the `rename`; an `fsync` of
  the parent follows the `mkdir`, before the first segment file is created. Loud skip when
  `strace` is unavailable (a skip is not a pass). Wire it into CI as its own job.
  - Mutations: no parent fsync after the rename; none after the mkdir; rename before the WAL
    is dropped. Each fails the trace check.
- **E10 — observer on a seeded replica.** With a `DurableTap` that the test does **not**
  pre-seed: `open_seeded`'s cold start leaves the tap at 0 (the crate does not call the
  observer); `durable_lsn()` reads `s`; the tap's first value is `≥ s + 1` and appears only
  after the replica's first commit.
  - Mutation: `open_seeded` calls `on_durable(s)` itself ⇒ fails.

## Guardrails
- No change to `open-wal`'s `src/`, to `ring.rs`, to `slot.rs`, or to the wire protocol.
- No snapshot trait, stream type or `SNAPSHOT` frame.
- `checked_next()` for every LSN step.
- The only filesystem operations the crate performs outside the WAL are the ones in §10.1:
  one rename, directory creation, parent-directory fsyncs, and existence checks for
  `.stale.<k>`. It never deletes anything.
- The hold and `needs_reseed` touch only writer-thread state; the writer-side `acked[id]`
  store at Serve is the one shared write. If you find you need another, stop and flag it.
- Never weaken a test to make it pass.

## Before pushing, run
- `cargo test -p open-wal-replica` three times; `cargo clippy -p open-wal-replica
  --all-targets -- -D warnings` (also with `RUSTFLAGS="--cfg loom"`); `cargo fmt --all --
  --check`; `cargo +1.85.0 check -p open-wal-replica --all-targets --locked`.
- The loom suite (it must be unaffected: 12 of 12).
- Root `cargo test` (the root crate must be untouched; `git diff origin/main -- src tests
  benches fuzz` is empty).

## Definition of done
- §10.1 and §10.2 are implemented as specified; E1–E10 are green (E9 actually executed, not skipped), and every mutation row has
  been shown failing with its message, then reverted. The table goes in the PR description
  and the crate `CLAUDE.md`.
- No existing test was edited.
- Open a PR against `main` titled "open-wal-replica RM4: re-seed (§10)". State in the
  description what CI showed, only after observing it. The designer reviews against the diff
  and re-derives the mutation rows; the owner merges.
