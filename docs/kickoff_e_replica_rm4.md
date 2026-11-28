# Kickoff E — `open-wal-replica` RM4: re-seed (§10)

> **Status: DRAFT (2026-11-28).** Not dispatched. It waits on the owner's decision on the §10
> revision (the snapshot traits are replaced by `Receiver::open_seeded` and the primary's
> re-seed hold) and on one independent critique pass.

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
1. **`Receiver::open_seeded(dir, cfg, observer, installed) -> Result<(Receiver<O>, SeedReport)>`**
   (§10.1).
   - `None` behaves exactly as `open_with`. `open` / `open_with` keep their signatures.
   - `Some(s)`: `seed = s.checked_next()`; open with the seed; keep the log iff
     `durable_lsn ≥ s`; otherwise drop the WAL, set the directory aside, open again.
   - An open error is returned unchanged; nothing is set aside.
   - Set-aside: `rename(dir, <dir>.stale.<k>)`, smallest unused `k ≥ 0`; fsync the parent;
     create `dir`; fsync the parent; open. Never delete, never overwrite an existing
     `.stale.<k>`, never open a set-aside directory.
   - `SeedReport { set_aside: Option<PathBuf> }`.
2. **Re-seed hold on `Shipper`** (§10.2): `begin_reseed(&mut self, wal, id, s)`,
   `end_reseed(&mut self, id)`, `min_reseed_hold_lsn(&self)`.
   - Validation order: known `id`; `s < ring::MAX_LSN`; `s ≤ wal.durable_lsn()`;
     `s + 1 ≥ wal.oldest_lsn()` (with `checked_next`). Failure ⇒
     `ReplError::InvalidSnapshotLsn{s, oldest, durable}`, nothing recorded.
   - Cleared inside `poll` only when a join from that replica with `d ≥ s` is answered Serve.
   - Writer-thread state only: no atomic, nothing shared with network threads.
3. **`ReplError::InvalidSnapshotLsn{s, oldest, durable}`**; remove the never-constructed
   `SnapshotFailed` (the enum is `#[non_exhaustive]`, pre-1.0).
4. **Docs.** Crate `CLAUDE.md` (status entry, the integrator obligations for §10.1 steps 1–4
   and the §10.2 call order, fix the stale "§10 step 5" reference); crate-level rustdoc;
   `receiver.rs`'s module doc (it still says the seeded open "is RM4").

## Edits allowed to existing tests
None. If an existing test needs an edit, stop and flag it.

## Anti-vacuity rule
Each test below states what it observes that would differ on a broken build, and each
mutation row must be applied, the named test shown failing with its message, and reverted.
A test that passes with its mutation applied is too weak: fix the test, never the mutation.
A shipper-reported counter or status is reachability evidence, never the oracle.

## Tests — each shown failing on its mutation, then reverted
Use the existing shared oracles in `tests/common` (the forwarding proxy, the fake replica,
the `DurableTap` observer). New file `tests/reseed.rs` unless stated.

- **E1 — re-seed end to end (real `Shipper`, real `Receiver`, proxy between them).**
  - Setup: small segments; the replica receives a prefix, disconnects; the primary appends,
    rolls and checkpoints past it.
  - Flow: `run` returns `ReseedRequired{oldest}` with `oldest == wal.oldest_lsn()`; the test
    calls `begin_reseed(s)` with `s = wal.durable_lsn()` at that moment, keeps appending,
    then `open_seeded(.., Some(s))` and `run`.
  - Assert, from the replica's own WAL: `oldest_lsn == s + 1`; the records
    `s + 1..=primary durable` are byte-identical to what the test appended; from the proxy:
    the second session has `HELLO.d == s` and `SERVING.from == s + 1`, exactly one
    `RESEED_REQUIRED` frame in the whole run, no `ERR`; from disk: `SeedReport.set_aside` is
    `<dir>.stale.0`, and opening it with a plain `Wal::open` yields the old log with its old
    `durable_lsn`, byte-identical.
  - Variant: a **new** replica (empty directory, `installed = None` first) on a primary with
    `oldest_lsn > 1` follows the same flow; nothing is set aside (`set_aside == None`).
  - Mutations: seed = `s` ⇒ fails; seed = `s + 2` ⇒ fails; keep the stale log (no set-aside)
    ⇒ a second `RESEED_REQUIRED`; delete instead of rename ⇒ the set-aside assertion fails.
- **E2 — the keep / set-aside boundary (table test).** For a log with durable `d` and
  `installed`: `d < s` ⇒ set aside; `d == s` ⇒ kept; `d > s` ⇒ kept; `None` ⇒ kept; empty
  directory with `Some(s)` ⇒ seeded, nothing set aside; empty with `None` ⇒ starts at 1.
  - "Kept" is asserted from data: the pre-existing records are still readable and no
    `.stale.*` directory exists. "Seeded" is asserted from `oldest_lsn` / `durable_lsn` and
    from the first appended record's LSN.
  - The `d == s` row needs a log that genuinely ends at `s` with records in it (not a fresh
    seeded log), so that a wrong comparison destroys something observable.
  - Mutations: `≤` instead of `<` ⇒ the `d == s` row fails; `<` against `s + 1` ⇒ the same
    row fails; set aside on `None` ⇒ fails.
- **E3 — every §10.1 crash state converges.** Build each on-disk state by hand, then call
  `open_seeded(.., Some(s))` twice in a row:
  (a) old log intact; (b) `dir` absent with `<dir>.stale.0` present; (c) `dir` empty;
  (d) `dir` holding only `LOCK`; (e) `dir` holding a zero-length first segment file named for
  base `s + 1`; (f) the completed seeded log with records after `s`.
  - Assert after each call: `oldest_lsn == s + 1` (for (f): unchanged and its records still
    there), `durable_lsn ≥ s`, and the number of `.stale.*` directories (1 for (a) and (b),
    0 otherwise) is the same after the second call as after the first.
  - Mutation: the second open reuses the seed-less `open_with` ⇒ (b)–(e) start at LSN 1.
- **E4 — set-aside never clobbers.** With `<dir>.stale.0` and `.stale.1` already present
  (each holding a marker file), a set-aside uses `.stale.2` and both markers survive.
  - Mutation: always `.stale.0` ⇒ fails (rename error or lost marker, either is a failure).
- **E5 — an unrecoverable log is never set aside.** Corrupt the old log so `Wal::open` fails
  (mid-log corruption, WAL D5). `open_seeded(.., Some(s))` returns that `WalError`, the
  directory is byte-for-byte unchanged, no `.stale.*` exists.
  - Mutation: set aside on open error ⇒ fails.
- **E6 — `begin_reseed` validation and hold lifecycle (unit-level, real WAL).**
  - Table: unknown `id`; `s ≥ ring::MAX_LSN`; `s = durable + 1` (Err); `s = durable` (Ok);
    `s + 1 = oldest − 1` (Err); `s + 1 = oldest` (Ok). After every `Err`,
    `min_reseed_hold_lsn()` is unchanged.
  - `min_reseed_hold_lsn` is the minimum over replicas; a second `begin_reseed` for the same
    replica replaces its hold; `end_reseed` clears only that replica's.
  - Release, with a fake replica: a join with `d < s` that is served does not clear; a
    `RESEED_REQUIRED` answer does not clear; a rejected join does not clear; a session end
    does not clear; a join with `d ≥ s` answered Serve clears, and only that replica's.
  - Mutations: drop the `durable` check; drop the `oldest` check; clear on any join; clear on
    session end; never clear. Each fails a named row.
- **E7 — the hold prevents the loop (end to end), with its negative control.**
  - The test's checkpoint policy is `up_to = min(snapshot_lsn, min_reseed_hold_lsn())`, where
    `snapshot_lsn` tracks `durable_lsn`, i.e. a policy that would otherwise checkpoint
    everything. While the re-seed is in flight the primary appends and rolls at least five
    segments and checkpoints after each.
  - Assert: `wal.oldest_lsn() ≤ s + 1` until the replica is served; the proxy sees exactly one
    `RESEED_REQUIRED`; the replica converges.
  - **Negative control (same code, the hold term removed from the policy):** the proxy sees a
    second `RESEED_REQUIRED`. Anti-vacuity: assert `wal.oldest_lsn() > s + 1` before the
    replica reconnects, so the control is known to have checkpointed past the snapshot.
- **E8 — a snapshot ahead of the primary is never served.** A replica opened with
  `installed = Some(s)` where `s > primary durable` (no `begin_reseed`, simulating a provider
  bug) is answered `ERR(Contiguity)` and receives no record (proxy oracle). And
  `begin_reseed` for that `s` is `InvalidSnapshotLsn`. (No mutation row: §9 step 2 is covered
  by RM3's T3; this pins the re-seed entry to it.)
- **E9 — the hold is not persisted (R7), and that is loud.** Recreate the `Shipper` on the
  reopened WAL: `min_reseed_hold_lsn() == None`; re-registering the same `s` succeeds while
  `s + 1 ≥ oldest` and fails with `InvalidSnapshotLsn` after a checkpoint past it.
- **E10 — observer on a seeded replica.** With a `DurableTap`, the tap reads `s` right after
  `open_seeded` cold-starts and only advances after the replica's own commits.

## Guardrails
- No change to `open-wal`'s `src/`, to `ring.rs`, to `slot.rs`, or to the wire protocol.
- No snapshot trait, stream type or `SNAPSHOT` frame.
- `checked_next()` for every LSN step.
- The only filesystem operations the crate performs outside the WAL are the ones in §10.1:
  one rename, directory creation, parent-directory fsyncs, and existence checks for
  `.stale.<k>`. It never deletes anything.
- The hold touches only writer-thread state. If you find you need an atomic, stop and flag it.
- Never weaken a test to make it pass.

## Before pushing, run
- `cargo test -p open-wal-replica` three times; `cargo clippy -p open-wal-replica
  --all-targets -- -D warnings` (also with `RUSTFLAGS="--cfg loom"`); `cargo fmt --all --
  --check`; `cargo +1.85.0 check -p open-wal-replica --all-targets --locked`.
- The loom suite (it must be unaffected: 12 of 12).
- Root `cargo test` (the root crate must be untouched; `git diff origin/main -- src tests
  benches fuzz` is empty).

## Definition of done
- §10.1 and §10.2 are implemented as specified; E1–E10 are green, and every mutation row has
  been shown failing with its message, then reverted. The table goes in the PR description
  and the crate `CLAUDE.md`.
- No existing test was edited.
- Open a PR against `main` titled "open-wal-replica RM4: re-seed (§10)". State in the
  description what CI showed, only after observing it. The designer reviews against the diff
  and re-derives the mutation rows; the owner merges.
