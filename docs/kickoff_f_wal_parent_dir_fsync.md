# Kickoff F — `open-wal`: make a newly created WAL directory durable (parent-directory fsync)

You are fixing one durability gap in the root crate `open-wal`. It is small and independent of
the replica work.

## The gap
`Wal::open` (`src/wal.rs`, `open_seeded`) calls `std::fs::create_dir_all(dir)` and never
fsyncs the parent of anything it created. `cold_start` then fsyncs `dir` itself, which makes
the first segment's name durable *inside* `dir`, but `dir`'s own entry in its parent is not
durable. By POSIX a power loss after the first `commit() → Ok` can remove the whole directory:
a D1 violation. (Journaling filesystems very likely mask it, as they masked the missing
roll-time directory fsync in §14.4d; the fix is a portability safeguard of the same kind.)

## Read first
1. Root `CLAUDE.md`: prime directives, D1–D12, the fsync discipline rules.
2. `docs/wal_design_v7.md` §7.4, §8.4, §12, §14.4d (all three "Tooling" notes), §14.13.
3. `src/wal.rs`: `open_seeded`, `cold_start`, `fsync_dir`; `scripts/m8/dirfsync-presence.sh`.

## The rule to implement (NORMATIVE; write it into the spec as given)
1. **Levels this call creates.** Replace `create_dir_all` with an explicit walk: for each
   missing ancestor of `dir` and for `dir` itself, top-down, `mkdir` it and then fsync its
   parent directory, before going on.
2. **Every cold start.** Whenever `open` cold-starts (recovery found no log, including the
   §8.4 discard-to-empty case), fsync the parent of `dir` once more after the first segment
   is created and `dir` is fsynced, before `open` returns. This covers a directory that an
   earlier `open` created and then crashed (process crash) before its parent fsync.
   - **One tolerated failure:** if `dir` already existed when this call started and opening
     its parent fails with `PermissionDenied`, skip that fsync. A pre-provisioned directory
     under a parent the process cannot read is the deployment's responsibility. Every other
     failure, and any failure for a level this call created, is returned as `WalError::Io`.
3. **Reopening an existing log issues no parent fsync.** The steady-state `open` path gains
   no syscall.
4. Path details: the parent of a bare relative path is `.`; the filesystem root has no
   parent (skip). Use the existing `fsync_dir` helper so the platform behaviour matches the
   other directory fsyncs.
5. No `Wal` handle exists yet when these run, so a failure is an ordinary `Err` from `open`;
   nothing is poisoned.

**Invariant to state in the spec:** `open` never returns `Ok` from a cold start unless the
parent of `dir`, and the parent of every level it created, has been fsynced (subject to the
one tolerated failure).

## Guardrails
- **No change** to `append`, `commit`, `roll`, `checkpoint`, `recover_segment`, `classify`,
  `record::decode`, the reader, the sentinel rule, or the on-disk format.
- **Zero edits to existing tests, benches, bins and fuzz targets.** Run the existing suite
  unchanged before adding any test, and say so in the PR.
- Do not touch `crates/open-wal-replica`.
- **Do not bump the crate version.** The release is a separate owner decision.
- No new dependency.

## Tests
- **F1 — functional.** `Wal::open` on a missing nested path (`a/b/c`) succeeds, appends,
  commits, reopens; on a pre-existing empty directory likewise; a bare relative directory
  name works (run it in a temporary current directory inside a child process or a serialized
  test, not by changing the test binary's cwd unguarded).
- **F2 — syscall presence (Linux, `strace`), the primary guard.** New script
  `scripts/m8/parent-dirfsync-presence.sh`, modelled on `dirfsync-presence.sh`, with
  `strace -y` path annotation. Assert:
  - missing nested path: each `mkdir` is followed by an `fsync` on that level's **parent**
    before the next `mkdir`, and all of them precede the first `.wal` file's creation; one
    more `fsync` on `dir`'s parent follows the cold start's fsync of `dir`;
  - pre-existing empty directory: exactly one `fsync` on `dir`'s parent, after the fsync of
    `dir`;
  - reopening the existing log: **zero** fsyncs on `dir`'s parent.
  Wire it into `ci.yml` beside the existing `§14.4d dir-fsync presence (strace)` job. A run
  without `strace` is a loud failure, not a skip.
- **F3 — the tolerated failure (non-root only; loud skip under root).** A pre-existing empty
  `dir` whose parent is not readable: `open` succeeds. A missing `dir` under a parent where
  the fsync cannot be performed is an error. State in the PR whether CI executed it.
- **Mutation rows — each applied, the named test shown failing with its message, reverted:**
  | Mutation | Must fail |
  |---|---|
  | no parent fsync for created levels (plain `create_dir_all`) | F2 nested case |
  | no cold-start parent fsync | F2 pre-existing-empty case |
  | fsync `dir` instead of its parent | F2 (path check) |
  | parent fsync also issued on reopen of an existing log | F2 reopen case |
  | `PermissionDenied` tolerated for a level this call created | F3 |
- **Not claimed:** a behavioural power-loss test. On ext4/xfs/btrfs it would very likely be a
  negative result, as §14.4d's was. Say so in the spec; do not build one.

## Docs
- `docs/wal_design_v7.md`: a changelog entry (durability fix, no format or API change), the
  rule in §7.4 or §8.4 where directory creation belongs, a §14.4d-style note that the guard
  is syscall presence, and a §14.12/§14.13 row.
- Root `CLAUDE.md`: one line in the fsync-discipline rule, and a LATEST status entry with
  the mutation table.
- README or book only if they describe directory creation.
- **Fuzz clock:** unchanged (designer decision: no recovery, classifier or decoder change).

## Before pushing, run
`cargo test` and `cargo test --features fuzzing`; `cargo clippy --all-targets -- -D warnings`
(both feature configs); `cargo fmt --all -- --check`; `cargo +1.85.0 check --all-targets
--locked`; `cargo bench --no-run`; `cargo test -p open-wal-replica`; the fuzz smoke for all
four targets; both strace presence scripts; `shellcheck` on the new script; `actionlint` on
the workflow.

## Definition of done
The rule is implemented; F1–F3 are green with every mutation row shown failing; the existing
suite is green with zero edits; spec and `CLAUDE.md` updated. Open a PR against `main` titled
"open-wal: fsync the parent when open creates or cold-starts the WAL directory (D1)". Report
what CI showed only after observing it. The designer reviews against the diff; the owner
merges.
