# Kickoff A — open-wal spec v7 (three additive changes for replication)

You are implementing the **v6.1 → v7** delta of `open-wal`'s design spec. Three small,
**additive** changes needed by the `open-wal-replica` crate. No invariant D1–D12 is weakened, no
on-disk format change, no new destructive operation. **All defaults preserve v6 behavior, so the
entire existing suite must stay green with no test edits** — that is the central acceptance
test of this work, not a side effect. Tracking issue: *(the "Spec v7" issue)*.

## Read first
- `docs/wal_design_v7_delta.md` (the full delta — in the tracking issue's `<details>` block until
  you land it) and `docs/wal_design_v6.md` §6, §8.4, §8.5, §9, §11, §15.3, §15.7, §17.
- `CLAUDE.md` (repo root) — re-read the hard rules. This work touches cold start and the reader;
  it must not touch the write path, recovery classification, or fsync discipline.
- `src/config.rs` (`WalConfig`), `src/wal.rs` (`cold_start`, `open_with`, `reader_from`,
  the private `oldest_lsn` field), `src/reader.rs` (`Reader::new`, the skip-to-`from` loop).

## The three changes (verified against current code — these are the exact sites)

1. **`WalConfig::initial_lsn: Lsn`** — default `Lsn(1)`. In `cold_start` (the only place base
   `1` is hard-coded) create `{initial_lsn:020}.wal` with `base_lsn = initial_lsn`, so
   `oldest_lsn = initial_lsn`, `durable_lsn = initial_lsn − 1`, first `append` ⇒ `initial_lsn`.
   `open()` rejects `Lsn(0)` with `InvalidConfig`. **Cold-start-only:** recovery of a non-empty
   directory ignores it (the log is authoritative — do not turn it into an assertion; a seeded
   replica that later checkpoints will legitimately have `oldest_lsn > initial_lsn`).
   No recovery logic changes: `base > 1` is already the post-checkpoint state recovery handles.
2. **`pub fn oldest_lsn(&self) -> Lsn`** on `Wal` — expose the existing private field. Equals
   `RecoveryReport::oldest_lsn` at open; advances on `checkpoint`.
3. **`reader_from(from)` seeks to the containing segment.** Today it opens `segments[0]` (the
   oldest segment of the log) and `Reader::next` skips `lsn < from` across the whole log. Change
   it to binary-search the sorted segment bases for the greatest `base ≤ from` and open *that*
   segment, then scan it from its start skipping to `from`. **Semantics unchanged** (same
   records, same order, D6/D7 intact); only the start segment changes. This is what §8.5
   already says; the impl was broader.

## Spec/doc edits (land in the same PR)
- Produce `docs/wal_design_v7.md` from v6.1 + the delta: changelog entry; §6 (`initial_lsn`,
  `oldest_lsn()`), §8.4 cold start at `initial_lsn`, §8.5 containing-segment note, §11
  validation, **§15.3 reconciled** (the in-process observer is a consumer-implemented trait
  impl — the "v1 built-in" claim was never shipped; only `NullObserver` exists), **§15.7**
  pointer to the replica crate + the WAL's responsibility boundary, **§17 Decision 7** (what
  was added and, explicitly, what was NOT: no checkpoint observer, no reader gating, no
  `truncate_after`, no `WalReader` — all deferred with reasons).
- Update `CLAUDE.md` status + the one-line summary of the three additions. Update the mdBook
  getting-started/config pages for `initial_lsn` and `oldest_lsn()` (brief; link the spec).

## Tests (write them; the §14 mapping is normative)
- **§14.1:** `initial_lsn` validation (`Lsn(0)` ⇒ `InvalidConfig`; default `Lsn(1)`; a large
  `N`); cold start at `N` ⇒ `oldest_lsn == N`, `durable_lsn == N−1`, first append ⇒ `N`;
  `oldest_lsn()` equals the report at open and advances after `checkpoint`.
- **§14.2 (proptest):** a log cold-started at arbitrary `N`, driven through appends/commits/
  rolls/checkpoints/reopens, is always dense from its `oldest_lsn` (D2) and byte-identical on
  replay (D6); reopening with a *different* `initial_lsn` is ignored (D7 — recovery wins).
- **Reader seek (instrumented):** under `cfg(feature="fuzzing")` or a test hook, assert
  `reader_from(from)` opens the containing segment, not `segments[0]`, for `from` in each
  segment of a multi-segment log; and `reader_from` still yields exactly the records `≥ from`
  in order (D6). Include `from` == a segment's `base`, `base−1`, and past `durable_lsn`.
- **§14.4a/§14.4c:** cold-start-at-`N` crash points (after create / after header / before
  dir-fsync) recover per §8.4 (D9) — same machinery as the roll case, now with `base = N`.
- **The existing suite passes unchanged.** Do not edit any existing test. If one fails, the
  change is not additive — stop and report.

## Guardrails
- Do **not** touch `append`/`commit`, `recover_segment`/`classify`, the sentinel logic, fsync
  calls, or `checkpoint` deletion logic. If you think you need to, stop and flag it.
- Do **not** add an "in-process observer" built-in, a checkpoint observer, reader gating, or any
  truncate operation. These are explicitly out of scope (Decision 7).
- `cargo test` (both feature configs), `cargo clippy --all-targets -- -D warnings`,
  `cargo fmt --check`, MSRV `cargo +1.85.0 check --all-targets --locked`, `mdbook build`, and
  the per-PR fuzz smoke + differential must all stay green. The differential harness exercises
  recovery, which you are not changing — if it flips, investigate, never adjust.

## Branch / PR
New branch off `main`. One PR. Commits reference "v7" + the section (e.g. "v7 §8.4: cold start
at initial_lsn"). In the PR description: confirm the three sites changed and nothing else in
`src/` behavior; confirm zero existing-test edits; link the tracking issue.

## Definition of done
`initial_lsn`, `oldest_lsn()`, and the seeking `reader_from` implemented with their tests;
`docs/wal_design_v7.md` landed with Decision 7; existing suite green **unchanged**; all gates
clean. This unblocks `open-wal-replica` RM3+.
