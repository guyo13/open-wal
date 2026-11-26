# Kickoff D — `open-wal` Spec v7.2: resumable reader position

You are implementing **Spec v7.2** of `open-wal` (the root crate): a read-only addition that
lets a reader report its position and a later reader start there. It exists so the replica's
catch-up (RM3, next) does not rescan a segment prefix on the primary's writer thread for
every chunk.

## Read first
1. Root `CLAUDE.md`: prime directives, D1–D12, hard rules.
2. `docs/wal_design_v7_2_delta.md`. **It is the build contract.** If it is not on your base
   branch, copy it and this kickoff verbatim from the designer branch:
   `git show origin/claude/epic-hopper-2jb32q:docs/wal_design_v7_2_delta.md`, and the same for
   `docs/kickoff_d_wal_v7_2.md`.
3. `docs/wal_design_v7.md` §6, §6.1, §8.5, §10, §12, §14.7, §15.4.
4. `src/reader.rs`, `src/wal.rs` (`reader_from`, `containing_segment_idx`), and
   `src/segment.rs` (`read_record_at`).

## Scope — exactly this
1. `ReaderPos` (opaque, `Copy`, with `next_lsn()`), `Reader::position()` and
   `Wal::reader_at(pos)`, with the semantics of the delta's §8.7, checks in the stated order.
2. `WalError::InvalidPosition`, non-poisoning.
3. A `#[cfg(any(test, feature = "fuzzing"))]` constructor for arbitrary `ReaderPos` values.
4. Apply the delta's spec edits to `docs/wal_design_v7.md` (status v7.2, changelog, §6, §8.5,
   new §8.7, §10, §12, §15, §17 Decision 8, §14 tests and the §14.12 matrix).
5. Book and README: a short "resuming a reader" section with a mirrored snippet test.
6. Root `CLAUDE.md`: a v7.2 line in the "Spec" paragraph and a LATEST status entry.

## Guardrails
- **No change** to `append`, `commit`, `roll`, `checkpoint`, `recover_segment`, `classify`,
  `record::decode`, the sentinel rule, any fsync call, or the on-disk format. If the work
  seems to need one, stop and flag it.
- `Reader::next`'s behaviour and its zero-allocation hot path stay as they are. The one
  permitted internal change is keeping the last segment's coordinates when the stream ends
  cleanly, so that `position()` can report the end.
- **Zero edits to existing tests, benches, bins and fuzz targets.** Run the existing suite
  unchanged before adding any new test, and say so in the PR.
- Do not touch `crates/open-wal-replica` (RM3 will adopt the API).
- Do not bump the crate version; 0.3.0 is a separate release step.
- Sequencing uses `checked_next()`, never the saturating `next()`.
- Never weaken a test to make it pass. Every oracle must be independent of the code under
  test: compare against `reader_from`, against the records the test itself appended, or
  against `/proc` counters, never against a value `reader_at` reports about itself.

## Tests
Implement every test in the delta's "Tests" section, and demonstrate **every mutation row**:
apply the mutation, show the named test failing with its message, then revert. Record the
table (mutation → test → failure message → reverted) in the PR description and in the
`CLAUDE.md` status entry. A test that passes with its mutation applied is too weak: fix the
test, never the mutation. If a row cannot fail for a structural reason, explain exactly why
in the PR instead of claiming it.

## Before pushing, run
- `cargo test`, and `cargo test --features fuzzing`.
- `PROPTEST_CASES=2000` on the two new property tests, once.
- `cargo clippy --all-targets -- -D warnings`, with and without `--features fuzzing`.
- `cargo fmt --all -- --check`.
- `cargo +1.85.0 check --all-targets --locked`.
- `cargo bench --no-run`.
- `cargo test -p open-wal-replica` (it must still build and pass, untouched).
- The fuzz smoke for all four targets (as `ci.yml` does); they must be unaffected.
- `mdbook build`, if the book changed.

## Definition of done
- The delta's API and semantics are implemented.
- All new tests are green, with every mutation row shown failing.
- The existing suite is green with zero edits.
- The spec, book and `CLAUDE.md` are updated.
- Open a PR against `main` titled "Spec v7.2: resumable reader position (`Reader::position`,
  `Wal::reader_at`)". The designer reviews it against the diff; the owner merges.
