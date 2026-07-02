# open-wal fuzz targets (M9, §14.5)

cargo-fuzz / libFuzzer targets that drive the recovery parser and codec with
adversarial input to prove **D11** (bounded recovery: terminate, never panic /
OOB / unbounded-alloc / unbounded-scan for *any* input bytes) and the
tail-vs-corruption classifier (D4/D5/D10).

## Targets

| Target | Slice | What it fuzzes |
|---|---|---|
| `recovery` | F1 | A whole **directory of segment files** (adversarial filenames + `base_lsn`s) driven through the real public `Wal::open`, plus a secondary single-file `recover_segment` probe. Asserts the bounded forward scan never exceeds `scan_bound(max_record_size)`. |
| `decode` | F2 | The **single-record decoder** in isolation: raw bytes as the decode buffer × a boundary-biased `max_record_size` set. Asserts bounds-soundness of any returned record (payload ≤ max, framed ≤ buf, ≥ 20, 8-aligned, header+payload ≤ framed). Corpus seeded with genuine CRC-valid frames so the Record path is reached. |
| `structure` | F3 | **Structure-aware classifier**: a valid dense segment + one localized mutation (flip CRC/body, zero `rec_type`, extend length, tamper padding, reserved type) driving the real `Wal::open`. Sharp oracle: interior corruption fatal (D5), last corruption truncates (D4), surviving suffix dense + byte-identical (D6/D10), idempotent reopen (D7). |
| `model` | F4 | **Op-script oracle**: decodes fuzzer bytes into a `WalConfig` + `Vec<Op>` and drives the M6 stateful executor (`tests/model/mod.rs::run`, reused verbatim) against an independent oracle — panics on any D1/D2/D3/D6/D7/D8 breach. Process-crash model only (page cache survives), not power loss. Slowest target (real `fdatasync` per `Commit`). |

## Running

Requires the nightly toolchain and `cargo-fuzz` (`cargo install cargo-fuzz`).

```bash
# Build all targets (link-checks the libFuzzer harness).
cargo +nightly fuzz build

# Short smoke run.
cargo +nightly fuzz run recovery -- -runs=100000

# Long run (the §14.13 release gate: >= 24 CPU-hours PER TARGET, zero crashes,
# bounded-scan counter never exceeded, accumulated SINCE the last parser/format
# change — currently 2b198e7, the all-zero-header sentinel fix).
cargo +nightly fuzz run recovery
```

The crate depends on `open-wal` with the `fuzzing` feature, which exposes the
internal parse entry points (the `open_wal::fuzzing` module) and compiles in the
bounded-scan instrumentation. **The public `Wal::open` is the primary surface
under test**; the re-exported helpers are for the secondary direct-probe mode and
for generating valid input bytes.

## Corpus

`corpus/<target>/` holds the seed corpus. For `recovery` it is the
fuzzer-grown, `cargo fuzz cmin`-minimized coverage-preserving set (so it includes
inputs that reach the deeper multi-segment-continuity states a cold-start fuzzer
takes longest to discover — and which a hand-authored byte seed cannot easily
encode for a typed-`Arbitrary` target). Regrow + re-minimize after any
parser/format change. **A reproducible crash input is gold**: minimize it
(`cargo +nightly fuzz cmin` / `tmin`) and commit it into `corpus/<target>/` (or
`artifacts/<target>/`) as a regression seed, then fix the underlying bug — never
tune the test to hide it.

### Regrow log (the "since the last format change" clock in §14.13)

A format change resets the §14.13 CPU-hour clock and mandates a regrow so the
committed corpus reflects the *current* classification. Record each regrow here
so the next format change has a precedent:

- **`2b198e7` (all-zero-header sentinel fix, issue #26)** — regrown + `cargo fuzz
  cmin`'d for all four targets on the post-fix format (the fix changed how
  `rec_type==0, crc≠0` is classified: sentinel → `Invalid` → `TornMidLog`/torn-tail,
  a path the pre-fix corpus never exercised). Minimized entries: recovery
  174→316, structure 130→129, decode 17→40, model 321→348; per-target coverage
  rose (recovery 780→892, structure 561→592, model 798→839); zero crashes. The
  24-CPU-hour/target gate clock therefore starts at `2b198e7`.

## Coverage & regression trust model

**How the committed corpus is consumed (be explicit, so this is documented rather
than assumed):** the corpus is used **as-is** — no CI job re-derives `cargo fuzz
coverage`. The per-target coverage numbers in the regrow log above (e.g. recovery
780→892) are a **one-time provenance record** of the `2b198e7` regrow, not a
self-correcting metric. Re-deriving coverage in CI would need the nightly
sanitizer/coverage toolchain and would be informational-only (same contingent
posture as the rest of `fuzz.yml`); we deliberately do **not** add that job — its
value would be marginal because the **durable** regression proof does not live in a
coverage number at all (see below).

**The durable, churn-proof anchors** for the recovery classifier — in particular the
issue-#26 interior `rec_type→0 ⇒ TornMidLog` path this whole hardening arc exists to
catch — are two **deterministic** tests, independent of what `cmin` keeps:

- `tests/differential.rs` — its scenario matrix contains the interior `rec_type→0`
  case (and the tail case) as explicit, named, exact-match assertions (variant +
  offset + `max_lsn`), run against an independent reference parser (§14.9).
- `recovery::rec_type_zeroed_interior_is_fatal_tornmidlog` (+ the tail companion
  `…_at_tail_is_torn_tail_and_zeroed`) in `src/recovery.rs` — the unit-level
  fail-before/pass-after regression tests for the fix.

Those are the proof. The **fuzzer** additionally starts from that shape: the
`structure` corpus already carries an interior-`ZeroRecType` entry (verified by
replaying the target's `arbitrary` decode + record-build + mutation-selection logic
over the committed corpus — 1 interior-`ZeroRecType` and 11 total-`ZeroRecType`
entries as of the `2b198e7` regrow). It survives `cmin` because the
`rec_type→0 → CRC-fail → classify → TornMidLog` path is a **distinct coverage arm**,
so a coverage-preserving minimization keeps at least one entry reaching it. We
therefore do **not** commit a redundant hand-authored seed; if a future regrow's
`cmin` ever drops the shape (re-run the presence check), the two deterministic tests
above still hold the line, and a fresh interior-`ZeroRecType` seed can be re-added
then.

## CI

`.github/workflows/fuzz.yml` runs the targets time-boxed on a schedule / manual
dispatch (informational until the N-CPU-hour gate is met on a dedicated runner —
the same honest stopgap as the LazyFS/dm-flakey gates). A short per-PR smoke in
`ci.yml` reds a PR on any reproducible crash (a real D11 bug).