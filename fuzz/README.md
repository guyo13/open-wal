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

# Long run (the §14.13 release gate: N CPU-hours, zero crashes, bounded-scan
# counter never exceeded since the last parser/format change).
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

## CI

`.github/workflows/fuzz.yml` runs the targets time-boxed on a schedule / manual
dispatch (informational until the N-CPU-hour gate is met on a dedicated runner —
the same honest stopgap as the LazyFS/dm-flakey gates). A short per-PR smoke in
`ci.yml` reds a PR on any reproducible crash (a real D11 bug).