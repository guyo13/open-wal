# `scripts/` — developer harnesses

## `lazyfs-gate.sh` — run the M3 power-loss gate

The M3 LazyFS gate (`tests/lazyfs_gate.rs`, §14.4b + §14.4g) proves the durability
contract under **power loss**: it runs the WAL against a real
[LazyFS](https://github.com/dsrhaslab/lazyfs)/FUSE mount and injects
`lazyfs::clear-cache`, which drops everything not `fdatasync`'d. Those tests are
`#[ignore]` by default and need a built + mounted LazyFS plus three env vars
(`LAZYFS_MNT`/`LAZYFS_FIFO`/`LAZYFS_LOG`). This script sets all of that up so you
don't have to.

### Quick start (Linux)

```bash
scripts/lazyfs-gate.sh deps    # once: apt-installs fuse3 + libfuse3-dev + build tools
scripts/lazyfs-gate.sh all     # clone+build LazyFS, mount, run the gate, always unmount
```

`all` should finish with the three gate tests passing:

```
test cold_start_segment_survives_power_loss ... ok
test committed_records_survive_power_loss ... ok
test zeroed_tail_survives_power_loss ... ok
```

### Subcommands

| Command   | What it does |
|-----------|--------------|
| `deps`    | Install `fuse3 libfuse3-dev g++ cmake make git pkg-config` via apt (uses `sudo` if not root). Opt-in — `all` does **not** run it. |
| `build`   | Clone `dsrhaslab/lazyfs` at the pinned commit and build it (skips if already built; `build --force` to rebuild). Enables `user_allow_other`. |
| `mount`   | Generate the config, mount LazyFS, and poll until it's ready. |
| `run`     | `cargo test --test lazyfs_gate -- --ignored --test-threads=1` against the mount. |
| `unmount` | Tear the mount + daemon down (idempotent). |
| `env`     | Print `export LAZYFS_*=…` lines, e.g. `eval "$(scripts/lazyfs-gate.sh env)"`. |
| `all`     | `build` → `mount` → `run`, always unmounting on exit. |

### Configuration (env vars, all optional)

| Var           | Default                          | Meaning |
|---------------|----------------------------------|---------|
| `LAZYFS_SRC`  | `$TMPDIR/lazyfs-src`             | Where LazyFS is cloned/built. |
| `LAZYFS_REF`  | pinned commit                    | LazyFS commit to build (reproducibility). |
| `LAZYFS_MNT`  | `$TMPDIR/open-wal-lazyfs/mnt`    | FUSE mount the WAL writes into. |
| `LAZYFS_ROOT` | `…/root`                         | FUSE backing directory. |
| `LAZYFS_FIFO` | `…/faults.fifo`                  | Faults FIFO (`clear-cache`). |
| `LAZYFS_LOG`  | `…/lazyfs.log`                   | LazyFS logfile — the test's completion barrier. |
| `LAZYFS_CACHE`| `0.25GB`                         | Page-cache size (bigger = slower startup). |

### In CI

`.github/workflows/lazyfs.yml` drives this same script (`deps` → cache → `build`
→ `mount` → `run` → always `unmount`) on PRs and `main` that touch the recovery
code. It runs **informational** (`continue-on-error`) until proven stable on the
hosted runners, then can be made required.

### Gotchas this script handles for you (so you don't re-discover them)

- **Config section is `[filesystem]`** (not `[file_system]`/`[file system]`), or
  LazyFS ignores `logfile` and the test's log barrier never fires.
- **`fusermount3`**, not `fusermount` (fuse3); falls back to `umount -l`.
- **Kills the daemon with `pkill -x lazyfs`** — `pkill -f lazyfs` would also match
  the script's own command line and kill your shell.
- **Invokes the LazyFS binary with absolute paths** — the upstream
  `mount-lazyfs.sh` resolves `./build/lazyfs` relative to the current directory.
- **Polls until mounted** — the page-cache pre-allocation takes a few seconds.
- **No `fifo_path_completed`** — LazyFS opens that FIFO `O_WRONLY` once at startup
  and gates all command processing on a persistent reader; the logfile barrier is
  used instead.
- The WAL itself handles LazyFS having **no `fallocate`** (its `segment::create`
  zero-fill fallback) — nothing to do here.

### Platform note

LazyFS is **Linux + FUSE only**. macOS/Windows can't run this gate; the rest of
the suite (`cargo test`) is cross-platform and FUSE-free.

## `perf-gate.sh` — run the §14.7 performance regression gate

Runs the criterion benches (`benches/wal.rs`) and compares a run against a stored
baseline against the §14.7 thresholds: **throughput / median-time > 10%** or
**commit-latency p999 > 20%** regression fails the gate.

Two data sources, on purpose: the throughput/time delta comes from criterion's own
`target/criterion/**/estimates.json` (the **median** point estimate — the mean is
outlier-sensitive on noisy infra), and the p999 delta comes from the bench's own
`target/perf/commit_latency_<batch>.json` (criterion stores no arbitrary
percentiles, so the commit-latency bench emits p50/p99/p999 from an HdrHistogram).

### Quick start

```bash
scripts/perf-gate.sh baseline main      # run benches, store baseline "main"
# ... make a change, rebuild ...
scripts/perf-gate.sh baseline pr        # store baseline "pr"
scripts/perf-gate.sh check  main pr     # exit non-zero if it regressed past thresholds
```

For a fast smoke (instead of a full criterion run):

```bash
BENCH_ARGS="--warm-up-time 0.3 --measurement-time 0.8 --sample-size 10" \
  scripts/perf-gate.sh baseline main
```

### Subcommands

| Command | What it does |
|---------|--------------|
| `baseline <name>` | Run all four bench groups, `--save-baseline <name>`, and snapshot the p999 histogram JSON into `target/perf/<name>/`. |
| `compare <base> <new>` | Print median-time + p999 deltas of two stored baselines (informational; never non-zero). |
| `check <base> <new>` | Like `compare`, but **exit non-zero** if any benchmark regresses past the thresholds. This is the gate. |
| `inspect <name>` | Dump the stored medians for a baseline (needs `jq`). |

Tunable via env: `THROUGHPUT_REGRESS_PCT` (default 10), `P999_REGRESS_PCT`
(default 20), `BENCH_ARGS` (extra criterion args). Needs `python3` (JSON + math).

### In CI

`.github/workflows/bench.yml` runs this on a nightly schedule + manual dispatch and
uploads the results as an artifact. Like the LazyFS gate it runs **informational**
(`continue-on-error`): hosted runners share CPUs and have variable fsync timing, so
a hard gate would flap. The thresholds stay a **real** gate on a controlled,
pinned-CPU-governor (self-hosted) runner — enforcement there is the
`OPEN-pending-controlled-runner` item, *not* a permanent downgrade.

### Device note

Absolute numbers are **device/filesystem-dependent** — on CI/tmpfs the `fdatasync`
cost is unrepresentative, so these catch gross regressions and show the curve
*shape*, not headline throughput. Real durability-throughput numbers belong on
documented target hardware (§14.8 H1/H2). `baseline` prints `uname`/fs/cpu so a
stored baseline records where it was taken.
