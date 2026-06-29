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

## `m8/` — hardware/platform durability gates (§14.8 + §14.4d)

M8 is the milestone whose gates **cannot be honestly self-certified in a sandbox**
(an `fdatasync` to tmpfs/RAM returns in ~1.5µs — the data was never at risk, so a
power-pull there passes *vacuously*). These scripts split into what genuinely runs
on any host and what the **owner** must run on real (or properly cache-configured
virtual) hardware. The owner-facing procedure is `docs/m8-runbook.md`. Nothing here
fakes green — the owner-run gates print loud "NOT EXERCISED"/OPEN banners.

| Script | Gate | Runs in sandbox/CI? |
|---|---|---|
| `m8/storage-check.sh` | **H2** vacuous-pass guard (deny-by-default FS/cache classification + empirical loss probe) | static part **yes** |
| `m8/fsync-fault.sh` | **H3 §12 poison state machine** (LD_PRELOAD EIO shim) | **yes — green** |
| `m8/dm-flakey.sh` | **H3 physical** + **§14.4d** dir-fsync negative control | **nightly CI** (hosted ubuntu VMs reach dm-flakey; `m8-dmflakey.yml`); the build sandbox lacked it |
| `m8/power-pull.sh` | **H1** power-pull (≥50 cycles, zero acked loss) | no (needs a cuttable target) |
| `m8/evidence.sh` | shared **§5 evidence-ledger** JSON emitter (reused by the gates above) | n/a (helper) |

### Runs here (CI-safe)

```bash
scripts/m8/storage-check.sh classify .     # PASS on durable block FS, FAIL on tmpfs/overlay
scripts/m8/fsync-fault.sh                  # build the EIO shim + run the §12 poison gate (green)
```

`fsync-fault.sh` LD_PRELOADs `tests/fault/eio_preload.c` to fail the commit's libc
`fdatasync` and asserts the §12 poison machine (FsyncFailed, no `durable_lsn`
advance, split-batch rest-at-seg1-max, handle poison). An anti-vacuous guard
asserts the EIO actually fired — running without the shim **fails loudly**. It is an
*application-logic* test of the WAL's reaction to a flush failure, **not** a
durability test and **not** a substitute for dm-flakey/power-pull (the shim returns
a fake EIO with data still in cache, and only catches the libc `fdatasync` — the
rustix directory fsync needs the block-layer gate).

### Owner-run (real hardware)

```bash
scripts/m8/dm-flakey.sh check              # detect device-mapper; loud OPEN banner if absent
sudo scripts/m8/dm-flakey.sh h3 ext4       # physical fsync-failure → poison
sudo scripts/m8/dm-flakey.sh dirfsync-negative ext4   # §14.4d (certify on ext4; FS-/timing-sensitive)
scripts/m8/power-pull.sh cycle             # prints the ≥50-cycle power-pull procedure
```

See `docs/m8-runbook.md` for cut mechanisms (and why `sysrq-b`/`reboot` are **not**
valid cuts), the network side-channel topology, the FS matrix, and the §14.4d
filesystem-dependence caveat.

### CI automation (Tier 1 / Tier 3)

- **Tier 1 — `.github/workflows/m8-dmflakey.yml`** (push-to-main paths-filtered +
  nightly + `workflow_dispatch`; not per-PR): `modprobe dm-flakey` then runs
  `dm-flakey.sh h3 ext4` and `dirfsync-negative ext4` as **hard** gates (FAIL reds the
  build; INCONCLUSIVE is a loud warning, never a pass), with xfs/btrfs informational.
  #16 PASS requires a **source-confirmed block-layer EIO** (`dmesg`) ANDed with WAL
  poison; #17 runs a **`drop_writes` positive control** first (if drop_writes is inert,
  exit 4 HARNESS — louder than INCONCLUSIVE). **Best-effort + loud skip:** if a runner
  lacks dm-flakey it emits a `::warning::` and the gate stays OPEN — **a green run
  carrying that warning is NOT a passed gate**, never faked green.
- **Tier 3 — `.github/workflows/m8-macos.yml`** (`macos-latest`; per-PR paths-filtered
  + push-to-main + `workflow_dispatch`): `cargo test --test macos_fullfsync` (H4
  **Half A** — the no-privilege routing/smoke; the `dtruss` Half B stays owner-run per
  #19). Per-PR because a macOS-only `F_FULLFSYNC`-routing regression is invisible to
  the Linux PR CI (the `cfg(macos)` path does not compile there).
- Both upload the `evidence.sh` §5 JSON as a workflow artifact **every run**, and post
  it to the gate's tracking issue (#16/#17/#19) **only on a manual `workflow_dispatch`**
  (the human sign-off) — never on the per-PR/push/cron runs, which stay loud as a red
  build.

`m8/evidence.sh emit [out=PATH] KEY=VALUE …` builds the §5 JSON (dotted keys nest;
bare ints/bools unquoted; `@`-prefixed values are JSON literals; `timestamp` defaults
to UTC-now). The gates write `evidence-<gate>.json` under `$WAL_M8_EVIDENCE_DIR`.

## `m9/soak.sh` — run the §14.10 soak / endurance test

Drives `tests/soak.rs` (`#[ignore]`d): a single long-lived `Wal` through a sustained
randomized append/commit/`checkpoint(durable)`/crash-recover workload, re-checking the
§14.3 durability envelope (D1/D2/D3/D6/D8) against an independent oracle after every
recover, with bounded-growth gates on **fd count** / **segment-dir disk** /
**RSS** / **commit-latency p999**. Deterministic (seeded LCG ⇒ a failure reproduces
from `WAL_SOAK_SEED`).

```bash
scripts/m9/soak.sh 5            # 5-second smoke
scripts/m9/soak.sh 14400 123    # 4-hour run, seed 123
WAL_M9_EVIDENCE=soak.json scripts/m9/soak.sh 5   # also write a §5 evidence ledger
```

| Arg / env | Default | Meaning |
|---|---|---|
| `$1` / `WAL_SOAK_SECONDS` | `5` (script) | Wall-clock duration. **< 600 s = smoke** (not the gate); ≥ 600 s accrues gate evidence. |
| `$2` / `WAL_SOAK_SEED` | test's fixed seed | LCG seed. |
| `WAL_M9_EVIDENCE` | unset | Path for the assembled §5 evidence JSON (reuses `m8/evidence.sh`). |

**Honest framing (same stopgap as LazyFS/bench):** a short run proves the
driver/monitors/oracle work but is **NOT** the §14.13 gate — that is a **multi-hour**
run on a dedicated runner with zero resource regression and zero oracle violation. A
failure (a resource leak or an invariant breach) is a real bug and reds the run.
`.github/workflows/soak.yml` runs this nightly + on dispatch (informational/contingent),
uploading the evidence artifact; it is **not** a per-PR gate.
