#!/usr/bin/env bash
# §14.7 performance regression gate.
#
# Runs the criterion benches (`benches/wal.rs`) and compares a "new" run against a
# stored "base" baseline against the §14.7 thresholds:
#
#   * throughput / median-time regresses > 10%  (THROUGHPUT_REGRESS_PCT)
#   * commit-latency p999 regresses     > 20%   (P999_REGRESS_PCT)
#
# TWO DATA SOURCES, wired deliberately:
#   * throughput / median-time  ← criterion's target/criterion/<group>/<id>/<name>/estimates.json
#       (the `median.point_estimate`, NOT the mean — on noisy infra the mean is
#        outlier-sensitive and would trip the gate spuriously).
#   * commit-latency p999       ← the bench's own target/perf/<name>/commit_latency_<batch>.json
#       (criterion stores only point estimates — mean/median/std_dev/MAD/slope — and
#        cannot reconstruct a true per-operation p999, so the bench emits it itself).
#
# CI tier (§14.11): NIGHTLY / manual only — never per-PR. In CI this gate runs
# INFORMATIONAL (`continue-on-error`) until proven stable on a controlled,
# pinned-CPU-governor runner, exactly like the LazyFS gate. The thresholds remain a
# real, enforced gate on such a runner; "informational now" is the hosted-runner
# stopgap, NOT a permanent downgrade. Absolute numbers are device/filesystem-
# dependent (CI/tmpfs fsync is unrepresentative) — this catches gross regressions and
# shows curve shape, not headline throughput (§14.8 H1/H2 is the hardware-number gate).
#
# Requirements: a Rust toolchain, `python3` (JSON + percentage math). `jq`, if
# present, is used only for the ad-hoc `inspect` helper.

set -euo pipefail

THROUGHPUT_REGRESS_PCT="${THROUGHPUT_REGRESS_PCT:-10}"
P999_REGRESS_PCT="${P999_REGRESS_PCT:-20}"
# Extra args forwarded to the bench harness, e.g. BENCH_ARGS="--measurement-time 1
# --warm-up-time 0.3 --sample-size 10" for a quick run. Default: full criterion run.
BENCH_ARGS="${BENCH_ARGS:-}"
# A stable p999 needs many commit() samples; warn (do not gate) if a baseline's
# commit_latency histogram has fewer than this — typically a reduced BENCH_ARGS run.
# Default 1000: p999 is a directly-measured quantile only at >=1000 samples (below
# that HdrHistogram interpolates), so this fires whenever p999 isn't truly measured.
MIN_P999_SAMPLES="${MIN_P999_SAMPLES:-1000}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRIT_DIR="$ROOT/target/criterion"
PERF_DIR="$ROOT/target/perf"

usage() {
  cat <<'EOF'
Usage: scripts/perf-gate.sh <command> [args]

  baseline <name>          Run the benches and store them as baseline <name>
                           (criterion --save-baseline + a snapshot of the p999
                           histogram JSON into target/perf/<name>/).
  compare  <base> <new>    Print the median-time and p999 deltas of two stored
                           baselines (informational; never exits non-zero).
  check    <base> <new>    Like compare, but EXIT NON-ZERO if any benchmark
                           regresses past the thresholds. This is the gate.
  inspect  <name>          Dump the stored medians for baseline <name> (needs jq).

Environment:
  THROUGHPUT_REGRESS_PCT  median-time regression threshold, percent (default 10)
  P999_REGRESS_PCT        commit-latency p999 regression threshold, percent (default 20)
  BENCH_ARGS              extra args for the criterion harness (e.g. a quick run)

NOTE: gate on a FULL bench run. A stable p999 needs many commit() samples, so a
reduced BENCH_ARGS (e.g. --sample-size 10) is fine for a smoke but must NOT be used
for `check` — `check`/`compare` warn when a histogram has too few samples.

Environment threshold for that warning:
  MIN_P999_SAMPLES        warn if a commit_latency histogram has fewer (default 1000)

Examples:
  scripts/perf-gate.sh baseline main
  # ... make a change ...
  scripts/perf-gate.sh baseline pr
  scripts/perf-gate.sh check main pr
EOF
}

# Record where the baseline was taken — absolute numbers are device-dependent.
print_env() {
  echo "## perf-gate environment"
  echo "uname:  $(uname -srm)"
  echo "fs:     $(stat -f -c '%T' "$ROOT" 2>/dev/null || echo '?') at $ROOT"
  echo "cpus:   $(nproc 2>/dev/null || echo '?')"
  echo "rustc:  $(rustc --version 2>/dev/null || echo '?')"
  echo
}

run_benches() {
  local name="$1"
  print_env
  echo "## running benches, saving baseline '$name' ..."
  # shellcheck disable=SC2086
  cargo bench --bench wal -- --save-baseline "$name" $BENCH_ARGS
  # Snapshot the p999 histogram files (the bench overwrites the un-namespaced copies
  # each run, so they must be archived per baseline).
  mkdir -p "$PERF_DIR/$name"
  cp "$PERF_DIR"/commit_latency_*.json "$PERF_DIR/$name/" 2>/dev/null || {
    echo "warning: no commit_latency_*.json produced (did the bench run?)" >&2
  }
  echo "## baseline '$name' stored."
}

# The comparison core. Args: base new mode(check|compare). Exits non-zero in check
# mode iff a threshold is breached.
compare_py() {
  CRIT_DIR="$CRIT_DIR" PERF_DIR="$PERF_DIR" \
  THROUGHPUT_REGRESS_PCT="$THROUGHPUT_REGRESS_PCT" P999_REGRESS_PCT="$P999_REGRESS_PCT" \
  MIN_P999_SAMPLES="$MIN_P999_SAMPLES" \
  python3 - "$@" <<'PY'
import glob, json, os, sys

base, new, mode = sys.argv[1], sys.argv[2], sys.argv[3]
crit = os.environ["CRIT_DIR"]
perf = os.environ["PERF_DIR"]
thr_t = float(os.environ["THROUGHPUT_REGRESS_PCT"])
thr_p = float(os.environ["P999_REGRESS_PCT"])
min_p999_samples = int(os.environ["MIN_P999_SAMPLES"])

def load(path):
    with open(path) as f:
        return json.load(f)

breaches = []
print(f"{'benchmark':<34} {'metric':<10} {'base':>14} {'new':>14} {'delta%':>9}  status")
print("-" * 90)

# 1) median-time deltas from criterion (covers throughput + latency + recovery).
ids = set()
for p in glob.glob(os.path.join(crit, "*", "*", base, "estimates.json")):
    rel = os.path.relpath(p, crit)
    parts = rel.split(os.sep)            # <group>/<id>/<base>/estimates.json
    ids.add((parts[0], parts[1]))
for group, ident in sorted(ids):
    bp = os.path.join(crit, group, ident, base, "estimates.json")
    npth = os.path.join(crit, group, ident, new, "estimates.json")
    if not (os.path.exists(bp) and os.path.exists(npth)):
        continue
    b = load(bp)["median"]["point_estimate"]
    n = load(npth)["median"]["point_estimate"]
    delta = (n - b) / b * 100.0 if b else 0.0
    bad = delta > thr_t
    if bad:
        breaches.append(f"{group}/{ident} median-time +{delta:.1f}% (> {thr_t:.0f}%)")
    status = "REGRESSED" if bad else "ok"
    print(f"{group + '/' + ident:<34} {'med-time':<10} {b:>14.1f} {n:>14.1f} {delta:>+8.1f}%  {status}")

# 2) commit-latency p999 deltas from the bench's own histogram snapshots.
low_sample = []
for bp in sorted(glob.glob(os.path.join(perf, base, "commit_latency_*.json"))):
    fn = os.path.basename(bp)
    npth = os.path.join(perf, new, fn)
    if not os.path.exists(npth):
        continue
    bd, nd = load(bp), load(npth)
    b, n = bd["p999_ns"], nd["p999_ns"]
    # A stable p999 needs many samples; flag a baseline taken with a reduced run.
    for tag, d in ((base, bd), (new, nd)):
        if d.get("samples", 0) < min_p999_samples:
            low_sample.append(f"{tag}/{fn}: {d.get('samples', 0)} samples")
    delta = (n - b) / b * 100.0 if b else 0.0
    bad = delta > thr_p
    label = fn.replace("commit_latency_", "commit_lat/b").replace(".json", "")
    if bad:
        breaches.append(f"{label} p999 +{delta:.1f}% (> {thr_p:.0f}%)")
    status = "REGRESSED" if bad else "ok"
    print(f"{label:<34} {'p999':<10} {b:>14.1f} {n:>14.1f} {delta:>+8.1f}%  {status}")

print("-" * 90)
if low_sample:
    print(
        f"\nWARNING: p999 is unstable below {min_p999_samples} samples — do not gate on "
        "this run (use a full bench run, not a reduced BENCH_ARGS):",
        file=sys.stderr,
    )
    for x in low_sample:
        print(f"  - {x}", file=sys.stderr)
if breaches:
    print(f"\n{len(breaches)} regression(s):")
    for x in breaches:
        print(f"  - {x}")
    if mode == "check":
        sys.exit(1)
else:
    print("\nno regressions past thresholds.")
PY
}

cmd="${1:-}"
case "$cmd" in
  baseline)
    [ $# -eq 2 ] || { usage; exit 2; }
    run_benches "$2"
    ;;
  compare)
    [ $# -eq 3 ] || { usage; exit 2; }
    compare_py "$2" "$3" compare
    ;;
  check)
    [ $# -eq 3 ] || { usage; exit 2; }
    compare_py "$2" "$3" check
    ;;
  inspect)
    [ $# -eq 2 ] || { usage; exit 2; }
    command -v jq >/dev/null || { echo "jq not found" >&2; exit 2; }
    find "$CRIT_DIR" -path "*/$2/estimates.json" -print -exec \
      jq '{median: .median.point_estimate, mean: .mean.point_estimate}' {} \;
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    echo "unknown command: $cmd" >&2
    usage
    exit 2
    ;;
esac
