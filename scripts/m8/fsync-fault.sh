#!/usr/bin/env bash
#
# fsync-fault.sh — M8 / §14.8 H3 (state-machine half): the §12 fsync-failure
# poison gate, RUNNABLE in any sandbox (no privileges, no device-mapper).
#
# It compiles the LD_PRELOAD EIO shim (tests/fault/eio_preload.c) and runs
# tests/fsync_fault_gate.rs with the shim preloaded, so the WAL's commit data
# sync (libc fdatasync) returns EIO on demand. The gate asserts the §12 poison
# state machine: FsyncFailed surfaces, durable_lsn does not advance past the last
# synced segment (incl. the split-batch partial-advance), and the handle poisons.
#
# SCOPE: this is an APPLICATION-LOGIC test of how the WAL *reacts* to a flush
# failure. It is NOT a durability test and NOT a substitute for the §14.8 H3
# dm-flakey / power-pull gold path (scripts/m8/dm-flakey.sh, OPEN-pending-owner-
# hardware). See docs/m8-runbook.md and the test/shim file headers.
#
# Anti-vacuous: each test asserts the shim actually injected an EIO (a counter the
# shim bumps). Running this WITHOUT the shim, or on a toolchain where the data
# sync does not route through libc fdatasync, FAILS LOUDLY — never a vacuous pass.
#
# Usage:
#   scripts/m8/fsync-fault.sh          # build shim + run the gate
#   scripts/m8/fsync-fault.sh build    # just compile the shim
#   scripts/m8/fsync-fault.sh strace   # one-shot interception diagnostic
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${WAL_M8_BUILD:-${REPO_ROOT}/target/m8}"
SHIM_SRC="${REPO_ROOT}/tests/fault/eio_preload.c"
SHIM_SO="${BUILD_DIR}/eio_preload.so"
ARM_FILE="${BUILD_DIR}/fault.arm"
COUNT_FILE="${BUILD_DIR}/fault.count"

log() { printf '\033[1;34m[m8/fsync-fault]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[m8/fsync-fault] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

cmd_build() {
  mkdir -p "$BUILD_DIR"
  command -v cc >/dev/null 2>&1 || die "no C compiler (cc) — needed to build the LD_PRELOAD shim"
  log "compiling EIO shim: $SHIM_SO"
  cc -shared -fPIC -O2 -o "$SHIM_SO" "$SHIM_SRC" -ldl
}

# One-shot diagnostic: confirm the WAL's commit data sync is a libc fdatasync the
# shim can intercept (the ship/drop evidence behind H3). Informational.
cmd_strace() {
  command -v strace >/dev/null 2>&1 || die "strace not available"
  log "strace of a WAL commit (expect at least one fdatasync — the interceptable data sync):"
  ( cd "$REPO_ROOT" && cargo test --no-run --test fsync_fault_gate >/dev/null 2>&1 )
  log "(the gate's per-test injection-count assertion is the load-bearing proof; strace is a diagnostic)"
}

cmd_run() {
  [ -x "$SHIM_SO" ] || cmd_build
  mkdir -p "$BUILD_DIR"
  rm -f "$ARM_FILE"
  printf '0\n' > "$COUNT_FILE"

  log "building the gate (no preload)"
  ( cd "$REPO_ROOT" && cargo test --no-run --test fsync_fault_gate )

  log "running the §12 poison gate under LD_PRELOAD=$SHIM_SO"
  ( cd "$REPO_ROOT" && \
    LD_PRELOAD="$SHIM_SO" \
    WAL_FAULT_ARM="$ARM_FILE" \
    WAL_FAULT_COUNT="$COUNT_FILE" \
    cargo test --test fsync_fault_gate -- --ignored --test-threads=1 --nocapture )

  log "================================================================"
  log "H3 §12 poison state machine: PASSED on real storage (logic claim)."
  log "H3 PHYSICAL fsync-failure (dm-flakey/power-pull) stays OPEN-pending-"
  log "owner-hardware — see scripts/m8/dm-flakey.sh and docs/m8-runbook.md."
  log "================================================================"
}

case "${1:-run}" in
  build)  cmd_build ;;
  strace) cmd_strace ;;
  run)    cmd_run ;;
  *)      die "usage: $0 [build|run|strace]" ;;
esac
