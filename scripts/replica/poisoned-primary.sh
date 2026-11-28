#!/usr/bin/env bash
# open-wal-replica RM3 T8b — the serve ceiling on a POISONED primary.
#
# Builds the LD_PRELOAD EIO shim (tests/fault/eio_preload.c — the same shim as
# the WAL's §14.8 H3 gate, scripts/m8/fsync-fault.sh) and runs
# crates/open-wal-replica/tests/poisoned_primary.rs under it, single-threaded,
# so the primary's `wal.commit()` fails on a chosen fdatasync and poisons the
# handle while the segment file holds CRC-valid records above anything a
# `commit() -> Ok` covered. The tests assert that `Shipper::poll` on that handle
# serves nothing above the released watermark.
#
# Anti-vacuous: each test asserts the shim actually injected an EIO; without
# the shim (or with interception broken) the tests fail loudly, never pass.
#
# Scope: the shim returns a *fake* EIO and drops no data — a logic test of the
# ceiling, not a durability test (same caveat as the H3 gate).
#
# Usage: scripts/replica/poisoned-primary.sh [build|run]
#   WAL_REPLICA_FAULT_CARGO_FLAGS   extra `cargo test` flags (e.g. `--release`,
#                                   to run without debug assertions)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${WAL_REPLICA_FAULT_BUILD:-${REPO_ROOT}/target/replica-fault}"
SHIM_SRC="${REPO_ROOT}/tests/fault/eio_preload.c"
SHIM_SO="${BUILD_DIR}/eio_preload.so"
ARM_FILE="${BUILD_DIR}/fault.arm"
COUNT_FILE="${BUILD_DIR}/fault.count"
CARGO_FLAGS="${WAL_REPLICA_FAULT_CARGO_FLAGS:-}"

log() { printf '\033[1;34m[replica/poisoned-primary]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[replica/poisoned-primary] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

cmd_build() {
  mkdir -p "$BUILD_DIR"
  command -v cc >/dev/null 2>&1 || die "no C compiler (cc) — needed to build the LD_PRELOAD shim"
  log "compiling EIO shim: $SHIM_SO"
  cc -shared -fPIC -O2 -o "$SHIM_SO" "$SHIM_SRC" -ldl
}

cmd_run() {
  [ "$(uname -s)" = "Linux" ] || die "Linux only (LD_PRELOAD + /proc/thread-self/io)"
  [ -x "$SHIM_SO" ] || cmd_build
  mkdir -p "$BUILD_DIR"
  rm -f "$ARM_FILE"
  printf '0\n' > "$COUNT_FILE"

  log "building the tests (no preload)"
  # shellcheck disable=SC2086  # deliberate word splitting of the extra flags
  ( cd "$REPO_ROOT" && cargo test ${CARGO_FLAGS} -p open-wal-replica --no-run --test poisoned_primary )

  log "running T8b under LD_PRELOAD=$SHIM_SO"
  # shellcheck disable=SC2086
  ( cd "$REPO_ROOT" && \
    LD_PRELOAD="$SHIM_SO" \
    WAL_FAULT_ARM="$ARM_FILE" \
    WAL_FAULT_COUNT="$COUNT_FILE" \
    cargo test ${CARGO_FLAGS} -p open-wal-replica --test poisoned_primary -- --ignored --test-threads=1 --nocapture )

  local n
  n="$(cat "$COUNT_FILE")"
  [ "$n" -ge 2 ] || die "the shim injected only $n EIO(s): the tests verified nothing"
  log "T8b PASSED: $n EIO(s) injected; nothing above the released watermark was served."
}

case "${1:-run}" in
  build) cmd_build ;;
  run)   cmd_run ;;
  *)     die "usage: $0 [build|run]" ;;
esac
