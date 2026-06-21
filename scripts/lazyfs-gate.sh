#!/usr/bin/env bash
#
# lazyfs-gate.sh — build, mount, and run the M3 LazyFS power-loss gate.
#
# The gate (`tests/lazyfs_gate.rs`, §14.4b + §14.4g) needs a LazyFS/FUSE mount:
# the WAL writes into the mount, data lives in LazyFS's page cache, and a
# `lazyfs::clear-cache` command drops everything not `fdatasync`'d — a faithful
# power loss. This script makes that environment reproducible locally, in CI, and
# in agent sandboxes, so nobody has to re-derive the (several non-obvious) setup
# steps. It is idempotent: re-running any subcommand is safe.
#
# Usage:
#   scripts/lazyfs-gate.sh deps      # install fuse3 + build deps (apt; opt-in)
#   scripts/lazyfs-gate.sh build     # clone + build LazyFS at the pinned commit
#   scripts/lazyfs-gate.sh mount     # (re)mount LazyFS, wait until ready
#   scripts/lazyfs-gate.sh run       # cargo test the gate against the mount
#   scripts/lazyfs-gate.sh unmount   # tear the mount + daemon down
#   scripts/lazyfs-gate.sh env       # print `export LAZYFS_*` lines
#   scripts/lazyfs-gate.sh all       # build + mount + run (+ always unmount)
#
# Config via env vars (all have defaults):
#   LAZYFS_SRC   where LazyFS is cloned/built   (default: $TMPDIR/lazyfs-src)
#   LAZYFS_REF   pinned LazyFS commit            (default: the verified SHA below)
#   LAZYFS_MNT   FUSE mount dir the WAL uses     (default: $TMPDIR/open-wal-lazyfs/mnt)
#   LAZYFS_ROOT  FUSE backing (root) dir         (default: .../root)
#   LAZYFS_FIFO  faults FIFO (clear-cache)       (default: .../faults.fifo)
#   LAZYFS_LOG   LazyFS logfile (barrier)        (default: .../lazyfs.log)
#   LAZYFS_CACHE page-cache size                 (default: 0.25GB)
#
set -euo pipefail

# --- configuration -----------------------------------------------------------

LAZYFS_URL="${LAZYFS_URL:-https://github.com/dsrhaslab/lazyfs.git}"
# Pinned to the exact commit the M3 gate was verified against (reproducibility).
LAZYFS_REF="${LAZYFS_REF:-045a0b3a1126725e693934e29d3ba15e08cc39ec}"

_tmp="${TMPDIR:-/tmp}"
LAZYFS_SRC="${LAZYFS_SRC:-${_tmp%/}/lazyfs-src}"
_work="${LAZYFS_WORK:-${_tmp%/}/open-wal-lazyfs}"
LAZYFS_MNT="${LAZYFS_MNT:-${_work}/mnt}"
LAZYFS_ROOT="${LAZYFS_ROOT:-${_work}/root}"
LAZYFS_FIFO="${LAZYFS_FIFO:-${_work}/faults.fifo}"
LAZYFS_LOG="${LAZYFS_LOG:-${_work}/lazyfs.log}"
LAZYFS_CACHE="${LAZYFS_CACHE:-0.25GB}"

LAZYFS_BIN="${LAZYFS_SRC}/lazyfs/build/lazyfs"
LAZYFS_CFG="${_work}/config.toml"
LAZYFS_MOUNT_OUT="${_work}/mount.out"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() { printf '\033[1;34m[lazyfs-gate]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[lazyfs-gate] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# Run a privileged command as root if we are not already (CI runners use sudo).
as_root() {
  if [ "$(id -u)" -eq 0 ]; then "$@"; else sudo "$@"; fi
}

# --- subcommands -------------------------------------------------------------

cmd_deps() {
  command -v apt-get >/dev/null 2>&1 || die "deps only supports apt; install fuse3 + libfuse3-dev + g++/cmake/make/git manually"
  log "installing fuse3 + build deps via apt"
  as_root apt-get update -y
  as_root apt-get install -y g++ cmake make git pkg-config fuse3 libfuse3-dev
}

cmd_build() {
  local force="${1:-}"
  if [ -x "$LAZYFS_BIN" ] && [ "$force" != "--force" ]; then
    log "LazyFS already built at $LAZYFS_BIN (use 'build --force' to rebuild)"
  else
    if [ ! -d "$LAZYFS_SRC/.git" ]; then
      log "cloning LazyFS into $LAZYFS_SRC"
      git clone "$LAZYFS_URL" "$LAZYFS_SRC"
    fi
    log "checking out pinned ref $LAZYFS_REF"
    git -C "$LAZYFS_SRC" fetch --quiet origin "$LAZYFS_REF" 2>/dev/null || git -C "$LAZYFS_SRC" fetch --quiet origin
    git -C "$LAZYFS_SRC" checkout --quiet "$LAZYFS_REF"
    log "building libpcache"
    ( cd "$LAZYFS_SRC/libs/libpcache" && ./build.sh )
    log "building lazyfs"
    ( cd "$LAZYFS_SRC/lazyfs" && ./build.sh )
    [ -x "$LAZYFS_BIN" ] || die "build finished but $LAZYFS_BIN is missing"
  fi
  # `allow_other` (used by the mount) needs this in /etc/fuse.conf for non-root.
  if [ -w /etc/fuse.conf ] || [ "$(id -u)" -ne 0 ]; then
    if ! grep -qs '^user_allow_other' /etc/fuse.conf 2>/dev/null; then
      log "enabling user_allow_other in /etc/fuse.conf"
      echo 'user_allow_other' | as_root tee -a /etc/fuse.conf >/dev/null
    fi
  fi
  log "LazyFS ready: $LAZYFS_BIN"
}

_is_mounted() { mount | grep -q " on ${LAZYFS_MNT} "; }

cmd_unmount() {
  if _is_mounted; then
    log "unmounting $LAZYFS_MNT"
    fusermount3 -u "$LAZYFS_MNT" 2>/dev/null \
      || fusermount3 -uz "$LAZYFS_MNT" 2>/dev/null \
      || as_root umount -l "$LAZYFS_MNT" 2>/dev/null \
      || true
  fi
  # Exact-name match only: `pkill -f lazyfs` would also match this script.
  pkill -x lazyfs 2>/dev/null || true
  sleep 0.5
}

cmd_mount() {
  [ -x "$LAZYFS_BIN" ] || die "LazyFS not built — run 'scripts/lazyfs-gate.sh build' first"

  cmd_unmount
  mkdir -p "$_work" "$LAZYFS_MNT" "$LAZYFS_ROOT"
  rm -f "$LAZYFS_LOG" "$LAZYFS_MOUNT_OUT"
  rm -f "$LAZYFS_FIFO"; mkfifo "$LAZYFS_FIFO"

  # Canonical config (verified against lazyfs/config/default.toml). NOTE the
  # section is [filesystem] (not [file_system]/[file system]) or `logfile` is
  # ignored and the test's log barrier never fires. We deliberately do NOT set
  # fifo_path_completed: LazyFS opens that FIFO O_WRONLY once at startup and
  # gates ALL command processing on a persistent reader — the logfile barrier
  # (LAZYFS_LOG) is robust instead.
  cat > "$LAZYFS_CFG" <<EOF
[faults]
fifo_path="${LAZYFS_FIFO}"

[cache]
apply_eviction=false

[cache.simple]
custom_size="${LAZYFS_CACHE}"
blocks_per_page=1

[filesystem]
log_all_operations=false
logfile="${LAZYFS_LOG}"
EOF

  log "mounting LazyFS at $LAZYFS_MNT (root=$LAZYFS_ROOT, cache=$LAZYFS_CACHE)"
  # Invoke the binary directly with absolute paths (the upstream mount-lazyfs.sh
  # resolves ./build/lazyfs relative to cwd — a common footgun). `setsid … &`
  # detaches the daemon so it survives this subcommand returning (CI steps).
  setsid "$LAZYFS_BIN" "$LAZYFS_MNT" \
    --config-path "$LAZYFS_CFG" \
    -o allow_other -o modules=subdir -o "subdir=${LAZYFS_ROOT}" -s \
    >"$LAZYFS_MOUNT_OUT" 2>&1 < /dev/null &

  # Poll until mounted — the page-cache pre-allocation takes a few seconds, so a
  # fixed sleep is unreliable.
  local i
  for i in $(seq 1 60); do
    if _is_mounted; then log "mounted (after ${i}x0.5s)"; return 0; fi
    sleep 0.5
  done
  log "mount did not come up; daemon output:"; cat "$LAZYFS_MOUNT_OUT" >&2 || true
  die "LazyFS failed to mount at $LAZYFS_MNT"
}

cmd_run() {
  _is_mounted || die "LazyFS not mounted — run 'scripts/lazyfs-gate.sh mount' first"
  log "running the gate (single-threaded; clear-cache is global to the mount)"
  ( cd "$REPO_ROOT" && \
    LAZYFS_MNT="$LAZYFS_MNT" LAZYFS_FIFO="$LAZYFS_FIFO" LAZYFS_LOG="$LAZYFS_LOG" \
    cargo test --test lazyfs_gate -- --ignored --test-threads=1 --nocapture )
}

cmd_env() {
  echo "export LAZYFS_MNT=\"${LAZYFS_MNT}\""
  echo "export LAZYFS_FIFO=\"${LAZYFS_FIFO}\""
  echo "export LAZYFS_LOG=\"${LAZYFS_LOG}\""
}

cmd_all() {
  trap cmd_unmount EXIT
  cmd_build
  cmd_mount
  cmd_run
}

# --- dispatch ----------------------------------------------------------------

case "${1:-}" in
  deps)    cmd_deps ;;
  build)   shift; cmd_build "${1:-}" ;;
  mount)   cmd_mount ;;
  run)     cmd_run ;;
  unmount) cmd_unmount ;;
  env)     cmd_env ;;
  all)     cmd_all ;;
  *)
    grep -E '^#( |$)' "$0" | sed -E 's/^# ?//' | sed -n '1,28p'
    exit 1
    ;;
esac
