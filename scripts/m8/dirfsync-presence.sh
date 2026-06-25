#!/usr/bin/env bash
#
# dirfsync-presence.sh — M8 §14.4d Tier 1 (PRIMARY, deterministic, FS-independent).
#
# The reliable regression guard for the roll-time directory fsync (§7.4 step 5) is
# "removing fsync_dir changes the SYSCALL TRACE" — NOT "removing it loses data",
# which is FS-dependent and timing-flaky (journaling filesystems transitively
# persist a new file's directory entry on the segment's own fsync, masking the
# omission — the "All File Systems Are Not Created Equal", OSDI '14 result; see
# §18). The behavioral power-loss form (scripts/m8/dm-flakey.sh dirfsync-negative)
# is a CLOSED, documented negative result: it does not reproduce on any Linux config
# tested (ext4/xfs/btrfs journal; journal-less ext4 incl. "ext2"-format via the ext4
# driver; journaled ext4 data=writeback) — so THIS Tier-1 check is what carries the
# §14.4d DoD row. It is the dir-fsync analogue of the H4 F_FULLFSYNC presence
# assertion: prove the syscall is ISSUED, not that the filesystem loses data without it.
#
# SCOPE / BOUNDARY (same as H4's F_FULLFSYNC-presence check): this proves the
# directory `fsync` is *issued on the directory fd* and that omitting fsync_dir is
# detectable. It does NOT prove the fd was opened with correct semantics, nor that
# the call's return value is checked, nor durability itself — that is issuance, not
# durability. Don't over-trust it beyond catching a removed/misdirected dir-fsync.
#
# Method: strace the roll path of `power_pull_workload` (tiny segments ⇒ several
# rolls) and assert, via strace -y fd→path annotation, that:
#   * the CORRECT build issues `fsync` on the WAL *directory* fd once per roll
#     (plus once at cold-start) — i.e. directory fsyncs scale with rolls; and
#   * the `inject_no_dir_fsync` build issues the cold-start dir-fsync ONLY (the
#     per-roll fsync_dir is compiled out), so it issues strictly fewer.
# Segment data syncs are `fdatasync` (sync_data) on `.wal` fds and are ignored.
# `fsync_dir` uses rustix raw syscalls, but strace traces at the syscall layer so
# it is caught regardless of libc.
#
# Needs ONLY strace — no dm-flakey, no privileges, no specific filesystem — so it
# runs per-PR (.github/workflows/ci.yml). Exit 0 = PASS, non-zero = FAIL.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

log() { printf '\033[1;34m[m8/dirfsync-presence]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[m8/dirfsync-presence] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

command -v strace >/dev/null 2>&1 || die "strace not installed (apt-get install -y strace)"

# Tiny segments ⇒ ~tens of records/segment ⇒ several rolls over 200 records.
SEG=4096
MAXREC=256
TOTAL=200
BATCH=4
PAYLOAD=64

# Build a workload variant, strace it through several rolls, and echo the count of
# `fsync` syscalls issued on the WAL *directory* fd. $1 = tag; $2.. = cargo flags.
trace_dir_fsyncs() {
  local tag="$1"; shift
  local bin="$WORK/wal_${tag}"
  ( cd "$REPO_ROOT" && cargo build --quiet --bin power_pull_workload "$@" )
  cp "$REPO_ROOT/target/debug/power_pull_workload" "$bin"

  local waldir="$WORK/waldir_${tag}"
  rm -rf "$waldir"; mkdir -p "$waldir"
  local rp; rp="$(realpath "$waldir")"
  local trace="$WORK/trace_${tag}.txt"

  # -y annotates each fd with its path: `fsync(7</abs/waldir>) = 0`. -f follows any
  # threads. The workload commits to a `stdout` sink (no fsync) ⇒ the only `fsync`
  # syscalls are fsync_dir's; segment data uses `fdatasync`.
  WAL_SEGMENT_SIZE="$SEG" WAL_MAX_RECORD_SIZE="$MAXREC" \
    strace -y -f -e trace=fsync,fdatasync -o "$trace" \
      "$bin" "$waldir" stdout "$TOTAL" "$BATCH" "$PAYLOAD" >/dev/null 2>&1 || true

  # Directory fsyncs: `fsync(<n></abs/waldir>)` — exact directory path, not a child.
  local dirf segf
  dirf="$(grep -cE "fsync\([0-9]+<${rp}>\)" "$trace" || true)"
  segf="$(grep -cE "fdatasync\([0-9]+<${rp}/[^>]*\.wal>\)" "$trace" || true)"
  log "${tag}: directory fsync=${dirf}  segment fdatasync=${segf}  (trace $(wc -l < "$trace") lines)"
  # Anti-vacuous: the workload must actually have synced segment data, else the
  # trace filter or the run is broken and a zero dir-fsync count is meaningless.
  [ "$segf" -ge 1 ] || die "${tag}: no segment fdatasync seen — workload/trace broken (cannot trust the dir-fsync count)"
  echo "$dirf"
}

log "Tier-1 §14.4d: proving the roll-time directory fsync (§7.4 step 5) is ISSUED."
log "building + tracing CORRECT build (dir-fsync present)…"
correct="$(trace_dir_fsyncs correct)"
log "building + tracing INJECT build (--features inject_no_dir_fsync)…"
inject="$(trace_dir_fsyncs inject --features inject_no_dir_fsync)"

# Assertions:
#  * inject build still issues the cold-start dir-fsync (unconditional in cold_start)
#    ⇒ ≥1; it must NOT issue the per-roll ones.
#  * correct build issues cold-start + one per roll ⇒ strictly more, and ≥2 (cold
#    start + ≥1 roll). `correct > inject` is the regression guard: deleting the
#    roll's fsync_dir collapses correct down to the cold-start count == inject.
fail=0
[ "$inject" -ge 1 ] || { log "FAIL: inject build issued ZERO directory fsyncs (expected the unconditional cold-start one)."; fail=1; }
[ "$correct" -ge 2 ] || { log "FAIL: correct build issued <2 directory fsyncs ($correct) — expected cold-start + ≥1 roll. Did the roll's fsync_dir regress?"; fail=1; }
[ "$correct" -gt "$inject" ] || { log "FAIL: correct ($correct) did not exceed inject ($inject) directory fsyncs — the per-roll fsync_dir (§7.4 step 5) is missing in the shipping build."; fail=1; }

[ "$fail" -eq 0 ] || die "§14.4d Tier-1 (dir-fsync presence) FAILED — see counts above."

log "PASS: correct=${correct} directory fsyncs (cold-start + per-roll) > inject=${inject} (cold-start only)."
log "The roll-time fsync_dir is issued and its omission is detectable — deterministic, FS-independent (§14.4d Tier 1)."
