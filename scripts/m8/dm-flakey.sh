#!/usr/bin/env bash
#
# dm-flakey.sh — M8 / §14.8 H3 (physical fsync-failure) + §14.4d (dir-fsync
# omission negative control). OWNER-RUN on a privileged Linux host with
# device-mapper + the dm-flakey target.
#
# WHY DM-FLAKEY (and not the §12 LD_PRELOAD shim). The shim (scripts/m8/
# fsync-fault.sh) intercepts the libc `fdatasync` symbol only and returns a *fake*
# EIO — it proves the §12 poison STATE MACHINE but not physical durability, and it
# cannot touch the WAL's directory fsync (rustix raw syscall) nor model the loss of
# an un-synced directory entry. dm-flakey injects at the BLOCK LAYER, so it:
#   - H3: errors real writes/flushes (catches the dir fsync too) ⇒ physical
#     fsync-failure → poison (§12);
#   - §14.4d: drops un-synced writes across a simulated power loss ⇒ a dir-fsync-
#     omitting build (`--features inject_no_dir_fsync`) loses the rolled segment's
#     filename and MUST fail recovery, while the correct build MUST pass.
#
# THIS HOST: if device-mapper / dm-flakey is unavailable, `check` prints a loud
# OPEN banner and exits non-zero. We NEVER fake green — exactly like the LazyFS
# gate's "NOT EXERCISED" stopgap. (Probed: this sandbox's kernel has no
# CONFIG_BLK_DEV_DM, so these run OPEN-pending-owner-hardware.)
#
# Subcommands:
#   check                 detect dm-flakey; loud OPEN banner if absent.
#   h3       [fs]         physical fsync-failure → poison (default fs=ext4).
#   dirfsync-negative [fs] §14.4d: correct build passes, inject build FAILS.
#   teardown              unmount + remove the dm device + loop.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="${WAL_M8_DM_WORK:-/tmp/open-wal-dm}"
IMG="${WORK}/backing.img"
IMG_SIZE_MB="${WAL_M8_DM_IMG_MB:-256}"
DM_NAME="open_wal_flakey"
DM_DEV="/dev/mapper/${DM_NAME}"
MNT="${WORK}/mnt"

log()  { printf '\033[1;34m[m8/dm-flakey]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[m8/dm-flakey] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }
open_banner() {
  printf '\033[1;33m' >&2
  cat >&2 <<EOF
================================================================
§14.8 H3 (physical) / §14.4d NEGATIVE CONTROL — NOT EXERCISED.
device-mapper / dm-flakey is unavailable on this host, so these
gates CANNOT run here. They are OPEN-pending-owner-hardware.
Run on a privileged Linux host with CONFIG_BLK_DEV_DM + dm-flakey
(or use a real power-pull, scripts/m8/power-pull.sh). Never marked
green from a sandbox. See docs/m8-runbook.md.
================================================================
EOF
  printf '\033[0m' >&2
}

as_root() { if [ "$(id -u)" -eq 0 ]; then "$@"; else sudo "$@"; fi; }

cmd_check() {
  local ok=1
  command -v dmsetup >/dev/null 2>&1 || { log "missing: dmsetup"; ok=0; }
  [ -e /dev/mapper/control ] || { log "missing: /dev/mapper/control (device-mapper not in kernel)"; ok=0; }
  if command -v dmsetup >/dev/null 2>&1; then
    dmsetup targets 2>/dev/null | grep -q '^flakey' || { log "missing: dm-flakey target"; ok=0; }
  fi
  if [ "$ok" -ne 1 ]; then
    open_banner
    exit 3
  fi
  log "device-mapper + dm-flakey present — H3/§14.4d CAN run on this host."
}

# Build a loop-backed ext4 (or xfs/btrfs) on top of a dm-flakey device that is in
# normal "up" mode. Returns with $DM_DEV mounted at $MNT.
setup() {
  local fs="${1:-ext4}"
  cmd_check
  command -v "mkfs.${fs}" >/dev/null 2>&1 || die "mkfs.${fs} not installed"
  mkdir -p "$WORK" "$MNT"
  [ -f "$IMG" ] || dd if=/dev/zero of="$IMG" bs=1M count="$IMG_SIZE_MB" status=none
  local loop
  loop="$(as_root losetup -f --show "$IMG")"
  echo "$loop" > "$WORK/loop"
  local sectors
  sectors="$(as_root blockdev --getsz "$loop")"
  # Normal operation: up forever (down 0 ⇒ never drops/errors).
  as_root dmsetup create "$DM_NAME" --table "0 $sectors flakey $loop 0 1 0"
  as_root "mkfs.${fs}" -q "$DM_DEV"
  as_root mount "$DM_DEV" "$MNT"
  as_root chmod 0777 "$MNT"
  log "ready: ${fs} on $DM_DEV mounted at $MNT (backing $loop)"
  echo "$sectors" > "$WORK/sectors"
}

# Reload the dm table into a fault mode, on demand (suspend/load/resume).
#   mode=error_writes  -> writes/flushes return EIO (H3 fsync-failure)
#   mode=drop_writes   -> writes are silently dropped (un-synced data "lost")
flakey_fault() {
  local mode="$1" loop sectors
  loop="$(cat "$WORK/loop")"; sectors="$(cat "$WORK/sectors")"
  as_root dmsetup suspend "$DM_NAME"
  # up 0 / down 60 ⇒ immediately and continuously in the down state for 60s.
  as_root dmsetup load "$DM_NAME" --table "0 $sectors flakey $loop 0 0 60 1 $mode"
  as_root dmsetup resume "$DM_NAME"
  log "dm-flakey now in '$mode' mode"
}

flakey_up() {
  local loop sectors
  loop="$(cat "$WORK/loop")"; sectors="$(cat "$WORK/sectors")"
  as_root dmsetup suspend "$DM_NAME"
  as_root dmsetup load "$DM_NAME" --table "0 $sectors flakey $loop 0 1 0"
  as_root dmsetup resume "$DM_NAME"
}

cmd_teardown() {
  as_root umount "$MNT" 2>/dev/null || true
  as_root dmsetup remove "$DM_NAME" 2>/dev/null || true
  [ -f "$WORK/loop" ] && as_root losetup -d "$(cat "$WORK/loop")" 2>/dev/null || true
  rm -f "$WORK/loop" "$WORK/sectors"
  log "torn down"
}

# H3: physical fsync-failure poisons the handle (§12). Run a workload; mid-run
# flip the device to error_writes; the next commit's fdatasync gets EIO ⇒ the
# workload exits 7 (poisoned). dm-flakey hits the block layer, so this also covers
# the rustix raw-syscall directory fsync the §12 shim cannot.
cmd_h3() {
  local fs="${1:-ext4}"
  setup "$fs"
  trap cmd_teardown EXIT
  ( cd "$REPO_ROOT" && cargo build --bin power_pull_workload >/dev/null 2>&1 )
  local wal="$MNT/h3wal"; mkdir -p "$wal"
  log "running workload; will inject error_writes after 2s"
  ( sleep 2; flakey_fault error_writes ) &
  set +e
  WAL_SEGMENT_SIZE=65536 WAL_MAX_RECORD_SIZE=256 \
    "$REPO_ROOT/target/debug/power_pull_workload" "$wal" stdout 0 8 64 >/dev/null 2>&1
  local rc=$?
  set -e
  flakey_up
  if [ "$rc" -eq 7 ]; then
    log "PASS (H3): a failed fdatasync poisoned the handle (exit 7) — §12 upheld at the block layer."
  else
    die "H3 FAIL/INCONCLUSIVE: workload exited $rc (expected 7 = poisoned). Did the error window land on a commit?"
  fi
}

# §14.4d: the dir-fsync omission negative control. With tiny segments the workload
# rolls frequently. We drop un-synced writes across a simulated power loss, then
# recover. The CORRECT build's dir-fsync forced each new segment's filename to disk
# before the drop window, so recovery keeps the post-roll records; the INJECT build
# (--features inject_no_dir_fsync) skipped that dir-fsync, so the rolled segment's
# filename can be lost and recovery MUST fail (or lose acked post-roll records).
#
# FS-DEPENDENCE CAVEAT: this is most reliably reproducible on ext4. On xfs/btrfs the
# inject build may NOT fail under dm-flakey due to their metadata-ordering/journaling
# semantics — a non-failure there reflects FS differences, NOT a working dir-fsync.
# Do not read "inject build didn't fail on btrfs" as "dir-fsync omission is harmless."
# Run one build of the workload through a roll + simulated power loss, then verify.
# Echoes the verifier's exit code (0 PASS / 1 FAIL / 2 INCONCLUSIVE).
_d44d_run_one() {
  local tag="$1"; shift          # "correct" | "inject"
  local feat=("$@")              # extra cargo flags (e.g. --features inject_no_dir_fsync)
  local wal="$MNT/d44d_${tag}"
  local cap="$WORK/cap_${tag}.txt"   # capture lives on $WORK (/tmp) — OFF the dm device
  rm -rf "$wal" "$cap"; mkdir -p "$wal"; : > "$cap"

  ( cd "$REPO_ROOT" && cargo build --bin power_pull_workload "${feat[@]}" >/dev/null 2>&1 )

  # Tiny segments ⇒ frequent rolls. Bounded run so it finishes before the cut.
  # The capture (file sink) is fsync'd to /tmp; the WAL is on the dm device.
  WAL_SEGMENT_SIZE=4096 WAL_MAX_RECORD_SIZE=256 \
    "$REPO_ROOT/target/debug/power_pull_workload" "$wal" "file:$cap" 2000 4 64 >/dev/null 2>&1 || true

  # Simulate power loss: drop any not-yet-written-back data, then drop the page
  # cache by unmounting, then "reboot" by remounting in up mode and recover.
  flakey_fault drop_writes
  as_root umount "$MNT" 2>/dev/null || true
  flakey_up
  as_root mount "$DM_DEV" "$MNT"

  ( cd "$REPO_ROOT" && cargo build --bin power_pull_verify >/dev/null 2>&1 )
  WAL_SEGMENT_SIZE=4096 WAL_MAX_RECORD_SIZE=256 \
    "$REPO_ROOT/target/debug/power_pull_verify" "$wal" "$cap" >&2
  return $?
}

# §14.4d negative control: correct build MUST pass, inject build MUST fail.
cmd_dirfsync_negative() {
  local fs="${1:-ext4}"
  setup "$fs"                     # cmd_check inside ⇒ loud OPEN + exit on a non-dm host
  trap cmd_teardown EXIT
  [ "$fs" = "ext4" ] || log "FS-DEPENDENCE: §14.4d is validated on ext4; on '$fs' a non-failure of the inject build may reflect FS metadata-journaling differences, NOT a working dir-fsync (see docs/m8-runbook.md)."

  printf '\033[1;33m%s\033[0m\n' "[m8/dm-flakey] §14.4d is timing-sensitive and OWNER-VALIDATED: the cut must land shortly after a roll, before the FS lazily writes back the new directory entry. Tune the workload bound / cut timing per host; interpret per the FS caveat. This is OPEN-pending-owner-run, never self-certified green." >&2

  log "=== correct build (dir-fsync present) — expect PASS (post-roll records survive) ==="
  set +e; _d44d_run_one correct; local rc_correct=$?; set -e

  log "=== inject build (--features inject_no_dir_fsync) — expect FAIL (rolled segment filename lost) ==="
  set +e; _d44d_run_one inject --features inject_no_dir_fsync; local rc_inject=$?; set -e

  log "----------------------------------------------------------------"
  log "correct build verify rc=$rc_correct (0=PASS)  |  inject build verify rc=$rc_inject (1=FAIL expected)"
  if [ "$rc_correct" -eq 0 ] && [ "$rc_inject" -eq 1 ]; then
    log "§14.4d NEGATIVE CONTROL DEMONSTRATED: correct build passed, inject build lost acked post-roll data. The dir-fsync is necessary and the harness catches its omission."
  else
    log "§14.4d INCONCLUSIVE on this host/run: the asymmetry did not reproduce (correct=$rc_correct inject=$rc_inject). This is expected to be timing/FS-sensitive — tune the cut timing (see runbook) and prefer ext4. Do NOT read a non-failing inject build as 'dir-fsync omission is harmless'."
  fi
}

case "${1:-check}" in
  check)             cmd_check ;;
  h3)                shift; cmd_h3 "${1:-ext4}" ;;
  dirfsync-negative) shift; cmd_dirfsync_negative "${1:-ext4}" ;;
  teardown)          cmd_teardown ;;
  *)                 die "usage: $0 {check|h3 [fs]|dirfsync-negative [fs]|teardown}" ;;
esac
