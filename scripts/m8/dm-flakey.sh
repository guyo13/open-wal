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
EVIDENCE_DIR="${WAL_M8_EVIDENCE_DIR:-$WORK}"

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

# Emit a §5 evidence artifact for a gate (scripts/m8/evidence.sh). Best-effort:
# a missing python3 must not mask the gate verdict — only the artifact.
emit_evidence() {  # emit_evidence <tag> KEY=VALUE ...
  local tag="$1"; shift
  mkdir -p "$EVIDENCE_DIR"
  if "$REPO_ROOT/scripts/m8/evidence.sh" emit out="$EVIDENCE_DIR/evidence-${tag}.json" "$@"; then
    log "evidence: $EVIDENCE_DIR/evidence-${tag}.json"
  else
    log "WARNING: evidence artifact for '$tag' could not be written (verdict stands)."
  fi
}

cmd_check() {
  local ok=1
  command -v dmsetup >/dev/null 2>&1 || { log "missing: dmsetup"; ok=0; }
  [ -e /dev/mapper/control ] || { log "missing: /dev/mapper/control (device-mapper not in kernel)"; ok=0; }
  if command -v dmsetup >/dev/null 2>&1; then
    # MUST be `as_root`: `dmsetup targets` opens /dev/mapper/control, which needs
    # root. As an unprivileged user it fails with "Failure to communicate with
    # kernel device-mapper driver" and prints nothing — which would FALSELY look
    # like "flakey absent" even when the module is loaded (the hosted-CI footgun
    # that made the first dm-flakey run loud-skip while `sudo dmsetup targets` in
    # the provisioning step plainly showed `flakey`).
    as_root dmsetup targets 2>/dev/null | grep -q '^flakey' || { log "missing: dm-flakey target"; ok=0; }
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
  if [ -f "$WORK/loop" ]; then
    as_root losetup -d "$(cat "$WORK/loop")" 2>/dev/null || true
  fi
  rm -f "$WORK/loop" "$WORK/sectors"
  log "torn down"
}

# Scan the NEW kernel-log lines since $1 (a prior `dmesg | wc -l`) for a real
# block-layer I/O error — the SOURCE confirmation that an injected EIO actually hit
# the device (#16), not merely that the WAL reacted. Prints "1" if found, else "0".
_block_eio_since() {
  local pre="$1"
  as_root dmesg 2>/dev/null | tail -n "+$((pre + 1))" \
    | grep -qiE 'i/o error|buffer i/o error|blk_update_request.*error|critical (target|medium) error' \
    && echo 1 || echo 0
}

# One H3 attempt: run the workload while injecting error_writes mid-run, capturing
# the workload's stderr AND the kernel log so we can SOURCE-CONFIRM the EIO actually
# reached the device (not just infer it from the WAL's reaction — #16).
#   return 0  PASS         — rc==7 (poisoned) AND the poison line is present AND a
#                            real block-layer EIO was observed in the window: an
#                            injected EIO reached a commit's fdatasync (§12 upheld).
#   return 1  FAIL         — rc==0: the workload returned SUCCESS while writes were
#                            erroring ⇒ the WAL ignored a durability failure.
#   return 2  INCONCLUSIVE — the error window did not demonstrably hit a commit (no
#                            poison), OR the WAL poisoned but no block-layer EIO was
#                            observed (possible misattribution / unreadable dmesg)
#                            ⇒ retry; never counted as a pass.
# Sets H3_LAST_RC / H3_LAST_FIRED / H3_LAST_BLOCK_EIO / H3_LAST_DMESG_OK.
H3_LAST_RC=""
H3_LAST_FIRED=""
H3_LAST_BLOCK_EIO=""
H3_LAST_DMESG_OK=""
_h3_attempt() {
  local delay="$1"
  local wal="$MNT/h3wal" err="$WORK/h3.err"
  rm -rf "$wal"; mkdir -p "$wal"; : > "$err"

  # Snapshot the kernel-log length so we only scan lines produced in THIS window.
  # dmesg needs root; if it is unreadable we cannot source-confirm ⇒ INCONCLUSIVE.
  local pre dmesg_ok=1
  pre="$(as_root dmesg 2>/dev/null | wc -l)" || dmesg_ok=0
  if ! { [ -n "$pre" ] && [ "$pre" -ge 0 ] 2>/dev/null; }; then
    dmesg_ok=0; pre=0
  fi

  ( sleep "$delay"; flakey_fault error_writes ) &
  local inj=$!
  set +e
  WAL_SEGMENT_SIZE=65536 WAL_MAX_RECORD_SIZE=256 \
    "$REPO_ROOT/target/debug/power_pull_workload" "$wal" stdout 0 8 64 >/dev/null 2>"$err"
  local rc=$?
  set -e
  wait "$inj" 2>/dev/null || true

  local fired=0 block_eio=0
  grep -q "handle poisoned" "$err" && fired=1
  [ "$dmesg_ok" -eq 1 ] && block_eio="$(_block_eio_since "$pre")"

  flakey_up
  H3_LAST_RC="$rc"; H3_LAST_FIRED="$fired"
  H3_LAST_BLOCK_EIO="$block_eio"; H3_LAST_DMESG_OK="$dmesg_ok"

  # PASS requires BOTH halves ANDed: WAL poisoned AND a real block-layer EIO observed
  # in the window. Poison without an observed EIO ⇒ INCONCLUSIVE (misattribution
  # window closed), never a pass.
  if [ "$rc" -eq 7 ] && [ "$fired" -eq 1 ] && [ "$block_eio" -eq 1 ]; then return 0; fi
  if [ "$rc" -eq 0 ]; then return 1; fi
  return 2
}

# H3: physical fsync-failure poisons the handle (§12). dm-flakey errors writes at
# the BLOCK layer, so this also covers the rustix raw-syscall directory fsync the
# §12 LD_PRELOAD shim cannot. ANTI-VACUOUS (amended #16): a clean exit with no EIO
# actually injected is INCONCLUSIVE, never PASS — we assert the workload poisoned
# *because* an injected EIO reached a commit. Bounded retry absorbs a window that
# misses a commit (timing), without ever passing vacuously.
# Exit: 0 PASS · 1 FAIL · 2 INCONCLUSIVE · (3 ENV-UNAVAILABLE from `check`).
cmd_h3() {
  local fs="${1:-ext4}"
  setup "$fs"
  trap cmd_teardown EXIT
  ( cd "$REPO_ROOT" && cargo build --bin power_pull_workload >/dev/null 2>&1 )

  # Default 4 attempts: PASS now ANDs two conditions (WAL poison + observed block EIO),
  # so allow one extra window to absorb a dmesg/timing miss before declaring INCONCLUSIVE.
  local max="${WAL_M8_H3_ATTEMPTS:-4}" attempts=0 verdict="INCONCLUSIVE" rc_class
  local delays=(2 1 3 2)
  while [ "$attempts" -lt "$max" ]; do
    local delay="${delays[$attempts]:-2}"
    attempts=$((attempts + 1))
    log "H3 attempt ${attempts}/${max} (inject error_writes after ${delay}s)"
    set +e; _h3_attempt "$delay"; rc_class=$?; set -e
    if [ "$rc_class" -eq 0 ]; then verdict="PASS"; break; fi
    if [ "$rc_class" -eq 1 ]; then verdict="FAIL"; break; fi
    if [ "$H3_LAST_DMESG_OK" -eq 0 ]; then
      log "INCONCLUSIVE this attempt: kernel log UNREADABLE (need root dmesg) — cannot SOURCE-CONFIRM the block-layer EIO (#16); retrying."
    else
      log "INCONCLUSIVE this attempt (workload rc=${H3_LAST_RC}, poisoned=${H3_LAST_FIRED}, block_eio_observed=${H3_LAST_BLOCK_EIO}) — the error window did not land on a commit with a confirmed EIO; retrying."
    fi
  done

  local pass=0 fail=0 verdict_exit=2
  case "$verdict" in
    PASS) pass=1; verdict_exit=0
      log "PASS (H3): a real block-layer EIO was observed AND poisoned the handle (§12 upheld physically; source-confirmed, covers the dir fsync too)." ;;
    FAIL) fail=1; verdict_exit=1
      log "FAIL (H3): the workload returned SUCCESS while writes were erroring — the WAL did NOT poison on a durability failure (§12 violation)." ;;
    *)    verdict_exit=2
      log "INCONCLUSIVE (H3): could not land the error window on a commit in ${attempts} attempt(s). NOT a pass — re-run (timing-sensitive on this host)." ;;
  esac

  emit_evidence h3 \
    gate=H3-physical \
    "target.uname=$(uname -sr)" "target.host=$(hostname)" \
    "storage.fs=$fs" "storage.block_device=$DM_DEV" \
    "storage.write_cache=n/a (dm-flakey)" "storage.h2_probe=n/a (fault-injection, not power-loss)" \
    "cut.mechanism=dm-flakey error_writes" cut.valid=true \
    run.cycles_required=1 "run.cycles_pass=$pass" "run.fail=$fail" "run.inconclusive_rerun=$((attempts - 1))" \
    "detail.workload_rc=$H3_LAST_RC" "detail.injection_fired=$H3_LAST_FIRED" \
    "detail.block_layer_eio_observed=$H3_LAST_BLOCK_EIO" "detail.dmesg_readable=$H3_LAST_DMESG_OK" \
    "verdict=$verdict"

  return "$verdict_exit"
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

# POSITIVE CONTROL (amended #17): independently confirm dm-flakey `drop_writes`
# actually drops an UN-synced write on THIS host/run, using the same mechanism the
# negative control relies on. Without it, an exhausted retry budget cannot tell
# "timing didn't land" (benign INCONCLUSIVE) from "drop_writes never dropped anything"
# (a structurally dead negative control that certifies nothing while looking like
# flakiness). Write a marker but DO NOT sync it (it stays a dirty page, not yet a
# bio — dm suspend's flush touches in-flight bios, not page cache), enter drop_writes,
# then umount (forces writeback ⇒ bios ⇒ dropped); remount and check it is gone.
#   return 0  drop is functional (the un-synced marker vanished across the cut)
#   return 1  drop did NOT take effect (marker survived) ⇒ harness/env problem
_d44d_drop_positive_control() {
  local marker="$MNT/.m8_drop_probe"
  as_root rm -f "$marker" 2>/dev/null || true
  echo "m8-drop-probe" > "$marker"      # dirty page, intentionally NOT fsync'd
  flakey_fault drop_writes
  as_root umount "$MNT" 2>/dev/null || true
  flakey_up
  as_root mount "$DM_DEV" "$MNT"
  as_root chmod 0777 "$MNT"
  if [ -e "$marker" ]; then
    as_root rm -f "$marker" 2>/dev/null || true
    return 1                            # survived ⇒ drop_writes dropped nothing
  fi
  return 0
}

# §14.4d negative control: correct build MUST pass, inject build MUST fail.
# ANTI-VACUOUS (amended #17): the asymmetry is timing-sensitive, so we grant a
# bounded retry BUDGET (default 5) to reproduce it; a budget exhausted WITHOUT the
# asymmetry is INCONCLUSIVE (never a pass, never a code-red). A correct build that
# itself loses acked data is a genuine FAIL (red). The inject build merely not
# failing is INCONCLUSIVE, not "dir-fsync omission is harmless".
# A POSITIVE CONTROL runs first: if drop_writes doesn't actually drop here, the
# negative control is non-functional ⇒ exit 4 (HARNESS, louder than INCONCLUSIVE).
# Exit: 0 PASS · 1 FAIL · 2 INCONCLUSIVE · 3 ENV-UNAVAILABLE (`check`) · 4 HARNESS.
cmd_dirfsync_negative() {
  local fs="${1:-ext4}"
  setup "$fs"                     # cmd_check inside ⇒ loud OPEN + exit on a non-dm host
  trap cmd_teardown EXIT
  [ "$fs" = "ext4" ] || log "FS-DEPENDENCE: §14.4d is validated on ext4; on '$fs' a non-failure of the inject build may reflect FS metadata-journaling differences, NOT a working dir-fsync (see docs/m8-runbook.md)."

  printf '\033[1;33m%s\033[0m\n' "[m8/dm-flakey] §14.4d is timing-sensitive: the cut must land shortly after a roll, before the FS lazily writes back the new directory entry. A bounded retry budget reproduces it; an exhausted budget is INCONCLUSIVE, never self-certified green. Interpret per the FS caveat." >&2

  # POSITIVE CONTROL first: if drop_writes doesn't actually drop on this host, the
  # whole negative control is non-functional — that is a HARNESS/ENV problem (exit 4),
  # LOUDER than a benign timing INCONCLUSIVE, and must NOT be mistaken for "the inject
  # build didn't fail, so dir-fsync omission is harmless."
  log "positive control: confirming dm-flakey drop_writes drops an un-synced write on this host..."
  local drop_pc="pass"
  set +e; _d44d_drop_positive_control; local pc_rc=$?; set -e
  if [ "$pc_rc" -ne 0 ]; then
    drop_pc="fail"
    log "§14.4d POSITIVE CONTROL FAILED: an un-synced marker SURVIVED a drop_writes cut — drop_writes is NOT dropping writes here (mounted sync? wrong dm table? FS write-through?). The negative control is non-functional and certifies NOTHING on this runner. This is a harness/env problem, not a benign INCONCLUSIVE."
    emit_evidence 14.4d \
      gate=14.4d \
      "target.uname=$(uname -sr)" "target.host=$(hostname)" \
      "storage.fs=$fs" "storage.block_device=$DM_DEV" \
      "storage.write_cache=n/a (dm-flakey)" "storage.h2_probe=n/a (fault-injection, not power-loss)" \
      "cut.mechanism=dm-flakey drop_writes" cut.valid=false \
      run.cycles_required=1 run.cycles_pass=0 run.fail=0 run.attempts_used=0 \
      "detail.drop_positive_control=fail" \
      "verdict=HARNESS_FAIL"
    return 4
  fi
  log "positive control OK: drop_writes drops un-synced writes on this host — the negative control is live."

  local budget="${WAL_M8_D44D_BUDGET:-5}" attempt=0 verdict="INCONCLUSIVE"
  local rc_correct=-1 rc_inject=-1
  while [ "$attempt" -lt "$budget" ]; do
    attempt=$((attempt + 1))
    log "=== §14.4d attempt ${attempt}/${budget} ==="
    log "--- correct build (dir-fsync present) — expect PASS (post-roll records survive) ---"
    set +e; _d44d_run_one correct; rc_correct=$?; set -e
    log "--- inject build (--features inject_no_dir_fsync) — expect FAIL (rolled segment filename lost) ---"
    set +e; _d44d_run_one inject --features inject_no_dir_fsync; rc_inject=$?; set -e
    log "attempt ${attempt}: correct rc=${rc_correct} (0=PASS)  |  inject rc=${rc_inject} (1=FAIL expected)"
    if [ "$rc_correct" -eq 0 ] && [ "$rc_inject" -eq 1 ]; then
      verdict="PASS"; break
    fi
    if [ "$rc_correct" -eq 1 ]; then
      # The CORRECT build lost acked post-roll data ⇒ a real regression, not timing.
      verdict="FAIL"; break
    fi
    log "asymmetry not reproduced this attempt (timing/FS-sensitive) — retrying."
  done

  local pass=0 fail=0 verdict_exit=2
  case "$verdict" in
    PASS) pass=1; verdict_exit=0
      log "§14.4d NEGATIVE CONTROL DEMONSTRATED in ${attempt} attempt(s): correct PASS, inject FAIL. The dir-fsync is necessary and the harness catches its omission." ;;
    FAIL) fail=1; verdict_exit=1
      log "§14.4d FAIL: the CORRECT build lost acked post-roll data (correct rc=${rc_correct}). That is a real durability regression, not a timing artefact." ;;
    *)    verdict_exit=2
      log "§14.4d INCONCLUSIVE after ${attempt} attempts (correct=${rc_correct} inject=${rc_inject}). Timing/FS-sensitive — NOT a pass, NOT a code failure. Prefer ext4; tune the cut timing (runbook). NEVER read a non-failing inject build as 'dir-fsync omission is harmless'." ;;
  esac

  emit_evidence 14.4d \
    gate=14.4d \
    "target.uname=$(uname -sr)" "target.host=$(hostname)" \
    "storage.fs=$fs" "storage.block_device=$DM_DEV" \
    "storage.write_cache=n/a (dm-flakey)" "storage.h2_probe=n/a (fault-injection, not power-loss)" \
    "cut.mechanism=dm-flakey drop_writes" cut.valid=true \
    run.cycles_required=1 "run.cycles_pass=$pass" "run.fail=$fail" \
    "run.attempts_used=$attempt" "run.inconclusive_rerun=$((attempt - 1))" \
    "detail.correct_rc=$rc_correct" "detail.inject_rc=$rc_inject" \
    "detail.drop_positive_control=$drop_pc" \
    "verdict=$verdict"

  return "$verdict_exit"
}

case "${1:-check}" in
  check)             cmd_check ;;
  h3)                shift; cmd_h3 "${1:-ext4}" ;;
  dirfsync-negative) shift; cmd_dirfsync_negative "${1:-ext4}" ;;
  teardown)          cmd_teardown ;;
  *)                 die "usage: $0 {check|h3 [fs]|dirfsync-negative [fs]|teardown}" ;;
esac
