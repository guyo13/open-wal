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

# Resolve a usable `cargo` even when this script is invoked via `sudo` — which drops
# the invoking user's ~/.cargo/bin from root's PATH, the cause of "cargo: command not
# found" when run as `sudo scripts/m8/dm-flakey.sh …` (rustup installs cargo for the
# user, not root). Prefer cargo on PATH (the CI case: non-root user, only dmsetup/
# mount need sudo); otherwise build AS the invoking user with their rustup toolchain,
# so artifacts land in $REPO_ROOT/target (owned by them) and root just execs them.
# Run from a `( cd "$REPO_ROOT" && _cargo … )` subshell — sudo preserves the cwd.
_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    cargo "$@"
    return
  fi
  if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ]; then
    local uhome
    uhome="$(getent passwd "$SUDO_USER" 2>/dev/null | cut -d: -f6)"
    [ -n "$uhome" ] || uhome="/home/$SUDO_USER"
    sudo -u "$SUDO_USER" env "HOME=$uhome" \
      "PATH=$uhome/.cargo/bin:/usr/local/bin:/usr/bin:/bin" \
      cargo "$@"
    return
  fi
  echo "cargo not found and no SUDO_USER to build as — install Rust or run as a user with cargo on PATH" >&2
  return 127
}

# Emit a §5 evidence artifact for a gate (scripts/m8/evidence.sh). A gate's PASS
# MUST be backed by a ledger artifact (#15) — so this NEVER leaves a gate without
# a file: if the emitter fails (e.g. transient python3 issue — the cause of the #16
# evidence-h3.json gap in run 28193051238, which the H3 args reproduce green
# locally), it surfaces the emitter's stderr loudly AND writes a minimal fallback
# JSON so the ledger always has *something* for the gate. The verdict still stands
# regardless — the artifact is downstream of it.
emit_evidence() {  # emit_evidence <tag> KEY=VALUE ...
  local tag="$1"; shift
  mkdir -p "$EVIDENCE_DIR"
  local out="$EVIDENCE_DIR/evidence-${tag}.json" err
  if err="$("$REPO_ROOT/scripts/m8/evidence.sh" emit out="$out" "$@" 2>&1)"; then
    log "evidence: $out"
  else
    log "WARNING: evidence.sh FAILED for '$tag' (verdict stands). Emitter output:"
    printf '%s\n' "$err" >&2
    # Fallback so the gate is never artifact-less (#15). Pull a verdict=... arg if
    # present so the fallback still records the outcome.
    local v="UNKNOWN" kv
    for kv in "$@"; do case "$kv" in verdict=*) v="${kv#verdict=}" ;; esac; done
    if printf '{"gate":"%s","verdict":"%s","note":"evidence.sh emit failed; fallback artifact — see step log"}\n' \
      "$tag" "$v" > "$out" 2>/dev/null; then
      log "wrote fallback evidence: $out"
    else
      log "ERROR: could not even write the fallback artifact to $out"
    fi
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

# Let udev finish its block-device bookkeeping before/after create+mkfs+remove.
# udevd grabs short-lived locks on a just-formatted/unmounted dm/loop device, the
# source of the "Device or resource busy" seen running filesystems back-to-back.
_udev_settle() { as_root udevadm settle >/dev/null 2>&1 || true; }

# After a simulated power loss on a JOURNAL-LESS fs (ext2), the unclean mount can be
# refused or flag corruption, and orphan inodes (data blocks present, directory
# entry dropped) need clearing. Run `fsck -y` before remounting so the mount is
# clean and the dropped dirent is reflected as a vanished file (orphan → lost+found
# → not in the WAL's readdir). Journaling fs's (ext4/xfs/btrfs) recover on mount, so
# this is a deliberate no-op for them. `M8_FS` is set by cmd_dirfsync_negative.
_post_cut_fsck() {
  case "${M8_FS:-}" in
    ext2 | ext3)    as_root "fsck.${M8_FS}" -y "$DM_DEV" >/dev/null 2>&1 || true ;;
    ext4-writeback) as_root fsck.ext4 -y "$DM_DEV" >/dev/null 2>&1 || true ;;
  esac
}

# Resolve the mkfs binary, mkfs flags, and mount options for a filesystem token.
# Most tokens map to `mkfs.<fs>`; the pseudo-token `ext4-writeback` is the §14.4d
# bounded attempt suggested by the designer: a JOURNAL-LESS ext4 (`-O ^has_journal`)
# mounted `data=writeback` — the ext4 driver's *weakest* ordering, which the kernel
# docs warn "can leave stale data exposed … in case of an unclean shutdown" (exactly
# this class of bug). NOTE on "ext2": the standalone ext2 driver was removed in Linux
# 6.9, so on modern kernels `mount -t ext2` is serviced by the ext4 subsystem
# (journal-less) — a real ext2 *driver* is not exercised; that is why we lever the
# ext4 driver's ordering directly here instead of chasing ext2.
_fs_spec() {  # _fs_spec <fs>; sets MKFS_BIN, MKFS_EXTRA[], MOUNT_OPTS[]
  MKFS_EXTRA=(); MOUNT_OPTS=()
  case "$1" in
    # NOTE: data=writeback is a JOURNAL data mode — it REQUIRES a journal, so it is
    # NOT combinable with `-O ^has_journal` (that combo fails to mount: "bad
    # option/superblock"). The ext4 driver's weakest *ordering* is therefore a
    # journaled ext4 mounted data=writeback (metadata journaled, data unordered —
    # kernel docs: "can leave stale data exposed on unclean shutdown"). Journal-less
    # ext4 is the separate "ext2"-format case already shown to mask the omission.
    ext4-writeback) MKFS_BIN=mkfs.ext4; MOUNT_OPTS=(-o data=writeback) ;;
    *)              MKFS_BIN="mkfs.$1" ;;
  esac
}

# Build a loop-backed filesystem on top of a dm-flakey device that is in normal "up"
# mode. Returns with $DM_DEV mounted at $MNT. Accepts ext4/ext2/xfs/btrfs and the
# pseudo-token ext4-writeback (see _fs_spec).
setup() {
  local fs="${1:-ext4}"
  cmd_check
  local MKFS_BIN; local -a MKFS_EXTRA MOUNT_OPTS
  _fs_spec "$fs"
  command -v "$MKFS_BIN" >/dev/null 2>&1 || die "$MKFS_BIN not installed"

  # Reclaim anything a crashed prior run (or a still-settling udev hold) left
  # behind, so `dmsetup create` cannot fail "Device or resource busy" — the error
  # seen running fs's back-to-back in one CI step.
  as_root dmsetup remove "$DM_NAME" 2>/dev/null \
    || as_root dmsetup remove -f --deferred "$DM_NAME" 2>/dev/null || true
  _udev_settle

  mkdir -p "$WORK" "$MNT"
  [ -f "$IMG" ] || dd if=/dev/zero of="$IMG" bs=1M count="$IMG_SIZE_MB" status=none
  local loop
  loop="$(as_root losetup -f --show "$IMG")"
  echo "$loop" > "$WORK/loop"
  local sectors
  sectors="$(as_root blockdev --getsz "$loop")"
  # Normal operation: up forever (down 0 ⇒ never drops/errors).
  as_root dmsetup create "$DM_NAME" --table "0 $sectors flakey $loop 0 1 0"
  _udev_settle
  # Zero the first 16 MiB so any prior fs's superblock/signature is gone — the
  # backing image is reused across fs's, and mkfs.xfs/btrfs refuse to overwrite a
  # detected fs (the "appears to contain an existing filesystem (ext4)" failure),
  # while mke2fs would otherwise need -F. Zeroing is FS-agnostic and bulletproof.
  as_root dd if=/dev/zero of="$DM_DEV" bs=1M count=16 status=none 2>/dev/null || true
  as_root "$MKFS_BIN" "${MKFS_EXTRA[@]}" "$DM_DEV" >/dev/null 2>&1 || die "$MKFS_BIN ${MKFS_EXTRA[*]} failed on $DM_DEV"
  as_root mount "${MOUNT_OPTS[@]}" "$DM_DEV" "$MNT"
  as_root chmod 0777 "$MNT"
  log "ready: ${fs} (${MKFS_BIN} ${MKFS_EXTRA[*]}; mount ${MOUNT_OPTS[*]:-defaults}) on $DM_DEV at $MNT (backing $loop)"
  echo "$sectors" > "$WORK/sectors"
}

# Reload the dm table into a fault mode, on demand (suspend/load/resume).
#   mode=error_writes  -> writes/flushes return EIO (H3 fsync-failure)
#   mode=drop_writes   -> writes are silently dropped (un-synced data "lost")
flakey_fault() {
  local mode="$1" loop sectors
  loop="$(cat "$WORK/loop")"; sectors="$(cat "$WORK/sectors")"
  # CRITICAL: --noflush --nolockfs when ENTERING the fault mode. The table is
  # still in UP mode at suspend time (the fault table loads on the next line), so
  # a default `dmsetup suspend` would FLUSH in-flight I/O and FREEZE the
  # filesystem (lockfs ⇒ a full sync) — persisting the very un-synced data we are
  # about to drop, to the backing store, BEFORE drop_writes is active. That is
  # exactly why the §14.4d positive control failed ("un-synced marker SURVIVED a
  # drop_writes cut") on hosted CI AND on real hardware: the freeze-sync wrote the
  # dirty marker out before the cut. It also silently defeats the negative control
  # (the inject build's un-synced dir entry gets persisted too ⇒ no asymmetry).
  # Skipping the flush + freeze leaves the un-synced data dirty so the subsequent
  # umount writeback hits the drop_writes target and is genuinely lost.
  as_root dmsetup suspend --noflush --nolockfs "$DM_NAME"
  # up 0 / down 60 ⇒ immediately and continuously in the down state for 60s.
  as_root dmsetup load "$DM_NAME" --table "0 $sectors flakey $loop 0 0 60 1 $mode"
  as_root dmsetup resume "$DM_NAME"
  log "dm-flakey now in '$mode' mode"
}

flakey_up() {
  local loop sectors
  loop="$(cat "$WORK/loop")"; sectors="$(cat "$WORK/sectors")"
  # CRITICAL: --noflush --nolockfs. flakey_up is called while the device is in a
  # FAULT mode (error_writes/drop_writes). A plain `dmsetup suspend` flushes
  # outstanding I/O and freezes the filesystem (an implicit sync) — both issue
  # writes/flushes THROUGH the erroring target, so the suspend ioctl itself
  # returns EIO ("suspend ioctl ... failed: Input/output error", observed on
  # hosted CI). Skipping the flush + fs-freeze makes the table reload back to
  # normal safe; any not-yet-written pages are written after resume (in up mode).
  as_root dmsetup suspend --noflush --nolockfs "$DM_NAME"
  as_root dmsetup load "$DM_NAME" --table "0 $sectors flakey $loop 0 1 0"
  as_root dmsetup resume "$DM_NAME"
}

cmd_teardown() {
  as_root umount "$MNT" 2>/dev/null || true
  _udev_settle
  # Retry the dm remove: udev can briefly hold the just-unmounted device, so a
  # single remove sometimes fails "Device or resource busy". Fall back to a
  # deferred remove, settle, and retry a few times before giving up.
  local attempt
  for attempt in 1 2 3 4 5; do
    as_root dmsetup remove "$DM_NAME" 2>/dev/null && break
    as_root dmsetup remove -f --deferred "$DM_NAME" 2>/dev/null && break
    log "teardown: dm remove busy (attempt ${attempt}/5) — settling and retrying"
    sleep 0.3; _udev_settle
  done
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

  # Record the verdict inputs BEFORE restoring the device, so a (now-unexpected)
  # flakey_up hiccup cannot abort the function and be misread as a durability FAIL
  # — the bug that made the first real run report a false §12 violation. flakey_up
  # is best-effort here (teardown reclaims the device regardless); `|| log` keeps
  # set -e from aborting on it.
  H3_LAST_RC="$rc"; H3_LAST_FIRED="$fired"
  H3_LAST_BLOCK_EIO="$block_eio"; H3_LAST_DMESG_OK="$dmesg_ok"
  flakey_up || log "WARN: flakey_up did not restore the device (teardown will reclaim it); this attempt's verdict already recorded."

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
  ( cd "$REPO_ROOT" && _cargo build --bin power_pull_workload >/dev/null 2>&1 )

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

# §14.4d: the dir-fsync omission negative control, via a SYNCHRONIZED MID-RUN CUT.
# The CORRECT build's roll-time dir-fsync forced the new segment's filename to disk;
# the INJECT build (--features inject_no_dir_fsync) skipped it, so on a journal-less
# fs that segment's directory entry can be lost ⇒ recovery's readdir misses it ⇒ the
# acked post-roll records vanish (a D1 violation the verifier flags) ⇒ inject FAIL.
#
# WHY SYNCHRONIZED (not run-to-completion). A workload that runs to completion and is
# cut afterwards CANNOT show the omission even on ext2: by the cut, every rolled
# dirent has been written back (observed: PR #21 run 28193051238 — positive control
# LIVE, asymmetry still absent). The `dirfsync_cut_workload` bin instead rolls ONCE,
# puts an acked record in the brand-new (still-dirty-dirent) segment, signals ready
# OFF-device, and BLOCKS — so we activate drop_writes and cut INSIDE the un-synced
# window (default dirty_expire ⇒ ~30s slack; the cut is unhurried, not a sub-ms race).
#
# FS-DEPENDENCE (corrected, PR #21): the behavioral asymmetry has NOT been
# demonstrated on any config tested. ext4/xfs/btrfs mask it via the journal; an
# "ext2"-format volume is, on modern kernels (standalone ext2 driver removed in
# Linux 6.9), serviced by the EXT4 DRIVER journal-less and masks it too via the ext4
# driver's metadata/writeback — NOT any ext2 mechanism (the dmesg confirms "mounting
# ext2 file system using the ext4 subsystem"). The `ext4-writeback` token is the last
# bounded attempt (journal-less ext4 + data=writeback, the weakest ordering); if it
# also doesn't reproduce, §14.4d's behavioral form is a documented negative result.
# The DETERMINISTIC, FS-independent guard is the Tier-1 strace presence check
# (scripts/m8/dirfsync-presence.sh, per-PR). The `dirfsync_cut_workload` bin rolls
# ONCE, puts an acked record in the brand-new (still-dirty-dirent) segment, signals
# ready OFF-device, and BLOCKS — so the cut lands INSIDE the un-synced window.
# Echoes 0 PASS / 1 FAIL / 2 INCONCLUSIVE (incl. "the cut was missed").
_d44d_run_one() {
  local tag="$1"; shift          # "correct" | "inject"
  local feat=("$@")              # extra cargo flags (e.g. --features inject_no_dir_fsync)
  local wal="$MNT/d44d_${tag}"
  local cap="$WORK/cap_${tag}.txt"      # side channel — OFF the dm device (/tmp)
  local ready="$WORK/ready_${tag}.txt"  # roll-ready signal — OFF the dm device (/tmp)
  rm -rf "$wal" "$cap" "$ready"; mkdir -p "$wal"

  # Build the workload + verifier CHECKED — a silent build failure would otherwise
  # look identical to "workload exited before the roll signal" (missing binary ⇒
  # immediate exec failure). Surface a build break loudly and distinctly.
  local bl="$WORK/build_${tag}.log"
  if ! ( cd "$REPO_ROOT" && _cargo build --bin dirfsync_cut_workload "${feat[@]}" ) >"$bl" 2>&1; then
    log "§14.4d/${tag}: BUILD FAILED for dirfsync_cut_workload ${feat[*]} — see below (HARNESS, not a gate result):"
    cat "$bl" >&2
    return 2
  fi
  ( cd "$REPO_ROOT" && _cargo build --bin power_pull_verify ) >>"$bl" 2>&1 || { log "§14.4d/${tag}: BUILD FAILED for power_pull_verify:"; cat "$bl" >&2; return 2; }

  # Launch the synchronized-cut workload. Its stderr is CAPTURED (not /dev/null'd) so
  # that if it dies before signalling we can show exactly why (a panic, a config
  # rejection, an ext2-specific error) instead of a blind INCONCLUSIVE.
  local werr="$WORK/workload_${tag}.err"; : > "$werr"
  WAL_SEGMENT_SIZE=4096 WAL_MAX_RECORD_SIZE=256 \
    "$REPO_ROOT/target/debug/dirfsync_cut_workload" "$wal" "$cap" "$ready" >/dev/null 2>"$werr" &
  local wpid=$!

  # Wait (bounded, 10s) for the ready signal: the roll has happened and the new
  # segment's dirent is un-synced. If the workload exits first, show its stderr.
  local waited=0
  while [ ! -s "$ready" ]; do
    if ! kill -0 "$wpid" 2>/dev/null; then
      wait "$wpid" 2>/dev/null; local wrc=$?
      log "§14.4d/${tag}: workload exited (rc=${wrc}) before the roll signal — INCONCLUSIVE. Workload stderr:"
      cat "$werr" >&2
      return 2
    fi
    sleep 0.1; waited=$((waited + 1))
    if [ "$waited" -ge 100 ]; then
      kill -9 "$wpid" 2>/dev/null || true; wait "$wpid" 2>/dev/null || true
      log "§14.4d/${tag}: no ready signal within 10s — INCONCLUSIVE. Workload stderr so far:"
      cat "$werr" >&2
      return 2
    fi
  done

  # THE CUT (ordering is load-bearing): activate drop_writes BEFORE reaping the
  # process / unmounting, so no writeback of the dirty dirent can beat the cut. Then
  # kill the blocked workload (releases the mount), umount (writeback dropped),
  # flakey_up, fsck the journal-less fs, remount, and verify.
  flakey_fault drop_writes
  kill -9 "$wpid" 2>/dev/null || true; wait "$wpid" 2>/dev/null || true
  as_root umount "$MNT" 2>/dev/null || as_root umount -l "$MNT" 2>/dev/null || true
  flakey_up
  _post_cut_fsck            # journal-less (ext2/ext4-writeback) needs an fsck first
  as_root mount "$DM_DEV" "$MNT"

  WAL_SEGMENT_SIZE=4096 WAL_MAX_RECORD_SIZE=256 \
    "$REPO_ROOT/target/debug/power_pull_verify" "$wal" "$cap" >&2
  return $?
}

# POSITIVE CONTROL (amended #17): independently confirm dm-flakey `drop_writes`
# actually drops an UN-synced write on THIS host/run, using the same mechanism the
# negative control relies on. Without it, an exhausted retry budget cannot tell
# "timing didn't land" (benign INCONCLUSIVE) from "drop_writes never dropped anything"
# (a structurally dead negative control that certifies nothing while looking like
# flakiness). Write a marker but DO NOT sync it (it stays a dirty page). Entering
# drop_writes via `flakey_fault` now suspends with --noflush --nolockfs, so the
# transition neither flushes nor freeze-syncs the fs — the dirty marker stays
# un-persisted (an earlier default suspend's lockfs freeze synced it out, which is
# what made this very control fail on CI and on real hardware), enter drop_writes,
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
  _post_cut_fsck            # journal-less (ext2) needs an fsck before a clean mount
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
  # Default to the journal-less ext4 + data=writeback BOUNDED ATTEMPT (the ext4
  # driver's weakest ordering — designer's PR #21 lever). The behavioral asymmetry
  # has NOT been demonstrated on any config tested so far: ext4/xfs/btrfs mask it via
  # the journal, and a plain "ext2"-format volume is, on modern kernels (the
  # standalone ext2 driver was removed in Linux 6.9), serviced by the ext4 driver
  # journal-less — so it masks it too via the ext4 driver's metadata/writeback, NOT
  # via any ext2 mechanism. The deterministic guard is the Tier-1 strace presence
  # check (scripts/m8/dirfsync-presence.sh, per-PR); this behavioral path is the
  # last bounded attempt before §14.4d is finalized as a documented negative result.
  local fs="${1:-ext4-writeback}"
  M8_FS="$fs"                     # consumed by _post_cut_fsck (journal-less fsck)
  setup "$fs"                     # cmd_check inside ⇒ loud OPEN + exit on a non-dm host
  trap cmd_teardown EXIT
  case "$fs" in
    ext4-writeback) log "FS: journaled ext4 mounted data=writeback — the ext4 driver's WEAKEST ordering (data=writeback REQUIRES a journal; metadata still journaled). §14.4d's last bounded attempt; CLOSED as a documented negative result — it masks the omission like every other config (the dirent rides the metadata journal on the segment's own fsync). Tier-1 strace is the gate." ;;
    ext2 | ext3) log "FS: '$fs' is, on modern kernels, serviced by the EXT4 DRIVER journal-less (the standalone ext2 driver was removed in Linux 6.9) — NOT a real ext2 driver. The omission has so far been masked here by the ext4 driver's metadata/writeback, not by any ext2 mechanism. Prefer 'ext4-writeback'." ;;
    *) log "FS-DEPENDENCE: '$fs' journals ⇒ §14.4d is INCONCLUSIVE-BY-DESIGN here (a file fsync transitively persists the new dir entry, masking the omission — AFSNCE OSDI '14). A non-failing inject build is NOT 'dir-fsync omission is harmless'. The deterministic guard is the Tier-1 strace presence check." ;;
  esac

  printf '\033[1;33m%s\033[0m\n' "[m8/dm-flakey] §14.4d behavioral control is timing-sensitive: the cut must land before the FS writes back the new directory entry. A bounded retry budget reproduces it; an exhausted budget is INCONCLUSIVE, never self-certified green. Interpret per the FS note above." >&2

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
      log "§14.4d INCONCLUSIVE after ${attempt} attempts (correct=${rc_correct} inject=${rc_inject}). NOT a pass, NOT a code failure. The behavioral asymmetry has not been demonstrated on any Linux config tested (the new segment's dir entry reaches disk transitively via the file's own fdatasync — journal on ext4/xfs/btrfs; the ext4 driver's metadata/writeback on a journal-less mount, incl. 'ext2'-format which modern kernels service via ext4). The deterministic guard is the Tier-1 strace presence check (scripts/m8/dirfsync-presence.sh). NEVER read a non-failing inject build as 'dir-fsync omission is harmless'." ;;
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
