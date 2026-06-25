#!/usr/bin/env bash
#
# storage-check.sh — M8 / §14.8 H2: the vacuous-pass guard.
#
# THE #1 RULE OF M8: a durability test on storage where un-synced data is NOT
# actually lost passes VACUOUSLY (the data was never at risk). A green result on
# non-durable storage is the worst possible outcome. This script is the guard
# H1 (power-pull) depends on: it refuses to certify storage it cannot
# AFFIRMATIVELY recognize as a real, durable, block-backed filesystem.
#
# DENY-BY-DEFAULT, not a blocklist. The one time the guard matters is a storage
# config nobody anticipated, so "I don't recognize this, therefore it's fine" is
# exactly the failure mode we reject. Verdicts:
#   - PASS         only an affirmatively-recognized durable FS on a real block
#                  device (ext4/xfs/btrfs/… on /dev/*), with the cache mode read.
#   - FAIL         tmpfs / ramfs / overlay / 9p / virtiofs — known non-durable or
#                  pass-through; un-synced data is not at risk ⇒ vacuous.
#   - INCONCLUSIVE unrecognized FS, no resolvable block device, or unreadable
#                  cache mode ⇒ treated as FAIL (deny-by-default).
#
# The cache mode is reported and LABELLED but is not, by itself, a failure: a
# device with a volatile write-back cache is durable IFF it honours flushes
# (PLP / honest hardware). Proving that needs a real power cut — the empirical
# loss-probe (probe-write / probe-verify below), which the OWNER runs in H1.
# See docs/m8-runbook.md.
#
# Usage:
#   scripts/m8/storage-check.sh [DIR]            # classify DIR (default: cwd)
#   scripts/m8/storage-check.sh classify [DIR]   # same, explicit
#   scripts/m8/storage-check.sh probe-write DIR  # owner H1: write un-synced marker
#   scripts/m8/storage-check.sh probe-verify DIR # owner H1: after cut+reboot, marker MUST be gone
set -euo pipefail

log()  { printf '\033[1;34m[m8/storage]\033[0m %s\n' "$*" >&2; }
pass() { printf '\033[1;32m[m8/storage] PASS:\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m[m8/storage] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }

# Filesystems where un-synced (and sometimes even synced) data is not durably at
# risk on this host: RAM-backed or pass-through to another trust domain.
NONDURABLE_FS="tmpfs ramfs overlay overlayfs aufs squashfs 9p virtiofs nfs nfs4 cifs smbfs fuse fuse.lazyfs"
# Filesystems we affirmatively recognise as real, block-backed, durable-capable.
DURABLE_FS="ext2 ext3 ext4 xfs btrfs zfs f2fs reiserfs jfs"

# Resolve the parent whole-disk for a /sys/block cache lookup (vda1 -> vda).
parent_disk() {
  local src="$1" base pk
  base="$(basename "$src")"
  if command -v lsblk >/dev/null 2>&1; then
    pk="$(lsblk -no pkname "$src" 2>/dev/null | head -1 || true)"
    [ -n "$pk" ] && { echo "$pk"; return; }
  fi
  # Fallback: strip a trailing partition number (vda1->vda, nvme0n1p1->nvme0n1).
  echo "$base" | sed -E 's/p?[0-9]+$//'
}

cmd_classify() {
  local dir="${1:-$PWD}"
  [ -e "$dir" ] || fail "path does not exist: $dir"

  local fstype src
  fstype="$(df --output=fstype "$dir" 2>/dev/null | tail -1 | tr -d ' ' || true)"
  src="$(df --output=source "$dir" 2>/dev/null | tail -1 | tr -d ' ' || true)"
  [ -n "$fstype" ] || fail "could not determine filesystem type for $dir (INCONCLUSIVE ⇒ deny)"

  log "target:    $dir"
  log "filesystem: $fstype"
  log "source:    $src"

  # 1. Known non-durable / pass-through ⇒ FAIL (vacuous-pass risk).
  for f in $NONDURABLE_FS; do
    if [ "$fstype" = "$f" ]; then
      fail "filesystem '$fstype' is RAM-backed or pass-through — un-synced data is NOT durably at risk here, so any power-pull/H1 result on it is VACUOUS. Use a real block device."
    fi
  done

  # 2. Must be an affirmatively-recognised durable FS (deny-by-default).
  local recognised=0
  for f in $DURABLE_FS; do
    [ "$fstype" = "$f" ] && recognised=1
  done
  [ "$recognised" -eq 1 ] || fail "filesystem '$fstype' is not in the recognised-durable allowlist ($DURABLE_FS). INCONCLUSIVE ⇒ deny-by-default. If it IS durable, add it explicitly after verifying with the empirical probe."

  # 3. Must sit on a real block device.
  case "$src" in
    /dev/*) : ;;
    *) fail "source '$src' is not a /dev block device — cannot establish durability. INCONCLUSIVE ⇒ deny." ;;
  esac

  # 4. Read + label the write-cache mode (reported, not by itself fatal).
  local disk wc="unknown"
  disk="$(parent_disk "$src")"
  if [ -r "/sys/block/$disk/queue/write_cache" ]; then
    wc="$(cat "/sys/block/$disk/queue/write_cache")"
  fi
  log "block device: /dev/$disk   write_cache: $wc"
  case "$wc" in
    "write back")
      log "LABEL: volatile write-back cache present — durable ONLY if the device honours"
      log "       flushes (power-loss-protected / honest hardware). A consumer SSD/HDD that"
      log "       lies about flush WILL lose acked data. Confirm with the empirical probe (H1)."
      ;;
    "write through")
      log "LABEL: write-through cache — no volatile device cache to lose. Still verify the"
      log "       virtualization layer (host cache mode = none/writethrough) for VM targets."
      ;;
    *)
      log "LABEL: write-cache mode unreadable ('$wc'). Cannot confirm flush behaviour from"
      log "       sysfs; the empirical power-cut probe (H1) is the authority. Treat as"
      log "       UNVERIFIED until the owner runs probe-write/probe-verify across a real cut."
      ;;
  esac

  pass "$fstype on /dev/$disk is an affirmatively-recognised durable block device."
  log "NOTE: this is a STATIC classification. The definitive vacuous-pass guard is the"
  log "empirical loss probe (probe-write → real power cut → probe-verify), owner-run in H1."
}

# --- Empirical loss probe (owner-run across a REAL power cut, part of H1) -------
# Writes a marker file and does NOT fsync it (nor its directory). After a genuine
# power cut + reboot the marker MUST be gone; if it survives, either it raced a
# flush or the storage does not actually lose un-synced data (vacuous-pass risk).
PROBE_NAME=".m8_unsynced_marker"

cmd_probe_write() {
  local dir="${1:?usage: probe-write DIR}"
  local marker="$dir/$PROBE_NAME"
  # O_SYNC deliberately NOT used; we want this to live only in the page cache.
  printf 'm8-unsynced-%s\n' "$(date +%s)" > "$marker"
  log "wrote UN-SYNCED marker: $marker"
  log "NOW cut power HARD (PDU / hypervisor force-stop — NOT 'reboot', NOT sysrq-b)."
  log "After reboot run: scripts/m8/storage-check.sh probe-verify '$dir'"
}

cmd_probe_verify() {
  local dir="${1:?usage: probe-verify DIR}"
  local marker="$dir/$PROBE_NAME"
  if [ -e "$marker" ]; then
    fail "the un-synced marker SURVIVED the cut. Storage did NOT lose un-synced data — a power-pull/H1 result here would be VACUOUS (data was never at risk). Do NOT certify H1 on this target."
  fi
  pass "un-synced marker is gone after the cut — storage genuinely loses un-synced data. H1 on this target is meaningful (continue to the full H1 acked-LSN run)."
}

case "${1:-classify}" in
  classify)     shift || true; cmd_classify "${1:-$PWD}" ;;
  probe-write)  shift; cmd_probe_write "${1:-}" ;;
  probe-verify) shift; cmd_probe_verify "${1:-}" ;;
  /*|./*|../*)  cmd_classify "$1" ;;          # bare path argument
  *)            cmd_classify "${1:-$PWD}" ;;
esac
