#!/usr/bin/env bash
#
# power-pull.sh — M8 / §14.8 H1 driver (OWNER-RUN on real, cuttable hardware).
#
# H1 is the only TRUE durability test and CANNOT be self-certified in a sandbox:
# it requires a genuine hard power cut on storage that actually loses un-synced
# data. This script makes the owner's run deterministic; it does NOT (and cannot)
# perform the cut. See docs/m8-runbook.md for the full procedure and topology.
#
# Topology:
#   [WAL host, gets cut]  power_pull_workload --(seq,watermark per ack)-->  TCP
#   [external host]       receiver: nc/socat append to capture.txt (durable off-box)
#   [after cut+reboot]    power_pull_verify <wal_dir> <capture.txt>  asserts D1
#
# Subcommands:
#   workload <wal_dir> <sink> [total] [batch] [payload]
#                         run sustained committed load (H2-gated). sink e.g.
#                         tcp:10.0.0.2:9099  (default network sink — durable off-box)
#   receiver <port> <capture_file>
#                         run on the EXTERNAL host: capture the side channel.
#   verify <wal_dir> <capture_file>
#                         after the cut+reboot: assert every acked LSN survived.
#   cycle                 print the ≥50-cycle owner procedure (the cut is manual).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STORAGE_CHECK="${REPO_ROOT}/scripts/m8/storage-check.sh"

log() { printf '\033[1;34m[m8/power-pull]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[m8/power-pull] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

build_bins() {
  ( cd "$REPO_ROOT" && cargo build --bin power_pull_workload --bin power_pull_verify >/dev/null 2>&1 )
}

cmd_workload() {
  local dir="${1:?usage: workload <wal_dir> <sink> [total] [batch] [payload]}"
  local sink="${2:?sink required, e.g. tcp:HOST:PORT}"
  shift 2 || true
  mkdir -p "$dir"

  # VACUOUS-PASS GUARD: refuse to run H1 on storage that does not lose un-synced
  # data. A green H1 there tests nothing. The static classification runs here; the
  # owner MUST also have passed the empirical probe (storage-check.sh probe-*).
  log "H2 precondition: classifying WAL storage (vacuous-pass guard)…"
  "$STORAGE_CHECK" classify "$dir" || die "H2 guard FAILED — refusing to run a vacuous H1. Use durable block storage and pass the empirical probe first."

  build_bins
  log "starting workload (Ctrl-C or the power cut ends it)…"
  exec "${REPO_ROOT}/target/debug/power_pull_workload" "$dir" "$sink" "$@"
}

cmd_receiver() {
  local port="${1:?usage: receiver <port> <capture_file>}"
  local cap="${2:?capture_file required}"
  log "capturing side channel on tcp/:$port -> $cap (run this on the EXTERNAL host)"
  # Prefer socat (robust), then ncat/nc. Each appends received lines to $cap.
  if command -v socat >/dev/null 2>&1; then
    exec socat -u "TCP-LISTEN:${port},reuseaddr,fork" "OPEN:${cap},creat,append"
  elif command -v ncat >/dev/null 2>&1; then
    exec ncat -lk "$port" >> "$cap"
  elif command -v nc >/dev/null 2>&1; then
    log "using plain nc (-lk); if your nc lacks -k, restart it per workload run."
    exec nc -lk "$port" >> "$cap"
  else
    die "no socat/ncat/nc on the external host — install one, or use a serial/file sink (see runbook)."
  fi
}

cmd_verify() {
  local dir="${1:?usage: verify <wal_dir> <capture_file>}"
  local cap="${2:?capture_file required}"
  build_bins
  "${REPO_ROOT}/target/debug/power_pull_verify" "$dir" "$cap"
}

cmd_cycle() {
  cat >&2 <<'EOF'
H1 power-pull — owner procedure (≥50 cycles, zero acked loss to pass; §14.8 H1):

  ONE-TIME, on the cuttable target:
    1. Pick a WAL dir on durable block storage (NOT tmpfs/overlay).
    2. Prove it genuinely loses un-synced data — the vacuous-pass guard:
         scripts/m8/storage-check.sh probe-write  <wal_dir>
         <hard power cut, then reboot>
         scripts/m8/storage-check.sh probe-verify <wal_dir>   # marker MUST be gone
       If the marker survives, the storage does not lose un-synced data — STOP,
       any H1 result would be vacuous.
    3. On the EXTERNAL host, start the receiver:
         scripts/m8/power-pull.sh receiver 9099 capture.txt

  EACH CYCLE (repeat >= 50 times):
    4. On the target, run the workload into the network sink:
         scripts/m8/power-pull.sh workload <wal_dir> tcp:<external_host>:9099 0 64 64
    5. Let it commit for a while, then HARD-CUT power:
         - PDU outlet off, or hypervisor force-stop / `virsh destroy` (NOT graceful).
         - DO NOT use `reboot`, `shutdown`, or `echo b > /proc/sysrq-trigger`:
           those are warm reboots that do NOT clear the device cache and do NOT
           model power loss — they make H1 VACUOUS.
    6. Reboot the target. Verify the cycle:
         scripts/m8/power-pull.sh verify <wal_dir> capture.txt
       PASS (exit 0) = every acked LSN survived. FAIL (exit 1) = acked data lost
       (a D1 violation — investigate the device's flush honesty / cache mode).
       INCONCLUSIVE (exit 2) = side-channel gap; re-run the cycle.
    7. Reset the WAL dir (or keep appending) and repeat.

  PASS the gate only after >= 50 consecutive cycles with zero FAIL. Record the
  device, cache mode (storage-check.sh classify), and cut mechanism in the runbook.
EOF
}

case "${1:-cycle}" in
  workload) shift; cmd_workload "$@" ;;
  receiver) shift; cmd_receiver "$@" ;;
  verify)   shift; cmd_verify "$@" ;;
  cycle)    cmd_cycle ;;
  *)        die "usage: $0 {workload|receiver|verify|cycle}" ;;
esac
