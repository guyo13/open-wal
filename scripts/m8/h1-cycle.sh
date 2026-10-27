#!/usr/bin/env bash
#
# h1-cycle.sh — M8 / §14.8 H1 power-pull AUTOMATION (OWNER-RUN on a wired rig).
#
# H1 is the only TRUE durability test and CANNOT be self-certified in a sandbox:
# it needs a genuine HARD power cut (mains interrupt) on storage that actually
# loses un-synced data, ≥50 consecutive cycles with zero acked-LSN loss (D1).
# This orchestrator drives that loop end-to-end on the OWNER's physical rig; it
# reuses the proven binaries/scripts and adds NO src/ code.
#
# Topology (docs/m8-infra-plan.md §3.1, docs/m8-runbook.md):
#   [CONTROLLER laptop — NEVER cut]  this script + collector + smart-plug driver
#        | ssh / scp (wired Ethernet)            ^ TCP seq,watermark (durable off-box)
#        v                                        |
#   [TARGET — gets cut]  power_pull_workload on the DUT medium (microSD/USB-SSD/eMMC)
#        |   (mains) -> power strip -> Pi/BBB PSUs
#   [SMART PLUG local HTTP API] <-- this script toggles to CUT/RESTORE
#
# The cut is a REAL mains interrupt via the smart plug. sysrq-b / reboot / shutdown
# are warm/graceful and DO NOT model power loss — they are NOT valid cuts.
#
# Subcommands:
#   deploy        cross-built ARM bins + storage-check.sh -> target ($H1_BIN_DIR)
#   calibrate     §3.4 vacuous-pass GATE: prove the DUT loses un-synced data across a
#                 REAL cut (un-synced marker must be GONE). Vacuous => abort, no cycles.
#   cycle         the ≥50-consecutive-PASS loop (§3.5). FAIL stops the run.
#   run           (default) config-check -> calibrate -> cycle -> emit §5 evidence.
#   config        print the resolved config and exit (no hardware touched).
#
# Exit codes (distinct per cause, so CI never misattributes a rig problem to durability):
#   0 PASS   N consecutive PASS with the §3.4 H2 probe proven.
#   1 FAIL   verdict=FAIL — a durability failure of the (WAL, device) pair. Three sub-causes:
#            (a) power_pull_verify found an acked LSN missing (D1, verify exit 1);
#            (b) recovery REFUSED the log after a proven cut (verify exit 5 — acked records
#                unreadable, §3.6) — the failing cycle's WAL dir is snapshotted to evidence;
#            (c) the durable calibration probe was lost on a proven+matched device
#                (storage_probe exit 3 — the medium lied about fdatasync/dir-fsync, §3.6).
#   2 INCONCLUSIVE  cut not proven (boot_id unchanged), target didn't return, side-channel gap,
#                   or the DUT didn't remount as the same device in a cycle. Re-run, uncounted.
#   3 VACUOUS  §3.4 calibration: the DUT did NOT lose un-synced data (verdict=OPEN). Hard abort.
#   4 HARNESS  rig/config error (plug HTTP, missing socat/bins/config, mountpoint as WAL dir,
#              arch/medium mismatch). Not a durability verdict.
#
# Honesty rails (M8 ground rules): the calibration GATE runs first and aborts loudly
# if the DUT didn't lose un-synced data; every cut is PROVEN via the target's boot_id
# (an unchanged boot_id ⇒ the cut did not happen ⇒ INCONCLUSIVE, never PASS/"survived");
# INCONCLUSIVE never counts toward 50; a FAIL stops the run; verdict=PASS is emitted ONLY
# when the H2 probe proved loss AND fail==0 AND zero counted INCONCLUSIVE. Never fakes green.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# --- Config (env, with defaults) --------------------------------------------
# Target / DUT
H1_TARGET_SSH="${H1_TARGET_SSH:-}"                  # e.g. pi@10.0.0.3 (passwordless key auth)
H1_WAL_DIR="${H1_WAL_DIR:-}"                         # DUT WAL dir on the target (own ext4 partition)
H1_DUT_MEDIUM="${H1_DUT_MEDIUM:-unspecified}"        # microSD | USB-SSD | eMMC(BeagleBone)
H1_BIN_DIR="${H1_BIN_DIR:-/home/${H1_TARGET_USER:-pi}/m8}"  # where deploy puts bins on the target
H1_LOCAL_BIN_DIR="${H1_LOCAL_BIN_DIR:-${REPO_ROOT}/target/aarch64-unknown-linux-gnu/release}"
# Side channel
H1_CONTROLLER_IP="${H1_CONTROLLER_IP:-}"            # IP the target streams seq,watermark to
H1_PORT="${H1_PORT:-9099}"
# Smart plug (pluggable). shelly = Gen2/Gen3/Plus RPC (the owner's Shelly Plug S Gen3).
H1_PLUG_TYPE="${H1_PLUG_TYPE:-shelly}"              # shelly | shelly-gen2 | shelly-gen3 | shelly-gen1 | tasmota
H1_PLUG_IP="${H1_PLUG_IP:-}"
H1_PLUG_ID="${H1_PLUG_ID:-0}"                        # Shelly switch id
H1_PLUG_DRY_RUN="${H1_PLUG_DRY_RUN:-0}"             # 1 = echo the URL instead of curling (no hardware)
# Loop tuning
H1_CYCLES="${H1_CYCLES:-50}"                         # required CONSECUTIVE PASS
H1_WORKLOAD_SECS="${H1_WORKLOAD_SECS:-5}"           # commit window before the cut
H1_OFF_SECS="${H1_OFF_SECS:-4}"                      # power-off duration
H1_BOOT_TIMEOUT="${H1_BOOT_TIMEOUT:-90}"            # seconds to wait for ssh after restore
H1_INFRA_FAIL_MAX="${H1_INFRA_FAIL_MAX:-5}"         # consecutive infra failures => abort
# Evidence
WAL_M8_EVIDENCE="${WAL_M8_EVIDENCE:-${REPO_ROOT}/m8-evidence/evidence-h1.json}"

log()  { printf '\033[1;34m[m8/h1]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m[m8/h1] WARN:\033[0m %s\n' "$*" >&2; }
pass() { printf '\033[1;32m[m8/h1] PASS:\033[0m %s\n' "$*" >&2; }
# die_code <exit-code> <msg…>: distinct exit codes make each terminal cause
# unmistakable in CI (mirrors dm-flakey.sh's HARNESS=4 convention):
#   1 = D1 FAIL   — power_pull_verify reported an acked LSN missing (durability).
#   2 = INCONCLUSIVE/infra — cut not proven, target didn't return, side-channel gap.
#   3 = VACUOUS   — §3.4 calibration proved storage did NOT lose un-synced data.
#   4 = HARNESS   — rig/config error (plug HTTP, missing socat/bins/config). NOT a durability verdict.
# die() is a HARNESS error (4) — a tooling/config failure must NEVER wear the D1-FAIL code.
# Only the cycle-loop verify-FAIL path uses die_code 1.
die_code() { local c="$1"; shift; printf '\033[1;31m[m8/h1] ERROR:\033[0m %s\n' "$*" >&2; exit "$c"; }
die()      { die_code 4 "$@"; }

# --- §5 verdict (one ledger per run, on EVERY terminal path) -----------------
# The §5 schema verdict is PASS | FAIL | INCONCLUSIVE | OPEN. We set VERDICT at each
# terminal path and emit exactly once via finish(); the EXIT trap (cmd_run) is only a
# safety net that emits the current VERDICT (default OPEN ⇒ "gate did not complete").
VERDICT="OPEN"
EVIDENCE_EMITTED=0
finish() {                       # finish <verdict>: emit the §5 ledger exactly once
  VERDICT="$1"
  [ "$EVIDENCE_EMITTED" = 1 ] && return 0
  EVIDENCE_EMITTED=1
  emit_evidence "$VERDICT"
}
on_exit() { finish "$VERDICT"; } # safety net for any unhandled exit ⇒ OPEN

# Loud OPEN banner — H1 is owner-run; this never fakes green.
banner_open() {
  printf '\033[1;31m' >&2
  cat >&2 <<EOF
================================================================================
 H1 power-pull is OPEN-pending-owner-run. $*
 It requires a wired rig with a cuttable target + a real mains cut (smart plug).
 No cycle counts until the §3.4 calibration proves the DUT loses un-synced data.
================================================================================
EOF
  printf '\033[0m' >&2
}

require() { [ -n "${!1:-}" ] || die "config $1 is required (see 'h1-cycle.sh config')"; }

ssh_target() {
  ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new \
    "$H1_TARGET_SSH" "$@"
}

# --- Cut proof (boot_id) ----------------------------------------------------
# A switch toggle that silently no-ops is NOT caught by the boot-wait alone (ssh
# answering does not prove the box rebooted). So every cut is PROVEN: read the
# target's boot_id before the cut and after it comes back; if unchanged, the kernel
# did not reboot ⇒ the cut did not happen ⇒ INCONCLUSIVE (never PASS, never "marker
# survived"). boot_id is a fresh random UUID per boot (/proc/sys/kernel/random/boot_id).
target_boot_id() { ssh_target 'cat /proc/sys/kernel/random/boot_id' 2>/dev/null | tr -d '[:space:]' || true; }
# cut_happened <before> <after>: 0 (true) iff both are non-empty and differ.
cut_happened() { [ -n "${1:-}" ] && [ -n "${2:-}" ] && [ "$1" != "$2" ]; }

# --- Smart-plug driver (pluggable by endpoint) ------------------------------
# shelly (Gen2/Gen3/Plus) share the RPC API; gen1 is the legacy /relay/0; tasmota
# is /cm?cmnd=Power. A silent no-op toggle is caught by the boot_id cut-proof above
# (unchanged boot_id ⇒ INCONCLUSIVE), not by the plug call succeeding.
plug_url() { # plug_url on|off
  local state="$1"
  case "$H1_PLUG_TYPE" in
    shelly|shelly-gen2|shelly-gen3|shelly-plus)
      local on=false; [ "$state" = on ] && on=true
      printf 'http://%s/rpc/Switch.Set?id=%s&on=%s' "$H1_PLUG_IP" "$H1_PLUG_ID" "$on" ;;
    shelly-gen1)
      local turn=off; [ "$state" = on ] && turn=on
      printf 'http://%s/relay/0?turn=%s' "$H1_PLUG_IP" "$turn" ;;
    tasmota)
      local p=Off; [ "$state" = on ] && p=On
      printf 'http://%s/cm?cmnd=Power%%20%s' "$H1_PLUG_IP" "$p" ;;
    *)
      die "unknown H1_PLUG_TYPE='$H1_PLUG_TYPE' (shelly|shelly-gen1|tasmota)" ;;
  esac
}

plug_set() { # plug_set on|off
  local state="$1" url
  url="$(plug_url "$state")"
  if [ "$H1_PLUG_DRY_RUN" = 1 ]; then
    log "DRY-RUN plug $state: GET $url"
    return 0
  fi
  require H1_PLUG_IP
  # -fsS: fail on HTTP error, silent, show errors. A failed toggle is fatal — a
  # missed cut would make the cycle vacuous.
  curl -fsS --max-time 10 "$url" >/dev/null \
    || die "smart-plug $state failed (GET $url) — cannot trust the cut. Aborting."
  log "plug $state ($H1_PLUG_TYPE @ ${H1_PLUG_IP:-dry})"
}

plug_off() { plug_set off; }
plug_on()  { plug_set on; }

# Wait for the target's ssh to come back after a restore. Returns 0 if reachable
# within H1_BOOT_TIMEOUT, 1 otherwise (caller treats as INCONCLUSIVE/infra).
wait_ssh() {
  local deadline=$(( SECONDS + H1_BOOT_TIMEOUT ))
  log "waiting for target ssh (timeout ${H1_BOOT_TIMEOUT}s)…"
  while [ "$SECONDS" -lt "$deadline" ]; do
    if ssh_target true 2>/dev/null; then
      log "target is up."
      return 0
    fi
    sleep 3
  done
  warn "target did not return within ${H1_BOOT_TIMEOUT}s."
  return 1
}

# --- Collector (off-box side channel; runs on the CONTROLLER) ---------------
COLLECTOR_PID=""
start_collector() { # start_collector <capture_file>
  local cap="$1"
  : > "$cap"  # fresh per-cycle capture
  if command -v socat >/dev/null 2>&1; then
    socat -u "TCP-LISTEN:${H1_PORT},reuseaddr,fork" "OPEN:${cap},creat,append" &
  elif command -v ncat >/dev/null 2>&1; then
    ncat -lk "$H1_PORT" >> "$cap" &
  elif command -v nc >/dev/null 2>&1; then
    nc -lk "$H1_PORT" >> "$cap" &
  else
    die "no socat/ncat/nc on the controller — install one for the off-box side channel."
  fi
  COLLECTOR_PID=$!
  log "collector listening on tcp/:${H1_PORT} -> $cap (pid $COLLECTOR_PID)"
}
stop_collector() {
  [ -n "$COLLECTOR_PID" ] || return 0
  kill "$COLLECTOR_PID" 2>/dev/null || true
  wait "$COLLECTOR_PID" 2>/dev/null || true
  COLLECTOR_PID=""
}

# --- deploy -----------------------------------------------------------------
cmd_deploy() {
  require H1_TARGET_SSH
  local wbin="${H1_LOCAL_BIN_DIR}/power_pull_workload"
  local vbin="${H1_LOCAL_BIN_DIR}/power_pull_verify"
  local pbin="${H1_LOCAL_BIN_DIR}/storage_probe"
  if [ ! -x "$wbin" ] || [ ! -x "$vbin" ] || [ ! -x "$pbin" ]; then
    die "cross-built bins not found in $H1_LOCAL_BIN_DIR (build power_pull_workload/power_pull_verify/storage_probe for aarch64 first; see the runbook)."
  fi
  log "deploying bins + storage-check.sh to ${H1_TARGET_SSH}:${H1_BIN_DIR}"
  ssh_target "mkdir -p '$H1_BIN_DIR'"
  scp -q "$wbin" "$vbin" "$pbin" "${REPO_ROOT}/scripts/m8/storage-check.sh" \
    "${H1_TARGET_SSH}:${H1_BIN_DIR}/"
  ssh_target "chmod +x '$H1_BIN_DIR/power_pull_workload' '$H1_BIN_DIR/power_pull_verify' '$H1_BIN_DIR/storage_probe' '$H1_BIN_DIR/storage-check.sh'"
  pass "deployed to ${H1_BIN_DIR}"
}

# --- calibration: the §3.4 vacuous-pass GATE --------------------------------
# Prove the DUT medium genuinely loses un-synced data across a REAL cut, BEFORE any
# cycle counts. Marker survives => storage didn't lose it => abort (vacuous H1).
# Sets H2_PROBE (PASS(marker gone) | FAIL(survived)) for the evidence ledger.
H2_PROBE="not-run"
cmd_calibrate() {
  require H1_TARGET_SSH; require H1_WAL_DIR
  # R8: the cross-built bins must match the target's arch, and the DECLARED medium must
  # match the box — a Pi run must not be filed as 'eMMC(BeagleBone)' and vice-versa.
  TARGET_ARCH="$(ssh_target 'uname -m' 2>/dev/null | tr -d '[:space:]' || true)"
  TARGET_DT_MODEL="$(ssh_target 'tr -d "\000" < /proc/device-tree/model 2>/dev/null' 2>/dev/null || true)"
  [ -n "$TARGET_ARCH" ] || die_code 4 "cannot read the target's 'uname -m' — rig/ssh error (HARNESS)."
  local want_arch
  case "$H1_DUT_MEDIUM" in
    'eMMC(BeagleBone)') want_arch=armv7l ;;
    *)                  want_arch=aarch64 ;;
  esac
  [ "$TARGET_ARCH" = "$want_arch" ] || die_code 4 "arch/medium mismatch — target reports '$TARGET_ARCH' but medium '$H1_DUT_MEDIUM' expects '$want_arch'. Wrong bins deployed, or the medium label is wrong (a Pi run mis-filed as BeagleBone, or vice-versa). (HARNESS)"

  # R7: H1_WAL_DIR must be a SUBDIRECTORY of the DUT mount, never the mountpoint itself
  # (a per-cycle `rm -rf` of the mountpoint would wipe the partition + EBUSY). And it must
  # be CREATED before classify, so a fresh/unformatted-then-formatted DUT whose dir doesn't
  # exist yet is not mis-recorded as a non-durable (VACUOUS) medium.
  if ssh_target "mountpoint -q '$H1_WAL_DIR'"; then
    die_code 4 "H1_WAL_DIR ($H1_WAL_DIR) is itself a MOUNTPOINT — it must be a SUBDIRECTORY of the DUT mount so per-cycle cleanup can't wipe the partition. (HARNESS)"
  fi
  ssh_target "mkdir -p '$H1_WAL_DIR'" || die_code 4 "cannot create H1_WAL_DIR ($H1_WAL_DIR) on the target (HARNESS)."

  # Static deny-by-default FS/cache check (storage-check.sh). A non-durable FS is a
  # vacuous-class abort (exit 3) just like a surviving marker.
  log "§3.4 calibration: static H2 classification of the DUT… (arch ${TARGET_ARCH}, medium ${H1_DUT_MEDIUM})"
  if ! ssh_target "'$H1_BIN_DIR/storage-check.sh' classify '$H1_WAL_DIR'"; then
    H2_PROBE="FAIL(non-durable FS)"
    finish "OPEN"
    die_code 3 "H2 static guard FAILED — the DUT is not a recognised durable block FS. Refusing a vacuous H1."
  fi
  # Capture the DUT's block device BEFORE the cut so we can detect a post-reboot
  # remount onto a different source (e.g. nofail fallback) that would confound verify.
  # R7: an empty df source is INCONCLUSIVE (cannot establish the device), never a silent skip.
  local src_before; src_before="$(ssh_target "df --output=source '$H1_WAL_DIR' 2>/dev/null | tail -1 | tr -d ' '" 2>/dev/null || true)"
  if [ -z "$src_before" ]; then
    H2_PROBE="INCONCLUSIVE(could not determine DUT block device before the cut)"
    finish "INCONCLUSIVE"
    die_code 2 "could not determine the DUT block device before the cut (df --output=source was empty) — INCONCLUSIVE, not a silent skip."
  fi
  CAL_SRC="$src_before"   # per-cycle remount check (R7) compares against this

  # Empirical loss probe via storage_probe — shares the WAL write(2) path (the reason
  # it's a binary, not the shell echo): durable pre-alloc (fdatasync+dir-fsync) plus an
  # un-synced positioned marker, so loss here predicts an un-acked WAL record lost here.
  log "§3.4 calibration: writing the durable pre-alloc + UN-SYNCED marker, then a REAL cut…"
  ssh_target "mkdir -p '$H1_WAL_DIR' && '$H1_BIN_DIR/storage_probe' write-unsynced-marker '$H1_WAL_DIR'"
  local boot_before; boot_before="$(target_boot_id)"
  # Cut immediately (no chance for an unrelated writeback to flush the marker).
  plug_off
  sleep "$H1_OFF_SECS"
  plug_on
  if ! wait_ssh; then
    H2_PROBE="INCONCLUSIVE(target did not return after calibration cut)"
    finish "INCONCLUSIVE"
    die_code 2 "calibration cut: target did not come back — fix the rig before running cycles."
  fi
  # PROVE the cut actually happened (boot_id changed). An unchanged boot_id means the
  # plug no-op'd / the box never lost power — INCONCLUSIVE, NOT "marker survived".
  local boot_after; boot_after="$(target_boot_id)"
  if ! cut_happened "$boot_before" "$boot_after"; then
    H2_PROBE="INCONCLUSIVE(cut did not happen; boot_id unchanged: ${boot_before:-?})"
    finish "INCONCLUSIVE"
    die_code 2 "calibration cut UNPROVEN — boot_id did not change (${boot_before:-?} == ${boot_after:-?}); the target did not reboot, so no real power cut occurred. Check the smart plug / wiring. (NOT a durability result.)"
  fi
  CUT_VALID="true"   # the calibration cut is boot_id-proven real
  # The DUT must come back as the SAME durable block device (re-classify + source match),
  # else a post-cut remount/damage would let verify-marker-gone read a vacuous pass.
  if ! ssh_target "'$H1_BIN_DIR/storage-check.sh' classify '$H1_WAL_DIR'"; then
    H2_PROBE="INCONCLUSIVE(DUT not a durable block FS after the cut)"
    finish "INCONCLUSIVE"
    die_code 2 "post-cut re-classify FAILED — the DUT did not come back as a durable block FS (remount/fsck fallback?). INCONCLUSIVE; fix the rig."
  fi
  local src_after; src_after="$(ssh_target "df --output=source '$H1_WAL_DIR' 2>/dev/null | tail -1 | tr -d ' '" 2>/dev/null || true)"
  if [ -n "$src_before" ] && [ "$src_before" != "$src_after" ]; then
    H2_PROBE="INCONCLUSIVE(DUT device changed across the cut: ${src_before} -> ${src_after:-?})"
    finish "INCONCLUSIVE"
    die_code 2 "the DUT block device changed across the cut (${src_before} -> ${src_after:-?}) — the WAL dir remounted elsewhere; a marker result would be meaningless. INCONCLUSIVE."
  fi
  # Two-sided marker verify (reached only AFTER the cut is boot_id-proven, the DUT
  # re-classifies as durable, and the block device matches $CAL_SRC):
  #   0 = gone (honest cut) / 1 = survived (vacuous) /
  #   3 = DURABLE-LOST (the durably-synced pre-alloc is gone ⇒ the medium lied about
  #       fdatasync/dir-fsync — a device-honesty FAIL, §3.6) /
  #   2 = INCONCLUSIVE (a real stat/open/read I/O error).
  local rc=0
  ssh_target "'$H1_BIN_DIR/storage_probe' verify-marker-gone '$H1_WAL_DIR'" || rc=$?
  case "$rc" in
    0)
      H2_PROBE="PASS(marker gone)"
      pass "§3.4 calibration PASSED — the DUT genuinely loses un-synced data (cut boot_id-proven). Cycles are meaningful." ;;
    1)
      # HARD abort — the single most important thing this gate can discover.
      H2_PROBE="FAIL(survived)"
      finish "OPEN"
      die_code 3 "§3.4 calibration FAILED (VACUOUS) — the un-synced marker SURVIVED a boot_id-proven cut. Storage did NOT lose un-synced data ⇒ any H1 here tests nothing. NO cycles run. (Check mount opts / device cache honesty.)" ;;
    3)
      # R6: on a proven+re-classified+source-matched device, a lost durable pre-alloc means
      # the medium acked fdatasync/dir-fsync then lost it — a lying-device verdict (§3.6).
      H2_PROBE="FAIL(durable probe lost on ${src_after:-$CAL_SRC}: device did not honor fdatasync/dir-fsync — §3.6; medium ${H1_DUT_MEDIUM})"
      finish "FAIL"
      die_code 1 "§3.4 calibration: DEVICE-HONESTY FAIL — the durably-synced probe (fdatasync + dir-fsync) was LOST across a boot_id-proven cut on ${src_after:-$CAL_SRC} (medium '${H1_DUT_MEDIUM}'). The medium lied about the flush (§3.6). This is a durability FAIL of the (WAL, device) pair, not infra — do NOT just re-run; the device is not honest hardware." ;;
    2)
      H2_PROBE="INCONCLUSIVE(real I/O error reading the probe after the cut)"
      finish "INCONCLUSIVE"
      die_code 2 "§3.4 calibration INCONCLUSIVE — a real I/O error while reading the durable probe after the cut. NOT a pass; re-run after fixing the rig." ;;
    *)
      die_code 4 "storage_probe verify-marker-gone exited ${rc} (unexpected) — harness error." ;;
  esac
}

# --- the cycle loop (§3.5) --------------------------------------------------
# Globals populated for the evidence ledger.
CYCLES_PASS=0
FAIL_COUNT=0
INCONCLUSIVE_RERUN=0
PER_CYCLE=""                 # JSON array body, e.g. "0,0,2,0"
CUT_VALID="false"            # cut.valid in §5: set true once the calibration cut is boot_id-proven
BOOT_PAIRS=""                # per-cycle [before,after] boot_id pairs (JSON array body)
CAL_SRC=""                   # DUT block device captured at calibration (per-cycle remount check, R7)
TARGET_ARCH="unknown"        # ssh 'uname -m' (R8)
TARGET_DT_MODEL="unknown"    # /proc/device-tree/model (R8)
append_per_cycle() { PER_CYCLE="${PER_CYCLE:+$PER_CYCLE,}$1"; }
# record_boot_pair <before> <after>: append the cut-proof pair for the evidence ledger.
record_boot_pair() {
  local b="${1:-}" a="${2:-}"
  BOOT_PAIRS="${BOOT_PAIRS:+$BOOT_PAIRS,}[\"${b:-unknown}\",\"${a:-unreachable}\"]"
}
# snapshot_wal_dir <remote_dir> <label>: tar the failing cycle's WAL dir off the target
# into the evidence dir for forensics (R4 — a recovery refusal is a durability finding).
snapshot_wal_dir() {
  local rdir="$1" label="$2"
  local out; out="$(dirname "$WAL_M8_EVIDENCE")/h1-waldir-${label}.tar.gz"
  mkdir -p "$(dirname "$out")"
  if ssh_target "tar czf - -C '$(dirname "$rdir")' '$(basename "$rdir")'" > "$out" 2>/dev/null; then
    log "snapshotted the failing cycle WAL dir -> $out"
  else
    warn "could not snapshot the WAL dir $rdir (continuing; the verdict stands)."
  fi
}

# Run ONE trial on a UNIQUE per-cycle WAL dir. Returns the raw verify code:
#   0 PASS · 1 FAIL (acked loss) · 5 RECOVERY-FAIL · 2 INCONCLUSIVE/infra.
# R5: each cycle uses its own fresh dir, so a device that rolls its FTL back to a prior
# cycle's directory CANNOT verify as PASS — this cycle's dir is simply absent (empty log
# ⇒ FAIL/RECOVERY-FAIL), and the old cycle's dir is never the path we verify.
run_one_cycle() {
  local cap="$1" cyc_dir="$2"
  # 1. target up
  wait_ssh || return 2
  # 2. fresh UNIQUE WAL dir for THIS cycle (independent trial; LSN space from 1)
  ssh_target "mkdir -p '$cyc_dir'" || return 2
  # 3. fresh collector
  start_collector "$cap"
  # 4. launch the committing workload (network sink; unbounded; batch 64; 64B payload)
  if ! ssh_target "nohup '$H1_BIN_DIR/power_pull_workload' '$cyc_dir' 'tcp:${H1_CONTROLLER_IP}:${H1_PORT}' 0 64 64 >/dev/null 2>&1 & echo started"; then
    stop_collector; return 2
  fi
  sleep "$H1_WORKLOAD_SECS"   # let it commit + stream thousands of acked lines
  # capture boot_id while the target is up (to prove the cut actually reboots it)
  local boot_before; boot_before="$(target_boot_id)"
  # 5. CUT (real mains interrupt) — the workload dies with the board
  plug_off
  # 6. wait, then RESTORE
  sleep "$H1_OFF_SECS"
  plug_on
  stop_collector
  # 7. wait for boot
  if ! wait_ssh; then record_boot_pair "$boot_before" ""; return 2; fi
  # PROVE the cut happened: boot_id must have changed. If not, the plug no-op'd / the
  # box never lost power ⇒ INCONCLUSIVE (return 2), NOT a PASS on an intact log.
  local boot_after; boot_after="$(target_boot_id)"
  record_boot_pair "$boot_before" "$boot_after"
  if ! cut_happened "$boot_before" "$boot_after"; then
    warn "cycle: boot_id unchanged (${boot_before:-?}) — the target did NOT reboot ⇒ the cut did not happen. INCONCLUSIVE."
    return 2
  fi
  # R7: the DUT must remount as the SAME durable block device before we trust a verify.
  # A DUT that didn't remount (or came back as a different source) is INCONCLUSIVE infra,
  # NEVER a D1 FAIL (we must not read a wiped/absent log as acked-loss).
  if ! ssh_target "'$H1_BIN_DIR/storage-check.sh' classify '$H1_WAL_DIR'"; then
    warn "cycle: the DUT did not re-classify as a durable block FS after the cut — INCONCLUSIVE."
    return 2
  fi
  local src_now; src_now="$(ssh_target "df --output=source '$H1_WAL_DIR' 2>/dev/null | tail -1 | tr -d ' '" 2>/dev/null || true)"
  if [ -n "$CAL_SRC" ] && [ "$src_now" != "$CAL_SRC" ]; then
    warn "cycle: DUT block device changed across the cut (${CAL_SRC} -> ${src_now:-?}) — INCONCLUSIVE, not a D1 FAIL."
    return 2
  fi
  # 8. ship the off-box capture TO the target and verify against THIS cycle's recovered WAL
  local remote_cap="/tmp/h1_capture_$$.txt"
  scp -q "$cap" "${H1_TARGET_SSH}:${remote_cap}" || return 2
  local rc=0
  ssh_target "'$H1_BIN_DIR/power_pull_verify' '$cyc_dir' '$remote_cap'" || rc=$?
  ssh_target "rm -f '$remote_cap'" 2>/dev/null || true
  return "$rc"
}

cmd_cycle() {
  require H1_TARGET_SSH; require H1_WAL_DIR; require H1_CONTROLLER_IP
  local capdir; capdir="$(mktemp -d)"
  local infra_fail=0 n=0 prev_dir=""
  log "starting H1 cycle loop — need ${H1_CYCLES} CONSECUTIVE PASS (medium: ${H1_DUT_MEDIUM})."
  while [ "$CYCLES_PASS" -lt "$H1_CYCLES" ]; do
    n=$(( n + 1 ))
    local cap="${capdir}/capture_${n}.txt"
    # R5: a UNIQUE dir per cycle, nonce from the controller, under the DUT mount.
    local nonce; nonce="$(date +%s%N)-${RANDOM}${RANDOM}"
    local cyc_dir="${H1_WAL_DIR}/cycle-${n}-${nonce}"
    log "cycle attempt #${n} (consecutive PASS so far: ${CYCLES_PASS}/${H1_CYCLES}; dir: cycle-${n}-…)"
    local rc=0
    run_one_cycle "$cap" "$cyc_dir" || rc=$?
    case "$rc" in
      0)
        CYCLES_PASS=$(( CYCLES_PASS + 1 )); infra_fail=0
        append_per_cycle 0
        pass "cycle #${n}: PASS (${CYCLES_PASS}/${H1_CYCLES})"
        # R5: drop the PREVIOUS cycle's dir only now that THIS cycle's verify is done.
        if [ -n "$prev_dir" ]; then ssh_target "rm -rf '$prev_dir'" 2>/dev/null || true; fi
        prev_dir="$cyc_dir" ;;
      1)
        FAIL_COUNT=$(( FAIL_COUNT + 1 )); infra_fail=0
        append_per_cycle 1
        snapshot_wal_dir "$cyc_dir" "cycle${n}-FAIL"
        finish "FAIL"
        die_code 1 "cycle #${n}: FAIL — an ACKED LSN was absent after the cut (D1 violation). STOPPING the run. Investigate per §3.6 (most likely a lying device on medium '${H1_DUT_MEDIUM}'; the snapshot + ledger record which LSN and medium)." ;;
      5)
        # R4: recovery REFUSED the log after a proven cut ⇒ acked records unreadable ⇒
        # a D1-class FAIL (§3.6), NOT infra. Record it and snapshot for forensics.
        FAIL_COUNT=$(( FAIL_COUNT + 1 )); infra_fail=0
        append_per_cycle 5
        snapshot_wal_dir "$cyc_dir" "cycle${n}-RECOVERY-FAIL"
        finish "FAIL"
        die_code 1 "cycle #${n}: RECOVERY-FAIL — recovery REFUSED the post-cut log (power_pull_verify exit 5). If acked records are now unreadable they are LOST: a D1-class failure of the (WAL, device) pair on medium '${H1_DUT_MEDIUM}' (§3.6), not infra. STOPPING the run; the WAL dir is snapshotted to the evidence dir." ;;
      2)
        # INCONCLUSIVE / infra (cut unproven, target didn't return, DUT didn't remount, or a
        # side-channel gap). UNCOUNTED and re-run; it does NOT reset the consecutive PASS streak
        # (a Pi that occasionally fails to boot shouldn't make 50 unreachable), but it DOES count
        # toward the consecutive-infra-fail ABORT ceiling. Runbook documents this.
        INCONCLUSIVE_RERUN=$(( INCONCLUSIVE_RERUN + 1 ))
        append_per_cycle 2
        infra_fail=$(( infra_fail + 1 ))
        warn "cycle #${n}: INCONCLUSIVE/infra — not counted (streak kept at ${CYCLES_PASS}/${H1_CYCLES}); consecutive infra ${infra_fail}/${H1_INFRA_FAIL_MAX}."
        if [ -n "$prev_dir" ]; then ssh_target "rm -rf '$prev_dir'" 2>/dev/null || true; fi
        prev_dir="$cyc_dir"
        if [ "$infra_fail" -ge "$H1_INFRA_FAIL_MAX" ]; then
          finish "INCONCLUSIVE"
          die_code 2 "${H1_INFRA_FAIL_MAX} consecutive infra failures — aborting (likely SD/OS corruption or a dead cut; re-flash, check the read-only overlay, wiring & smart plug)."
        fi ;;
      *)
        finish "INCONCLUSIVE"
        die_code 2 "cycle #${n}: power_pull_verify exited ${rc} (unexpected) — aborting." ;;
    esac
  done
  rm -rf "$capdir"
  pass "H1 cycle loop COMPLETE: ${CYCLES_PASS} consecutive PASS, ${FAIL_COUNT} FAIL, ${INCONCLUSIVE_RERUN} INCONCLUSIVE re-runs."
}

# --- evidence (§5) ----------------------------------------------------------
emit_evidence() {
  local verdict="$1"
  mkdir -p "$(dirname "$WAL_M8_EVIDENCE")"
  # Gather target identity (best-effort; never fatal here).
  local uname kernel host fstype src
  uname="$(ssh_target 'uname -sr' 2>/dev/null || echo unknown)"
  kernel="$(ssh_target 'uname -r' 2>/dev/null || echo unknown)"
  host="$(ssh_target 'hostname' 2>/dev/null || echo unknown)"
  fstype="$(ssh_target "df --output=fstype '$H1_WAL_DIR' 2>/dev/null | tail -1 | tr -d ' '" 2>/dev/null || echo unknown)"
  src="$(ssh_target "df --output=source '$H1_WAL_DIR' 2>/dev/null | tail -1 | tr -d ' '" 2>/dev/null || echo unknown)"
  # Optional: a run with fewer than the full 50 cycles is a "smoke" run, not a gate.
  local mode="gate"
  if [ "${H1_CYCLES:-50}" -lt 50 ] 2>/dev/null; then mode="smoke"; fi
  WAL_M8_EVIDENCE="$WAL_M8_EVIDENCE" "${REPO_ROOT}/scripts/m8/evidence.sh" emit \
    gate=H1 \
    "target.uname=${uname}" "target.kernel=${kernel}" "target.host=${host}" \
    "target.arch=${TARGET_ARCH:-unknown}" "target.device_tree_model=${TARGET_DT_MODEL:-unknown}" \
    "storage.fs=${fstype}" "storage.block_device=${src}" \
    "storage.dut_medium=${H1_DUT_MEDIUM}" "storage.h2_probe=${H2_PROBE}" \
    "storage.h2_cut_confirmed=${CUT_VALID}" \
    "cut.mechanism=smart-plug mains interrupt (${H1_PLUG_TYPE}@${H1_PLUG_IP:-n/a})" \
    "cut.valid=${CUT_VALID}" \
    "run.mode=${mode}" \
    "run.cycles_required=${H1_CYCLES}" "run.cycles_pass=${CYCLES_PASS}" \
    "run.fail=${FAIL_COUNT}" "run.inconclusive_rerun=${INCONCLUSIVE_RERUN}" \
    "run.per_cycle=@[${PER_CYCLE}]" \
    "run.boot_id_pairs=@[${BOOT_PAIRS}]" \
    "verdict=${verdict}"
  log "evidence written: $WAL_M8_EVIDENCE"
}

# --- config dump ------------------------------------------------------------
cmd_config() {
  cat >&2 <<EOF
H1 resolved config:
  target ssh        H1_TARGET_SSH    = ${H1_TARGET_SSH:-<unset>}
  DUT WAL dir       H1_WAL_DIR       = ${H1_WAL_DIR:-<unset>}
  DUT medium        H1_DUT_MEDIUM    = ${H1_DUT_MEDIUM}
  target bin dir    H1_BIN_DIR       = ${H1_BIN_DIR}
  local bin dir     H1_LOCAL_BIN_DIR = ${H1_LOCAL_BIN_DIR}
  controller IP     H1_CONTROLLER_IP = ${H1_CONTROLLER_IP:-<unset>}
  side-channel port H1_PORT          = ${H1_PORT}
  plug type         H1_PLUG_TYPE     = ${H1_PLUG_TYPE}
  plug ip           H1_PLUG_IP       = ${H1_PLUG_IP:-<unset>}
  plug switch id    H1_PLUG_ID       = ${H1_PLUG_ID}
  plug dry-run      H1_PLUG_DRY_RUN  = ${H1_PLUG_DRY_RUN}
  required PASS     H1_CYCLES        = ${H1_CYCLES}
  workload window   H1_WORKLOAD_SECS = ${H1_WORKLOAD_SECS}s
  off duration      H1_OFF_SECS      = ${H1_OFF_SECS}s
  boot timeout      H1_BOOT_TIMEOUT  = ${H1_BOOT_TIMEOUT}s
  infra-fail max    H1_INFRA_FAIL_MAX= ${H1_INFRA_FAIL_MAX}
  evidence out      WAL_M8_EVIDENCE  = ${WAL_M8_EVIDENCE}
  cut URLs: off=$(plug_url off)  on=$(plug_url on)
EOF
}

# --- run (default): calibrate -> cycle -> evidence --------------------------
# finish() emits the §5 ledger exactly once on every terminal path; the EXIT trap is
# only a safety net (emits the current VERDICT, default OPEN, on an unhandled exit).
# The terminal failures inside calibrate/cycle already set the right verdict + exit code.
cmd_run() {
  require H1_TARGET_SSH; require H1_WAL_DIR; require H1_CONTROLLER_IP
  banner_open "Starting an OWNER-RUN H1 campaign now."
  trap on_exit EXIT
  # The §3.4 calibration GATE is the FIRST step of every run; a vacuous DUT aborts
  # (exit 3) before any cycle counts.
  cmd_calibrate
  cmd_cycle
  finish "PASS"
  pass "H1 PASSED locally for medium '${H1_DUT_MEDIUM}': ${CYCLES_PASS} consecutive cycles, H2 probe ${H2_PROBE}. The OWNER signs off on #18 — the agent never self-certifies H1."
}

case "${1:-run}" in
  deploy)    shift; cmd_deploy ;;
  calibrate) shift; cmd_calibrate ;;
  cycle)     shift; cmd_cycle ;;
  config)    shift; cmd_config ;;
  run)       shift; cmd_run ;;
  *)         die "usage: $0 {deploy|calibrate|cycle|run|config}" ;;
esac
