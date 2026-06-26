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
# Honesty rails (M8 ground rules): the calibration GATE runs first and aborts loudly
# if the DUT didn't lose un-synced data; INCONCLUSIVE never counts toward 50; a FAIL
# stops the run; verdict=PASS is emitted ONLY when the H2 probe proved loss AND
# fail==0 AND zero counted INCONCLUSIVE. Nothing here fakes green.
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
# unmistakable in CI. 1 = D1 FAIL (acked loss), 2 = INCONCLUSIVE/infra, 3 = VACUOUS
# calibration (the loudest — storage did not lose un-synced data). die() = generic 1.
die_code() { local c="$1"; shift; printf '\033[1;31m[m8/h1] ERROR:\033[0m %s\n' "$*" >&2; exit "$c"; }
die()      { die_code 1 "$@"; }

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

# --- Smart-plug driver (pluggable by endpoint) ------------------------------
# shelly (Gen2/Gen3/Plus) share the RPC API; gen1 is the legacy /relay/0; tasmota
# is /cm?cmnd=Power. A switch toggle that silently no-ops is caught downstream by
# the post-cut boot-wait (the target must actually go away and come back).
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
  # Static deny-by-default FS/cache check (storage-check.sh). A non-durable FS is a
  # vacuous-class abort (exit 3) just like a surviving marker.
  log "§3.4 calibration: static H2 classification of the DUT…"
  if ! ssh_target "'$H1_BIN_DIR/storage-check.sh' classify '$H1_WAL_DIR'"; then
    H2_PROBE="FAIL(non-durable FS)"
    finish "OPEN"
    die_code 3 "H2 static guard FAILED — the DUT is not a recognised durable block FS. Refusing a vacuous H1."
  fi

  # Empirical loss probe via storage_probe — shares the WAL write(2) path (the reason
  # it's a binary, not the shell echo): un-synced data that is lost here predicts an
  # un-acked WAL record lost here.
  log "§3.4 calibration: writing an UN-SYNCED marker (WAL write path), then a REAL cut…"
  ssh_target "mkdir -p '$H1_WAL_DIR' && '$H1_BIN_DIR/storage_probe' write-unsynced-marker '$H1_WAL_DIR'"
  # Cut immediately (no chance for an unrelated writeback to flush the marker).
  plug_off
  sleep "$H1_OFF_SECS"
  plug_on
  if ! wait_ssh; then
    H2_PROBE="FAIL(target did not return after calibration cut)"
    finish "INCONCLUSIVE"
    die_code 2 "calibration cut: target did not come back — fix the rig before running cycles."
  fi
  # Marker MUST be gone. storage_probe exits 0 (gone ⇒ honest cut) / 1 (survived ⇒ vacuous).
  if ssh_target "'$H1_BIN_DIR/storage_probe' verify-marker-gone '$H1_WAL_DIR'"; then
    H2_PROBE="PASS(marker gone)"
    pass "§3.4 calibration PASSED — the DUT genuinely loses un-synced data. Cycles are meaningful."
  else
    # HARD abort — the single most important thing this gate can discover. Distinct
    # exit 3, evidence emitted with h2_probe=FAIL(survived) and verdict=OPEN.
    H2_PROBE="FAIL(survived)"
    finish "OPEN"
    die_code 3 "§3.4 calibration FAILED (VACUOUS) — the un-synced marker SURVIVED the cut. Storage did NOT lose un-synced data ⇒ any H1 here tests nothing. NO cycles run. (Check mount opts / cut fidelity.)"
  fi
}

# --- the cycle loop (§3.5) --------------------------------------------------
# Globals populated for the evidence ledger.
CYCLES_PASS=0
FAIL_COUNT=0
INCONCLUSIVE_RERUN=0
PER_CYCLE=""                 # JSON array body, e.g. "0,0,2,0"
append_per_cycle() { PER_CYCLE="${PER_CYCLE:+$PER_CYCLE,}$1"; }

# Run ONE trial. Echoes nothing; returns: 0 PASS, 1 FAIL, 2 INCONCLUSIVE/infra.
run_one_cycle() {
  local cap="$1"
  # 1. target up
  wait_ssh || return 2
  # 2. fresh WAL dir (independent durability trial; LSN space from 1)
  ssh_target "rm -rf '$H1_WAL_DIR' && mkdir -p '$H1_WAL_DIR'" || return 2
  # 3. fresh collector
  start_collector "$cap"
  # 4. launch the committing workload (network sink; unbounded; batch 64; 64B payload)
  if ! ssh_target "nohup '$H1_BIN_DIR/power_pull_workload' '$H1_WAL_DIR' 'tcp:${H1_CONTROLLER_IP}:${H1_PORT}' 0 64 64 >/dev/null 2>&1 & echo started"; then
    stop_collector; return 2
  fi
  sleep "$H1_WORKLOAD_SECS"   # let it commit + stream thousands of acked lines
  # 5. CUT (real mains interrupt) — the workload dies with the board
  plug_off
  # 6. wait, then RESTORE
  sleep "$H1_OFF_SECS"
  plug_on
  stop_collector
  # 7. wait for boot
  wait_ssh || return 2
  # 8. ship the off-box capture TO the target and verify against the recovered WAL
  local remote_cap="/tmp/h1_capture_$$.txt"
  scp -q "$cap" "${H1_TARGET_SSH}:${remote_cap}" || return 2
  local rc=0
  ssh_target "'$H1_BIN_DIR/power_pull_verify' '$H1_WAL_DIR' '$remote_cap'" || rc=$?
  ssh_target "rm -f '$remote_cap'" 2>/dev/null || true
  return "$rc"
}

cmd_cycle() {
  require H1_TARGET_SSH; require H1_WAL_DIR; require H1_CONTROLLER_IP
  local capdir; capdir="$(mktemp -d)"
  local infra_fail=0 n=0
  log "starting H1 cycle loop — need ${H1_CYCLES} CONSECUTIVE PASS (medium: ${H1_DUT_MEDIUM})."
  while [ "$CYCLES_PASS" -lt "$H1_CYCLES" ]; do
    n=$(( n + 1 ))
    local cap="${capdir}/capture_${n}.txt"
    log "cycle attempt #${n} (consecutive PASS so far: ${CYCLES_PASS}/${H1_CYCLES})"
    local rc=0
    run_one_cycle "$cap" || rc=$?
    case "$rc" in
      0)
        CYCLES_PASS=$(( CYCLES_PASS + 1 )); infra_fail=0
        append_per_cycle 0
        pass "cycle #${n}: PASS (${CYCLES_PASS}/${H1_CYCLES})" ;;
      1)
        FAIL_COUNT=$(( FAIL_COUNT + 1 )); infra_fail=0
        append_per_cycle 1
        finish "FAIL"
        die_code 1 "cycle #${n}: FAIL — an ACKED LSN was absent after the cut (D1 violation). STOPPING the run. Investigate per §3.6 (most likely a lying device on medium '${H1_DUT_MEDIUM}'; the evidence records which LSN and medium)." ;;
      2)
        # INCONCLUSIVE / infra — never counts toward 50; reset the consecutive streak.
        INCONCLUSIVE_RERUN=$(( INCONCLUSIVE_RERUN + 1 ))
        append_per_cycle 2
        CYCLES_PASS=0
        infra_fail=$(( infra_fail + 1 ))
        warn "cycle #${n}: INCONCLUSIVE/infra (side-channel gap or target didn't return) — not counted; consecutive streak reset."
        if [ "$infra_fail" -ge "$H1_INFRA_FAIL_MAX" ]; then
          finish "INCONCLUSIVE"
          die_code 2 "${H1_INFRA_FAIL_MAX} consecutive infra failures — aborting (likely SD/OS corruption; re-flash, check the read-only overlay & wiring)."
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
  WAL_M8_EVIDENCE="$WAL_M8_EVIDENCE" "${REPO_ROOT}/scripts/m8/evidence.sh" emit \
    gate=H1 \
    "target.uname=${uname}" "target.kernel=${kernel}" "target.host=${host}" \
    "storage.fs=${fstype}" "storage.block_device=${src}" \
    "storage.dut_medium=${H1_DUT_MEDIUM}" "storage.h2_probe=${H2_PROBE}" \
    "cut.mechanism=smart-plug mains interrupt (${H1_PLUG_TYPE}@${H1_PLUG_IP:-n/a})" \
    cut.valid=true \
    "run.cycles_required=${H1_CYCLES}" "run.cycles_pass=${CYCLES_PASS}" \
    "run.fail=${FAIL_COUNT}" "run.inconclusive_rerun=${INCONCLUSIVE_RERUN}" \
    "run.per_cycle=@[${PER_CYCLE}]" \
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
