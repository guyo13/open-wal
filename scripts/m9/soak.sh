#!/usr/bin/env bash
#
# soak.sh — §14.10 soak / endurance runner (M9).
#
# Thin wrapper around `tests/soak.rs` (the `#[ignore]`d endurance driver): builds
# the test, runs it for a configurable duration, captures the one-line JSON summary
# it prints, and re-emits that summary as a §5-style evidence ledger (reusing the M8
# evidence emitter) so a long run leaves an artifact, not just a green checkmark.
#
# HONEST FRAMING (same stopgap as LazyFS / dm-flakey / bench): a SHORT run here (the
# in-session / per-dispatch default) is a smoke that proves the driver + monitors +
# oracle work — it is NOT the §14.13 soak gate. The real gate is a MULTI-HOUR run
# (owner / dedicated runner) with zero fd / disk / RSS / latency regression and zero
# oracle violation. A short green run is CONTINGENT; a failure (resource leak or an
# invariant breach) is a real bug and reds the run loudly.
#
# Usage:
#   scripts/m9/soak.sh [SECONDS] [SEED]
# Env (override or pass positionally):
#   WAL_SOAK_SECONDS  duration in wall-clock seconds (default 5 here; hours for the gate)
#   WAL_SOAK_SEED     LCG seed (default: the test's fixed seed)
#   WAL_SOAK_EVIDENCE optional path; the test appends its own JSON line too
#   WAL_M9_EVIDENCE   optional path for the assembled evidence ledger (this script)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

log() { printf '\033[1;34m[m9/soak]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[m9/soak] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

SECONDS_ARG="${1:-${WAL_SOAK_SECONDS:-5}}"
SEED_ARG="${2:-${WAL_SOAK_SEED:-}}"

export WAL_SOAK_SECONDS="$SECONDS_ARG"
[ -n "$SEED_ARG" ] && export WAL_SOAK_SEED="$SEED_ARG"

if [ "$SECONDS_ARG" -lt 600 ]; then
  log "SHORT run (${SECONDS_ARG}s): smoke only — NOT the §14.13 multi-hour soak gate."
else
  log "LONG run (${SECONDS_ARG}s): if green with zero regression/violation, records gate evidence."
fi

cd "$ROOT"

# Capture stdout so we can scrape the test's one-line JSON summary.
out_file="$(mktemp)"
trap 'rm -f "$out_file"' EXIT

set +e
cargo test --release --test soak -- --ignored --nocapture 2>&1 | tee "$out_file"
status="${PIPESTATUS[0]}"
set -e

summary_line="$(grep -m1 '^soak summary: ' "$out_file" | sed 's/^soak summary: //' || true)"

if [ "$status" -ne 0 ]; then
  die "soak FAILED (exit $status) — resource leak or invariant violation. See output above."
fi
[ -n "$summary_line" ] || die "soak exited 0 but printed no summary line (driver bug?)"

log "soak summary: $summary_line"

# Re-emit as an evidence ledger (verdict PASS only because exit 0 AND a summary
# exists; the multi-hour gate is still owner-observed).
verdict="PASS"
[ "$SECONDS_ARG" -lt 600 ] && verdict="PASS-SMOKE"
if [ -n "${WAL_M9_EVIDENCE:-}" ]; then
  WAL_M8_EVIDENCE="$WAL_M9_EVIDENCE" "$ROOT/scripts/m8/evidence.sh" emit \
    gate=soak seconds="$SECONDS_ARG" summary="@$(printf '%s' "$summary_line")" \
    verdict="$verdict" || log "evidence emission failed (non-fatal)"
fi

log "soak OK ($verdict)."
