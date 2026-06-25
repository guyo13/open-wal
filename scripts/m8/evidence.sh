#!/usr/bin/env bash
#
# evidence.sh — M8 §5 evidence-ledger emitter.
#
# Every M8 gate emits a machine-readable JSON result (schema: docs/m8-infra-plan.md
# §5) so the §14.13 DoD is satisfied by ARTIFACTS, not checkboxes. This helper
# assembles that JSON. It is reused by the dm-flakey gates (scripts/m8/dm-flakey.sh)
# and is shaped to also serve the future H1 self-hosted rig.
#
# python3 builds the JSON (escaping handled ⇒ always valid output), and the same
# python3 is the validator the verification step runs (`python3 -m json.tool`).
#
# Usage:
#   scripts/m8/evidence.sh emit [out=PATH] KEY=VALUE ...
#     - out=PATH (or $WAL_M8_EVIDENCE) is the output file; default: stdout.
#     - KEY may be dotted to NEST: `storage.fs=ext4` ⇒ {"storage":{"fs":"ext4"}}.
#     - VALUE typing: bare integers and true/false are emitted UNQUOTED; a value
#       prefixed with '@' is a JSON literal passed through verbatim
#       (e.g. `run.per_cycle=@[0,0,2,0]`); everything else is a JSON string.
#     - `timestamp` defaults to UTC-now (ISO-8601) if not supplied.
#
# Example:
#   scripts/m8/evidence.sh emit out=ev.json gate=H3-physical \
#     storage.fs=ext4 cut.mechanism="dm-flakey error_writes" cut.valid=true \
#     run.cycles_required=1 run.cycles_pass=1 verdict=PASS
set -euo pipefail

log() { printf '\033[1;34m[m8/evidence]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[m8/evidence] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

cmd_emit() {
  command -v python3 >/dev/null 2>&1 || die "python3 is required to assemble the evidence JSON"
  local out="${WAL_M8_EVIDENCE:-}"
  local -a pairs=()
  local kv
  for kv in "$@"; do
    case "$kv" in
      out=*) out="${kv#out=}" ;;
      *=*)   pairs+=("$kv") ;;
      *)     die "argument '$kv' is not KEY=VALUE" ;;
    esac
  done

  local json
  json="$(python3 - ${pairs[@]+"${pairs[@]}"} <<'PY'
import sys, json, datetime, re

obj = {}

def set_nested(d, dotted, value):
    parts = dotted.split('.')
    for p in parts[:-1]:
        d = d.setdefault(p, {})
        if not isinstance(d, dict):
            raise SystemExit(f"evidence: key path '{dotted}' collides with a scalar")
    d[parts[-1]] = value

def coerce(v):
    if v.startswith('@'):                 # raw JSON literal pass-through
        return json.loads(v[1:])
    if re.fullmatch(r'-?\d+', v):         # bare integer
        return int(v)
    if v in ('true', 'false'):            # bare bool
        return v == 'true'
    return v                              # JSON string

for arg in sys.argv[1:]:
    k, _, v = arg.partition('=')
    set_nested(obj, k, coerce(v))

obj.setdefault(
    'timestamp',
    datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
)
print(json.dumps(obj, indent=2, ensure_ascii=False))
PY
)"

  if [ -n "$out" ]; then
    printf '%s\n' "$json" > "$out"
    log "wrote evidence: $out"
  else
    printf '%s\n' "$json"
  fi
}

case "${1:-}" in
  emit) shift; cmd_emit "$@" ;;
  *)    die "usage: $0 emit [out=PATH] KEY=VALUE ..." ;;
esac
