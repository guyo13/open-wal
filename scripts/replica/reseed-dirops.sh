#!/usr/bin/env bash
# open-wal-replica RM4 E9 — `Receiver::open_seeded`'s directory operations are
# durable and ordered (docs/replica_design_v1.md §10.1).
#
# Modelled on scripts/m8/dirfsync-presence.sh: strace two `#[ignore]`d targets
# of crates/open-wal-replica/tests/reseed.rs and check the ORDER of the
# syscalls the thread that ran `open_seeded` issued.
#
#   e9_target_set_aside   (a log that predates the snapshot is set aside)
#     1. every descriptor on the WAL's `LOCK` file is closed before the
#        `rename(dir, dir.stale.0)` — the WAL handle (and its directory lock)
#        is dropped first;
#     2. an `fsync` of the PARENT directory follows the rename, before `dir`
#        is created again;
#     3. an `fsync` of the PARENT follows that `mkdir(dir)`, before the first
#        segment file of the new log is created.
#   e9_target_missing_dir (a new replica: `dir` does not exist)
#     4. an `fsync` of the PARENT follows the `mkdir(dir)`, before the first
#        segment file is created; and nothing is renamed.
#
# SCOPE (same boundary as the §14.4d Tier-1 check): this proves the syscalls
# are ISSUED, on the right descriptors, in the right order. It does not prove
# a filesystem loses the entry without them.
#
# Needs only strace + python3 (no privileges, no specific filesystem).
# Exit 0 = PASS, 1 = FAIL, 77 = SKIPPED (strace unavailable — a skip is NOT a
# pass; CI installs strace, so a skip there is a red build).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

log() { printf '\033[1;34m[replica/reseed-dirops]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[replica/reseed-dirops] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }

if ! command -v strace >/dev/null 2>&1; then
  printf '\033[1;33m[replica/reseed-dirops] ##### SKIPPED — E9 NOT EXERCISED: strace is not installed (apt-get install -y strace). A skip is not a pass. #####\033[0m\n' >&2
  exit 77
fi
command -v python3 >/dev/null 2>&1 || die "python3 is needed to check the trace"

log "building the test binary (no trace)"
BIN="$(cd "$REPO_ROOT" && cargo test -p open-wal-replica --test reseed --no-run --message-format=json 2>/dev/null \
  | python3 -c '
import json, sys
exe = None
for line in sys.stdin:
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if m.get("reason") == "compiler-artifact" and m.get("target", {}).get("name") == "reseed" and m.get("executable"):
        exe = m["executable"]
print(exe or "")')"
[ -n "$BIN" ] && [ -x "$BIN" ] || die "could not locate the reseed test binary"

# Trace one target. $1 = test name. Echoes the parent directory it ran in.
trace_target() {
  local name="$1"
  local parent
  parent="$(realpath "$WORK")/$name"
  mkdir -p "$parent"
  # -ff: one file per thread (the order within the thread that ran
  # `open_seeded` is exact); -y: annotate descriptors with their paths.
  RESEED_E9_DIR="$parent" strace -f -ff -y -o "$WORK/trace.$name" \
    -e trace=open,openat,creat,close,rename,renameat,renameat2,mkdir,mkdirat,fsync,fdatasync \
    "$BIN" --ignored --exact "$name" --test-threads=1 >"$WORK/out.$name" 2>&1 \
    || { cat "$WORK/out.$name" >&2; die "$name: the traced test itself failed"; }
  grep -q "test result: ok. 1 passed" "$WORK/out.$name" \
    || { cat "$WORK/out.$name" >&2; die "$name: the traced test did not run (expected exactly 1 passed)"; }
  echo "$parent"
}

check() {
  # $1 = mode (set_aside | missing_dir), $2 = parent dir, $3 = trace prefix
  python3 - "$1" "$2" "$3" <<'PY'
import glob, re, sys

mode, parent, prefix = sys.argv[1], sys.argv[2], sys.argv[3]
d = parent + "/wal"
D, P = re.escape(d), re.escape(parent)
PAT = {
    "lock_open": re.compile(r'^open(at)?\(.*"%s/LOCK".*\) = \d+' % D),
    "lock_close": re.compile(r'^close\(\d+<%s/LOCK>\) = 0' % D),
    "rename": re.compile(r'^rename(at2?)?\(.*"%s", .*"%s\.stale\.\d+".*\) = 0' % (D, D)),
    "mkdir": re.compile(r'^mkdir(at)?\(.*"%s", .*\) = 0' % D),
    "parent_fsync": re.compile(r'^fsync\(\d+<%s>\) = 0' % P),
    "seg_create": re.compile(r'^(open(at)?\(.*"%s/\d+\.wal".*O_CREAT.*\)|creat\("%s/\d+\.wal".*\)) = \d+' % (D, D)),
}

def events(path):
    out = []
    for line in open(path, errors="replace"):
        for kind, pat in PAT.items():
            if pat.match(line):
                out.append((kind, line.rstrip()))
    return out

anchor = "rename" if mode == "set_aside" else "mkdir"
threads = [ev for ev in map(events, sorted(glob.glob(prefix + ".*"))) if any(k == anchor for k, _ in ev)]
if len(threads) != 1:
    sys.exit("FAIL: expected exactly one thread with a %s of %s, found %d (trace broken?)" % (anchor, d, len(threads)))
ev = threads[0]
kinds = [k for k, _ in ev]

def fail(msg):
    print("---- events of the traced thread ----", file=sys.stderr)
    for i, (k, line) in enumerate(ev):
        print("%3d %-12s %s" % (i, k, line), file=sys.stderr)
    sys.exit("FAIL (%s): %s" % (mode, msg))

def first(kind, after=-1):
    for i in range(after + 1, len(kinds)):
        if kinds[i] == kind:
            return i
    return None

def between(kind, lo, hi):
    return any(kinds[i] == kind for i in range(lo + 1, hi))

if mode == "set_aside":
    if kinds.count("rename") != 1:
        fail("expected exactly one rename of the WAL directory, saw %d" % kinds.count("rename"))
    r = first("rename")
    opens, closes = kinds[:r].count("lock_open"), kinds[:r].count("lock_close")
    # build_log's open + open_seeded's first open.
    if opens < 2:
        fail("anti-vacuity: expected >= 2 opens of LOCK before the rename, saw %d" % opens)
    if opens != closes:
        fail("the WAL's LOCK descriptor is still open at the rename (%d opened, %d closed before it): "
             "the WAL handle must be dropped before the directory is renamed" % (opens, closes))
    m = first("mkdir", r)
    if m is None:
        fail("no mkdir of the WAL directory after the rename")
    if not between("parent_fsync", r, m):
        fail("no fsync of the parent directory between the rename and the mkdir: the rename is not durable")
else:
    if "rename" in kinds:
        fail("a missing directory must not cause a rename")
    m = first("mkdir")

s = first("seg_create", m)
if s is None:
    fail("anti-vacuity: no segment file was created after the mkdir")
if not between("parent_fsync", m, s):
    fail("no fsync of the parent directory between the mkdir and the first segment file: "
         "the new log's directory entry is not durable")
print("PASS (%s): %d relevant syscalls; parent fsyncs: %d" % (mode, len(ev), kinds.count("parent_fsync")))
PY
}

log "E9: tracing the set-aside run"
p1="$(trace_target e9_target_set_aside)"
check set_aside "$p1" "$WORK/trace.e9_target_set_aside" || die "set-aside run: see above"

log "E9: tracing the missing-directory run"
p2="$(trace_target e9_target_missing_dir)"
check missing_dir "$p2" "$WORK/trace.e9_target_missing_dir" || die "missing-directory run: see above"

log "PASS: LOCK closed before the rename; parent fsync after the rename and after each mkdir, before the first segment file."
