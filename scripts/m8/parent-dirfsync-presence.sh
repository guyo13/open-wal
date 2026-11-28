#!/usr/bin/env bash
#
# parent-dirfsync-presence.sh — §8.4.1 F2 (PRIMARY, deterministic, FS-independent).
#
# The regression guard for "open makes a WAL directory it creates or cold-starts in
# durable by fsyncing the PARENT directory" (§8.4.1). Like dirfsync-presence.sh
# (§14.4d Tier 1) it asserts the SYSCALL TRACE, not data loss: journaling
# filesystems very likely persist a new directory's entry transitively, so a
# behavioural power-loss test would be a negative result (as §14.4d's was) and is
# deliberately not built.
#
# SCOPE / BOUNDARY: this proves the parent `fsync`s are ISSUED, on the right
# directory, in the right order. It does not prove durability itself.
#
# Method: strace `power_pull_workload` with `strace -y` (fd→path annotation) in
# three situations and check the ordered event stream:
#   1. NESTED   — `dir` = <root>/a/b/c, none of a, b, c exist:
#        each successful mkdir is followed by an fsync of THAT LEVEL'S PARENT
#        before the next mkdir; all of them precede the first `.wal` creation;
#        after the cold start's fsync of `dir`, one more fsync of `dir`'s parent
#        (so exactly two fsyncs of `dir`'s parent in total).
#   2. EXISTING-EMPTY — `dir` pre-created, empty:
#        no mkdir succeeds; exactly one fsync of `dir`'s parent, after the cold
#        start's fsync of `dir`.
#   3. REOPEN   — the log from (2) opened again:
#        ZERO fsyncs of `dir`'s parent (the steady-state open gains no syscall).
#
# Needs only strace + python3 — no privileges, no specific filesystem — so it runs
# per-PR (.github/workflows/ci.yml). A missing strace is a FAILURE, never a skip.
# Exit 0 = PASS, non-zero = FAIL.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

log() { printf '\033[1;34m[m8/parent-dirfsync-presence]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[m8/parent-dirfsync-presence] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

command -v strace >/dev/null 2>&1 || die "strace not installed (apt-get install -y strace) — this gate FAILS without it, it does not skip"
command -v python3 >/dev/null 2>&1 || die "python3 not installed — needed to check the ordered syscall trace"

log "building power_pull_workload (shipping configuration)…"
( cd "$REPO_ROOT" && cargo build --quiet --bin power_pull_workload )
BIN="$WORK/workload"
cp "$REPO_ROOT/target/debug/power_pull_workload" "$BIN"

ROOT="$(realpath "$WORK")/root"
mkdir -p "$ROOT"

# Trace one workload run on $2, writing records up to LSN $3, into
# $WORK/trace_$1.txt. Small counts: one segment, a few commits. (The reopen case
# asks for a higher total so the reopened log is actually appended to and synced.) mkdir/mkdirat cover both syscall ABIs (x86_64 / aarch64); openat
# is traced to see the first `.wal` creation.
trace_run() {
  local tag="$1" dir="$2" total="$3"
  strace -y -f -e trace=mkdir,mkdirat,openat,fsync,fdatasync -o "$WORK/trace_${tag}.txt" \
    "$BIN" "$dir" stdout "$total" 4 16 >/dev/null 2>&1 \
    || die "${tag}: the traced workload exited non-zero (see $WORK/trace_${tag}.txt)"
}

# check <case> <trace> <dir>: parse the trace into ordered events and assert.
check() {
  python3 - "$1" "$2" "$3" <<'PY'
import os, re, sys

case, trace, wal_dir = sys.argv[1], sys.argv[2], sys.argv[3]
parent = os.path.dirname(wal_dir)

MKDIR = re.compile(r'\bmkdir(?:at)?\((?:AT_FDCWD(?:<[^>]*>)?, )?"([^"]+)", [^)]*\)\s+= 0$')
FSYNC = re.compile(r'\bfsync\(\d+<([^>]+)>\)\s+= 0$')
FDATA = re.compile(r'\bfdatasync\(\d+<([^>]+\.wal)>\)\s+= 0$')
WALCREATE = re.compile(r'\bopenat\([^"]*"([^"]+\.wal)", [^)]*O_CREAT[^)]*\)\s+= \d+')
WALOPEN = re.compile(r'\bopenat\([^"]*"([^"]+\.wal)", [^)]*\)\s+= \d+')

events = []  # (kind, path) in trace order
for line in open(trace, encoding="utf-8", errors="replace"):
    line = line.rstrip("\n")
    for kind, rx in (("mkdir", MKDIR), ("fsync", FSYNC), ("fdatasync", FDATA),
                     ("walcreate", WALCREATE), ("walopen", WALOPEN)):
        m = rx.search(line)
        if m:
            events.append((kind, os.path.normpath(m.group(1))))
            break

def fail(msg):
    print(f"FAIL [{case}]: {msg}", file=sys.stderr)
    print(f"  dir    = {wal_dir}\n  parent = {parent}", file=sys.stderr)
    print("  ordered events:", file=sys.stderr)
    for i, (k, p) in enumerate(events):
        print(f"    {i:3d} {k:9s} {p}", file=sys.stderr)
    sys.exit(1)

def idx(kind, path, start=0, end=None):
    """Indices of (kind, path) events within [start, end)."""
    end = len(events) if end is None else end
    return [i for i in range(start, end) if events[i] == (kind, path)]

# Anti-vacuous: the run really synced segment data inside `dir`, so the trace
# filter and the path annotation work and a zero count below means something.
if not any(k == "fdatasync" and os.path.dirname(p) == wal_dir for k, p in events):
    fail("no fdatasync on a .wal file inside dir — workload/trace broken, counts are meaningless")

mkdirs = [(i, p) for i, (k, p) in enumerate(events) if k == "mkdir"]
creates = [i for i, (k, p) in enumerate(events) if k == "walcreate" and os.path.dirname(p) == wal_dir]
parent_fsyncs = idx("fsync", parent)
dir_fsyncs = idx("fsync", wal_dir)

if case == "nested":
    root = os.path.dirname(os.path.dirname(parent))  # dir = root/a/b/c
    want = [os.path.join(root, "a"), os.path.join(root, "a", "b"), wal_dir]
    if [p for _, p in mkdirs] != want:
        fail(f"expected successful mkdirs {want} top-down, got {[p for _, p in mkdirs]}")
    if not creates:
        fail("no .wal file was created inside dir (expected a cold start)")
    first_create = creates[0]
    for n, (i, level) in enumerate(mkdirs):
        nxt = mkdirs[n + 1][0] if n + 1 < len(mkdirs) else first_create
        if nxt < i:
            fail(f"the first .wal creation precedes mkdir {level}")
        level_parent = os.path.dirname(level)
        if not idx("fsync", level_parent, i + 1, nxt):
            what = "the next mkdir" if n + 1 < len(mkdirs) else "the first .wal creation"
            got = [f"{k} {p}" for k, p in events[i + 1:nxt]]
            fail(f"mkdir {level} is not followed by fsync of its PARENT {level_parent} "
                 f"before {what} (between them: {got})")
    if not dir_fsyncs:
        fail("no fsync of dir itself (the cold start's directory fsync)")
    cold = [i for i in dir_fsyncs if i > first_create]
    if not cold:
        fail("no fsync of dir after the first .wal creation")
    if not idx("fsync", parent, cold[0] + 1):
        fail(f"no fsync of dir's parent {parent} AFTER the cold start's fsync of dir")
    if len(parent_fsyncs) != 2:
        fail(f"expected exactly 2 fsyncs of dir's parent (one for creating dir, one "
             f"after the cold start), got {len(parent_fsyncs)}")
elif case == "existing-empty":
    if mkdirs:
        fail(f"unexpected successful mkdir on a pre-existing dir: {[p for _, p in mkdirs]}")
    if not creates:
        fail("no .wal file was created inside dir (expected a cold start)")
    cold = [i for i in dir_fsyncs if i > creates[0]]
    if not cold:
        fail("no fsync of dir after the first .wal creation")
    if len(parent_fsyncs) != 1:
        fail(f"expected exactly 1 fsync of dir's parent {parent} (the cold-start "
             f"parent fsync), got {len(parent_fsyncs)}")
    if parent_fsyncs[0] < cold[0]:
        fail("the fsync of dir's parent precedes the cold start's fsync of dir")
elif case == "reopen":
    if mkdirs:
        fail(f"unexpected successful mkdir on reopen: {[p for _, p in mkdirs]}")
    if creates:
        fail("a .wal file was created on reopen — this was a cold start, not a reopen")
    if not any(k == "walopen" and os.path.dirname(p) == wal_dir for k, p in events):
        fail("no existing .wal file was opened — not a reopen of an existing log")
    if parent_fsyncs:
        fail(f"reopening an existing log issued {len(parent_fsyncs)} fsync(s) of dir's "
             f"parent {parent}; the steady-state open must issue ZERO")
else:
    fail(f"unknown case {case}")

print(f"ok [{case}]: mkdirs={len(mkdirs)} fsync(dir)={len(dir_fsyncs)} "
      f"fsync(parent)={len(parent_fsyncs)}", file=sys.stderr)
PY
}

fail=0

log "case 1 NESTED: dir = <root>/a/b/c, nothing below <root> exists"
NESTED="$ROOT/a/b/c"
trace_run nested "$NESTED" 8
check nested "$WORK/trace_nested.txt" "$NESTED" || fail=1

log "case 2 EXISTING-EMPTY: dir pre-created and empty"
EXISTING="$ROOT/pre/wal"
mkdir -p "$EXISTING"
trace_run existing "$EXISTING" 8
check existing-empty "$WORK/trace_existing.txt" "$EXISTING" || fail=1

log "case 3 REOPEN: the log from case 2, opened again"
trace_run reopen "$EXISTING" 16
check reopen "$WORK/trace_reopen.txt" "$EXISTING" || fail=1

[ "$fail" -eq 0 ] || die "§8.4.1 F2 (parent-directory fsync presence) FAILED — see the case output above."

log "PASS: created levels fsync their parents top-down before the first segment; every cold start fsyncs dir's parent; a reopen issues none."
