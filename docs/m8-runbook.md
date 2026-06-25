# M8 — Hardware / Platform Durability Runbook (§14.8 + §14.4d)

This is the owner-facing runbook for the M8 gates that **cannot be honestly
self-certified in a sandbox**. It tells you exactly what to run, on what hardware,
and how to read the result. Every gate here is built and wired; the ones that need
real (or properly-cache-configured virtual) hardware are marked
**OPEN-pending-owner-run** and are verified by *you*, not by the agent.

> **THE #1 RULE — the vacuous-pass guard.** A durability test on storage where
> un-synced data is **not actually lost** passes *vacuously*: the data was never at
> risk. A green result on non-durable storage is the worst possible outcome. Every
> physical gate below first establishes that it runs on storage that genuinely loses
> un-synced data, and **fails loudly** otherwise. Never relax this.

## Component tiers at a glance

| Gate | What it proves | Runs in sandbox? | Status |
|---|---|---|---|
| **H2** cache-mode / lying-device | storage genuinely loses un-synced data | static part: **yes** | guard built; empirical probe owner-run |
| **H3 (§12 state machine)** | WAL poisons on fsync EIO (logic) | **yes — RUN+green** | `scripts/m8/fsync-fault.sh` passes here |
| **H3 (physical)** | block-layer fsync-failure → poison | no (needs dm) | **OPEN-pending-owner-hardware** |
| **§14.4d** Tier-1 dir-fsync presence | correct issues roll-time dir-fsync, inject does not (strace) | **yes — RUN+green** | `scripts/m8/dirfsync-presence.sh`, per-PR (`ci.yml`) |
| **§14.4d** Tier-2 behavioral (ext2) | inject build loses a rolled segment under power loss | no (needs dm; ext2 only) | **OPEN — Phase-1 expected-to-reproduce** |
| **§14.4d** Tier-3 (ext4/xfs/btrfs) | — | n/a | **INCONCLUSIVE-by-design** (journaling masks it) |
| **H1** power-pull | committed records survive a real cut (D1) | no (needs a cuttable target) | **OPEN-pending-owner-run** |
| **H4** macOS `F_FULLFSYNC` | macOS durable path issues `F_FULLFSYNC` | no (Linux host) | **OPEN-pending-macOS** |

The §12 state-machine shim and the H2 static guard **run green in CI/sandbox**
(`scripts/m8/fsync-fault.sh`, `scripts/m8/storage-check.sh`). Everything else is
your hardware run.

### CI automation (what now runs without your hardware)

Two gates that the build sandbox could not run are now **automated on hosted CI**
(nightly + manual), so you no longer drive them by hand for the common case:

| Workflow | Gate | Runner | Notes |
|---|---|---|---|
| `.github/workflows/ci.yml` (`dirfsync-presence`) | **§14.4d Tier-1** (dir-fsync presence) | `ubuntu-latest` | **per-PR**, deterministic, `strace` only — correct issues the roll-time dir-fsync, inject does not. |
| `.github/workflows/m8-dmflakey.yml` | **H3-physical (#16)** + **§14.4d Tier-2/3 (#17)** | `ubuntu-latest` | hosted VMs reach `dm-flakey`; H3 ext4 is the hard gate; §14.4d **certifies on ext2** (journal-less), ext4 is INCONCLUSIVE-by-design, xfs/btrfs informational. **Best-effort + loud skip** if a runner image lacks dm-flakey (gate stays OPEN, never faked). |
| `.github/workflows/m8-macos.yml` | **H4 Half A (#19)** | `macos-latest` | `cargo test --test macos_fullfsync` (routing/smoke). **Half B** (the `dtruss` trace, root + SIP) stays owner-run below. |

Both emit the §5 evidence ledger as a workflow artifact on **every** run, and post it
to the gate's issue **only on a manual `workflow_dispatch`** — the human sign-off
trail. A nightly regression is surfaced by the **red build**, not an issue comment
(quiet on the issue, loud as a build). To produce a durable sign-off comment for a
DoD flip, trigger the workflow manually (`workflow_dispatch`) and point to that run.

> **A green `m8-dmflakey` run is NOT automatically a passed gate.** If the run
> carries the loud `::warning:: dm-flakey unavailable` annotation, the gate **did not
> run** (best-effort + loud skip) and **#16/#17 stay OPEN** — the green check only
> means "nothing failed," not "the fault was injected." A gate closes only on a run
> whose log shows the actual injection: H3 ext4 **PASS** with a **source-confirmed
> block-layer EIO** (`detail.block_layer_eio_observed: 1` in the evidence), and the
> §14.4d behavioral control **on ext2** (the journal-less certifying fs) **PASS** with
> the **positive control live** (`detail.drop_positive_control: "pass"`). §14.4d on
> **ext4 is INCONCLUSIVE-by-design** (journaling masks the omission) and certifies
> nothing on its own — its deterministic guard is the per-PR Tier-1 strace presence
> check (`ci.yml`). An `INCONCLUSIVE` (timing) or a §14.4d `HARNESS_FAIL` (exit 4 —
> `drop_writes` not dropping) likewise certifies nothing. Read the uploaded evidence
> JSON, not just the check colour.

**Still owner-run (this runbook):** **H1** power-pull (needs a cuttable target), the
**H2 empirical loss-probe**, and **H4 Half B** (`dtruss`). The dm-flakey gates can
also still be run by hand here (e.g. on xfs/btrfs, or to certify on your own kernel).

---

## H2 — storage durability guard (precondition for H1)

`scripts/m8/storage-check.sh` is **deny-by-default**: it certifies storage only if
it affirmatively recognizes a real, durable, block-backed filesystem. tmpfs /
ramfs / overlay / 9p / virtiofs / NFS ⇒ **FAIL**. An unrecognized FS or an
unreadable cache mode ⇒ **INCONCLUSIVE ⇒ FAIL** (the one config nobody anticipated
is exactly where a blocklist fails).

```bash
scripts/m8/storage-check.sh classify /path/to/wal_dir   # static classification
```

It reports the block device and `write_cache` mode and labels the risk:
- `write back` — volatile device cache present; durable **only** if the device
  honours flushes (power-loss-protected / honest hardware). A consumer SSD/HDD that
  lies about flush **will lose acked data**.
- `write through` — no volatile device cache; still verify the virtualization layer
  (host `cache=none`/`writethrough` for VM targets).

### The empirical loss probe (the real guard — owner-run across a cut)

Static classification is necessary but not sufficient. Prove the device actually
loses un-synced data:

```bash
scripts/m8/storage-check.sh probe-write  /path/to/wal_dir   # writes an UN-synced marker
#   --- HARD power cut now (see H1 cut mechanisms) ---  then reboot
scripts/m8/storage-check.sh probe-verify /path/to/wal_dir   # marker MUST be gone
```

If the marker **survives**, the storage does not lose un-synced data — **STOP**, any
H1 result on it would be vacuous. Label such targets "PLP-cache only" and do not
make a durability claim for honest power loss on them.

---

## H3 — fsync-failure → poison (§12)

### H3 state-machine half — RUNS HERE (green)

```bash
scripts/m8/fsync-fault.sh        # builds the LD_PRELOAD EIO shim + runs the gate
```

This LD_PRELOADs `tests/fault/eio_preload.c`, which makes the WAL's commit data
sync (libc `fdatasync`) return EIO on demand, and asserts the §12 poison state
machine: `commit` surfaces `FsyncFailed`, `durable_lsn` does not advance past the
last synced segment (including the **split-batch** partial-advance, where
`durable_lsn` rests at segment 1's max), and the handle **poisons** so subsequent
`append`/`commit` return `Poisoned`. An anti-vacuous guard asserts the shim
actually injected an EIO — running without the shim **fails loudly**, never passes.

**Scope (do not oversell):** this is an *application-logic* test. The shim returns a
*fake* EIO with the data still in cache. It proves "we poison on EIO" — **not** "we
correctly treat the data as already-gone" (the Linux fsyncgate property, where a
failed `fsync` may have already discarded the dirty pages). That second property is
validated only by the physical gate below. The shim also only intercepts libc
`fdatasync`; the WAL's directory fsync uses rustix raw syscalls and is not
interceptable here (verified: `strace` shows 6 `fdatasync` + 3 `fsync`; the libc
shim catches all 6 `fdatasync`, zero `fsync`).

### H3 physical half — OWNER-RUN (dm-flakey)

On a privileged Linux host with `CONFIG_BLK_DEV_DM` + the `dm-flakey` target:

```bash
scripts/m8/dm-flakey.sh check        # confirms dm-flakey is available (loud OPEN if not)
sudo scripts/m8/dm-flakey.sh h3      # physical fsync-failure → poison
```

This builds a loop-backed ext4 on a `dm-flakey` device, runs a committing workload,
then flips the device to `error_writes` mid-run. The next commit's `fdatasync` (and,
because this is at the **block layer**, the rustix directory fsync too) gets EIO, so
the workload exits **7** = poisoned (§12 upheld physically). Run it on ext4/xfs/btrfs.

> This sandbox has **no device-mapper** (`CONFIG_BLK_DEV_DM` absent, no
> `/lib/modules`, `/dev/mapper/control` missing), so `h3`/`check` print the loud
> "NOT EXERCISED" banner and exit non-zero here. **Status: OPEN-pending-owner-hardware.**

---

## §14.4d — dir-fsync omission negative control (THREE TIERS)

A build that **skips the directory fsync** on roll (`--features inject_no_dir_fsync`)
must be **detectable**. The naive "it loses data on a power cut" form turned out to
be **not reproducible on any mainstream Linux journaling filesystem** (ext4/xfs/btrfs):
`fsync`ing the new segment file forces a journal/log commit whose running transaction
already contains the directory entry that created it, so the dirent reaches disk
transitively even though POSIX never promised it (*All File Systems Are Not Created
Equal*, OSDI '14). The `fsync_dir` is therefore a **portable-durability safeguard**
(ext2, `data=writeback`, non-Linux/networked FSes don't give the transitive guarantee)
and is **kept unconditionally**. The control is split three ways:

### Tier 1 — syscall presence (PRIMARY, deterministic, per-PR, no privileges)
The reliable regression guard is "deleting `fsync_dir` changes the syscall trace,"
not "it loses data." Runs on every PR (`.github/workflows/ci.yml`), needs only `strace`:

```bash
scripts/m8/dirfsync-presence.sh   # no root, no dm-flakey
```

It straces the roll path of both builds and asserts the **correct** build issues a
directory `fsync` per roll (cold-start + one per roll) while the **inject** build
issues only the cold-start one. Deterministic green: e.g. `correct=5` vs `inject=1`.
This is the dir-fsync analogue of the H4 `F_FULLFSYNC` presence check.

### Tier 2 — behavioral power-loss on ext2 (OWNER-RUN / CI; the only FS that shows it)
ext2 is **journal-less**, so `fdatasync(segment)` does **not** transitively persist the
parent dirent — the one substrate where the omission is behaviorally observable:

```bash
sudo -E scripts/m8/dm-flakey.sh dirfsync-negative ext2
```

> **Note on `sudo` + rustup:** `sudo` drops the invoking user's `~/.cargo/bin` from
> root's `PATH`, so a naive run hits `cargo: command not found`. The harness handles
> this — when `cargo` isn't on `PATH` it builds **as `$SUDO_USER`** (their rustup
> toolchain; artifacts land in `target/` and root just execs them). So `sudo -E
> scripts/m8/dm-flakey.sh …` works; no need to put cargo on root's PATH.

The harness uses a **synchronized mid-run cut** (`src/bin/dirfsync_cut_workload.rs`):
the workload rolls **once**, puts an acked record in the brand-new segment, signals
ready off-device, and **blocks** holding that segment's directory entry un-synced. The
harness then activates `drop_writes` *before* killing the workload / unmounting (so no
writeback can beat the cut), runs `fsck.ext2 -y`, remounts, and verifies. **Expected:**
correct → PASS, inject → FAIL ⇒ negative control demonstrated. **Phase-1 status:
expected to reproduce — verify** (observe a first PASS before hard-gating; until then an
INCONCLUSIVE is a warning, not red).

> **Why a synchronized cut (the key fix):** a run-to-completion workload cut *afterward*
> does **not** reproduce the omission even on ext2 — by the cut, every rolled dirent has
> aged out and been written back (`vm.dirty_expire_centisecs` default 30 s; observed
> empirically on PR #21 run 28193051238 with the positive control LIVE). Blocking the
> workload right after the roll keeps the dirent dirty with ~30 s of slack, so the cut
> lands *inside* the un-synced window at leisure — converting a sub-millisecond race
> into a robust gate. If it still doesn't reproduce after this, **stop and report** —
> that's a real finding, not something to tune blindly.

### Tier 3 — ext4/xfs/btrfs: INCONCLUSIVE-by-design
```bash
sudo scripts/m8/dm-flakey.sh dirfsync-negative ext4   # expected INCONCLUSIVE (journaling masks it)
```
Run for evidence and to catch a genuine **correct-build** regression (a correct-build
data loss is still a real FAIL). A non-failing **inject** build here is **expected** —
never read it as "dir-fsync omission is harmless." The deterministic guard for these is
Tier 1.

**Status:** Tier 1 **PASSES** (deterministic, per-PR). Tier 2 (ext2) **OPEN — Phase-1
expected-to-reproduce, owner/CI to observe a first PASS**. The agent never self-certifies
the behavioral tiers from a sandbox without dm-flakey.

---

## H1 — power-pull (the only true durability test; OWNER-RUN, ≥50 cycles)

**Topology.** The WAL host runs a committing workload that mirrors each ACKED
watermark to a side channel that is durable **independently of the at-risk device**
— the default is a **network sink** (TCP to another host), durable *by construction*
(once a line is on the other host, cutting power here cannot un-record it). Serial
and separate-block-device sinks exist as alternatives, but then *you* must guarantee
that channel survives the same cut.

```
[WAL host — gets cut]  power_pull_workload  --(seq,watermark per ack)-->  TCP
[external host]        receiver: socat/nc append to capture.txt (durable off-box)
[after cut + reboot]   power_pull_verify <wal_dir> capture.txt  → asserts D1/D6
```

**Correctness rules baked into the harness (so H1 can't lie):**
- The workload records a watermark **strictly after `commit()` returns `Ok(w)`** —
  never an appended-but-unconfirmed LSN. A partially-failed split commit returns
  `Err` and records nothing, which only makes the side channel *understate* the
  durable set (the safe direction). So the verify check is one-directional: every
  acked LSN ≤ the side-channel high-water **must** be present; extra recovered
  records are fine.
- Each line is `seq,watermark` with a contiguous `seq`. The verifier uses the
  watermark of the highest **contiguous** seq; any gap (a lost line — only possible
  on a lossy transport) is **INCONCLUSIVE**, never a silently-lower bar. Prefer TCP.
- **H1 still gates on H2** regardless of the side channel: a perfect sink doesn't
  save you if the WAL storage wasn't actually durable (then everything survives and
  H1 "passes" having tested nothing). `power-pull.sh workload` runs the H2
  classification first and refuses non-durable storage.

### Procedure

```bash
scripts/m8/power-pull.sh cycle      # prints the full ≥50-cycle procedure
```

One-time on the target: pick a durable WAL dir and pass the **H2 empirical probe**
(above). On the external host: `scripts/m8/power-pull.sh receiver 9099 capture.txt`.

Each cycle (≥50×):
1. `scripts/m8/power-pull.sh workload <wal_dir> tcp:<external>:9099 0 64 64`
2. Let it commit, then **HARD-CUT power** (see mechanisms below).
3. Reboot; `scripts/m8/power-pull.sh verify <wal_dir> capture.txt`.
   - exit 0 = PASS (every acked LSN survived), 1 = FAIL (acked loss — D1 violation),
     2 = INCONCLUSIVE (side-channel gap; re-run).

**PASS the gate** only after **≥50 consecutive cycles with zero FAIL.** Record the
device, cache mode, and cut mechanism.

### Cut mechanisms and their fidelity

| Mechanism | Fidelity | Notes |
|---|---|---|
| PDU outlet off / physical unplug | **best** | true loss of device cache |
| Hypervisor force-stop (`virsh destroy`, cloud force-stop) | **good** *iff* guest cache is write-through / `cache=none` | verify with H2; otherwise the host cache hides loss |
| **`echo b > /proc/sysrq-trigger`** | **NOT VALID** | a *warm reboot*: does **not** clear the device cache or model power loss; un-synced-but-cached data often survives ⇒ a **vacuous H1**. Use only to test the reboot/recovery path, never as the cut. |
| `reboot` / `shutdown` | **NOT VALID** | graceful; flushes caches. |

**Status: OPEN-pending-owner-run** on real (or properly cache-configured virtual)
hardware.

---

## H4 — macOS `F_FULLFSYNC` (OWNER-RUN on macOS)

On macOS/APFS a plain `fsync` does not flush the drive cache; durability requires
`fcntl(fd, F_FULLFSYNC)` (§8.3). The WAL routes every durable sync through
`segment::sync_data_fully`, which is `fcntl_fullfsync` on macOS.

- Smoke (plain macOS CI): `tests/macos_fullfsync.rs::commit_is_durable_on_macos`.
- Syscall-trace proof (root): run a commit under `dtruss -t fcntl` and confirm an
  `F_FULLFSYNC` fcntl appears. **Note:** on a SIP-enabled host dtruss does **not**
  symbolize the fcntl command — it prints the raw number `0x33` (== 51 ==
  `F_FULLFSYNC`, per `<sys/fcntl.h>`), so grep for both:

```bash
# automated (matches symbolic name OR the numeric 0x33 command):
sudo cargo test --test macos_fullfsync -- --ignored --nocapture
# or manual:
sudo dtruss -t fcntl target/debug/power_pull_workload /tmp/h4wal stdout 8 8 64 2>&1 \
  | grep -E 'F_FULLFSYNC|, 0x33,'
# expect lines like:  fcntl(0x4, 0x33, 0x0)  = 0 0
```

**Status: OPEN-pending-macOS** (cannot run on a Linux host; the cfg(macos) test does
not even compile on Linux — exercise it in macOS dev/CI).

---

## OS / FS matrix (§14.11)

Run the runnable physical gates (H3 physical, §14.4d) across **ext4 / xfs / btrfs**:

```bash
sudo scripts/m8/dm-flakey.sh h3 ext4
sudo scripts/m8/dm-flakey.sh h3 xfs
sudo scripts/m8/dm-flakey.sh h3 btrfs
sudo scripts/m8/dm-flakey.sh dirfsync-negative ext2   # §14.4d Tier-2: CERTIFY on ext2 (journal-less)
sudo scripts/m8/dm-flakey.sh dirfsync-negative ext4   # §14.4d Tier-3: INCONCLUSIVE-by-design (journaling masks it)
```

**tmpfs is logic-only and must NEVER carry a durability claim** — the H2 guard
enforces this by failing on tmpfs.
