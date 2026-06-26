# M8 Hardware-Durability Test Automation — Build Plan

**Status:** plan for implementation. The agent builds the harnesses, scripts, workflows, and runbook entries described here; the **owner triggers** the hardware gate and **signs it off**. Nothing here is self-certified from a sandbox.

**Covers issues:** [#16](https://github.com/guyo13/open-wal/issues/16) (H3-physical), [#17](https://github.com/guyo13/open-wal/issues/17) (§14.4d), [#18](https://github.com/guyo13/open-wal/issues/18) (H1 — the centerpiece), [#19](https://github.com/guyo13/open-wal/issues/19) (H4). #16/#18 acceptance criteria have already been amended (see "Acceptance criteria") — build against the corrected criteria, not the originals.

---

## 0. Principles (carried from M8 — do not relax)

1. **Vacuous-pass guard everywhere.** Every physical gate first proves, empirically, that its storage genuinely loses un-synced data, and **fails loudly** otherwise. A green result on non-durable storage is the worst possible outcome.
2. **INCONCLUSIVE is never PASS.** A run that couldn't establish its precondition, didn't inject its fault, lost side-channel continuity, or whose target didn't come back is INCONCLUSIVE → re-run; it never counts toward a required total.
3. **Evidence or it didn't happen.** Every gate emits a machine-readable artifact (schema in §5) attached to its issue. The §14.13 DoD is satisfied by artifacts, not checkboxes.
4. **Flag, don't fake.** If a tier can't run honestly in its environment, it says so loudly and stays OPEN — never a fabricated green.
5. **Owner triggers, agent builds.** The GA H1 workflow is `workflow_dispatch` only, run by the owner when the rig is physically ready.

---

## Acceptance criteria (amended in the issues)

Issues #16 and #18 have already been amended with the corrected acceptance criteria below. Build against these, not the originals:
- **#18 (H1):** pass = "≥50 cycles, zero FAIL **and zero INCONCLUSIVE**, on a target where the H2 empirical loss-probe demonstrated un-synced data is actually lost, probe artifact attached." H2 is a *gating precondition*, not a step.
- **#16 (H3-physical):** the anti-vacuous guard — a clean exit with no EIO actually injected is INCONCLUSIVE, not PASS.

---

## 1. Tier 1 — dm-flakey CI (free): #16 H3-physical + #17 §14.4d

`ubuntu-latest` GitHub-hosted runners are full VMs with `CONFIG_BLK_DEV_DM` and root, so `dm-flakey` runs there — the agent's sandbox lacked it, hosted CI does not. This converts the two scariest metadata/physical gates into CI signal at zero hardware cost.

**Build:** `.github/workflows/m8-dmflakey.yml`
- Triggers: `schedule` (nightly) + `workflow_dispatch`. Not per-PR (timing-sensitive).
- Steps: load `dm_flakey` (`sudo modprobe dm-flakey`); if unavailable, **skip loudly** (annotation), do not pass. Build the crate; run `sudo scripts/m8/dm-flakey.sh h3 ext4` and `dirfsync-negative ext4`.
- **#16 gate:** assert the fault was actually injected (workload poisoned *because* an EIO reached it; a clean exit with no EIO = INCONCLUSIVE). ext4 required; xfs/btrfs informational.
- **#17 gate:** `correct → PASS, inject → FAIL` on **ext4**, with a **bounded retry budget** (reproduce within N=5 attempts; record attempts used). Budget exhausted = INCONCLUSIVE (cut-timing on the runner), never a code failure and never a PASS. Carry the FS-dependence warning: certify ext4 only.
- Emit the §5 evidence artifact (uploaded + attach-to-issue step).
- Job is informational **only for environment availability** (dm absent → skip). The injection-fired / asymmetry assertions inside a run that *does* execute are **hard failures**, never `continue-on-error`.

This is the highest-leverage first task: free, and it closes the holes no laptop can.

---

## 2. Tier 3 — macOS CI (free): #19 H4 Half A

**Build:** `.github/workflows/m8-macos.yml`, `runs-on: macos-latest`, `workflow_dispatch` + nightly.
- Runs `cargo test --test macos_fullfsync` (the cfg-routing assertion — durable path resolves to `fcntl_fullfsync`, no privileges needed).
- Closes the **routing half** of the §14.8 H4 DoD row. The `dtruss` trace half (root + SIP) stays owner-run/self-hosted per #19 Half B.
- Emit the §5 artifact (runner OS/version).

---

## 3. GA H1 rig (the centerpiece): #18

The only true durability test — a real power cut to a no-PLP consumer device, ≥50 cycles, zero acked loss. Built as a manually-triggered self-hosted CI workflow so the owner runs it when the rig is wired, and the result is a hard, evidenced gate.

### 3.1 Topology

```
[ Controller — NOT cut ]                       [ Target (DUT) — gets cut ]
 spare laptop                                   Raspberry Pi 3 (64-bit Pi OS)
  - GitHub self-hosted runner (x64)             - power_pull_workload on the DUT medium
  - off-box TCP collector (capture file)  <---- - streams seq,watermark over Ethernet (TCP)
  - smart-plug driver (local HTTP API)   ----►  - mains via smart plug
  - cycle orchestrator + evidence emitter  ssh  - WAL on a dedicated writable partition
  - cross-compiles ARM bins, scp to Pi    ----► - rootfs read-only (overlay) so cuts don't corrupt the OS
        |
   smart plug (local API) ── mains ──► power strip ──► Pi 3 PSU  + BeagleBone PSU
```

The smart plug is the single **cut point**, not a single PSU: it feeds a small power strip carrying a dedicated 5 V supply for each board, so one toggle cuts both (Pi and BeagleBone) at once and the campaign is hands-free across all three media without rewiring. The board not running the active cycle is simply power-cycled along with it — harmless (boards tolerate power cycles; the read-only overlay protects each OS).

Why this split: the controller hosts the GitHub runner and must never be cut (a cut would kill a runner mid-job). The Pi 3 is the target — no battery, single power input, boots from the canonical "lying" medium (SD card), has wired Ethernet for a deterministic side channel + ssh.

### 3.2 Target (Pi) setup — documented in the runbook, scripted where possible

- **64-bit Raspberry Pi OS**, **wired Ethernet** (not WiFi — WiFi reassociation after each boot adds latency and flakiness to the side channel and ssh).
- **Read-only rootfs (overlayfs)** via `raspi-config` → Performance → Overlay FS. This is essential: repeatedly power-cutting a Pi corrupts a writable rootfs within a handful of cycles. With overlay, the OS survives indefinitely.
- **Dedicated writable WAL partition = the DUT.** The only thing exposed to power-cut writes is the WAL under test, on its own ext4 partition. Provide for **three DUT media**, each a different device class: (a) a partition on the **microSD** (Pi), (b) the **USB-SSD** (Pi), (c) the **onboard eMMC** of the **BeagleBone Black**. Run the gate against each; record which. (eMMC is soldered, managed NAND with its own controller/cache — the most production-realistic embedded medium and the one whose flush honesty is most likely to surprise you, so it broadens the device-honesty coverage the rig produces.)
  - For the BeagleBone target, boot its **rootfs from microSD with the read-only overlay** (same cut-corruption protection as the Pi) and put the **WAL on a dedicated eMMC partition** as the DUT — so the eMMC is the thing under test and the OS isn't the thing 50 cuts corrupt. The BBB is ARMv7/battery-less with a single 5 V input, so it's a valid smart-plug cut target; it is **not** a controller candidate (32-bit → no GitHub runner), only a DUT.
- **DUT media are consumables.** The boards themselves shrug off power cycling (effectively unlimited at these counts). The flash media is the wear surface, and the binding risk is **not** write-endurance — 50 cycles write only single-digit GB, orders of magnitude under any card's lifetime — but **sudden FTL (flash-translation-layer) corruption on a mid-write cut**, which can brick a whole card. The per-cut probability is low and *device-quality-dependent* (a cheap no-name microSD is the most exposed; the read-only overlay already protects the boot/OS card regardless). So: keep **one or two spares of each DUT medium**, keep a **pre-imaged OS/boot card** so a brick is a 5-minute re-flash rather than a re-setup, and treat a **mid-campaign card death as a recordable device-honesty finding** (that card is empirically not honest hardware — exactly the verdict the gate exists to render), not a rig failure. Record it in the evidence artifact and continue on a spare.
- **Cross-compiled binaries**, built on the controller (`aarch64-unknown-linux-gnu`, MSRV 1.85, via `cross` or rustup target + linker) and `scp`'d to the Pi: `power_pull_workload`, `power_pull_verify`, and `storage_probe`. Do not build on the Pi (slow, and we want the runner to control versions). `scripts/m8/h1-cycle.sh deploy` performs this `scp` (plus `storage-check.sh` for the static FS/cache classification).
- Passwordless ssh from controller → Pi (key auth) for unattended cycling.

### 3.3 Controller setup

- **GitHub self-hosted runner** labeled `[self-hosted, h1-rig]`, on the laptop (x64).
- **Off-box collector:** a tiny TCP server (the existing `scripts/m8/power-pull.sh receiver`, or a small bin) that appends `seq,watermark` to a per-cycle capture file and is durable independently of the DUT. This is the one component that must never share fate with the target.
- **Smart-plug driver:** local HTTP call, no cloud. For Shelly Gen2: `GET http://<plug-ip>/rpc/Switch.Set?id=0&on=false|true`; Gen1: `/relay/0?turn=off|on`; Tasmota: `/cm?cmnd=Power%20Off|On`. Make the endpoint pluggable (Shelly/Tasmota) via a config var.
- **Orchestrator:** `scripts/m8/h1-cycle.sh` implementing §3.5.

### 3.4 Cut-mechanism calibration + H2 gate — THE FIRST MILESTONE (before any cycle counts)

Do **not** run the 50-cycle loop until this passes. On the exact DUT medium (this is the
`h1-cycle.sh calibrate` step, run automatically as the first step of every `run`):
1. `storage_probe write-unsynced-marker <wal_dir>` — write a marker **without** `fdatasync` (it must sit in the page cache / device cache, not on stable flash). `storage_probe` writes via the **same `write(2)` path the WAL uses** (minus the sync), so "un-synced data lost here" predicts "un-acked WAL record lost here" — a shell `echo` could differ subtly and mis-measure.
2. Cut power via the smart plug; restore; wait for boot.
3. `storage_probe verify-marker-gone <wal_dir>` — the marker **MUST be absent** (exit 0 = gone; exit 1 = survived ⇒ vacuous, hard abort).
   - **Gone ⇒ the cut is real** (un-synced data is genuinely lost) ⇒ proceed.
   - **Survives ⇒ vacuous** (storage didn't lose un-synced data — e.g. mounted `sync`, or the probe accidentally flushed) ⇒ **abort, fail loudly**, do not run cycles. Investigate the mount / probe before continuing.

Capture this probe result in the evidence artifact; #18 cannot close without it.

### 3.5 The cycle loop (`h1-cycle.sh`)

Each cycle:
1. Ensure power ON; poll ssh until reachable (timeout ~90 s). If the Pi doesn't return → **INCONCLUSIVE (infra)**, restore and retry; do not count, and after K consecutive infra failures, abort loudly (likely SD/OS corruption — re-flash, check overlay).
2. ssh → recreate a **fresh** WAL dir on the DUT medium (each cycle is an independent durability trial; fresh LSN space from 1).
3. Start the collector with a fresh per-cycle capture file.
4. ssh → launch `power_pull_workload <wal_dir> tcp:<controller>:<port> ...` in the background; let it commit + stream for a few seconds (enough for thousands of records and many `seq,watermark` lines).
5. **CUT** power via the smart plug (instant — no graceful shutdown). The workload dies with it.
6. Wait (power fully off ~3–5 s); **RESTORE** power.
7. Wait for boot + ssh.
8. `scp` the capture to the Pi; ssh → `power_pull_verify <wal_dir> <capture>`; collect exit code.
   - `0 PASS` → count++.
   - `1 FAIL` → **stop the whole run** (a D1 violation — the entire point of the gate; investigate per §3.6).
   - `2 INCONCLUSIVE` (side-channel `seq` gap) → re-run cycle, don't count.
9. Append the cycle verdict to the evidence ledger.
10. Repeat until **50 consecutive PASS** (zero FAIL, zero INCONCLUSIVE counted).

### 3.6 Interpreting a FAIL — honest-hardware nuance (put in the runbook)

The GA gate certifies the **(WAL, device) pair**, and the contract's D1 promise is "survives power loss **on honest hardware**." So:
- A **FAIL** means an *acked* (i.e. `commit()`-returned, therefore `fdatasync`'d) record was absent after the cut. Given M0–M7 + the LazyFS gate already validate the WAL's recovery/fsync logic, the most likely cause is a **lying device** (the medium acked the flush but lost the data on power loss) — a device indictment, not necessarily a WAL bug. The artifact must record enough to attribute: which DUT medium, and that the lost LSN was below the side-channel high-water (i.e. genuinely acked).
- The **GA durability claim** requires a **PASS on a device whose flush-honesty is corroborated** (PLP, or at minimum consistent H2 behavior). Cheap consumer SD/USB media frequently lie — so a SD-card FAIL is a *useful finding about consumer media*, and the claim is then met on the more-trustworthy medium. Record the device with every PASS; the sign-off names it.
- This is the real value of the rig: it either certifies D1 on real media **or** empirically demonstrates the lying-device failure mode the whole durability-first design exists to respect.

### 3.7 The workflow — `.github/workflows/m8-h1.yml`

- Trigger: **`workflow_dispatch` only** (owner-run).
- `runs-on: [self-hosted, h1-rig]`.
- Steps: cross-compile ARM bins → `scp` to Pi → run §3.4 calibration (abort if vacuous) → run §3.5 loop → emit §5 artifact → upload as workflow artifact + attach to #18.
- Exit: green = 50 PASS; red = any FAIL or aborted calibration. INCONCLUSIVE handled by in-loop re-run.
- The valid/invalid cut rules from the runbook still apply (the cut here is a real smart-plug mains interruption — never `sysrq-b`/`reboot`).

---

## 4. Build sequence (by leverage)

1. **Tier 1 dm-flakey CI** — free, closes #16/#17 (the metadata + physical-poison holes no laptop can). Do first.
2. **Tier 3 macOS CI** — free, closes #19 Half A.
3. **GA H1 rig** — physical build: Pi setup (read-only root + DUT partitions) → controller (collector + plug driver + runner) → **§3.4 calibration milestone** → `m8-h1.yml` → owner runs 50 cycles per medium: microSD, then USB-SSD, then BeagleBone eMMC. The GA durability claim is strongest when met on whichever medium proves honest across the three; a FAIL on any one is a verdict on *that device's* cache behavior (§3.6), not the WAL.
4. Evidence ledger wired into all three from day one.

---

## 5. Evidence ledger (shared artifact schema)

Each gate emits JSON (attached to its issue per #15 ground rules):

```json
{
  "gate": "H1 | H3-physical | §14.4d | H4",
  "timestamp": "ISO-8601",
  "target": { "uname": "...", "kernel": "...", "host": "..." },
  "storage": { "fs": "ext4", "block_device": "/dev/...", "dut_medium": "microSD|USB-SSD|eMMC(BeagleBone)",
               "write_cache": "...", "h2_probe": "PASS(marker gone) | FAIL(survived)" },
  "cut": { "mechanism": "smart-plug mains interrupt (Shelly ...) | dm-flakey | n/a",
           "valid": true },
  "run": { "cycles_required": 50, "cycles_pass": 50, "fail": 0, "inconclusive_rerun": N,
           "per_cycle": [0,0,2,0, ...] },
  "verdict": "PASS | FAIL | INCONCLUSIVE | OPEN"
}
```

`verdict: PASS` is legitimate only when `h2_probe` proves the storage loses un-synced data and `fail == 0 && inconclusive(counted) == 0`.

---

## 6. What the agent must NOT do

- Run the H1 cycle loop before §3.4 calibration passes.
- Count an INCONCLUSIVE cycle toward the 50, or treat "fault never injected" / "marker survived" as PASS.
- Use `sysrq-b`, `reboot`, or `shutdown` as a cut (warm/graceful → vacuous).
- Self-certify H1/H3-physical/§14.4d from a sandbox, or mark H4 Half B done without a trace.
- `continue-on-error` an anti-vacuous assertion (env-availability skips are fine; "the test verified nothing" is a hard red).

---

## Appendix A — Hardware shopping list (region-agnostic)

> Mains plug/socket fit and local sourcing are deliberately kept out of this committed doc. For Israeli-mains specifics (Type-H sockets, Europlug vs. Schuko, adapters, where to buy), see the companion `m8-rig-israel-notes.md`.

You already have: Raspberry Pi 3, Pi Zero (spare/unused for this), BeagleBone Black (third DUT — eMMC medium; not a controller, it's 32-bit), microSD cards, a spare laptop (controller). To buy:

**Smart plug with a genuine local API (the enabling component).** Cloud-only plugs are unusable — the cut loop needs local control with no internet round-trip.

- **Recommended: Shelly Plug S (Gen3) or Shelly Plus Plug S.** Confirmed local HTTP/RPC API (`/rpc/Switch.Set?id=0&on=false`), cloud disable-able, very reliable, no flashing.
- **Alternative: a Tasmota-preflashed plug** — e.g. an **Athom** preflashed Tasmota plug. Local HTTP API out of the box (`/cm?cmnd=Power%20Off`), no flashing required.
- **Avoid** generic Tuya/eWeLink "smart plugs" unless explicitly Tasmota-flashable or documented local-API — most are cloud-default and a poor fit for a deterministic cut loop.
- Match the plug body / socket variant (and any adapter) to your mains region — see `m8-rig-israel-notes.md` for Israeli sockets.

**USB-SSD (the second DUT medium):** any cheap USB 3.0 SSD. Note in the evidence artifact that consumer USB-SSDs without PLP may lie about flushes — that's a finding, not a WAL bug.

**Powering both boards from one cut point (hands-free).** You need a single *cut point*, not a single PSU — so put the cut at the strip, and give each board its own proper supply. This avoids the brownout/under-voltage that a shared supply tends to cause, and a sagging supply directly wastes test cycles (flaky boots on restore).

- **Recommended — strip + two dedicated supplies (most foolproof, ~$15–20):**
  - **Raspberry Pi 3:** a **5.1 V / 3 A micro-USB** supply (the official-style Pi 3 PSU; 5.1 V compensates for cable drop, 3 A covers the Pi *plus* the bus-powered USB-SSD during that campaign).
  - **BeagleBone Black:** a **5 V / 2 A** supply with a **5.5 × 2.1 mm barrel, center-positive** (the BBB barrel-jack spec; don't power the BBB from mini-USB — ~500 mA is too little under load).
  - A cheap **power strip**; plug both supplies into it, and plug the strip into the smart plug. One toggle cuts both boards. (Total draw ≈ 15–20 W — trivial for any smart plug.) Match the strip and supply mains plugs to your region — see `m8-rig-israel-notes.md`.
- **Alternative — literal single PSU (~$12, fewer bricks but watch voltage):** a **5 V / 6 A** adapter (prefer one that outputs **5.1–5.2 V**) with a 5.5 × 2.1 mm barrel, a **1→2 barrel Y-splitter**, and a **5.5 × 2.1 mm-female → micro-USB** cable for the Pi. The 6 A covers Pi + BBB + SSD + simultaneous-boot inrush. Use short, thick cables — a single 5.0 V rail through a splitter sags easily and the Pi 3 is sensitive to under-voltage.

**Optional but recommended:**
- A couple of **spare microSD cards** of each DUT card type (treat as consumables per §3.2 — the binding risk is sudden FTL death on a cut, not write-wear, and a cheap SD is your most likely "lying device" demonstrator). Keep a pre-imaged OS/boot card too.
- A short Ethernet cable for the Pi (wired side channel — strongly preferred over WiFi).

**Cut at mains, not the laptop.** Never use the laptop as the DUT — its battery means a "power cut" isn't one. The externally-powered, battery-less Pi with a single interruptible input is the correct target.
