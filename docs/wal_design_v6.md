# Write-Ahead Log (WAL) Component — Design, Implementation & Test Specification

**Status:** Draft **v6.1** — all open decisions resolved; ready for implementation
**Target language:** Rust (stable)
**Intended audience:** A coding agent implementing the component, plus a human reviewer.
**Context:** Journal/WAL for an LMAX-style single-writer, event-sourced system with complex incremental business logic. Durability-first (acknowledge only after durable).

---

## Changelog (v6 → v6.1)

Normative corrections surfaced while implementing M3 (intra-segment recovery) and M4 (multi-segment), plus M6/M7 testing-status annotations; no contract section changes.

- **§6.2 — added an integrator note on transient `Locked` after a writer crash (additive clarification, no contract change).** A dead writer's `flock` is released during process teardown, which can lag the process's exit, so a crash-recovery reopen may briefly see a spurious `Locked`. The note tells integrators to tolerate a bounded transient `Locked` on the recovery reopen (short retry) and treat only a persistent `Locked` as a real concurrent-writer error — observable POSIX semantics, exercised by the §14.4a process-crash tests' bounded-retry reopen. (Surfaced while fixing an M7 CI flake; integrator guidance, not a behavior change.)

- **§14.7 (performance & regression) — IMPLEMENTED in M7; the regression-gate CI *enforcement* is OPEN-pending-controlled-runner.** Added the M7 status block to §14.7, the per-PR/nightly split to §14.11, and the M7 note to the §14.13 zero-alloc DoD row. `benches/wal.rs` implements the four criterion groups (throughput / commit-latency / recovery / split-batch) over the public API against a real `fdatasync`; since criterion reports no arbitrary percentiles, the commit-latency tail (p50/p99/p999) comes from an `hdrhistogram` persisted to `target/perf/`. `tests/zero_alloc.rs` is hardened (proves no-roll in the measured window via segment-file count + `durable_lsn` advance; adds a `max_record_size` variant). `scripts/perf-gate.sh` implements the >10% throughput/median-time and >20% p999 thresholds (median, not the outlier-sensitive mean, from criterion `estimates.json`; p999 from the histogram JSON) and was shown to flag an injected regression. Per the line's own "pin CPU governor", enforcement is real on a controlled runner; on hosted CI the gate runs **informational** (`bench.yml`, `continue-on-error`) like the LazyFS gate — a stopgap, not a downgrade. No `src/` change; testing-status annotations only. (Added during M7 implementation.)

- **§14.8 (hardware/platform durability) + §14.4d — IMPLEMENTED in M8 as honest-tiered harnesses + an owner runbook; the physical gates are OPEN-pending-owner-run, never self-certified.** Added `docs/m8-runbook.md` and `scripts/m8/{storage-check,fsync-fault,dm-flakey,power-pull}.sh`, the test-only bins `src/bin/power_pull_{workload,verify}.rs`, the LD_PRELOAD shim `tests/fault/eio_preload.c` + gate `tests/fsync_fault_gate.rs`, and `tests/macos_fullfsync.rs`. **What runs green in CI/sandbox:** (a) **H2** the deny-by-default storage durability guard (rejects tmpfs/overlay/unrecognized FS — the vacuous-pass guard H1 depends on); (b) the **H3 §12 poison *state machine*** — an LD_PRELOAD shim returns EIO from the commit's libc `fdatasync`, and the gate asserts `FsyncFailed`, no `durable_lsn` advance past the synced segment (incl. the split-batch partial-advance rest-at-seg1-max), and handle poison, with an anti-vacuous guard that the injection actually fired (running without the shim fails loudly). Interception was empirically confirmed (`strace`: 6 `fdatasync` all intercepted, 3 `fsync` — the rustix raw-syscall dir-fsync — none), bounding the shim to the data-sync poison path. **What is OPEN-pending-owner-run** (this sandbox's kernel has no `CONFIG_BLK_DEV_DM`, no cuttable target, and is Linux not macOS): **H3 physical** (dm-flakey `error_writes` → poison at the block layer), **§14.4d** (dir-fsync omission negative control via dm-flakey `drop_writes` — timing-/FS-sensitive, certified on ext4; the `inject_no_dir_fsync` build drives it), **H1** power-pull (≥50 cycles, zero acked loss; network side channel durable off-box, send-strictly-after-`commit() Ok`, contiguous-watermark verify, H2-gated), and **H4** macOS `F_FULLFSYNC` (`dtruss` trace). All OPEN gates print loud "NOT EXERCISED" banners rather than fake green, exactly as the LazyFS gate was handled. No `src/` contract change — harnesses + tests + docs only. (Added during M8 implementation.)
- **§14.8/§14.11 — M8 test AUTOMATION added (Tier 1 dm-flakey CI + Tier 3 macOS CI); no contract change.** The two metadata/physical gates the build sandbox could not run are now CI: `.github/workflows/m8-dmflakey.yml` (**push-to-main paths-filtered + nightly + manual, not per-PR**) runs **H3-physical (#16)** and **§14.4d (#17)** via `dm-flakey` on hosted `ubuntu-latest` VMs (which, unlike the build sandbox, reach the target) — ext4 is the hard gate (FAIL reds the build), xfs/btrfs informational, **best-effort + loud skip** if a runner lacks dm-flakey (**a green run carrying that skip-warning is NOT a passed gate**); and `.github/workflows/m8-macos.yml` (**per-PR paths-filtered + push-to-main + manual**) runs **H4 Half A (#19)** (`cargo test --test macos_fullfsync`, routing/smoke) on `macos-latest` — per-PR because a macOS-only `F_FULLFSYNC`-routing regression is invisible to the Linux PR CI (Half B `dtruss` stays owner-run). The amended anti-vacuous acceptance criteria are enforced **inside** `scripts/m8/dm-flakey.sh`: #16 PASS **ANDs** the WAL poison with a **source-confirmed block-layer EIO** scraped from `dmesg` in the injection window (poison without an observed EIO = INCONCLUSIVE, never PASS; bounded retry), #17 runs a **`drop_writes` positive control** first (an un-synced marker must vanish across the cut — if drop_writes is inert the negative control is non-functional ⇒ exit 4 HARNESS, louder than INCONCLUSIVE) then grants a bounded retry budget for the timing-sensitive asymmetry (exhausted = INCONCLUSIVE, never PASS; a correct-build loss = FAIL). Every gate emits a §5 evidence-ledger JSON (`scripts/m8/evidence.sh`, incl. `block_layer_eio_observed` / `drop_positive_control`) uploaded as a CI artifact each run and posted to the tracking issue **only on a manual `workflow_dispatch`** (the human sign-off; the automatic runs stay loud as a red build, quiet on the issue). H1 power-pull (#18) remains owner-run. No `src/` change. (Added during M8 test-automation; hardened per PR #14 review.)
- **§8.2 step 5 (active-segment forward scan) — condition corrected `lsn == expected_next_lsn` → `lsn >= expected_next_lsn`.** The equality form is wrong and would silently truncate acknowledged data (a **D5** violation): when the invalid record at offset X is itself a *corrupted acked record*, the next genuine record carries `expected_next_lsn + 1`, never `expected_next_lsn`, so the equality test never matches and recovery misclassifies mid-log corruption as a torn tail — discarding the valid records after X. It was also internally inconsistent with §14.4e ("finds the **next** valid record"). The corrected `>=` form matches every genuine continuation; soundness against a coincidental stale match still rests on the §8.2.1 durable zeroing of `[X, EOF)` keeping the post-tail region clear within the scan bound. (Found and fixed during M3 implementation.)
- **§14.3 (stateful model/oracle test) — IMPLEMENTED in M6 as an in-tree proptest harness; §14.5 F4 (the cargo-fuzz variant) DEFERRED to M9.** Added the M6 status note to §14.3, the F4 deferral to §14.5 and §14.13, and `§14.3` to the D7/D8 rows of the §14.12 matrix. The harness (`tests/model_oracle.rs` + the proptest-free executor `tests/model/mod.rs`) checks the §14.3 envelope as a refinement relation against an independent in-memory oracle after every state-machine crash/recover, and was shown to catch a seeded recovery-loss bug (D1/D3) and a seeded checkpoint over-delete (D8) per §14.0.3. No contract change — these are testing-status annotations only. (Added during M6 implementation.)
- **§14.4d (dir-fsync omission negative control) — re-assigned from LazyFS to the §14.8/M8 metadata-fault tooling.** The original wording ("a build that skips the directory fsync on roll MUST fail recovery under LazyFS `clear-cache`") assigned the wrong instrument. LazyFS's fault model is **data-only** (`clear-cache` = lost/un-fsynced *data*, plus `torn-op`/`torn-seq`); it intercepts metadata ops (`create`/`rename`/…) only to maintain its page cache and tracing, not as a *losable* directory entry. As a passthrough it persists a `create` to the backing filesystem immediately, so a freshly-rolled segment's directory entry is never dropped by `clear-cache` — and because the segment's *data* is independently `fdatasync`'d, omitting the parent-directory fsync produces **no observable loss** under LazyFS. The negative control therefore structurally cannot run there. It is a directory-entry/namespace-durability scenario, which is the same capability §14.8 H3 needs: **dm-flakey** (drop the metadata-journal write at the block layer) or a real **power-pull**. Resolution: **M4/LazyFS covers the *positive* split+roll power-loss case (a real D9 check); the §14.4d *negative control* moves to M8 under §14.8's metadata-fault tooling** and is tracked as an open release-gate item (§14.12, §14.13). The `inject_no_dir_fsync` build toggle and the scaffold test are retained for M8 to drive. The dir-fsync itself remains **required** on every roll (§7.4 step 5) — only the *test* tool changed, not the contract. (Found during M4 implementation.)

## Changelog (v5 → v6)

Resolves all remaining §17 open decisions (1–5); the spec is now closed for implementation.

- **§17.1 (record sizes) — resolved.** No spanning; `segment_size`/`max_record_size` user-configurable; payloads bounded ~1 MiB. Added an enforced precondition: **`open()` validates `max_record_size ≤ segment_size − 91` and returns `InvalidConfig`** (§5.3, §11); added `InvalidConfig` to the error enum (§10).
- **§17.2 (input vs output journal) — resolved, no spec change.** Integrator journals both by **instantiating the component twice** (independent LSN spaces); **serialization is out of WAL scope**; immutable+versioned logic makes input replay safe.
- **§17.3 (checkpoint) — resolved + corrected.** §9 now states the binding rule explicitly: `checkpoint(up_to)` requires **`up_to ≤ latest durable *snapshot* LSN`**, not `durable_lsn` (checkpointing to `durable_lsn` would delete log needed to replay onto the snapshot). Added the reader/backup retention-margin note.
- **§17.4 (`Reader`) — resolved.** Streaming `Reader::next` only; consumers copy the borrowed slice when they must retain it; no adapter, no mmap-iterator in v1.
- **§17.5 (Windows) — resolved: out of scope.** Platform tiering pinned in §8.3 and §14.11: Linux = production + §14.8 gate; macOS = dev/correctness; Windows = future.

## Changelog (v4 → v5)

Resolves §17 open decision 6 (external-reader support):

- **§15.3 — watermark publication is now a pluggable `DurabilityObserver` trait.** Built-ins: `NullObserver` (default, don't ship) and an in-process observer forwarding `durable_lsn` to a caller sink. Generic with `NullObserver` default ⇒ zero-cost when unused; fires cheaply/non-blocking on the writer thread after each durability advance; strictly downstream of durability so it cannot affect D1–D12; future channels (mmap'd cursor, network) are additive impls. `Wal` is now `Wal<O: DurabilityObserver = NullObserver>`.
- **§15.4 — retention floor resolved to gap-is-fatal.** No writer-side gating in v1; readers fatally error when `oldest_available_lsn > next_expected_lsn` (never silent skip). A future registered-min-LSN gating strategy is purely additive.
- **§6 API** `open` takes the observer; **§17 decision 6** marked resolved; tests for the observer contract and gap detection added to §15.8.

## Changelog (v3 → v4)

Adds first-class support for external (non-writer) access, so backup and primary/secondary are not left as footguns:

- **§15 added — External readers, backup, and replication (NORMATIVE).** Defines safe read-only tailing (shared/no lock; **tail-CRC-failure means "retry," not "truncate"**); the **durability-visibility gap** (a CRC-valid record is *not* necessarily durable — un-synced page-cache data is readable); a **durable-watermark channel** (same-machine mmap'd cursor / cross-machine in the shipping protocol) that divergence-sensitive readers MUST NOT cross; a **retention floor** so checkpoint cannot silently delete segments out from under a lagging reader; **backup** via sealed-segment copy + active-segment-via-recovery; and **replication** via an in-process shipping consumer (async vs sync, the latter gating client-ack on the replica's durable-ack).
- **§4 D12 added — Sealed-segment immutability.** A sealed segment's bytes never change until checkpoint deletes it; this underpins all of §15.
- **§1 Non-Goals refined:** the WAL provides the replication *substrate* (immutable format + watermark) but not the transport/ack/failover.
- Sections renumbered: Dependencies §16, Open decisions §17, References §18.

## Changelog (v2 → v3)

This revision folds in a second adversarial review targeting the v1→v2 seams. Changes that affect correctness:

- **§4.1 added — explicit non-guarantees.** `commit` is **not atomic**: the WAL guarantees per-record durability plus a monotonic dense prefix, not all-or-nothing batch durability. The apparent atomicity of a single-segment commit is incidental (one shared fsync) and MUST NOT be relied on.
- **§7.3 tightened:** the commit-time split operates on **whole records**, never raw bytes (a record must not span segments); an **empty "prefix that fits" is explicitly valid** (seal current segment, roll, continue); a **termination guarantee** is stated (the §5.3 `max_record_size` bound ensures any record fits in a fresh segment, so the split loop always makes progress).
- **§8.1 clarified:** an **empty active segment** (e.g. crash immediately after a roll) is valid and does **not** participate as `prev_segment` in cross-segment continuity; its `durable_lsn = base_lsn − 1`.
- **§8.2 truncation path made normative and portable:** torn-tail invalidation MUST be done by **explicitly `pwrite`-ing zeros from the truncation offset to segment EOF, then `fdatasync`** (a pure *data* write over already-allocated blocks — no alignment constraints, `fdatasync` is sufficient). `fallocate(PUNCH_HOLE)` is an **optional optimization** that, if used, MUST be followed by a **full `fsync`** (it mutates the extent tree — metadata) and validated per filesystem. *(Rationale: a naive `PUNCH_HOLE` is not the alignment hazard a reviewer suggested — the man page specifies partial blocks are zeroed — but its crash-durability is filesystem-dependent, so the data-write path is the safe default.)*
- **§14.4g strengthened:** the resurrection test now also asserts the zeroed post-truncation region **survives a power-loss cycle** (LazyFS clear-cache), which is what catches a `PUNCH_HOLE`-without-`fsync` regression.

## Changelog (v1 → v2)

This revision folds in an external design review. Changes that affect correctness:

- **§4 D2 reweakened** to "contiguous from the oldest *surviving* segment." Prefix deletion via checkpoint is legitimate; authorizing it is the integrator/snapshot's responsibility, **not** the WAL's. *No checkpoint watermark file* — it would add a crash-consistency surface that can be deleted alongside the segments it guards, giving false assurance. (Review 1.2)
- **§7 `append` is now segment-agnostic and pure-memory.** Segment boundaries are resolved at **commit** time, which may split a batch across two segments. The v1 "roll before encoding" rule is **removed** (it could write buffered records into the wrong segment). (Review 1.1)
- **§8.2 recovery hardened:** on torn-tail truncation the implementation **MUST zero/punch-hole from the truncation offset to segment EOF and fdatasync**; the tail-vs-corruption forward scan is now **bounded** by `max_record_size + framing`; recovery **MUST NOT materialize payloads in memory** (sparse per-segment index only). (Review 1.4, 2.1, 3.2)
- **§5.3 record-size formula corrected** (the 20-byte record header was omitted). CRC coverage **extended to include alignment padding**. (Review 2.2, 4.1)
- **§6 API:** `iter_from` no longer returns a `std::iter::Iterator` of borrowed slices (impossible for a buffered reader); replaced by a streaming `Reader` with an inherent `next`. **`FlushOnCommit` / `SyncPolicy` removed** — it reintroduced the async-durability window this system exists to avoid. (Review 3.1, 3.3)
- **§12 fsync-failure policy collapsed to a single behavior: poison on failure.** Staging-buffer/`last_lsn` state on failure is now defined, including the cross-segment partial-sync case. (Review 1.3)
- **§8.4 cold-start bootstrap** made explicit. (Review 4.2)

---

## How to use this document

This is a build contract. Implement in vertical slices, each with its own tests and acceptance criteria (§13). The agent should:

1. Implement strictly to the **Durability Contract (§4)** and **On-Disk Format (§5)**. These are normative; nothing else may weaken them.
2. Treat **§14 Testing Suite** as a co-equal deliverable. A milestone is "done" only when its mapped tests pass.
3. Surface ambiguity as an explicit question (see §17) rather than guessing.

Normative keywords **MUST / MUST NOT / SHOULD / MAY** per RFC 2119.

---

## 1. Goals and Non-Goals

### Goals
- A focused, embeddable, single-writer append-only durable log.
- Durability-first: a committed record survives process crash and power loss on honest hardware.
- Crash recovery that is correct, deterministic, and bounded by log size since the last checkpoint.
- Group-commit batching: one `fdatasync` amortized across many appended records (the LMAX throughput lever).
- Bounded, well-defined behavior on corrupted/torn on-disk data — never panic, never silently lose acknowledged data.
- Minimal dependencies; no background threads; no hidden I/O.

### Non-Goals
- **Not** a database (no keys/queries/indexes beyond LSN locating).
- **Not** multi-writer (exactly one writer, enforced at the type level).
- **Not** a replication system, and **not** the network transport for one. The WAL provides the *substrate* for external readers, backup, and replication — an immutable sealed-segment format plus a durable-watermark channel (§15) — but the shipping protocol, replica ack handshake, and failover logic are the integrator's.
- **Not** responsible for the daisy-chain cursor protocol that gates publication on durability — that is the integrator's. This component provides the durable substrate and the `durable_lsn()` watermark.
- **Not** the authority on whether a missing segment *prefix* is legitimate. Checkpoint deletes whole oldest segments; the snapshot that authorized the checkpoint lives in the integrator. The WAL trusts its surviving contiguous suffix and detects only *internal* gaps. (See §4 D2 and §9.)
- **No** transactions / multi-record atomicity in v1.

---

## 2. Grounding in proven systems

| Source system | What we borrow |
|---|---|
| **PostgreSQL WAL** | Per-record CRC; fixed-size pre-allocated segments; the *fsyncgate* lesson — a failed `fsync` may mean data is already lost, so the only safe response is to treat it as non-durable and fail loudly (PG PANICs). |
| **RocksDB WAL** | Record framing with per-record checksum + record type; reuse of pre-allocated files; explicit recovery *modes* — our torn-tail-vs-fatal policy mirrors `kTolerateCorruptedTailRecords` vs `kAbsoluteConsistency`. |
| **Apache Kafka log** | Segmented log with base-offset-encoded filenames; append-only; retention by whole-segment deletion (and, like Kafka, no manifest preventing external deletion — see §4 D2). |
| **ARIES** | Write-ahead principle; monotonic LSNs; idempotent, repeatable recovery. We are redo-only / event-sourced (no undo). |
| **LazyFS & crash-consistency literature** | Fault model and test methodology (lost writes, torn writes, POSIX persistence ordering/atomicity). See §14.4 and §18. |

Guiding insight: **the framing format is the well-understood part; the costly-to-get-wrong parts are fsync discipline, recovery edge cases, and replay determinism.** The test suite is weighted accordingly.

---

## 3. Architectural context (informative)

```
   (LMAX integrator — OUT OF SCOPE)
   ... business logic → output ring → Journal consumer (owns the Wal):
        for each available event: wal.append(ev)     // buffer + sequence, pure memory
        wal.commit()                                 // write + fdatasync (may span 2 segments)
        journal_cursor.store(durable_lsn, Release)    // gates the publish consumer
```

The component is the `Wal`, owned by exactly one thread. After `commit()` returns `Ok(w)`, records `≤ w` are durable — which is what makes the integrator's `Release` store of the cursor sound. The cursor itself is not part of this component.

---

## 4. Durability Contract (NORMATIVE)

Each invariant maps to tests in §14. All MUST hold on honest hardware (§8.3, §14.8).

- **D1 — Durability on commit.** After `commit()` returns `Ok(w)`, every record with `lsn ≤ w` is durable: it survives an immediate process crash *and* power loss.
- **D2 — Dense, gap-free surviving suffix.** At all times the durable content is a contiguous run of LSNs `P..=k`, where `P` is the `base_lsn` of the *oldest surviving segment* (`P = 1` until the first checkpoint). Recovery MUST never produce an *internal* gap (a missing LSN between `P` and `k`). Recovery does **not**, and by design **cannot**, distinguish an authorized prefix deletion (`P > 1` via checkpoint) from an unauthorized one; preventing unauthorized deletion is the integrator's responsibility, anchored by its durable snapshot.
- **D3 — At-most-tail loss on crash.** A crash MAY lose only records appended but not yet covered by a returned `commit()`. It MUST NOT lose any record `≤` the last returned `durable_lsn`.
- **D4 — Torn-tail truncation.** A partial/torn write at the physical tail MUST be detected (length bounds + CRC) and cleanly truncated; the truncated region MUST be invalidated per §8.2. A torn record MUST NOT be surfaced as valid.
- **D5 — Mid-log corruption is fatal, not silent.** A corrupt record that is *not* the tail (a structurally valid record with the correct next LSN exists after it, within the bounded scan window) MUST cause recovery to halt with a distinct, loud error. It MUST NOT be silently truncated (that would discard acknowledged data).
- **D6 — Read-back fidelity.** Replay MUST return exactly the records appended, in LSN order, byte-identical payloads.
- **D7 — Idempotent recovery.** open→use→close→open→… converges; recovery is deterministic and repeated cycles do not change recovered content or tail state.
- **D8 — Checkpoint safety.** `checkpoint(up_to)` MUST NOT remove any record with `lsn > up_to`, MUST NOT make any retained record unreadable, and MUST preserve D2 over the retained suffix.
- **D9 — Crash-anywhere recoverability.** A crash at any point inside `append`, `commit` (including between the two segment syncs of a split batch), segment roll, or `checkpoint` MUST leave the directory recoverable to a valid dense suffix on next `open`.
- **D10 — No buried garbage / no resurrection.** After a torn tail is truncated and new records are written, a subsequent recovery MUST NOT mistake leftover bytes — including a *stale but CRC-valid* record whose LSN happens to equal the current expected LSN — for a live record. (Enforced by zeroing on truncation, §8.2.)
- **D11 — Bounded recovery parsing.** Recovery MUST terminate and MUST NOT panic, read out of bounds, allocate unboundedly, or scan unboundedly for *any* input bytes, including adversarially corrupted ones.
- **D12 — Sealed-segment immutability.** Once a segment is *sealed* (a higher-`base_lsn` segment exists), the writer MUST NEVER modify its bytes; it may only be deleted *whole* by checkpoint. Torn-tail invalidation (§8.2.1) touches only the **active** segment, and only during recovery. This is the property external readers and backups rely on (§15) — a sealed segment copied at any instant is byte-identical to the same segment read later, until it is deleted.

### 4.1 Non-guarantees (read carefully)

- **`commit` is NOT atomic.** The WAL provides *per-record* durability and a *monotonic dense prefix* — not all-or-nothing durability of a `commit()` batch. A `commit` batch is a performance grouping (amortize one `fdatasync` over whatever was appended since the last commit), not a transactional unit. If a batch spans two segments (§7.3) and the first segment's `fdatasync` succeeds while the second's fails, the records in the first segment are durable, the rest are lost, and `durable_lsn` reflects exactly the synced prefix (§7.2, §12). This is contract-compliant: the result is still a dense prefix.
- **Single-segment commits only *appear* atomic.** When a whole batch fits in one segment it is covered by one shared `fdatasync`, so a failure loses the whole batch — but this all-or-nothing behavior is *incidental*, not guaranteed. No implementation logic or test MUST assume a `commit` batch is indivisible.
- **The atomicity primitive that IS provided is per-record.** A single record is all-or-nothing via its CRC and (since records never span segments, §5.3) is written within one segment under one `fdatasync`. If a caller needs several events to be atomic together, it MUST model them as a *single* record with a compound payload; it MUST NOT rely on commit-batch grouping for atomicity.
- **The WAL does not authorize prefix deletion.** See D2: a missing segment *prefix* is accepted as an assumed-authorized checkpoint; preventing unauthorized deletion is the integrator's responsibility.

---

## 5. On-Disk Format (NORMATIVE)

All multi-byte integers are **little-endian**. All CRCs are **CRC-32C (Castagnoli)** — hardware-accelerated, strong short-burst detection (same choice as ext4/RocksDB/iSCSI). **Use the `crc32c` crate (Castagnoli), not `crc32fast` (zlib/ISO-HDLC polynomial).**

### 5.1 Directory layout

```
<wal_dir>/
   00000000000000000001.wal      # segment whose first record LSN is 1
   00000000000000100001.wal      # base_lsn = 100001
   00000000000000200001.wal      # active segment (highest base_lsn)
   LOCK                          # advisory exclusive-writer lock file
```

- Filename = `{base_lsn:020}.wal` (20 decimal digits = `u64::MAX` width).
- **No manifest/CURRENT file.** State is derived by listing the directory, sorting segment files by `base_lsn`, and scanning. Rationale: a manifest is one more thing that can desync with reality.
- **Active segment** = greatest `base_lsn`. All others are **sealed**.
- Retained segments MUST form a contiguous suffix; recovery verifies cross-segment LSN continuity (§8.1).

### 5.2 Segment header (fixed 64 bytes at offset 0)

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 8 | `magic` | ASCII `WAL\0SEG1` |
| 8 | 2 | `format_version` | `u16` = 1 |
| 10 | 2 | `flags` | reserved, MUST be 0 |
| 12 | 8 | `base_lsn` | first LSN this segment may contain |
| 20 | 8 | `created_unix_nanos` | informational only; MUST NOT affect recovery (determinism) |
| 28 | 32 | `reserved` | MUST be zero |
| 60 | 4 | `header_crc` | CRC-32C over bytes `[0, 60)` |

A segment with invalid `magic`/`header_crc` is a fatal error (the header is written and synced at creation, before any record — it is never a torn tail).

### 5.3 Record framing

Records follow the segment header, contiguously.

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 4 | `crc` | CRC-32C over bytes `[4, 4 + 16 + length + pad)` — i.e. the rest of the header, the payload, **and** the alignment padding |
| 4 | 4 | `length` | `u32`, payload length |
| 8 | 8 | `lsn` | `u64` |
| 16 | 1 | `rec_type` | `1 = Full`; `0` = zero/sentinel (never a real record); `2..` reserved for future fragmentation |
| 17 | 1 | `rflags` | reserved, MUST be 0 |
| 18 | 2 | `reserved` | MUST be 0 |
| 20 | `length` | `payload` | opaque caller bytes |
| 20+length | `pad` | `padding` | zero bytes to the next 8-byte boundary; `pad = (8 − ((20 + length) mod 8)) mod 8` |

- `crc` is at the front so the reader validates the whole record once `length` is known. `length` and `padding` are inside CRC coverage, so a corrupted length or tampered padding is detected (closes the "hide bytes in padding" gap).
- A record MUST NOT span segments in v1. Therefore (accounting for the 64-byte segment header, the 20-byte record header, and up to 7 padding bytes):

  ```
  max_record_size  ≤  segment_size − 64 − 20 − 7      (i.e. segment_size − 91)
  ```

  **`open()` MUST validate this relation and reject a violating config with `InvalidConfig`** (it is a precondition, not a runtime surprise). Records exceeding `max_record_size` are rejected at `append` with `RecordTooLarge` (no silent truncation, no fragmentation in v1).
- Each segment is independently parseable; recovery of a segment never depends on another segment's *contents*, only on the cross-segment LSN-continuity check (§8.1).

### 5.4 Pre-allocation and the zero region

- On creation a segment is pre-allocated to `segment_size` (`fallocate` / `F_PREALLOCATE`), so the unwritten remainder is zero-filled.
- A `rec_type == 0` / all-zero record header during scan is the **end-of-records sentinel** for a partially-filled or cleanly-rolled segment.

---

## 6. Public API (NORMATIVE shape; names may be refined)

```rust
/// Log Sequence Number. Newtype to prevent mixing with byte offsets/counts.
/// Lsn(0) is reserved as "none"; real records are dense starting at 1.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct Lsn(pub u64);

pub struct WalConfig {
    pub segment_size: u64,     // bytes; pre-allocated per segment
    pub max_record_size: u32,  // hard upper bound on a single payload (see §5.3)
}

pub struct RecoveryReport {
    pub oldest_lsn: Lsn,        // P: base of oldest surviving segment (1 if never checkpointed)
    pub durable_lsn: Lsn,      // k: highest recovered durable LSN (== oldest_lsn-1 if empty suffix)
    pub tail_state: TailState,
    pub segments_scanned: usize,
}

pub enum TailState {
    Clean,
    /// A torn tail was detected, truncated, and zeroed at this offset of the active segment.
    TruncatedAt { segment_base: Lsn, offset: u64 },
}

impl<O: DurabilityObserver> Wal<O> {
    /// Open or create a WAL in `dir`, running full recovery. Acquires an exclusive
    /// advisory lock on the directory; fails with `Locked` if already held.
    /// `observer` is notified of each durability advance (§15.3); use `NullObserver`
    /// (the default type parameter) to not ship. open() with no observer arg uses Null.
    pub fn open(dir: &Path, config: WalConfig, observer: O)
        -> Result<(Wal<O>, RecoveryReport), WalError>;

    /// Sequence + buffer a record. Pure memory: no syscall, no allocation in steady state.
    /// The record is NOT durable until a later commit() returns Ok covering it.
    /// Errors only on RecordTooLarge or if the handle is poisoned (see §12).
    pub fn append(&mut self, payload: &[u8]) -> Result<Lsn, WalError>;

    /// Make all buffered records durable: write(2) + fdatasync, rolling segments as
    /// needed (a batch MAY span two segments, §7.3). Advances durable_lsn to the highest
    /// LSN whose segment has been successfully synced. Returns that durable watermark.
    /// On sync failure, poisons the handle (§12) after recording any partial progress.
    pub fn commit(&mut self) -> Result<Lsn, WalError>;

    pub fn durable_lsn(&self) -> Lsn;  // highest durable LSN
    pub fn last_lsn(&self) -> Lsn;     // highest assigned LSN (durable or buffered)

    /// Streaming reader for replay. NOT a std::iter::Iterator: the yielded &[u8] borrows
    /// the reader's internal buffer and is valid only until the next call. Zero-copy,
    /// zero per-record allocation. (See §6.1 on why std Iterator cannot express this.)
    pub fn reader_from(&self, from: Lsn) -> Result<Reader<'_>, WalError>;

    /// Delete whole segments fully superseded by `up_to` (oldest-first, dir-fsync'd).
    /// MUST preserve D8.
    pub fn checkpoint(&mut self, up_to: Lsn) -> Result<(), WalError>;
}

pub struct Reader<'w> { /* holds open segment + reusable buffer */ }
impl<'w> Reader<'w> {
    /// Lending-style next: the borrow is tied to &mut self, so each item is valid only
    /// until the following call. Returns None at end of log.
    pub fn next(&mut self) -> Option<Result<(Lsn, &[u8]), WalError>>;
}
```

### 6.1 Why `reader_from` is not a `std::iter::Iterator`

The standard `Iterator::next(&mut self) -> Option<Self::Item>` requires `Item` to be independent of the per-call borrow of `self`, so it **cannot** yield a `&[u8]` borrowed from the iterator's own reused I/O buffer (the lending/streaming-iterator problem). The inherent `Reader::next` above ties the borrow to `&mut self` per call, which compiles and stays zero-copy. *Alternatives the agent MAY choose instead, with trade-offs:* (a) yield owned `Vec<u8>`/`bytes::Bytes` and implement real `Iterator` (allocates per record — acceptable only if replay throughput is not critical); (b) an mmap-backed reader yielding `&'w [u8]` borrowed from a mapping with the `Wal`'s lifetime, which *can* implement `Iterator` (at the cost of the mmap complexity this design otherwise avoids). The streaming `Reader` is the default.

### 6.2 Single-writer enforcement
The write handle MUST be `Send` but **not `Sync`**; write methods take `&mut self`. Concurrent writers are a compile error. `open` MUST take an OS advisory exclusive lock (`flock` on `LOCK`) so a second *process* cannot open the same WAL for writing; failure is `Locked`.

> **Integrator note — transient `Locked` after a writer crash.** When a writer process dies (crash or kill), the OS releases its `flock` as part of process teardown, which can complete *slightly after* the process is otherwise gone. A crash-recovery path that immediately reopens the same directory may therefore observe a brief, spurious `Locked` even though no live writer holds it. Integrators SHOULD tolerate a **bounded transient `Locked` on the crash-recovery reopen** (retry with a short backoff, e.g. up to ~1 s) rather than treat the first `Locked` as fatal; a `Locked` that persists past that window is a genuine concurrent-writer error. (This is observable POSIX `flock` semantics, not a WAL defect — the §14.4a process-crash tests reopen via exactly such a bounded retry.)

---

## 7. Write path (NORMATIVE)

### 7.1 `append` (segment-agnostic, pure memory)

1. Reject if `payload.len() > max_record_size` (`RecordTooLarge`).
2. Reject if the handle is poisoned (`§12`).
3. Assign `lsn = last_lsn + 1`.
4. Encode the framed record into a reused in-memory staging buffer (no allocation in steady state). Update `last_lsn`.
5. Return `lsn`.

`append` performs **no I/O and no segment logic.** It does not know or care about segment boundaries. This preserves the pure-memory, allocation-free property the LMAX latency model depends on.

### 7.2 `commit`

1. If the staging buffer is empty, return `durable_lsn` unchanged.
2. Map the buffered bytes onto segments and write them, splitting at segment boundaries (§7.3). For each segment touched, in order: `write_all` the bytes destined for that segment (single `write(2)` per segment at the tracked offset), then `fdatasync` that segment (on macOS, `F_FULLFSYNC` — §8.3). After each segment's successful sync, advance `durable_lsn` to the highest LSN written to that segment.
3. On full success, clear the staging buffer and return `durable_lsn` (= `last_lsn`).
4. On any `write`/`fdatasync` error, **poison** the handle (§12). `durable_lsn` retains whatever was achieved by already-synced segments (it advances monotonically and never regresses). Return the error.

`append` is pure memory; `commit` is the only place that touches segments, fsyncs, or rolls. The group-commit batch is "everything appended since the last commit."

### 7.3 Commit-time segment boundary split

When the buffered records do not all fit in the active segment's remaining space, `commit` writes them across segments. The split MUST be performed on **whole-record boundaries** — never by slicing the staging buffer at a raw byte offset, which could split a single record across two segments and violate §5.3.

Loop until the staging buffer is fully written:

1. Determine the **prefix of whole records** that fits in the active segment's remaining space (sum of framed record sizes ≤ remaining space). This prefix MAY be empty (see below).
2. If the prefix is **non-empty**: `write_all` those whole records into the active segment, `fdatasync`, and advance `durable_lsn` to that prefix's last LSN. If records remain in the buffer, the active segment is now **sealed**.
3. If records remain in the buffer (the prefix did not cover everything): **roll** (§7.4) to a new segment whose `base_lsn` = the LSN of the first not-yet-written record. Continue the loop.

**Empty prefix is valid and MUST be handled (no deadlock).** If not even the first buffered record fits in the active segment's remaining space, the prefix is empty: write nothing, seal the active segment as-is (its remaining bytes are pre-allocated zeros, which serve as the §8.2 end-of-records sentinel — no write needed), roll (§7.4), and continue. A naive `while !buffer.is_empty()` that assumes progress on every iteration would otherwise spin forever on this case; the implementation MUST treat "seal + roll" as progress.

**Termination guarantee.** The §5.3 bound `max_record_size ≤ segment_size − 64 − 20 − 7` ensures *any* single record fits in a *fresh* segment. So an empty prefix can occur at most once per record before a roll, and after that roll the first record always fits — the loop strictly progresses and cannot spin.

A single batch MAY span more than two segments if it is large relative to `segment_size`; the implementation MUST handle the general N-segment case, not just two. Each segment touched gets its own `fdatasync` and its own monotonic `durable_lsn` advance — recall (§4.1) this means a `commit` is **not** atomic across the split.

Because a roll only happens here (during `commit`), a sealed segment is always fully synced up to its last record before the next segment exists — sealed segments therefore never contain a torn tail (relied on in §8.2).

### 7.4 Segment roll

To create a new segment with `new_base`:

1. Create `{new_base:020}.wal` (`O_CREAT|O_EXCL`).
2. `fallocate` to `segment_size`.
3. Write the 64-byte header (with `header_crc`).
4. `fdatasync` the new file (header durable).
5. **`fsync` the directory** so the new filename is durable. *(Skipping this is the classic gotcha; see §14.4d.)*
6. Switch the active segment; reset write offset to 64.

A crash between any of these steps MUST be recoverable (§8.4).

### 7.5 Allocation discipline
Steady-state `append` + `commit` (no roll) MUST perform **zero heap allocations** (reused staging and I/O buffers). Tested in §14.7.

---

## 8. Recovery path (NORMATIVE)

Runs in `open`, single-threaded, before any append.

### 8.1 Segment discovery and contiguity

1. List `<wal_dir>`, collect `*.wal`, parse `base_lsn` from each filename, **sort ascending** (never rely on directory iteration order — determinism).
2. Validate each segment header (§5.2). A bad header on any **sealed** segment is fatal.
3. Recover each segment's record range (§8.2). Verify **cross-segment continuity**: for each adjacent pair, `prev_segment_max_lsn + 1 == next_segment.base_lsn`. An internal gap here is fatal (`ContiguityViolation`, violates D2). **An empty segment has no `max_lsn` and MUST NOT participate as `prev_segment` in this check.** An empty segment is only valid as the **active** (highest-base) segment — e.g. after a crash immediately following a roll (§8.4) — in which case there is no subsequent segment to validate against. An empty *sealed* (non-highest-base) segment is a `ContiguityViolation` (fatal): a sealed segment must contain at least one record, since a roll only occurs to write records that did not fit.
4. `oldest_lsn (P)` = base of the lowest segment. `durable_lsn (k)` = max LSN of the active segment after its tail handling; for an **empty active segment**, `k = base_lsn − 1` (e.g. base 101 ⇒ `durable_lsn = 100`, matching the prior segment's max). A missing *prefix* (`P > 1`) is accepted silently (authorized-checkpoint assumption; see §4 D2).

### 8.2 Intra-segment scan, tail handling, and corruption detection

Scan each segment from offset 64, tracking `expected_next_lsn`.

For each record:
1. If `< 20` bytes remain or `rec_type == 0` / all-zero header ⇒ **end of this segment's records.**
2. Bound `length`: if `length > max_record_size` **or** `20 + length + pad > remaining_segment_bytes` ⇒ record invalid at this offset (candidate boundary; step 5).
3. Read payload + padding. Short read ⇒ invalid (step 5).
4. Compute CRC-32C over `[4, 4+16+length+pad)`; compare to `crc`. Check `lsn == expected_next_lsn`. Either mismatch ⇒ invalid (step 5).
5. **Classification of an invalid record at offset X:**
   - **Sealed segment:** sealed segments are fully synced before the next segment exists (§7.3), so they contain **no torn tail.** Any invalid record before the zero sentinel is **fatal corruption** (`Corruption`). No forward scan.
   - **Active segment:** perform a **bounded forward scan** — from `X+8`, step forward (8-byte aligned) at most `max_record_size + 28` bytes (record header + max padding), attempting to parse a structurally valid record whose `lsn >= expected_next_lsn`. (Every genuine continuation has `lsn >= expected_next_lsn`; soundness against a coincidental stale match rests on the §8.2.1 zeroing keeping the post-tail region clear within the bound. See the v6 → v6.1 changelog for why this is `>=` and not `==`.)
     - **Found ⇒ mid-log corruption ⇒ FATAL** (`TornMidLog`). The found record proves data after X was genuine and acknowledged; truncating would silently drop it (D5). *(This inference is sound because every prior truncation zeroed the post-tail region — §8.2.1 — so no stale-but-valid record can appear here; a valid record after a gap is therefore real corruption, not resurrection.)*
     - **Not found within the bound ⇒ torn tail.** Truncate logically at X, then **physically invalidate** the region from X to segment EOF, and record `TailState::TruncatedAt`; `durable_lsn` = last valid LSN before X. The invalidation MUST be durable before recovery completes (see §8.2.1).
6. On a valid record: record `(lsn → offset)` into the **sparse index** (see §8.5), set `expected_next_lsn = lsn + 1`, advance by the padded record size.

#### 8.2.1 Physical invalidation of a truncated tail (NORMATIVE)

The bytes from the truncation offset X to segment EOF MUST be made to read as zero **and that zeroing MUST be durable**, so that (a) no stale, possibly-CRC-valid record can be resurrected on a later recovery (D10), and (b) the zero region serves as the end-of-records sentinel (§5.4). The region MUST extend to **segment EOF**, not a bounded window: a previous generation may have written a longer record past X, and a shorter overwrite would otherwise leave a stale valid record exposed beyond any fixed bound.

- **Normative path — explicit zero write.** `pwrite` zero bytes over `[X, segment_size)` (the segment is fully pre-allocated, so these blocks already exist), then `fdatasync`. This is a pure *data* write over allocated blocks: there are **no alignment constraints**, and `fdatasync` is **sufficient and correct** (no metadata change). It is filesystem-independent. The cost is writing up to `segment_size` of zeros, incurred only on torn-tail recovery (rare, startup-only, sub-second on SSD) — acceptable, since recovery is not latency-critical.
- **Optional optimization — hole punch.** `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)` over `[X, segment_size)` is cheaper (it deallocates blocks; partial blocks at the edges are zeroed, so there is *no* alignment hazard). **However**, it mutates the extent tree (metadata), and its crash-durability is filesystem-dependent — so if used it MUST be followed by a **full `fsync`** (not `fdatasync`; on macOS, `F_FULLFSYNC`), and it MUST be validated on each target filesystem by §14.4g's power-loss assertion. It is **not** the default; choose it only with that validation in place.

The bounded scan makes recovery `O(segment) `with a small constant and immune to the garbage-CRC DoS (D11). Zeroing-on-truncate guarantees no stale record survives to be resurrected (D10) and keeps the active-segment corruption inference sound (D5).

### 8.3 Durability semantics by platform
**Platform tiering (v1):** **Linux** is the production target and the only platform on which the hardware-durability gate (§14.8) runs. **macOS** is a dev/correctness target — unit/property/fuzz tests run and `F_FULLFSYNC` is honored, but it carries **no production durability claim** and is **not** subject to §14.8. **Windows is out of scope for v1** (the `FlushFileBuffers` path below is a future addition, untested in v1).

- **Linux:** `fdatasync` (via `File::sync_data` / `rustix`) suffices for record data (segment pre-allocated ⇒ no metadata change on append). Segment *creation* requires file `fsync` **and directory `fsync`**.
- **macOS/APFS:** plain `fsync` does **not** flush the drive cache. Durability REQUIRES `fcntl(fd, F_FULLFSYNC)` everywhere this spec says "durable." (Frequent, severe portability bug.)
- **Windows (future, out of scope v1):** `FlushFileBuffers`; directory-durability semantics differ — document and test before claiming support.

### 8.4 Crash-during-roll / crash-during-checkpoint / cold start
- **Cold start (empty directory):** `open` creates `00000000000000000001.wal` with `base_lsn = 1`; `oldest_lsn = 1`, `durable_lsn = 0`, `tail_state = Clean`.
- **Empty active segment** (valid header, rest zero): adopted as active with no records; `durable_lsn = base_lsn − 1` (§8.1).
- **Highest-base file with absent/incomplete header** (crash after create/fallocate, before/after partial header write, before dir fsync): recovery MUST discard it (unlink) and treat the prior segment as active.
- **Split-batch crash** (crash after the first segment's `fdatasync`, before the second's): the first segment's records are durable and recovered; the second segment may be empty/torn and is handled by §8.2. Recovery yields a valid suffix (D9).
- **Interrupted checkpoint** (oldest-first deletion): survivors remain a contiguous suffix; recovery proceeds from the lowest survivor.

### 8.5 Memory discipline during recovery
Recovery MUST NOT materialize payloads in memory. It retains at most a **sparse index**: per segment, `(base_lsn, path, max_lsn)`, and optionally a coarse `(lsn → offset)` map for seeking. `reader_from(from)` locates the containing segment and scans within it from the start, skipping to `from`. Memory is `O(num_segments)`, not `O(num_records)` and never `O(total_bytes)`.

### 8.6 Determinism
Recovery MUST be a pure function of on-disk bytes — no wall clock, no environment, no filesystem iteration order. `created_unix_nanos` is informational and MUST NOT influence any decision.

---

## 9. Checkpoint / retention (NORMATIVE)

- A segment covering `[b, b')` (where `b'` is the next segment's base) is deletable iff `b' − 1 ≤ up_to`.
- The **active segment is never deleted.**
- Deletion proceeds **oldest-first**, then the directory is `fsync`'d. At any crash point survivors remain a contiguous suffix (D8/D9).
- Checkpoint only unlinks whole files; it never rewrites or compacts contents.
- Checkpoint advances `oldest_lsn (P)`.
- **The safe bound is the durable *snapshot* LSN, not the durable *record* LSN (NORMATIVE caller rule).** Recovery rebuilds state as *latest durable snapshot + replay of the log after it*, so the caller MUST only call `checkpoint(up_to)` with `up_to ≤ the LSN covered by its latest durable snapshot`. Since a snapshot's LSN is always `≤ durable_lsn`, that snapshot LSN — **not** `durable_lsn` — is the binding limit. Checkpointing up to `durable_lsn` would delete the log between the snapshot and `durable_lsn` that recovery needs to replay, silently capping recovery at the stale snapshot. The WAL trusts the caller and cannot verify this (the inverse of D8: D8 promises the WAL won't delete what you kept; this rule is your obligation not to ask it to delete what you can't rebuild).
- **Retention margin for readers/backup.** Because external readers and backups fail on a checkpointed-away gap (gap-is-fatal, §15.4), the checkpoint cadence SHOULD retain a margin (keep N segments, or hold a lag window) sized to the slowest expected reader/backup — or accept that a lagging consumer re-seeds from a fresh snapshot. This is an integrator policy, not a WAL guarantee.

---

## 10. Error handling (NORMATIVE)
`WalError` is a non-panicking enum with at least: `Io`, `Corruption { segment, offset, detail }`, `TornMidLog { segment, offset }`, `RecordTooLarge`, `InvalidConfig`, `BadSegmentHeader`, `Locked`, `FsyncFailed`, `ContiguityViolation`, `Poisoned`. The recovery parser MUST return errors, never panic, for all inputs (D11).

---

## 11. Configuration & concurrency
- **`segment_size`**: default 128 MiB. Larger ⇒ fewer rolls; smaller ⇒ finer retention; tests use tiny sizes to force rolls and splits.
- **`max_record_size`**: must satisfy `max_record_size ≤ segment_size − 91` (§5.3); `open()` validates this and returns `InvalidConfig` otherwise. Typical: ≤ 1 MiB payloads with a 64–128 MiB `segment_size` leaves large headroom.
- **Concurrency:** single-writer (`&mut self`, `!Sync`, dir lock). No threads, no background I/O. Cross-thread cursor coordination (the publish barrier) is the integrator's; see §14.6 for a recommended `loom` harness covering that boundary, since the Release/Acquire ordering there is a prime "costly to get wrong" hazard.

---

## 12. fsync-failure policy (NORMATIVE — single behavior: poison)

A failed `fdatasync`/`F_FULLFSYNC` is dangerous: on Linux a failed `fsync` may already have discarded the dirty pages (the PostgreSQL *fsyncgate*), so a naive retry can "succeed" against data that is gone. For a dense-LSN log there is **no safe way to resume**, because the API has no operation to rewrite a lost LSN slot, and continuing past it would create a permanent internal gap (violating D2). Therefore there is exactly one policy:

- A failed `write`/`fdatasync` (or directory fsync during roll) **poisons** the handle.
- The failing call returns `Err` (`FsyncFailed`/`Io`). All subsequent `append`/`commit` return `Poisoned`.
- **State on failure is defined:** `durable_lsn` reflects only segments whose `fdatasync` succeeded (monotonic, never regresses — important for the split-batch case in §7.3, where the first segment may be durable while the second failed). The staging buffer and `last_lsn` are left as-is but are unusable (handle poisoned). The only recovery path is to drop the handle, `open` afresh (which truncates any torn tail), and have the integrator rebuild forward state from its snapshot + the recovered durable suffix.
- A retry-forever policy is **forbidden** — indistinguishable from a deadlock to the publish side and may silently lie about durability.

*(The v1 `PropagateError`/"remain usable" option is removed: it was incompatible with a dense-LSN log.)*

---

## 13. Implementation milestones

Each milestone is independently testable; do not advance until mapped tests (§14) pass.

- **M0 — Foundations.** Crate skeleton, `Lsn`, `WalConfig`, `WalError`. CRC-32C wired to `crc32c`, verified against published vectors. *Tests: §14.1.*
- **M1 — Record codec.** Encode/decode per §5.3 (CRC includes padding), with bounds checks. *Tests: §14.1, §14.2 round-trip, §14.5 decoder fuzz.*
- **M2 — Single-segment write/read.** Segment-agnostic `append`; `commit` (single segment); pre-allocation; `fdatasync`/`F_FULLFSYNC`; streaming `Reader`; zero-alloc hot path. *Tests: §14.1, §14.2, §14.7.*
- **M3 — Intra-segment recovery.** Tail detection; **durable invalidation of `[X, EOF)` via `pwrite`-zeros + `fdatasync` (§8.2.1)**; **bounded** forward scan; sealed-vs-active classification; sparse index (no payload materialization); bad-header handling. *Tests: §14.4a, §14.4b, §14.4e, §14.4f, §14.4g (incl. durability-of-zeroing).*
- **M4 — Multi-segment + commit-time split.** Roll, dir fsync, filenames, cross-segment continuity (incl. empty active segment), **whole-record split across ≥2 segments**, empty-prefix seal/roll, partial-sync `durable_lsn` advance. *Tests: §14.2 (tiny segments), §14.2 P7 (empty-prefix), §14.4c (incl. split-batch crash), §14.4d.*
- **M5 — Checkpoint.** Oldest-first deletion, contiguous-suffix invariant, dir fsync, `oldest_lsn` advance. *Tests: §14.1 math, §14.2 P5, §14.4c.*
- **M6 — Stateful model test.** The generative oracle harness (§14.3).
- **M7 — Performance.** Criterion benchmarks + regression gates + zero-alloc assertion (§14.7).
- **M8 — Hardware/platform durability.** Power-pull; `dm-flakey` fsync-failure; macOS `F_FULLFSYNC`; OS/FS matrix (§14.8).
- **M9 — Hardening.** Continuous fuzzing, Miri, soak, CI matrix (§14.5, §14.6, §14.10, §14.11).

---

## 14. TESTING SUITE (the critical deliverable)

> A WAL is "conceptually simple but costly to get wrong." Bugs live in crashes, torn writes, lost caches, corruption, and platform fsync quirks. This suite is exhaustive about failure, and **every invariant D1–D11 is covered by at least one mechanically-runnable test** (traceability in §14.12).

### 14.0 Philosophy
1. **Invariants over examples** — prefer property/model tests asserting D1–D11; reserve example tests for known-tricky cases.
2. **Test the failure, not the success** — most effort in §14.4 (fault injection) and §14.5 (fuzzing).
3. **Make the suite falsifiable** — include deliberately-buggy builds and assert the suite *catches* them (§14.4d).
4. **Two crash models, never conflated:** **process crash** (SIGKILL — page cache survives; tests the state machine; cheap, run often) vs **power loss** (un-fsynced data lost; LazyFS or real power-pull; the model that actually validates D1/D3; expensive, run nightly/pre-release).

### 14.1 Unit tests (fast, every commit)
- **CRC-32C** vs published Castagnoli vectors; assert it is Castagnoli, not ISO-HDLC.
- **Record round-trip** for payload sizes 0, 1, 7, 8, 9 (alignment edges), `max_record_size`, and a spread; assert **padding is inside CRC coverage** (flip a padding byte ⇒ CRC fails).
- **Length bounds:** decoder rejects `length > max_record_size` and overrun without reading OOB.
- **Alignment/padding:** encoded size always 8-aligned; `pad` formula correct; padding bytes zero.
- **`max_record_size` formula (§5.3):** a max-sized record fits exactly in `segment_size`; `max_record_size + 1` payload is rejected; a max record never triggers an infinite roll.
- **Filename ↔ base_lsn** incl. `1` and `u64::MAX`.
- **Segment header** encode/decode + header CRC; corrupted header rejected.
- **Checkpoint eligibility math:** exhaustive small-case boundary check of `b' − 1 ≤ up_to` (off-by-one), incl. `up_to` exactly at, one below, one above a boundary.
- **LSN assignment:** monotone, dense, starts at 1, `Lsn(0)` never assigned.

### 14.2 Property-based tests (`proptest` / `bolero`)
For arbitrary `Vec<Vec<u8>>` (sizes incl. 0 and `max_record_size`; `segment_size` small to force rolls **and** commit-time splits):
- **P1 Fidelity (D6).** append all → commit → reopen → replay yields identical sequence byte-for-byte, in order.
- **P2 Density (D2).** Arbitrary append/commit interleavings ⇒ recovered LSNs are exactly `P..=k` dense (here `P=1`, no checkpoint).
- **P3 Commit-boundary invariance (D1).** "commit after every append" vs "commit once" ⇒ identical durable content.
- **P4 Cross-segment + split integrity (D2,D6).** Tiny segments forcing many rolls **and** batches that span ≥2 segments ⇒ full sequence reconstructed; `durable_lsn` advances monotonically across the split.
- **P5 Checkpoint preservation (D8).** Arbitrary `up_to` ⇒ no record `> up_to` lost or unreadable; recovered suffix is dense from the new `oldest_lsn`.
- **P6 Idempotent recovery (D7).** open→close repeated N times ⇒ stable `durable_lsn`, `oldest_lsn`, `tail_state`, content.
- **P7 Whole-record split + empty-prefix (§7.3).** With `segment_size` chosen so records frequently land near the boundary — including cases where the next record does **not** fit in the remaining space (forcing an empty prefix → seal + roll) and where a record fills the segment to within a few bytes — assert: no record is ever split across segments (every record decodes whole on replay, D6); each sealed segment contains ≥1 record; the split loop terminates (no spin) for all generated batches; and recovery reconstructs the dense sequence (D2). Include a degenerate case where the active segment's remaining space is smaller than the smallest record header (e.g. 4 bytes left).

Shrinking enabled.

### 14.3 Model-based / stateful oracle test (centerpiece)
Maintain an oracle: the **committed** set `(lsn, payload)` (a returned `commit`) and the **appended-but-uncommitted** set. Drive a randomized program — `Append`, `Commit`, `SimulatedCrashAndRecover`, `Checkpoint(up_to)`, `Reopen` — against both the real `Wal` and the oracle. After every `SimulatedCrashAndRecover`, assert:
- Recovered durable set ⊇ oracle committed set (D1/D3).
- Recovered set is dense `P..=k`, `k ≥` committed watermark (D2).
- Records `≤ k` match oracle content byte-for-byte (D6).
- Any record beyond the committed watermark is allowed only if it preserves density (never a hole).

Run with high iteration count in CI and as a fuzz target (seed/op-script from `cargo-fuzz`, §14.5 F4).

> **M6 status — IMPLEMENTED as an in-tree proptest harness; F4 fuzz-target variant DEFERRED to M9** (like F1–F3). `tests/model_oracle.rs` generates randomized op-scripts (`Op::{Append, Commit, Checkpoint, CrashAndRecover, Reopen}`) and drives them through the proptest-free executor in `tests/model/mod.rs` against an independent in-memory oracle (`BTreeMap` committed set + staged `Vec` + `oldest_lsn`/`durable_lsn`/`max_ckpt_up_to` watermarks). After every recovery it asserts the envelope above as a **refinement relation** (⊇, dense, byte-identical, density-preserving tail) plus `durable_lsn`/`oldest_lsn` monotonicity and D7 idempotence across no-mutation reopens; a terminal reopen anchors the D8 "checkpoint didn't over-delete" check with an authoritative `RecoveryReport.oldest_lsn`. This harness models the **state machine** crash (drop the handle without committing ⇒ reopen), per §14.0 — it does **not** validate power-loss durability or torn tails (that is §14.4b/c, already passing). Case count is `PROPTEST_CASES`-overridable (§14.11). The executor is factored so the deferred F4 cargo-fuzz target (and a later optional LazyFS-backed crash variant) drives the *identical* `run(cfg, ops)` with zero duplication. Falsifiability (§14.0.3) was demonstrated: a seeded recovery loss bug trips the D1/D3 check and a seeded checkpoint over-delete trips the D8 check, each shrinking to a minimal op-script.

### 14.4 Crash-consistency & fault injection (core)

#### 14.4a Process-crash matrix (SIGKILL)
Child runs a scripted workload; parent SIGKILLs at scripted points — **before and after each `write`, each `fdatasync`, each segment-create step, each checkpoint unlink, and between the two segment syncs of a split batch.** Parent reopens and asserts D2/D3/D6/D9. (Page cache survives SIGKILL ⇒ validates the state machine, not power-loss durability.)

#### 14.4b LazyFS — lost & torn writes (power-loss model) — **gold standard**
Mount the WAL dir on **LazyFS** (FUSE filesystem simulating POSIX persistence; injects *lost* and *torn* writes; the academically-validated tool used to find data-loss bugs in PostgreSQL, etcd, Zookeeper, Redis, LevelDB). Inject:
- **`clear-cache` (lost writes / fsyncgate):** clear un-fsynced data at a point, optionally crash after. Place the injection both *before* and *after* a `commit`'s `fdatasync`. Assert: every **committed** record survives; uncommitted may vanish but recovery yields a clean dense suffix, no holes, no corruption (**D1, D3, D4**).
- **`torn-op` (torn record):** split a `write` into parts, persist some. Assert torn-tail detection + truncation + **zeroing of the post-tail region**, and that no torn record is surfaced as valid (**D4, D10**).
- **`torn-seq` (partial multi-write persistence):** assert recovery selects a valid dense suffix (**D2, D4**).

#### 14.4c Enumerated crash points inside operations (under LazyFS)
Crash + cache-clear immediately before and after each durability boundary; recover and assert the contract:
- **Commit (single segment):** after `write`, before `datasync` (record non-durable); after `datasync` (durable — recovery may surface a record the caller never acked; that is a valid suffix, D3).
- **Commit (split batch):** **after the first segment's `fdatasync`, before the roll; after the roll, before the second segment's write; after the second `fdatasync`.** Assert `durable_lsn` reflects exactly the synced segments and the recovered suffix is dense (**D9**, partial-sync advance).
- **Segment roll:** after create/before fallocate; after fallocate/before header; after header/before file fsync; after file fsync/before dir fsync (segment may vanish — recovery discards it); after dir fsync/before first record. Each recovers to a valid suffix (**D9**).
- **Checkpoint:** after unlinking segment k, before k+1; before the dir fsync. Contiguous suffix, no holes (**D8, D9**).

#### 14.4d Negative control — dir-fsync omission detector
A deliberately-buggy build that **skips the directory fsync** on roll MUST be **detectable**: the correct build issues the roll-time directory fsync, the `inject_no_dir_fsync` build does not, and the harness catches the difference. *(Proves the harness can catch the classic gotcha.)*

> **Tooling (corrected v6.2 — see changelog): the behavioral power-loss form is FS-dependent and is NOT reproducible on ext4/xfs/btrfs.** Empirically (PR #20, run on hosted dm-flakey + owner Fedora 43), a buggy build that omits the roll's directory fsync **does not** lose data on any mainstream Linux journaling filesystem, because `fsync`ing the newly-created segment file (which the WAL does at segment creation and every commit) forces a journal/log commit whose running transaction **already contains the directory entry that created the file** — so the dirent reaches disk transitively even though POSIX never promised it (*All File Systems Are Not Created Equal*, OSDI '14 — §18). This is FS behavior, not a WAL property: the explicit `fsync_dir` is a **portable-durability safeguard** (ext2, ext4 `data=writeback`, and non-Linux/networked FSes do **not** provide the transitive guarantee) and is **retained unconditionally**. The control is therefore split into three tiers:
>
> 1. **Primary (deterministic, FS-independent, per-PR) — syscall-presence.** `scripts/m8/dirfsync-presence.sh` (wired into `ci.yml`) straces the roll path and asserts the correct build issues `fsync` on the directory fd once per roll while the inject build does not (cold-start fsync only). This is the dir-fsync analogue of the H4 `F_FULLFSYNC` presence check: prove the syscall is **issued**, which is the actual regression to catch — deterministic and FS-independent, so it gates every PR.
> 2. **Secondary (behavioral power-loss) — a synchronized mid-run cut; CLOSED as a documented negative result.** `scripts/m8/dm-flakey.sh dirfsync-negative <fs>` with `src/bin/dirfsync_cut_workload.rs`: roll once, ack a record into the new segment, then **block** with the dirent dirty so the harness activates `drop_writes` and cuts *inside* the un-synced window (not a sub-ms race). Empirically (PR #21, owner Fedora 43) the inject build **recovers fully on every config tested** — ext4/xfs/btrfs, journal-less ext4 (incl. plain `ext2`-format, which modern kernels service via the **ext4 driver** since the standalone ext2 driver was removed in Linux 6.9 — dmesg: "mounting ext2 file system using the ext4 subsystem"), and the ext4 driver's weakest ordering, a journaled ext4 mounted `data=writeback`. In all of them the new segment's directory entry reaches disk transitively via the file's **own `fdatasync`** (the journal on journaling configs; the ext4 driver's metadata/writeback on journal-less). **The earlier "ext2 block-adjacency" mechanism is retracted** (no real ext2 driver was ever exercised), and the exact mechanism was **not isolated**. There is no readily-available Linux filesystem on which the omission is behaviorally observable, so the behavioral form is a **documented negative result** — not a gap: Tier-1 carries the gate, and `fsync_dir` remains a correct POSIX-portability safeguard.
> 3. **Documented INCONCLUSIVE-by-design — ext4/xfs/btrfs (and journal-less "ext2").** Run for evidence and to catch a genuine correct-build regression, but a non-failing inject build there is **expected** (the dirent reaches disk via the file's own fsync), never read as "dir-fsync omission is harmless."
>
> *(Superseded the earlier "dm-flakey makes the buggy build fail on ext4" expectation, which was backwards. The `inject_no_dir_fsync` toggle drives both Tier-1 and Tier-2. §14.12/§14.13 track the DoD — satisfied by Tier-1.)*

> **Historical note (v6.1): this negative control belongs to M8, not LazyFS.** A missing parent-directory fsync risks only the *directory entry*, which is a metadata/namespace-durability fault. **LazyFS cannot model it** — its faults are data-only (`clear-cache`/`torn-op`/`torn-seq`) and, as a passthrough, it persists a `create` to the backing fs immediately; with the segment's data independently `fdatasync`'d, omitting the dir-fsync yields no observable loss under `clear-cache`. The detector therefore runs under the **§14.8 metadata-fault tooling** (dm-flakey) — refined in v6.2 above into the three-tier form.

> **Tooling (corrected v6.1 — see changelog): this negative control belongs to M8, not LazyFS.** A missing parent-directory fsync risks only the *directory entry*, which is a metadata/namespace-durability fault. **LazyFS cannot model it** — its faults are data-only (`clear-cache`/`torn-op`/`torn-seq`) and, as a passthrough, it persists a `create` to the backing fs immediately; with the segment's data independently `fdatasync`'d, omitting the dir-fsync yields no observable loss under `clear-cache`. The detector therefore runs under the **§14.8 metadata-fault tooling** (dm-flakey: drop the directory's metadata-journal write at the block layer; or a real power-pull). **M4 status:** the dir-fsync is implemented and required on every roll (§7.4), and the LazyFS gate covers the *positive* split+roll power-loss case (D9); the `inject_no_dir_fsync` build toggle + scaffold test exist and compile, but the negative control is **OPEN pending M8** (§14.12, §14.13). It must not be reported as satisfied until dm-flakey/power-pull actually makes the buggy build fail.

#### 14.4e Corruption / bit-flip injection
Flip bits in a **sealed** segment and in the **active** segment at: (i) payload of the **last** record, (ii) payload of a **middle** record, (iii) the `length` field, (iv) the `crc` field, (v) the segment header, (vi) a **padding** byte. Assert:
- Last-record (active) corruption ⇒ torn-tail truncation + zeroing, recoverable (**D4**).
- **Middle-record corruption of an acked record (active segment) ⇒ the bounded forward scan finds the next valid record ⇒ FATAL `TornMidLog`, never silent truncation (D5).** *(This is the case a naive "first-invalid-is-the-tail" recovery would silently lose; it must be a fatal error.)*
- Any corruption in a **sealed** segment ⇒ FATAL (**D5**).
- Padding-bit flip ⇒ CRC failure ⇒ classified as above (proves padding is covered).
- Segment-header corruption ⇒ segment rejected, FATAL.

#### 14.4f Truncation / short-file injection
Truncate a segment at arbitrary offsets — mid-header, mid-record, between records, mid-padding. Recovery yields a valid suffix and never panics (**D4, D11**).

#### 14.4g Buried-garbage / resurrection test (D10) — **must include the stale-valid-record case AND the durability of zeroing**
Construct the specific resurrection hazard: write records, induce a torn tail such that a **stale but CRC-valid record whose LSN equals the post-truncation `expected_next_lsn`** sits physically beyond the truncation point; recover (which MUST zero `[X, EOF)` per §8.2.1); write new, *shorter* records over the start of that region; crash again; recover. Assert the second recovery does **not** resurrect the stale record (it was zeroed) and yields the correct dense suffix. Also assert the simpler case: torn tail → recover → shorter new records → crash → recover yields no spurious records.

**Durability-of-zeroing assertion (catches `PUNCH_HOLE`-without-`fsync` and any non-durable invalidation).** After recovery truncates and invalidates `[X, EOF)`, inject a **power-loss cycle** (LazyFS `clear-cache` + crash) *before* writing any new records, then reopen: the region `[X, EOF)` MUST still read as zeros. A build that punches a hole without the full `fsync` (§8.2.1) — or otherwise relies on non-durable invalidation — MUST fail this assertion (the stale blocks reappear). Run this on each target filesystem in the §14.11 matrix.

*(This test is the enforcement check for the §8.2.1 requirement; with zeroing disabled, or with a hole-punch that is not durably synced, it MUST fail.)*

### 14.5 Fuzzing (`cargo-fuzz` / libFuzzer + `arbitrary`)
- **F1 Recovery-parser fuzz (highest priority).** Arbitrary bytes as a segment file / directory of segments. Parser MUST never panic, never read OOB (verify under ASan/Miri), never infinite-loop, never allocate unboundedly, and the **forward scan MUST stay within its bound** (assert via an instrumented counter). Always terminates with `Ok(suffix)` or clean `Err` (**D11**). *(**M9 — IMPLEMENTED** as `fuzz/fuzz_targets/recovery.rs` (cargo-fuzz + libFuzzer + `arbitrary`, ASan). Its **primary** surface is the real public `Wal::open` driven over an adversarial directory of segment files — fuzzer-controlled filenames + `base_lsn`s (out-of-order/duplicate/gapped/`0`/malformed-name), valid-header dense bodies and pure garbage — so discovery → sort → header validation → the §8.4 incomplete-highest discard → cross-segment continuity → `recover_segment` are all under test; a secondary single-file `recover_segment` probe asserts the bound directly. The **bounded-scan counter** is instrumented on the **real** `forward_scan_finds_valid` loop (feature `fuzzing`, compiled out of release) and asserted against `recovery::scan_bound(max_record_size)` — the **same** symbol that bounds the loop, so the two cannot drift; falsifiability shown by widening the loop past `scan_bound` and watching the in-loop `assert!` trip (`distance 4128 > 4124`), then reverting. Built + smoke-green here (60 000 runs, exit 0, no crash); the **N-CPU-hour release gate stays OPEN** (`fuzz.yml` nightly/dispatch + a blocking per-PR smoke in `ci.yml`). **Framing (do not over-read):** the bounded-scan counter holds **structurally** — the loop is `while p <= end` with `end = start + scan_bound(..)`, so `distance ≤ scan_bound` for *every* input; the `assert!` can only ever trip on a future change that **decouples the loop window from `scan_bound`**. So it is a **drift/regression guard**, not the headline D11 finding. The substantive D11 proof in F1 is the **no-panic / no-OOB / no-unbounded-alloc / termination** surface over adversarial inputs — the crash-free fuzzing (60 000 runs now, the N-CPU-hour gate later).)*
- **F2 Decoder fuzz** — single-record decoder in isolation.
- **F3 Structure-aware fuzz** — `arbitrary`-generated mostly-valid segments with localized mutations (flip CRC, extend length, zero a region, tamper padding), driving the tail-vs-corruption classifier.
- **F4 Operation-script fuzz** — drive the §14.3 oracle harness from fuzzer-provided op scripts. **DEFERRED to M9** (like F1–F3); the §14.3 in-tree proptest harness (M6, `tests/model_oracle.rs`) is the interim generative coverage, and its executor (`tests/model/mod.rs::run`) is already proptest-free so the F4 target reuses it verbatim.
- Maintain a corpus; run continuously; release gate: N CPU-hours, zero new crashes.

### 14.6 Concurrency & memory-model
- **Miri** on unit + property suites — UB, uninitialized reads, **alignment** (essential if any `unsafe`/zero-copy header casting is used).
- **`!Sync` compile-fail test** (`trybuild`): sharing a write handle across threads fails to compile; two concurrent `&mut` borrows rejected.
- **Directory-lock test:** a second `open` of the same dir (same process; and a second process where feasible) fails with `Locked`.
- **`loom` harness for the integration barrier (recommended, integrator-facing).** Model the journal-consumer→publish-consumer handoff; prove that under all interleavings the publish side never treats a record as publishable before the corresponding `commit` returned, given Release store / Acquire load on the shared cursor.

### 14.7 Performance & regression (`criterion`)
- **Throughput:** records/s and MB/s for 64 B / 256 B / 4 KiB / 64 KiB payloads.
- **Commit latency** p50/p99/p999 vs batch size (1, 8, 64, 512, 4096) — validates the group-commit self-regulation curve.
- **Recovery time** vs log size and segment count.
- **Split-batch overhead:** commit latency when a batch spans a segment boundary vs when it does not (quantify the extra fsync).
- **Zero-allocation assertion:** an allocation counter (`dhat` / custom allocator) proves steady-state `append`+`commit` (no roll) and `Reader::next` perform **zero** heap allocations after warm-up.
- **Regression gates:** stored baselines; CI fails if throughput regresses > 10% or p999 > 20% (tune to runner variance; pin CPU governor).

> **M7 status (IMPLEMENTED, with one enforcement caveat tracked honestly).** `benches/wal.rs`
> implements all four criterion groups (throughput, commit-latency, recovery, split-batch) over
> the public API against a real `fdatasync`, with fixtures built outside the measured closure.
> Because criterion reports only point estimates (mean/median), **not** arbitrary percentiles, the
> commit-latency group records per-iteration timings into an `hdrhistogram` and emits p50/p99/p999
> itself — persisted to `target/perf/commit_latency_<batch>.json`. The **zero-allocation
> assertion** (`tests/zero_alloc.rs`) is hardened: it now proves the measured window did **not**
> roll (segment-file count + `durable_lsn` advance, both checked outside the counted region) and
> adds a `max_record_size`-payload variant. The **regression gate** (`scripts/perf-gate.sh`)
> implements the thresholds — throughput/**median-time** (median, not the outlier-sensitive mean)
> from criterion's `estimates.json`, **p999** from the histogram JSON (criterion has no percentile
> to read) — with subcommands `baseline`/`compare`/`check` and a falsifiability demo (it flags an
> injected regression). **The "CI fails …" enforcement is honored on a controlled, pinned-CPU-
> governor runner — exactly what this line's own "pin CPU governor" assumes.** On hosted runners
> the gate runs **informational** (`continue-on-error`, `bench.yml`), like the LazyFS gate, because
> shared CPUs / variable fsync make a hard gate flap; the thresholds stay a real gate on a
> controlled runner and enforcement is **OPEN-pending-controlled-runner**, never dropped. Absolute
> numbers on hosted/tmpfs hardware are unrepresentative (curve shape, not headline throughput — the
> real numbers are the §14.8 H1/H2 hardware gate). *(Added in M7.)*

### 14.8 Hardware durability validation (the real guarantee)
- **H1 Power-pull (only true durability test).** On target storage, sustained committed writes record the highest acked LSN to a side channel; hard-cut power (PDU/VM force-stop, not graceful); on reboot, `open` and assert every acked LSN ≤ side-channel value is present (**D1**). ≥ M cycles (e.g. 50) with zero loss to pass. Pre-release and on hardware change.
- **H2 Cache-mode / lying-device check.** Verify/​document VM/cloud block-device cache mode (`cache=none`/`writethrough`). Label devices that pass only with PLP cache; flag consumer devices that lose acked data.
- **H3 fsync-failure injection (`dm-flakey`/`dm-dust`).** Make the device return `EIO` on writes/flushes. Assert: failed `fdatasync` does **not** advance `durable_lsn` past the failed segment, surfaces `FsyncFailed`, and **poisons** the handle (§12); subsequent ops return `Poisoned`. Include the split-batch case (first segment synced, second fails ⇒ `durable_lsn` at the first segment's max, then poisoned).
- **H4 macOS `F_FULLFSYNC` check.** Assert the durable path issues `F_FULLFSYNC` on macOS (syscall trace / `dtruss` in a test, or build-time assertion).

### 14.9 Differential / reference testing (optional, high value)
A deliberately slow, obviously-correct **reference parser** (separate code path, possibly another language) run alongside the production parser on the fuzz corpus and model-harness outputs. Any divergence in classification (valid / torn-tail / fatal-corruption / truncation offset) is a bug.

### 14.10 Soak / endurance
Multi-hour randomized workload with periodic injected crashes+recoveries and checkpoints. Monitor: invariant violations, fd leaks, **disk-space leaks** (unreclaimed segments), memory growth, latency drift.

### 14.11 CI matrix
- **Per-PR (fast):** §14.1, §14.2 (reduced), §14.6 compile-fail + Miri subset, §14.7 alloc assertion (enforced, `ci.yml` `cargo test`) + `cargo bench --no-run` (benches must compile, can't bitrot), **§14.8 H4 Half A** *(M8: `m8-macos.yml` on `macos-latest`, paths-filtered to the durable-path sources + the test; per-PR + push-to-main + manual — a macOS-only `F_FULLFSYNC`-routing regression is invisible to the Linux PR CI because the `cfg(macos)` path does not compile there; `dtruss` Half B stays owner-run per #19)*.
- **Nightly:** full §14.2/§14.3 (high iteration), §14.4 LazyFS suite, §14.5 fuzz (time-boxed), §14.7 benchmarks + gates *(M7: `bench.yml`, schedule + manual; the gate runs **informational** on hosted runners until a controlled/pinned-governor runner makes the §14.7 thresholds enforceable — same stopgap as the LazyFS gate)*, §14.9 differential, **§14.8 H3-physical + §14.4d** *(M8: `m8-dmflakey.yml`, push-to-main + schedule + manual, not per-PR; hosted ubuntu VMs reach `dm-flakey`, so these are real CI gates there — ext4 hard with a source-confirmed block-layer EIO (#16) and a `drop_writes` positive control (#17), xfs/btrfs informational, **best-effort + loud skip** if a runner lacks dm-flakey)*. **H4 Half A** is per-PR not nightly — see Per-PR row addendum below.
- **Pre-release / manual:** §14.8 H1 power-pull on target hardware, §14.10 soak. *(The nightly §14.8 dm-flakey/macOS gates above also accept `workflow_dispatch`; a manual run posts its §5 evidence to the tracking issue as the human sign-off, while the nightly cron stays artifact-only and surfaces regressions as a red build.)*
- **OS matrix:** Linux (primary — sole platform for the §14.8 hardware-durability gate), macOS (dev/correctness — exercises `F_FULLFSYNC`; unit/property/fuzz only, not §14.8). Windows is out of scope for v1 (§8.3).
- **FS matrix (Linux):** ext4, xfs, btrfs (CoW), tmpfs (logic only — never durability claims).

### 14.12 Traceability matrix (invariant → tests)

| Invariant | Covered by |
|---|---|
| D1 Durability on commit | §14.3, §14.4b clear-cache, §14.4c, §14.8 H1 |
| D2 Dense gap-free suffix | §14.2 P2/P4, §14.3, §14.4b torn-seq, §14.1 (contiguity), §14.8 |
| D3 At-most-tail loss | §14.3, §14.4a, §14.4b, §14.4c, §14.8 H1 |
| D4 Torn-tail truncation | §14.4b torn-op, §14.4e (i), §14.4f, §14.4g |
| D5 Mid-log corruption fatal | §14.4e (ii)(iii)(v), sealed-segment cases |
| D6 Read-back fidelity | §14.2 P1, §14.3 |
| D7 Idempotent recovery | §14.2 P6, §14.3 (no-mutation reopen) |
| D8 Checkpoint safety | §14.1 math, §14.2 P5, §14.3 (terminal reopen), §14.4c |
| D9 Crash-anywhere recoverable | §14.4a, §14.4c (incl. split-batch & roll) |
| D10 No buried garbage / resurrection | §14.4g (incl. stale-valid-record case) |
| D11 Bounded recovery parsing | §14.5 F1/F2/F3, §14.4f, §14.6 Miri, bounded-scan counter |
| D12 Sealed-segment immutability | §14.4h sealed-segment-immutability, concurrent-tailer, backup round-trip |

### 14.13 Definition of Done (release gate)
- Every row of §14.12 has ≥ 1 passing test.
- Every enumerated crash point in §14.4c (including split-batch and roll sub-cases) has a test.
- §14.4d negative control catches the injected bug **and** the correct build passes. *(M8 — **satisfied by Tier-1.** **Tier-1 (primary) PASSES, deterministic + per-PR:** `scripts/m8/dirfsync-presence.sh` (in `ci.yml`) straces the roll path and asserts the correct build issues the roll-time directory `fsync` while `--features inject_no_dir_fsync` does not — verified green (`correct=5` dir-fsyncs vs `inject=1`). FS-independent syscall-presence regression guard; the row's satisfier. **Tier-2 (behavioral power-loss) — CLOSED as a documented negative result (PR #21, owner Fedora 43):** the synchronized mid-run cut (`dirfsync_cut_workload`, `dirfsync-negative <fs>`) blocks the workload with the new segment's dirent un-synced and cuts inside the window, yet the inject build recovers fully on **every** config tested — ext4/xfs/btrfs, journal-less ext4 (incl. `ext2`-format, serviced by the ext4 driver on modern kernels — standalone ext2 driver removed in Linux 6.9), and journaled ext4 `data=writeback` (the driver's weakest ordering). The dirent reaches disk via the file's own `fdatasync` everywhere; the earlier "ext2 block-adjacency" claim is **retracted** and the mechanism was not isolated. No readily-available Linux FS exposes it behaviorally ⇒ honest negative result, not a gap. **Tier-3 — ext4/xfs/btrfs (+ journal-less "ext2") INCONCLUSIVE-by-design**, never red on a masked miss. `fsync_dir` retained unconditionally as a POSIX-portability safeguard. Earlier "certified on ext4" was wrong; the harness loud-skips where dm-flakey is absent rather than fake green; the positive split+roll power-loss case passes under LazyFS in M4.)*
- §14.4g resurrection test passes **and** is demonstrated to fail both (a) if zeroing-on-truncate is disabled and (b) if the invalidation is not durably synced (the power-loss-of-zeroing assertion).
- Fuzzers F1–F4: ≥ N CPU-hours since the last parser/format change, zero outstanding crashes; bounded-scan counter never exceeds the bound. *(**M9 in progress. F1 (recovery-parser) IMPLEMENTED** — `fuzz/fuzz_targets/recovery.rs`, primary surface the real `Wal::open` over an adversarial multi-segment directory, bounded-scan counter instrumented on the real scan loop and asserted against the shared `scan_bound` symbol (falsifiability demonstrated). Built + smoke-green (60 000 runs, zero crashes); CI is `fuzz.yml` (nightly/dispatch, time-boxed, contingent) + a blocking per-PR smoke in `ci.yml`. The **N-CPU-hour gate itself stays OPEN** — a hosted short slice does not meet it; carry until a dedicated runner accrues the hours. **Framing:** the "bounded-scan counter never exceeds the bound" clause is satisfied **structurally** (the loop window *is* `scan_bound`), so it is a drift/regression guard, not the headline — the substantive D11 proof is the crash-free / no-OOB / termination surface over adversarial inputs (the running fuzz). **F2/F3/F4 still pending** within M9 (interim coverage as before: §14.5 F2–F3 by the codec proptest; F4 by the M6 oracle harness whose `run(cfg, ops)` the cargo-fuzz target reuses verbatim).)*
- §14.8 H1: ≥ M power-pull cycles on target hardware, zero acked-record loss. *(M8: the **harness + runbook are built** — `src/bin/power_pull_{workload,verify}.rs` + `scripts/m8/power-pull.sh`, with the off-box network side channel, send-strictly-after-`commit() Ok` ack-ordering, contiguous-watermark conservative verify, and the H2 vacuous-pass gate as a precondition; the mechanical chain was dry-run green on loopback. **OPEN-pending-owner-run** for the actual ≥50-cycle power-pull on real/cache-configured hardware (no cuttable target in the sandbox). H3 fsync-failure poison: the **§12 state machine RUNS green** via the LD_PRELOAD shim (`scripts/m8/fsync-fault.sh`); the **physical** dm-flakey half now runs **nightly + manual on hosted CI** (`m8-dmflakey.yml`, best-effort + loud skip) instead of owner-only. H4 macOS `F_FULLFSYNC` **Half A** (routing/smoke) now runs on **macOS CI** (`m8-macos.yml`); Half B (`dtruss` trace) stays owner-run (root + SIP). See `docs/m8-runbook.md`.)*
- Zero-allocation assertion (§14.7) passes for append/commit and `Reader::next`. *(M7: PASSES — hardened to also prove no-roll in the measured window and to cover a `max_record_size` payload. The §14.7 benches + regression gate exist; **gate enforcement is OPEN-pending-controlled-runner** — informational on hosted CI per §14.11, a real gate on a pinned-governor runner.)*
- Miri clean on covered suites.

---

## 15. External readers, backup, and replication (NORMATIVE where marked)

The WAL is single-writer, but its on-disk format is deliberately *tailable*: a separate process can read and follow the log during live operation, and segments can be copied for backup. This section defines the contracts that make external access **safe**, so that nobody builds a diverging secondary or a torn backup by accident. The core writer contract (single-writer, durability-first) is unchanged; everything here is additive and read-only with respect to the WAL.

### 15.0 What the writer guarantees to external consumers
- **Tailable format.** Records are self-describing, LSN-stamped, and CRC-protected; segments are discoverable by sorted `base_lsn` filename. A reader uses the §8.2 record-scan logic.
- **Sealed-segment immutability (D12).** A sealed segment is byte-identical from the instant it is sealed until checkpoint deletes it. This is what makes backup trivial and tailing coherent.
- **A durable watermark (§15.3).** The writer publishes `durable_lsn` out-of-band so readers can tell which records are durable. The writer does **not** push notifications, manage reader cursors, or perform any network I/O.

### 15.1 The durability-visibility gap (NORMATIVE — the central hazard)
The writer's `write(2)` makes bytes visible in the shared page cache **before** `fdatasync`. Therefore a cross-process reader can read records that are complete and **CRC-valid but not yet durable**. On power loss those records vanish from the primary.

- **A CRC-valid record is NOT necessarily durable.** CRC proves *completeness and integrity*, not *synced-ness*. A reader MUST NOT treat CRC validity as a durability signal.
- A reader that must not diverge from the primary's durable state (any replication consumer; a backup that must exactly match a recovery point) MUST NOT consume records beyond the **published durable watermark** (§15.3), regardless of how many CRC-valid records are visible past it.
- A pure backup-and-restore consumer MAY ignore the watermark (see §15.6): capturing a few un-synced records is harmless because restore re-runs recovery and the result is still an internally consistent dense prefix.

### 15.2 Read-only attach and tailing semantics (NORMATIVE for a correct reader)
- **Attach:** a read-only opener (e.g. `WalReader::open`) MUST take a *shared* lock or none — never the writer's exclusive `LOCK` (§6.2). It has no write capability and MUST NOT modify, truncate, or zero any file.
- **Scan:** use the §8.2 record logic (length-bound → CRC → LSN-continuity).
- **Tail CRC-failure is "retry," not "truncate."** This is the key difference from recovery. A short read, a `rec_type==0` sentinel, or a CRC failure at the current tail means *"the writer has not finished writing here yet"* — the reader waits and re-reads, and MUST NOT conclude corruption or truncation. (Recovery, which holds the exclusive lock and knows writing has stopped, treats the same condition as a torn tail; a live reader cannot, because the writer may be mid-`write`.) A reader never truncates anything regardless.
- **Segment-roll following:** when a higher-`base_lsn` segment file appears, the current segment is sealed; finish it (to its zero sentinel) and advance. Until a successor appears, the current segment is still active — keep polling.
- **Growth detection:** `inotify`/`FSEvents` or interval polling. Implementation choice; not normative.
- **Consume boundary:** the highest record the reader may *act on* is `min(highest contiguously-valid record, published durable watermark)` for divergence-sensitive consumers; `highest contiguously-valid record` for pure backup.

### 15.3 Durable-watermark publication — `DurabilityObserver` (NORMATIVE; resolved)

The writer publishes `durable_lsn` through a **pluggable observer** so v1 stays scoped while leaving room for future channels. The observer publishes only the *watermark* (an LSN); shipping actual record bytes is a downstream consumer's job (§15.7), not the observer's.

```rust
/// Fires after each successful durability advance, on the writer thread.
pub trait DurabilityObserver {
    /// `durable_lsn` is the new (monotonic) durable watermark.
    /// MUST be cheap and non-blocking: an atomic release-store or a queue push.
    /// MUST NOT perform I/O, block, or panic. Runs synchronously inside commit().
    fn on_durable(&mut self, durable_lsn: Lsn);
}

/// Default. Zero-cost: inlines to nothing.
pub struct NullObserver;
impl DurabilityObserver for NullObserver { fn on_durable(&mut self, _: Lsn) {} }
```

- **Static dispatch, null default:** the `Wal` is generic over the observer with `NullObserver` as the default — `Wal<O: DurabilityObserver = NullObserver>` — so the don't-ship case compiles away to nothing (no vtable call on the commit path). Use `dyn` only if runtime strategy selection is ever needed.
- **When it fires:** at the end of `commit`/`sync`, after `durable_lsn` has advanced, including on the split-batch partial-failure path (it is notified of the *achieved* watermark before `commit` returns `Err` and the handle poisons, so the watermark always reflects true durable state). `durable_lsn` is monotonic.
- **Downstream of durability — cannot affect correctness.** Because it fires only after durability is achieved, an observer can never compromise D1–D12. Its contract is "cheap, non-blocking, must not panic"; it has no path to fail durability.
- **v1 built-ins:** `NullObserver` (default, don't ship) and an **in-process observer** that forwards `durable_lsn` to a caller-supplied sink (e.g. an `AtomicU64` release-store, or an `mpsc`/ring push) which a separate shipping/replication consumer drains. **Future** strategies (a same-machine mmap'd cursor for cross-process readers; a network watermark) are additive new impls requiring no core change.

A cross-process reader that has no in-process channel obtains the watermark from whatever a future strategy publishes (e.g. the mmap'd cursor); until such a strategy ships, cross-process *replication* readers are out of scope (backup, §15.6, needs no watermark). The watermark is **advisory for integrity** (the reader still CRC-validates every record) but **authoritative for durability** (a divergence-sensitive reader MUST NOT cross it).

### 15.4 Retention floor — gap is fatal (NORMATIVE; resolved)
`checkpoint` (§9) deletes whole sealed segments; a lagging reader/backup may still need them. This is an *availability* concern, orthogonal to durability. **v1 policy: an unexpected gap is fatal on the reader side; the writer gains no new machinery.**

- The writer keeps doing plain `checkpoint(up_to)` (delete oldest-first, dir-fsync). It does **not** track reader positions or gate deletion on readers in v1.
- An already-open fd to a segment survives `unlink` (Linux), so a reader **mid-segment** is unaffected by a concurrent delete and finishes that segment normally.
- **Gap detection (NORMATIVE reader rule):** if the **oldest available LSN is greater than the reader's next-expected LSN**, the records the reader still needs were checkpointed away — the reader MUST fail with a distinct fatal error. It MUST NOT silently skip to the oldest available record. (A reader jumping from LSN 100 to 100 000 unnoticed is the footgun this rule forbids.) Recovery from a fatal gap is operational: re-seed the reader from a fresh snapshot/backup and resume.
- **Not a contradiction with D2.** A missing *prefix* is fine for the *writer's* recovery (its snapshot authorized the checkpoint) but fatal for a *reader* that still needed those records — same bytes, different consumer, both correct.
- **Forward-compatible.** A future registered-min-LSN gating strategy (writer respects the slowest reader) is purely additive: it prevents the gap from ever arising, and readers that handled the fatal case simply stop hitting it. The on-disk format is unaffected.

### 15.5 Sealed-segment immutability in practice
Per D12, sealed segments never change. Backups and readers MAY therefore cache, copy, or checksum a sealed segment once and trust it indefinitely (until deletion). Only the **active** segment is mutable, and only by append (and, at recovery, tail zeroing). A reader distinguishes the active segment as the highest-`base_lsn` file present.

### 15.6 Backup (cross-process file copy)
The recommended, low-coordination pattern — the `pg_basebackup` + WAL-archiving / Kafka closed-segment-copy tradition:
- **Sealed segments:** copy freely and concurrently; they are immutable (D12).
- **Active segment:** copy as-is. On restore, run normal recovery (§8), which truncates any torn/partial tail — so a copied-mid-append active segment is always restorable.
- **Watermark not required.** A backup that captures un-synced records is still a consistent dense prefix; restore makes it durable. (Only adopt the watermark if the backup must match an exact durability point.)
- **Coordinate with the retention floor (§15.4)** so a long-running copy is not undercut by a concurrent checkpoint.

### 15.7 Replication (primary/secondary) — prefer an in-process shipping consumer
For a secondary, prefer a **log-shipping consumer in the writer's process** (a sibling to the publish consumer on the daisy-chain) over cross-process file tailing. It learns the durable watermark either from the integrator's existing journal cursor or from the in-process `DurabilityObserver` (§15.3), reads the newly-durable record range via the `Reader`, and ships it. It works across machines (file tailing does not) and exposes the sync/async choice directly. The secondary writes its **own** WAL from the shipped stream and is itself a durability-first consumer.

- **The replica MUST NOT be allowed ahead of the primary's durable state** — ship only records `≤ durable_lsn`. This is the divergence guard; it is the §15.1 hazard applied to replication.
- **Async replication:** ship after local durability; the client was already acked on local durability; the replica is durable *eventually*; a failover may lose the un-shipped durable tail (bounded by replication lag).
- **Synchronous replication:** the client ack is gated on local durability **and** the replica's durable-ack. This is an *additional* barrier on the ack path, not the same one as shipping — it is the recursion of the daisy chain: "publish" is redefined to mean "durable here *and* durable on the replica." This is what actually *guarantees durability on the replica before acknowledging*.
- The WAL component's responsibility ends at *providing committed records in order plus the watermark*; the transport, ack handshake, and failover are the integrator's (§1 Non-Goals).

### 15.8 Tests for this section (added to §14)
- **§14.4h Concurrent tailer (property + fault).** A reader (thread/process) tails while the writer appends/commits/rolls/checkpoints. Assert the reader: observes a monotonic dense prefix; never surfaces a partial/torn record as valid (treats tail-CRC-failure as retry); follows segment rolls without gap; and **never acts on a record beyond the published watermark** — verified by delaying the writer's `fdatasync` and asserting the watermarked reader does not surface the buffered records until the sync completes.
- **Watermark-divergence (under LazyFS).** Writer `write`s a batch, the watermark has *not* advanced, inject power-loss + crash; assert a watermarked reader had **not** consumed the lost records (no divergence), while an un-watermarked reader *would* have (demonstrating why the watermark is mandatory for replication). This is the headline replication-safety test.
- **Sealed-segment immutability (D12).** Hash each sealed segment at seal time; after arbitrary further writer activity (appends, rolls, a full recovery cycle), assert the hash is unchanged until checkpoint deletes the segment.
- **Checkpoint-under-reader.** A reader holds an open fd to a segment; the writer checkpoints/deletes it; assert the open fd still reads the segment fully (open-fd-survives-unlink), and that a reader which had *not* yet opened a since-deleted segment detects the gap as a **fatal error**, never a silent skip — specifically, that `oldest_available_lsn > next_expected_lsn` triggers the fatal-gap path (§15.4).
- **`DurabilityObserver` contract.** `NullObserver` is a verified no-op (compiles away — assert zero added allocations/branches on the commit path via §14.7). The in-process observer receives a **monotonic** `durable_lsn` exactly after each commit's durability advance, including the achieved watermark on a split-batch partial failure *before* the poison. Assert the observer is **never** notified of an LSN before its `fdatasync` completed (pair with the watermark-divergence test). Assert an observer that panics is a contract violation surfaced in tests, and that observer behavior can never change recovered state (it is downstream of durability).
- **Backup round-trip.** Copy sealed segments + the active segment mid-write; restore via recovery; assert the restored WAL is a valid dense prefix equal to the source up to some `k`.

Add **D12** to the §14.12 traceability matrix (covered by the immutability and concurrent-tailer tests).

---

## 16. Suggested dependencies
- `crc32c` — Castagnoli CRC, hardware-accelerated. *(Not `crc32fast` — wrong polynomial.)*
- `rustix` (or `libc`) — `fallocate` for segment pre-allocation (required); `fdatasync`/`fsync`, directory `fsync`, macOS `F_FULLFSYNC` via `fcntl`, `flock`. (`FALLOC_FL_PUNCH_HOLE` only if the optional truncation optimization in §8.2.1 is adopted — the normative path needs just `pwrite`.)
- `proptest` or `bolero` — property testing (`bolero` unifies fuzz + property).
- `arbitrary` — structured fuzz inputs.
- `cargo-fuzz` (libFuzzer) and/or `afl`.
- `criterion` — benchmarks.
- `loom` — concurrency model checking for the integration-barrier harness.
- `trybuild` — compile-fail tests for `!Sync`/single-writer.
- Miri — `rustup component add miri`.
- **External (not crates):** LazyFS (build from source; FUSE), `dm-flakey`/`dm-dust` (device-mapper), a power-cuttable target/VM for H1.

Keep the runtime dependency set minimal.

---

## 17. Decisions (all resolved for v1)
1. **Record-spanning segments — RESOLVED.** v1 forbids spanning. `segment_size` and `max_record_size` are user-configurable; payloads are bounded under ~1 MiB (no out-of-band-blob pattern needed at that size), with the `max_record_size ≤ segment_size − 91` bound enforced at `open()` (§5.3). Fragmentation (`rec_type` 2..) remains a reserved v2 feature.
2. **Input vs output journal — RESOLVED (no effect on this spec).** The integrator journals **both** input and output by **instantiating this component twice** (two directories, two locks, two `durable_lsn`s, two recoveries). The two logs have **independent LSN spaces** — correlate them in payloads if needed, not via the WAL. **Serialization format is an application concern, explicitly out of WAL scope** (the WAL stores opaque bytes; the only constraint is `max_record_size`). Immutable + versioned business logic makes input-journal replay deterministic and safe. The component contract is unchanged either way.
3. **Checkpoint trigger ownership — RESOLVED.** Integrator-owned (its snapshot consumer). The binding rule (§9) is `up_to ≤ latest durable **snapshot** LSN` (not `durable_lsn`); retain a margin for readers/backup given gap-is-fatal (§15.4). The WAL trusts the caller.
4. **`Reader` shape — RESOLVED.** Streaming `Reader::next` (lending, zero-copy) is the only v1 shape. Consumers that must retain a record (e.g. async replication shipping) `.to_vec()`/`Bytes`-copy the borrowed slice themselves — a copy paid at the network boundary anyway. No owned/`Bytes` adapter and no mmap-backed `Iterator` in v1.
5. **Windows — RESOLVED: out of scope for v1.** Platform tiering: Linux = production + hardware-durability gate (§14.8); macOS = dev/correctness (`F_FULLFSYNC`, no production claim, not in §14.8); Windows = future (`FlushFileBuffers`), untested in v1 (§8.3, §14.11).
6. **External-reader support (§15) — RESOLVED for v1.** (a) Watermark publication is a pluggable **`DurabilityObserver`** (§15.3) with two built-ins: `NullObserver` (default, don't ship) and an in-process observer forwarding `durable_lsn` to a caller sink; static dispatch with `NullObserver` default keeps it zero-cost; future channels (mmap'd cursor, network) are additive impls. (b) Retention floor is **gap-is-fatal** (§15.4): no writer-side gating in v1; readers fatally error when `oldest_available_lsn > next_expected_lsn`; a future min-LSN gating strategy is additive. Sealed-segment immutability (D12) is implemented regardless, so backup works in v1 even though cross-process *replication* readers await a future watermark strategy.

---

## 18. References
- LazyFS — *When Amnesia Strikes: Understanding and Reproducing Data Loss Bugs with Fault Injection* (VLDB); repo `dsrhaslab/lazyfs`. Simulates POSIX persistence; injects lost/torn writes; used to study PostgreSQL, etcd, Zookeeper, Redis, LevelDB, PebblesDB.
- Jepsen — filesystem fault work (`jepsen.io/filesystem`).
- *All File Systems Are Not Created Equal* (OSDI '14); *Finding Crash-Consistency Bugs with Bounded Black-Box Crash Testing* (ALICE/CrashMonkey, UTSASLab).
- PostgreSQL WAL internals and the *fsyncgate* discussion (2018).
- RocksDB WAL format and recovery modes (`kTolerateCorruptedTailRecords`, `kAbsoluteConsistency`, `kPointInTimeRecovery`).
- Apache Kafka log/segment design.
- ARIES (Mohan et al.).
- Reference Rust implementations to read (not depend on): `walogs`, `qdrant/wal`, `zowens/commitlog`; and "Building Segmented Logs in Rust" by arindas.
