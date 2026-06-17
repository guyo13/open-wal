# CLAUDE.md — WAL component

Operating guide for working in this repository. Read this fully before writing code, and re-read **§4, §5, §8, §12, §14, §15** of `docs/design.md` before touching durability, recovery, the on-disk format, or external-reader/observer hooks. `docs/design.md` is the **build contract**; this file is the always-in-context summary of what must never be violated.

---

## What this is

A focused, embeddable, **single-writer** append-only **write-ahead log** for an LMAX-style, event-sourced system. **Durability-first**: a committed record survives process crash and power loss on honest hardware. Not a database, not multi-writer, no background threads, minimal dependencies. The WAL stores **opaque byte payloads** — serialization/encoding is entirely the caller's concern, never the WAL's (the only payload constraint is `max_record_size`). The integrator may run **two instances** (e.g. an input journal and an output journal); they are independent, with separate LSN spaces.

The entire value of this component is **correct behavior under crashes and fault injection.** Code that has not been *executed* against the fault-injection tests (§14.4, §14.5, §14.8) is not trustworthy and does not count as done. The feedback loop is the deliverable — favor running tests over reasoning about them.

---

## Prime directives

1. **Implement to the contract.** §4 (Durability Contract) and §5 (On-Disk Format) in `docs/design.md` are normative. Nothing may weaken them. If a change would, stop and flag it.
2. **Tests are co-equal with code.** Every milestone lists mapped tests in §14. A milestone is **not done** until those tests pass. Write the test in the same change as the code it covers.
3. **When uncertain, ask — don't guess.** §17 records the scope decisions and rationale (all resolved for v1). For anything ambiguous beyond those, raise it rather than silently resolving it. An autonomous wrong guess here is a silent data-loss bug.
4. **Never weaken a test to make it pass.** If a fault-injection test fails, the implementation is wrong, not the test.

---

## Invariants (D1–D12) — preserve all of these in every change

- **D1** After `commit()` returns `Ok(w)`, every record `lsn ≤ w` survives process crash **and** power loss.
- **D2** Durable content is a dense, gap-free run `P..=k` (`P` = oldest surviving segment's base). Never an *internal* gap.
- **D3** A crash loses at most the un-committed tail; nothing `≤` the last returned `durable_lsn`.
- **D4** A torn tail is detected and truncated; the truncated region is durably invalidated (§8.2.1). A torn record is never surfaced as valid.
- **D5** Mid-log corruption (a valid record exists after the bad one) is **fatal and loud**, never silently truncated.
- **D6** Replay returns exactly the records appended, in LSN order, byte-identical.
- **D7** Recovery is deterministic and idempotent across repeated open/close.
- **D8** `checkpoint(up_to)` never removes a record `> up_to`, never makes a retained record unreadable.
- **D9** A crash *anywhere* (append, commit incl. between the two syncs of a split batch, roll, checkpoint) recovers to a valid dense suffix.
- **D10** No buried garbage / no resurrection: a stale-but-CRC-valid record (even one whose LSN matches the expected next) is never resurrected. Enforced by zeroing-to-EOF on truncation.
- **D11** Recovery terminates and never panics, reads OOB, allocates unboundedly, or scans unboundedly — for **any** input bytes.
- **D12** Sealed-segment immutability: once a segment is sealed (a higher-`base_lsn` segment exists), the writer never modifies its bytes; it is only deleted whole by checkpoint. Tail-zeroing touches only the **active** segment, only at recovery. (This is what makes backup and tailing safe — §15.)

### Non-guarantees (do not assume these)
- **`commit` is NOT atomic.** Per-record durability + dense prefix only. A split batch may leave the first segment durable and the rest lost — that is contract-compliant. Single-segment commits *look* atomic (one shared `fdatasync`) but that is incidental; **no logic or test may assume a commit batch is indivisible.**
- The only atomicity primitive is **per-record**. Multi-event atomicity, if ever needed, is modeled as a single record — never via commit grouping.
- The WAL does **not** authorize prefix deletion; that is the integrator's job (§4 D2).

### External access — v1 scope (§15)
- **Backup works in v1** (copy immutable sealed segments + active segment; restore runs recovery). Relies on **D12**, so D12 must hold from the moment segments seal — implement it in M4, not later.
- **In-process replication/shipping works in v1** via a consumer fed by the `DurabilityObserver` hook or the integrator's journal cursor.
- **Cross-process *replication* readers are deferred** (they await a future watermark-publishing `DurabilityObserver` impl, e.g. an mmap'd cursor). Don't build them in v1; don't let their absence block backup or in-process shipping.

---

## Hard rules / known footguns (these are the silent-corruption traps)

- **CRC:** use the **`crc32c`** crate (Castagnoli). **NOT `crc32fast`** (wrong polynomial — silent and catastrophic). CRC covers `[4, 4+16+length+pad)` — payload **and** padding.
- **`append` is pure memory and segment-agnostic** — no syscall, no allocation in steady state, no segment logic. All segment/roll/fsync logic lives in **`commit`** (§7.2–7.3).
- **Commit-time split is on WHOLE RECORDS, never a raw byte slice** (a record must not span segments, §5.3). An **empty "prefix that fits" is valid** → seal current segment, roll, continue (it counts as progress; do not spin). Termination is guaranteed by the `max_record_size` bound — keep that bound.
- **fsync discipline:**
  - Record data in a pre-allocated segment → `fdatasync` (`File::sync_data`) suffices.
  - Segment **creation** → `fdatasync` the file **and `fsync` the directory** (the dir-fsync is the classic gotcha; §14.4d exists to catch its omission).
  - **macOS:** `fdatasync`/`fsync` do NOT flush the drive cache — you MUST use `fcntl(F_FULLFSYNC)` everywhere durability is required.
- **Torn-tail invalidation (§8.2.1):** normative path is `pwrite` zeros over `[X, segment_size)` then **`fdatasync`** (pure data write — no alignment issue, `fdatasync` is correct). Must extend to **EOF**, not a bounded window. `PUNCH_HOLE` is optional only, and **only with a full `fsync`** + per-filesystem validation via §14.4g.
- **A failed `fdatasync`/`fsync` POISONS the handle.** Never retry-forever, never advance `durable_lsn` past a failed segment, never offer a "remain usable" path (§12).
- **Recovery never materializes payloads in memory** — sparse per-segment index only (§8.5). Multi-GB logs must not OOM `open()`.
- **Determinism in recovery:** no wall clock, no env, no filesystem iteration order (sort segments explicitly). `created_unix_nanos` is informational and must not influence any decision.
- **`Wal` write handle is `Send` but `!Sync`**, methods take `&mut self`; `open` takes an exclusive directory lock. Concurrent writers must be a compile error.
- **`DurabilityObserver` lives in CORE, not a "replication" module** — it is referenced by the §6 API. Shape: `Wal<O: DurabilityObserver = NullObserver>`; `NullObserver` is the zero-cost default (static dispatch, no vtable on the commit path). `on_durable(durable_lsn)` fires on the writer thread after each durability advance and MUST be cheap, non-blocking, no I/O, must-not-panic. It is **strictly downstream of durability and can never affect D1–D12** — it publishes the watermark only; shipping record bytes is a separate consumer's job (via `Reader`). Do not put network/blocking work in it.
- **Readers: gap is fatal (§15.4).** A reader fatally errors when `oldest_available_lsn > next_expected_lsn` — it MUST NOT silently skip. The writer's `checkpoint` gains no reader-gating machinery in v1.
- **Checkpoint bound: `up_to ≤ durable *snapshot* LSN`, NEVER `durable_lsn` (§9).** Recovery = latest durable snapshot + replay of the log after it; checkpointing to `durable_lsn` deletes the log between the snapshot and `durable_lsn` that replay needs, silently capping recovery at the stale snapshot. The integrator must honor this (it's the inverse of D8: D8 = the WAL won't delete what you kept; this = you must not ask it to delete what you can't rebuild). The WAL trusts the caller.
- **`open()` validates config (§5.3):** reject `max_record_size > segment_size − 91` with `InvalidConfig` at open — never discover it at roll time.

---

## Milestone order (do them in sequence; gates are mandatory)

| M | Scope | Done when |
|---|---|---|
| **M0** | Crate skeleton, `Lsn`/`WalConfig`/`WalError`, CRC-32C | §14.1 CRC vectors pass |
| **M1** | Record codec (encode/decode, bounds, padding-in-CRC) | §14.1 codec + §14.2 round-trip + §14.5 decoder fuzz |
| **M2** | Single-segment `append`/`commit`/`Reader`; pre-alloc; zero-alloc hot path | §14.1, §14.2, §14.7 alloc assertion |
| **M3** | **Intra-segment recovery** — tail detect, durable zero-to-EOF, bounded scan, sealed-vs-active, sparse index | **GATE — see below** |
| **M4** | Multi-segment + commit-time whole-record split, empty active segment, dir fsync, **sealed-segment immutability (D12)** | §14.2 P4/P7, §14.4c (split-batch), §14.4d, §14.4h immutability |
| **M5** | Checkpoint (oldest-first, dir fsync, contiguous suffix) | §14.1 math, §14.2 P5, §14.4c |
| **M6** | Stateful model/oracle harness | §14.3 high-iteration |
| **M7** | Benchmarks + regression gates + zero-alloc | §14.7 |
| **M8** | Hardware/platform durability (power-pull, dm-flakey, F_FULLFSYNC) | §14.8 |
| **M9** | Continuous fuzzing, Miri, soak, CI matrix | §14.5/§14.6/§14.10/§14.11 |

### The M3 gate (do not skip)
**Do not start M4 until §14.4b (LazyFS lost/torn writes) and §14.4g (resurrection + durability-of-zeroing) pass.** Intra-segment recovery is where this design's correctness actually lives; everything after it is comparatively mechanical. If the environment cannot run LazyFS (see below), **stop and report it** rather than marking M3 done — do not proceed on unverified recovery.

---

## Environment & tooling

- Rust **stable**. Keep runtime deps minimal: `crc32c`, `rustix` (fallocate/fdatasync/dir-fsync/F_FULLFSYNC/flock). Test/dev: `proptest` or `bolero`, `arbitrary`, `cargo-fuzz`, `criterion`, `loom`, `trybuild`; Miri via `rustup component add miri`.
- **Always run before declaring work complete:** `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`. For recovery/parser changes also run the relevant fuzz target for a bounded time.
- **Platform tiering (v1):** Linux = production + the §14.8 hardware-durability gate; macOS = dev/correctness only (`F_FULLFSYNC` honored, unit/property/fuzz run, but no production durability claim and not subject to §14.8); **Windows is out of scope for v1**.
- **Fault-injection environment:** §14.4b (LazyFS) needs **FUSE + Linux**; §14.8 (`dm-flakey`, power-pull) needs **privileges/a VM/CI runner**. These may not be available in the default sandbox. M0–M2 and most of M3's unit/property/fuzz layers need none of this and can iterate immediately; the LazyFS gate in M3 and all of M8 must run where FUSE/privileges exist. If they're unavailable, say so explicitly and leave the gate open — never fake or skip it.

---

## Working style here

- Build in vertical slices per milestone; small, reviewable changes.
- Put the failing test first where practical; never land code for a milestone without its mapped §14 tests.
- Reference invariants by ID in commit messages / PR descriptions (e.g. "M3: torn-tail truncation, D4/D10").
- If you find the design doc itself is wrong or underspecified, flag it and propose a fix — do not silently diverge from it.

---

## Project status (keep this updated)

- **Current milestone:** M0 — not started
- **M3 gate:** NOT yet passed (LazyFS §14.4b + §14.4g pending)
- **Known environment limitations:** _(record here whether FUSE/privileges are available)_
