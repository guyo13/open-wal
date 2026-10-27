# open-wal design spec — v6.1 → v7 delta

**Status:** Draft v7 delta — additions required by the `open-wal-replica` companion crate.
**Apply to:** `docs/wal_design_v6.md` → produce `docs/wal_design_v7.md`. This document lists
every change; everything not mentioned is unchanged. All changes are **additive** — no
invariant D1–D12 is weakened, no on-disk format change, no new destructive operation.

> **Correction (post-kickoff):** the cold-start seed is exposed through a new
> **`OpenOptions`-style builder** — `Wal::options().seed(n).observer(o).open(dir, cfg)` — **not**
> as a `pub` field on `WalConfig`. A new `pub` field is source-breaking for every exhaustive
> struct literal (`WalConfig { segment_size, max_record_size }`) — ~20 internal sites plus
> external users of the published 0.2 crate. The builder is truly additive (zero
> existing-source edits, no semver break; `open`/`open_with` stay as untouched conveniences),
> reflects the seed's one-shot nature, and composes seed with observer — a real need, since a
> **replica may host downstream consumers** off its own observer (replica spec §7.1). `WalConfig`
> is unchanged.

Motivation: async master/slave replication (§15.7) is now realized by a separate crate. It
needs exactly three small things from the WAL, all verified against the current code:
1. a way to cold-start a log at an LSN > 1 (the `OpenOptions::seed` cold-start seed) — a replica
   seeded from a snapshot at LSN *N* must be born at base *N+1*;
2. a **live** `oldest_lsn()` getter — today it exists only in `RecoveryReport` at open time,
   so after `checkpoint` advances the retention floor there is no way to read it, and the
   primary needs it at catch-up time to decide "serve from log" vs "re-seed";
3. `reader_from` should seek to the **containing** segment (as §8.5 already says) instead of
   opening the oldest segment and skipping forward through the whole log.

Plus two honesty reconciliations in §15 (the in-process observer built-in was never shipped;
replication is now a companion crate, and the WAL's responsibility boundary is restated).

---

## Changelog (v6.1 → v7) — add at the top of the changelog

- **§6 / §11 — `Wal::options()` builder with `.seed(initial_lsn)` and `.observer(o)`.**
  `WalConfig` is **unchanged**; `open`/`open_with` are untouched conveniences. The seed is
  honored **only** when `.open` cold-starts an empty directory: the first segment is created
  with `base_lsn = seed`, so `oldest_lsn = seed`, `durable_lsn = seed − 1`. If the directory
  already holds a log the seed is **ignored** and normal recovery runs (the on-disk log is
  authoritative). `.open` validates `seed ≥ Lsn(1)` (`Lsn(0)` is reserved) → `InvalidConfig`.
  This lets a replica mirror the primary's LSN space after being seeded from a snapshot, and
  lets that replica also carry an observer (to host downstream consumers). The on-disk format
  already supports `base_lsn > 1` (post-checkpoint recovery relies on it), so no recovery logic
  changes — `cold_start` simply takes the seed where it previously hard-coded `1`.
- **§6 — `Wal::oldest_lsn()` live getter.** Exposes the current retention floor `P` (already
  tracked internally; advances on `checkpoint`). Previously readable only via `RecoveryReport`.
- **§8.5 / §6 — `reader_from` seeks to the containing segment.** Implementation change to
  match the existing §8.5 text: locate the greatest `base_lsn ≤ from` via the sorted segment
  list (binary search) and open *that* segment, rather than opening the oldest segment and
  skipping forward. Makes a replay from `from` `O(one segment scan)` instead of `O(retained log)`.
  Semantics unchanged (same records, same order); D6/D7 unaffected. (Reason: catch-up reads
  in the replication crate call `reader_from` with a mid-log `from`; scanning from the oldest
  segment each time was the v6 behavior and is unacceptable for large retained logs.)
- **§15.3 — reconciled.** v6 claimed two v1 built-in observers (`NullObserver` and an
  in-process observer forwarding to a caller sink). Only `NullObserver` shipped. Corrected: the
  `DurabilityObserver` **trait** is the extension point; the in-process forwarding observer is
  provided by consumers (e.g. the replication crate implements it) and is **not** a WAL built-in.
- **§15.7 — replication is now a companion crate.** The WAL's responsibility boundary is
  restated precisely: it provides `append`/`commit`/`durable_lsn`/`last_lsn`/`oldest_lsn`/
  `reader_from`/`checkpoint`/`initial_lsn` and the observer trait. Transport, shipping,
  catch-up, re-seed, checkpoint policy, and promotion are the crate's (`open-wal-replica`).
- **§17 — Decision 7 added** (below). Explicitly NOT added: a checkpoint observer, writer-side
  reader gating (still deferred per §15.4), or a suffix-truncate operation (deferred to a
  leader-election era; see Decision 7).

---

## §6 Public API — amend

`WalConfig` is **unchanged**. Add an `OpenOptions`-style builder; `open`/`open_with` stay as
untouched conveniences (`open(d,c)` ≡ `options().open(d,c)`;
`open_with(d,c,o)` ≡ `options().observer(o).open(d,c)`):
```rust
pub struct OpenOptions<O: DurabilityObserver = NullObserver> { seed: Lsn, observer: O }

impl Wal<NullObserver> {
    /// Start from defaults: seed = Lsn(1), observer = NullObserver.
    pub fn options() -> OpenOptions<NullObserver>;
}
impl<O: DurabilityObserver> OpenOptions<O> {
    /// Cold-start seed: the first LSN of a FRESH log (must be ≥ Lsn(1)). Used by replicas
    /// seeded from a snapshot at LSN N (pass N+1). Ignored if `dir` already holds a log.
    pub fn seed(self, initial_lsn: Lsn) -> Self;
    /// Attach a durability observer (changes the `O` type parameter).
    pub fn observer<P: DurabilityObserver>(self, o: P) -> OpenOptions<P>;
    /// Open or create the WAL, running full recovery. Validates the seed.
    pub fn open(self, dir: &Path, config: WalConfig)
        -> Result<(Wal<O>, RecoveryReport), WalError>;
}
```
*Why a builder, not a config field:* a new `pub` field breaks every exhaustive
`WalConfig { .. }` literal (source-breaking for the published crate), and a one-shot seed that is
ignored on any existing log is not a steady-state config property. *Why a builder, not more
`open_*` functions:* seed and observer are independent optional dimensions (four combinations
already, more plausible later — e.g. a read-only mode), and the seed+observer case is a real
need: a replica seeded from a snapshot may also host downstream consumers off its observer
(replica spec §7.1). A builder keeps that a two-method chain instead of a function
cross-product. Future open-time options go on `OpenOptions`, never as new positional openers.

Add to `impl Wal<O>`:
```rust
/// Current retention floor `P`: base LSN of the oldest surviving segment.
/// Advances on `checkpoint`. Equals `RecoveryReport::oldest_lsn` at open.
pub fn oldest_lsn(&self) -> Lsn;
```

`reader_from` doc: note it locates the containing segment (§8.5) — cost is one segment scan
from that segment's start to `from`, not a whole-log scan.

## §8.4 Cold start — amend

- **Cold start (empty directory):** `.open` creates `{seed:020}.wal` with `base_lsn = seed`,
  where `seed` is the `OpenOptions` seed (`1` by default and for `open`/`open_with`);
  `oldest_lsn = seed`, `durable_lsn = seed − 1`, `tail_state = Clean`. The first `append`
  yields `Lsn(seed)`. (A cold start at `N` is byte-identical in structure to the
  post-checkpoint state `P = N` that recovery already handles — §8.1 / §4 D2.)
- The "highest-base file with absent/incomplete header ⇒ discard, prior segment becomes
  active; emptied bases ⇒ cold start" rule is unchanged and now cold-starts at the seed.

## §8.5 — amend (conformance note)

`reader_from(from)` MUST locate the containing segment (greatest `base_lsn ≤ from`, via the
sorted per-segment index) and scan **that** segment from its start, skipping to `from`. It
MUST NOT open the oldest segment of the log. Memory discipline unchanged.

## §11 Configuration — amend

- **`OpenOptions::seed(initial_lsn)`**: `.open` rejects `Lsn(0)` with `InvalidConfig`.
  Cold-start-only; has no effect on an existing directory (do not use it as an assertion — a
  seeded replica that later checkpoints will legitimately have `oldest_lsn > initial_lsn`).
  `WalConfig` has no new fields.

## §15.3 — amend (replace the "v1 built-ins" sentence)

> **v1 built-in:** `NullObserver` (default, don't ship). The in-process forwarding observer
> is **not** a WAL built-in: `DurabilityObserver` is a public trait, and any consumer (e.g.
> `open-wal-replica`) implements it — typically an atomic release-store of `durable_lsn`
> plus a cheap wake, per the contract (cheap, non-blocking, no I/O, must not panic).

## §15.7 — amend (add at the end)

> **Realized by `open-wal-replica`** (async master/slave, static primary, v1). The WAL's
> responsibility ends at: dense durable records via `reader_from`, the durable watermark
> (commit's return value / the observer), `oldest_lsn()` for catch-up decisions, and
> `OpenOptions::seed` so a seeded replica mirrors the primary's LSN space (and may carry an
> observer to host downstream consumers — replica spec §7.1). Shipping, transport,
> catch-up, re-seed, checkpoint policy, and promotion are the crate's. Two WAL properties
> the crate relies on and MUST NOT be weakened: (a) records `≤ durable_lsn` are durable and
> never change (D1, D12); (b) a replica that only ever appends records `≤` the primary's
> `durable_lsn`, in order, is always a **dense prefix** of the primary and never needs a
> destructive operation to reconcile — which is why async master/slave requires no
> suffix-truncate.

## §17 — add Decision 7

7. **Replication support — RESOLVED (v7).** The WAL gains exactly: an `OpenOptions` builder
   (`Wal::options().seed(n).observer(o).open(dir, cfg)` — the cold-start seed is a builder
   option, not a `WalConfig` field, because a new `pub` field is source-breaking for exhaustive
   literals in the published crate and a one-shot seed is not steady-state config; a builder
   rather than more `open_*` functions because seed and observer compose — a replica may host
   downstream consumers), `Wal::oldest_lsn()` (live retention floor), and a `reader_from` that
   seeks to the containing segment. All truly additive (zero existing-source edits;
   `open`/`open_with` unchanged); no invariant weakened. **Explicitly NOT
   added:** (a) a "checkpoint observer" — checkpoint coordination is the integrator's existing
   `checkpoint(up_to)` policy consulting the crate's `min_replica_acked_lsn()`; (b) writer-side
   reader gating (§15.4 stays gap-is-fatal); (c) a suffix-truncate (`truncate_after`) — not
   needed while replicas are always prefixes of the primary; it becomes necessary only with
   leader election (a replica ahead of a new leader must truncate) or to let a former primary
   rejoin without re-seeding. Both are deferred; a former primary MUST be re-seeded in v1.
   (d) a standalone read-only `WalReader` (§15.2 pattern) — deferred to v2; it would move all
   shipping I/O off the writer thread and enable cross-process readers, and is a drop-in
   shipper swap for the crate (wire protocol and replica side unchanged).

---

## Tests to add (§14)

- **§14.1:** `options().seed(..)` validation (`Lsn(0)` ⇒ `InvalidConfig`; `Lsn(1)`; a large
  `N`); cold start at `N` ⇒ `oldest_lsn == N`, `durable_lsn == N−1`, first append ⇒ `N`;
  `options().seed(N).observer(o).open(..)` yields `Wal<O>` whose observer fires with the
  seeded LSNs; `open`/`open_with` still cold-start at `1` and equal their `options()` forms;
  `oldest_lsn()` equals `RecoveryReport::oldest_lsn` at open and advances after `checkpoint`.
- **§14.2 (property):** a log cold-started at arbitrary `N`, driven through appends/commits/
  rolls/checkpoints/reopens, is always dense from its `oldest_lsn` (D2) and byte-identical on
  replay (D6); `options().seed(..)` on a non-empty dir with a *different* seed is ignored
  (D7 — recovery authoritative).
- **§14.2 P-reader:** `reader_from(from)` for arbitrary `from` yields exactly the records
  `≥ from` in order (unchanged D6), and — instrumented under `cfg(fuzzing)` or a test hook —
  opens the containing segment, not `segments[0]`.
- **§14.4a/§14.4c:** cold-start-at-`N` crash points (after create / after header / before
  dir-fsync) recover per §8.4 (D9) — same paths as the roll case, now with `base = N`.
- Existing suite must stay green **with zero source edits** (`WalConfig` and `open`/`open_with`
  are unchanged — this is the test of "additive").
