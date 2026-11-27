# open-wal design spec — v7.1 → v7.2 delta: resumable reader position

**Status:** Draft v7.2 delta. Owner decision (2026-11-26): this lands **before** replica RM3.
**Apply to:** `docs/wal_design_v7.md`. Everything not mentioned is unchanged.
**Nature:** additive and **read-only**. No change to `append`, `commit`, recovery, the on-disk
format, checkpoint, or any fsync. No invariant D1–D12 is touched.

## Motivation

`Wal::reader_from(from)` positions by scanning the containing segment from its base, reading
and CRC-checking every record before `from` (§8.5). A `Reader` borrows `&Wal`, so it cannot
be kept across calls that need `&mut Wal`.

The replica's catch-up (replica spec §9.1) runs on the primary's **writer thread**, one
bounded chunk per call. Each chunk therefore reopens a reader and pays the prefix rescan
again. The rescan costs two `pread`s per record. Measured, hot cache, one 32 MiB segment:

| Payload | Full-segment rescan |
|---|---|
| 4 KiB | 19.6 ms |
| 100 B | 284 ms |
| 0 B | 1.34 s |

That is a writer stall of O(records in the segment) per chunk. v7.2 removes it **for every
chunk after the first**: a reader can report where it is, and a later reader can start
there.

The first `reader_from` of a catch-up still pays one prefix skip. That skip runs inside a
single `Reader::next` call and cannot be chunked. It is paid once per catch-up session, not
once per chunk, and the replica's stall bound (replica spec §9.1) must say so.

## API (add to §6)

```rust
/// (v7.2) An opaque, `Copy` position in the log: "the next record this reader would
/// attempt". It borrows nothing, so it outlives the `Reader` and any `&mut Wal` call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReaderPos { /* segment_base: Lsn, offset: u64, next_lsn: Lsn — private */ }

impl ReaderPos {
    /// The LSN of the record at this position (the next one a resumed reader yields).
    pub fn next_lsn(&self) -> Lsn;
}

impl<'w> Reader<'w> {
    /// (v7.2) Where this reader is. `None` when it cannot be resumed (see below).
    pub fn position(&self) -> Option<ReaderPos>;
}

impl<O: DurabilityObserver> Wal<O> {
    /// (v7.2) A reader that starts exactly at `pos`, without scanning the segment prefix.
    pub fn reader_at(&self, pos: ReaderPos) -> Result<Reader<'_>, WalError>;
}
```

New error variant (§10): `WalError::InvalidPosition`. It is **non-poisoning**: a read-side
argument error that touches no state.

## Semantics (new §8.7, NORMATIVE)

### `Reader::position`
- It returns the position of the next record the reader would attempt: the base LSN of the
  segment it is in, the byte offset in that segment, and the LSN that record must carry.
- It is pure: no I/O, no allocation.
- Immediately after `reader_from(from)`, before any `next`, the position is the **start of
  the containing segment** (offset 64), not `from`, and `position().next_lsn()` is that
  segment's base LSN. The reader has not skipped yet; the skip happens inside the first
  `next`, and records below `from` are never yielded.
- **A position does not carry `from`.** If a reader ends before it reaches `from` (for
  example `reader_from(Lsn(100))` on a log that ends at 10), its end position has
  `next_lsn = 11`, and a reader resumed there later yields from 11, not from 100.
- After `next` yields record `n`, `position().next_lsn() == n + 1`.
- At a **clean end of log**, `position` is still `Some`: the end position. A clean end means
  `next` returned `None` because the scan of the **last** segment reported
  `ScanOutcome::CleanEnd`, i.e. the sentinel, or fewer than 20 bytes left in a segment
  filled to the brim. A reader resumed there later yields whatever was committed since. The
  implementation must therefore record that the final outcome was `CleanEnd` and keep that
  segment's index and offset.
- It returns `None` when the reader cannot be resumed:
  - after it yielded LSN `u64::MAX` (nothing can follow, v7.1);
  - after `next` returned `Some(Err(_))`;
  - after the stream ended on an invalid record or an LSN-continuity break, i.e. any end
    other than `CleanEnd` in the last segment.

### `Wal::reader_at`
`reader_at(pos)` MUST NOT scan the segment prefix. What it guarantees depends on where
`pos` came from:

- **A genuine position:** one returned by `Reader::position()` on this log, with no
  torn-tail truncation below it since (see "Reopen" below). `reader_at(pos)` MUST succeed,
  or fail with `ContiguityViolation` exactly when `reader_from(pos.next_lsn())` would. On
  success it yields **exactly** what `reader_from(pos.next_lsn())` yields.
- **Any other value** (forged, or from a different log; `ReaderPos` has no public
  constructor): no panic, bounded I/O, and either an error or a dense run of CRC-valid
  records starting at `next_lsn`.
  - Exact equivalence cannot be promised here without a prefix scan. Payloads are opaque,
    so a payload may itself contain bytes that form a CRC-valid record at an 8-aligned
    offset, and no local check can tell that from a real record boundary.

Checks, in this order:

1. `pos.next_lsn < oldest_lsn` ⇒ `ContiguityViolation`. This is the same fatal gap as
   `reader_from` (§15.4).
2. **Segment-base rule.** If `next_lsn` equals the base of a live segment `j`, the reader
   starts at the beginning of segment `j` (offset 64), whatever `segment_base` and `offset`
   say. No further check applies.
   - This is exactly `reader_from(base_j)`, which scans no prefix.
   - It is what keeps a sealed-segment-end position valid after `checkpoint` deletes that
     segment: the reader finishes a segment, the position still names it, and its
     `next_lsn` is the next segment's base.
3. Structural checks; any failure ⇒ `InvalidPosition`:
   - `segment_base` is the base of a live segment, at index `i`;
   - `64 ≤ offset ≤ segment_size`, and `offset` is a multiple of 8;
   - `segment_base < next_lsn`; and if segment `i + 1` exists, `next_lsn <` its base. (The
     equal cases were taken by rule 2.)
4. One read at `offset` (the same bounded `read_record_at` the reader uses):
   - a valid record with `lsn == next_lsn` ⇒ `Ok`;
   - a valid record with any other LSN, or an invalid record ⇒ `InvalidPosition`;
   - `CleanEnd` ⇒ `Ok` only if segment `i` is the active one and `next_lsn > durable_lsn`
     (a genuine end of log). Otherwise ⇒ `InvalidPosition`. On a non-poisoned handle the
     file holds exactly the records `≤ durable_lsn`, because staged records are not in the
     file, so this test is both necessary and sufficient.

Consequences:
- A position stays valid across `append`, `commit` and roll on the same handle. Sealed
  segments are immutable (D12), and the active segment only grows.
  - An end position in the active segment becomes "a record with `lsn == next_lsn`" after
    the next commit.
  - It falls under the segment-base rule if the segment is sealed without another record
    (the empty-prefix roll, §7.3).
- **Reopen.** A position survives closing and reopening the same directory if its
  `next_lsn ≤ durable_lsn + 1` as recovered. A position above that pointed into a tail that
  recovery truncated and zeroed.
  - Right after the reopen, `reader_at` on such a position reads zeros with
    `next_lsn > durable_lsn`, so it returns `Ok` and yields nothing.
  - After later commits it is no longer a genuine position. **The caller MUST discard, at
    reopen, every position with `next_lsn > durable_lsn + 1`.** `reader_at` cannot detect
    this later.
- Every record any reader yields is still CRC-checked and LSN-checked by `Reader::next`,
  exactly as today.
- The equivalence is stated for a **non-poisoned** handle. On a poisoned handle the file may
  hold written-but-unsynced records above `durable_lsn`. `reader_from` already exposes them;
  `reader_at` adds nothing new. Neither has a contract there.

### Cost
`reader_at` costs one segment-file open plus one record read. `Reader::next` is unchanged;
its zero-allocation guarantee (§14.7) is unchanged.

## Other spec edits
- **Header / changelog:** status becomes v7.2; add a "Changelog (v7.1 → v7.2)" entry
  summarizing this delta.
- **§8.5:** add a v7.2 note: "`reader_at` (§8.7) starts at a recorded position and does not
  scan the prefix; `reader_from` is unchanged."
- **§10:** add `InvalidPosition` (non-poisoning).
- **§12:** add `InvalidPosition` next to `LsnExhausted` as a non-poison condition.
- **§15.2 / §15.7:** one sentence: a consumer that reads in bounded steps on the writer
  thread (the replica's catch-up) should carry a `ReaderPos` between steps.
- **§17 Decision 8 (new):** resumable reader position.
  - Rejected alternative: `Wal::segment_bases()` with segment-aligned chunks. It exposes
    layout and still rescans a partial segment.
  - Rejected alternative: an owned or `'static` reader. It would outlive `&mut Wal` calls and
    need interior synchronization, against §6.2.

## Fuzz clock (§14.13) — designer decision
v7.2 changes no recovery classification and no decoder: `recover_segment`, `classify` and
`record::decode` are untouched. The F1–F4 clocks therefore **do not reset**. The new surface
(arbitrary `ReaderPos` values) is covered by the property test below, which must be
high-iteration-overridable (`PROPTEST_CASES`).

## Tests (add to §14; each must be shown failing on a broken build)

- **§14.1 unit.**
  - A position taken after each record in a multi-segment log resumes to the identical
    remainder (LSNs and bytes).
  - The position at a sealed segment's end resumes into the next segment.
  - The end-of-log position is `Some`. Resumed immediately it yields nothing; after a further
    commit it yields the new records. This includes the case where the next commit rolls
    without writing to that segment.
  - `position()` is `None` after `u64::MAX` was yielded (seeded log).
  - A position below `oldest_lsn` after a checkpoint ⇒ `ContiguityViolation`. Assert the
    exact variant: with rule 1 removed the same call returns `InvalidPosition`.
  - A sealed-segment-end position, then a checkpoint that deletes that segment ⇒ `Ok`, and
    the identical remainder (the segment-base rule).
  - `position()` before any `next` is the containing segment's start, with `next_lsn` equal
    to its base.
  - `position()` is `None` after `Some(Err(_))`, and after an end on an invalid record
    (corrupt the active file under an open handle).
  - A reader that ended below its `from` resumes from its end position, not from `from`.
  - Reopen after a fabricated torn tail: a position above the recovered `durable_lsn + 1`
    yields nothing right after the reopen.
  - Embedded frame, documenting the limit of the forged-position guarantee: a payload that
    contains a CRC-valid record at an 8-aligned offset. A forged position pointing at it is
    accepted and yields a dense CRC-valid run; no genuine position ever points there.
  - `position()` allocates nothing (a new test file with a counting allocator; the existing
    zero-alloc file is not edited).
  - `InvalidPosition` leaves the handle usable (append, commit and read all still work).
- **§14.2 P9 (property), resume equivalence.**
  - Run a seeded random op-script (append / commit / roll-forcing sizes / checkpoint /
    reopen) on tiny segments.
  - At random points, take a position from a reader. **At capture time** assert, against
    the test's own model of what it appended, that `pos.next_lsn()` is the last yielded LSN
    + 1 (or the segment base before any `next`, or `durable_lsn + 1` at the end of log).
  - Later, after more ops, resume with `reader_at(pos)` and compare the stream with the
    **test's own record model** from `pos.next_lsn()` on, and also with
    `reader_from(pos.next_lsn())`.
  - Either all three agree, or `reader_at` and `reader_from` both fail with
    `ContiguityViolation`.
  - After a reopen, the test discards positions as the Reopen rule requires.
  - Comparing only `reader_at` with `reader_from` is not enough: a `position()` that is
    wrong but self-consistent would pass.
- **§14.2 P10 (property), arbitrary positions never lie.**
  - Under `--features fuzzing`, build `ReaderPos` values from arbitrary fields, biased near
    valid ones: a real position with `offset ± 1 / ± 8 / ± one record`, `next_lsn ± 1`, the
    base swapped for another live base or a non-base.
  - The generated payloads are random bytes, which never contain a CRC-valid record. Under
    that condition, on a non-poisoned handle, `reader_at` must either return an error or
    yield exactly what `reader_from(next_lsn)` yields. It must never panic.
  - The constructor is exposed only as `#[cfg(any(test, feature = "fuzzing"))]`, e.g.
    `fuzzing::reader_pos(..)`.
- **No-prefix-scan proof (Linux).**
  - In one multi-MiB segment of small records, take a position near the end.
  - Measure the reading thread's `/proc/thread-self/io` `rchar` around `reader_at(pos)` plus
    one `next`.
  - It must be at most two framed records plus slack, while `reader_from(next_lsn)` plus one
    `next` in the same test reads at least the prefix. That second measurement is the
    positive control.
  - Assert the LSN and payload of the record read, against the test's own model.
  - The slack must cover the test's own two reads of `/proc/thread-self/io`.
  - Where that file or its `rchar` field is absent (no `CONFIG_TASK_IO_ACCOUNTING`), the
    test skips with a loud message. It does not fail and does not pass silently.
- **Zero-alloc gate (§14.7):** `tests/zero_alloc.rs` passes unchanged.
- **Docs:** a book snippet for `position` / `reader_at`, mirrored by a test as in
  `tests/book_examples_v7.rs`.

**Mutation rows** (apply, show the named test failing, revert):

| Mutation | Must fail |
|---|---|
| `reader_at` implemented as `reader_from(pos.next_lsn())` | no-prefix-scan proof |
| drop the `lsn == next_lsn` check in rule 4 | P10 |
| drop the alignment / bounds checks in rule 3 | Expected **not** to fail P10: rule 4 catches every such case. Confirm it, record it as "defence in depth, no falsifier", and keep the checks. Do not weaken P10. |
| accept any clean end in rule 4 (no "genuine end" test) | P10 |
| `position()` after a yielded record reports the yielded record's own offset and LSN (off by one record) | P9's capture-time assertion, and the unit round-trip |
| end-of-log `position()` returns `None` | the end-of-log unit test |
| skip rule 1 (no `oldest_lsn` check) | the checkpoint unit test (exact variant) |
| remove the segment-base rule (rule 2) | the sealed-end-then-checkpoint unit test, and P9 |
