//! Streaming replay reader (§6, §6.1).
//!
//! [`Reader`] is a **lending** iterator: each yielded `&[u8]` borrows the
//! reader's reused buffer and is valid only until the next [`Reader::next`]
//! call. This is why it is not a `std::iter::Iterator` (§6.1) — the standard
//! trait cannot express an item tied to the per-call borrow. The reuse keeps
//! replay zero-copy and, after warm-up, zero-allocation (§14.7).
//!
//! **M4 scope:** the reader follows the log across **multiple segments**. It
//! holds an immutable borrow of the `Wal`'s `dir` and sorted `segments` list
//! and opens each segment by path on demand, advancing to the next
//! higher-`base_lsn` segment when the current one's records end (§15.2
//! segment-roll following). Crossing a segment boundary opens a file (and may
//! allocate); the within-segment [`Reader::next`] hot path stays zero-alloc
//! (§7.5 scopes the zero-alloc guarantee to the no-roll steady state).

use std::fs::File;
use std::path::Path;

use crate::Lsn;
use crate::error::Result;
use crate::record::RECORD_HEADER_SIZE;
use crate::segment::{self, ScanOutcome};

/// An opaque, `Copy` position in the log (v7.2, §8.7): "the next record this
/// reader would attempt".
///
/// Obtained from [`Reader::position`] and handed back to
/// [`Wal::reader_at`](crate::Wal::reader_at) to start a later reader exactly
/// there, without rescanning the segment prefix the way
/// [`Wal::reader_from`](crate::Wal::reader_from) must. It borrows nothing, so it
/// outlives the `Reader` it came from and any `&mut Wal` call (`append`,
/// `commit`, `checkpoint`) made in between.
///
/// A position is only meaningful for the log it was taken from. Across a
/// close/reopen of the same directory, the caller MUST discard every position
/// whose [`next_lsn`](ReaderPos::next_lsn) is above the recovered
/// `durable_lsn + 1` (it pointed into a tail that recovery truncated, §8.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReaderPos {
    /// `base_lsn` of the segment the position is in.
    pub(crate) segment_base: Lsn,
    /// Byte offset of the record within that segment.
    pub(crate) offset: u64,
    /// The LSN the record at this position must carry.
    pub(crate) next_lsn: Lsn,
}

impl ReaderPos {
    /// The LSN of the record at this position — the next one a reader resumed
    /// here yields.
    #[must_use]
    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// Test/fuzz hook (§14.2 P10, v7.2): build a position from arbitrary
    /// fields. There is deliberately no public constructor — a real position
    /// only ever comes from [`Reader::position`].
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn from_parts(segment_base: Lsn, offset: u64, next_lsn: Lsn) -> ReaderPos {
        ReaderPos {
            segment_base,
            offset,
            next_lsn,
        }
    }

    /// Test/fuzz hook (§14.2 P10, v7.2): `(segment_base, offset, next_lsn)`.
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn parts(&self) -> (Lsn, u64, Lsn) {
        (self.segment_base, self.offset, self.next_lsn)
    }
}

/// Streaming reader over the log, starting at a caller-chosen LSN.
///
/// Holds an immutable borrow of the `Wal`'s directory and segment list (so the
/// writer's `&mut self` methods are excluded for the reader's lifetime), an
/// owned `File` for the segment currently being scanned, and a reusable I/O
/// buffer. Segments past the first are opened lazily as the scan crosses each
/// boundary.
pub struct Reader<'w> {
    dir: &'w Path,
    /// Sorted (ascending) `base_lsn`s of the segments to replay, oldest first.
    segments: &'w [Lsn],
    /// Index into `segments` of the segment `file` is currently open on.
    seg_idx: usize,
    /// The currently-open segment file, or `None` before the first record /
    /// after the stream ends.
    file: Option<File>,
    offset: u64,
    /// The LSN the next record must carry; `None` once a `u64::MAX` record has
    /// been yielded — the LSN space is exhausted, so nothing may follow (v7.1).
    expected_lsn: Option<Lsn>,
    from: Lsn,
    segment_size: u64,
    max_record_size: u32,
    buf: Vec<u8>,
    done: bool,
    /// Bookkeeping for [`Reader::position`] only (v7.2, §8.7) — never read by
    /// [`Reader::next`]. `false` once the reader can no longer be resumed: it
    /// returned `Some(Err(_))`, or its stream ended any way other than a
    /// `CleanEnd` scan of the last segment.
    resumable: bool,
}

impl<'w> Reader<'w> {
    /// Build a reader replaying `segments` (sorted ascending `base_lsn`s) under
    /// `dir`, starting at `segments[start]` — the segment containing `from`
    /// (§8.5, v7) — with `first` already opened on it, yielding records with
    /// `lsn >= from`. The scan begins at that segment's first record (LSN
    /// `segments[start]`) and skips forward to `from`; continuity carries
    /// `expected_lsn` across later segment boundaries.
    pub(crate) fn new(
        dir: &'w Path,
        segments: &'w [Lsn],
        start: usize,
        first: File,
        from: Lsn,
        segment_size: u64,
        max_record_size: u32,
    ) -> Reader<'w> {
        let pos = ReaderPos {
            segment_base: segments[start],
            offset: segment::HEADER_SIZE,
            next_lsn: segments[start],
        };
        Self::new_at(
            dir,
            segments,
            start,
            first,
            pos,
            from,
            segment_size,
            max_record_size,
            Vec::new(),
        )
    }

    /// Build a reader whose scan starts at `pos` inside `segments[start]`
    /// (v7.2, §8.7): the record at `pos.offset` must carry `pos.next_lsn`.
    /// `pos.segment_base` must equal `segments[start]` and `first` must be open
    /// on that segment — [`Wal::reader_at`](crate::Wal::reader_at) validates the
    /// position before calling this. `buf` is the read buffer to reuse.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_at(
        dir: &'w Path,
        segments: &'w [Lsn],
        start: usize,
        first: File,
        pos: ReaderPos,
        from: Lsn,
        segment_size: u64,
        max_record_size: u32,
        buf: Vec<u8>,
    ) -> Reader<'w> {
        debug_assert!(start < segments.len(), "start indexes a live segment");
        debug_assert_eq!(segments[start], pos.segment_base);
        Reader {
            dir,
            segments,
            seg_idx: start,
            file: Some(first),
            offset: pos.offset,
            expected_lsn: Some(pos.next_lsn),
            from,
            segment_size,
            max_record_size,
            buf,
            done: false,
            resumable: true,
        }
    }

    /// Where this reader is (v7.2, §8.7): the position of the next record it
    /// would attempt, to hand to [`Wal::reader_at`](crate::Wal::reader_at)
    /// later. Pure — no I/O, no allocation.
    ///
    /// - Right after [`Wal::reader_from`](crate::Wal::reader_from), before any
    ///   [`next`](Reader::next), this is the **start of the containing segment**
    ///   (its `next_lsn` is that segment's base, not `from`): the skip to `from`
    ///   happens inside the first `next`. A position does not carry `from`.
    /// - After `next` yields record `n`, `position().next_lsn() == n + 1`.
    /// - At a clean end of log it is still `Some` — the end position; a reader
    ///   resumed there later yields whatever was committed since.
    ///
    /// Returns `None` when the reader cannot be resumed: after it yielded LSN
    /// `u64::MAX` (nothing can follow), after `next` returned `Some(Err(_))`,
    /// or after the stream ended on an invalid record or an LSN-continuity
    /// break rather than a clean end of the last segment.
    #[must_use]
    pub fn position(&self) -> Option<ReaderPos> {
        if !self.resumable {
            return None;
        }
        Some(ReaderPos {
            segment_base: self.segments[self.seg_idx],
            offset: self.offset,
            next_lsn: self.expected_lsn?,
        })
    }

    /// Test/fuzz hook (§14.2 P-reader, v7): `base_lsn` of the segment this
    /// reader currently has open, or `None` once the stream has ended. Called
    /// right after [`Wal::reader_from`](crate::Wal::reader_from), it names the
    /// segment `reader_from` opened — which §8.5 requires to be the one
    /// containing `from`, never unconditionally the oldest.
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn open_segment_base(&self) -> Option<Lsn> {
        self.file.as_ref().map(|_| self.segments[self.seg_idx])
    }

    /// Advance to the next segment in the list, opening its file and resetting
    /// the scan offset to just past the header. Returns `Ok(false)` when there
    /// is no next segment (end of log); `seg_idx`/`offset` then stay on the last
    /// segment, where the scan stopped, so [`Reader::position`] can report the
    /// end position (v7.2). `expected_lsn` is **not** reset — the next segment's
    /// first record continues the dense LSN run (recovery validated
    /// cross-segment continuity, §8.1).
    fn advance_segment(&mut self) -> Result<bool> {
        if self.seg_idx + 1 >= self.segments.len() {
            self.file = None;
            return Ok(false);
        }
        self.seg_idx += 1;
        let name = segment::filename_for(self.segments[self.seg_idx]);
        self.file = Some(File::open(self.dir.join(name))?);
        self.offset = segment::HEADER_SIZE;
        Ok(true)
    }

    /// Yield the next record at or after `from`, or `None` at end of log.
    ///
    /// Lending-style: the returned borrow is tied to `&mut self` and is valid
    /// only until the following call (§6.1). Records before `from` are skipped.
    /// When the current segment's records end (sentinel / short read / invalid /
    /// LSN-continuity break), the reader follows the roll to the next segment;
    /// at the last segment that ends the stream cleanly. Torn-tail/corruption
    /// classification is a recovery concern (§8.2), not a live read.
    //
    // Deliberately not `std::iter::Iterator::next`: the lending borrow (item
    // tied to `&mut self`) cannot be expressed by the standard trait (§6.1).
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<(Lsn, &[u8])>> {
        // Copy the `Copy` fields so the per-iteration `&mut self.buf` borrow in
        // `read_record_at` does not conflict with reading `self.file`.
        let segment_size = self.segment_size;
        let max_record_size = self.max_record_size;

        loop {
            if self.done {
                return None;
            }
            let Some(file) = self.file.as_ref() else {
                self.done = true;
                return None;
            };

            match segment::read_record_at(
                file,
                self.offset,
                segment_size,
                max_record_size,
                &mut self.buf,
            ) {
                Err(e) => {
                    self.done = true;
                    self.resumable = false;
                    return Some(Err(e));
                }
                Ok(outcome @ (ScanOutcome::CleanEnd | ScanOutcome::Invalid)) => {
                    // End of this segment's records. Follow the roll to the next
                    // segment, if any; otherwise the stream is done. (A sentinel,
                    // short read, or invalid record ends the live stream cleanly;
                    // torn-tail/corruption classification is a recovery concern.)
                    match self.advance_segment() {
                        Ok(true) => continue,
                        Ok(false) => {
                            self.done = true;
                            // Resumable only from a genuine clean end of the
                            // last segment (v7.2, §8.7) — `position()`
                            // bookkeeping; what `next` returns is unchanged.
                            self.resumable = matches!(outcome, ScanOutcome::CleanEnd);
                            return None;
                        }
                        Err(e) => {
                            self.done = true;
                            self.resumable = false;
                            return Some(Err(e));
                        }
                    }
                }
                Ok(ScanOutcome::Record {
                    lsn,
                    payload_len,
                    framed_len,
                }) => {
                    if Some(lsn) != self.expected_lsn {
                        // A clean log has dense, in-order LSNs across segments; a
                        // break ends the valid records (defensive — recovery
                        // already validated continuity).
                        self.done = true;
                        self.resumable = false;
                        return None;
                    }
                    self.offset += framed_len as u64;
                    // `checked_next`, never the saturating `next()`: after
                    // `u64::MAX` no record may follow, so a (corrupt) duplicate
                    // `u64::MAX` ends the stream instead of being yielded (v7.1).
                    self.expected_lsn = lsn.checked_next();
                    if lsn < self.from {
                        continue;
                    }
                    let payload = &self.buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
                    return Some(Ok((lsn, payload)));
                }
            }
        }
    }
}
