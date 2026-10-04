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
    expected_lsn: Lsn,
    from: Lsn,
    segment_size: u64,
    max_record_size: u32,
    buf: Vec<u8>,
    done: bool,
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
        debug_assert!(start < segments.len(), "start indexes a live segment");
        Reader {
            dir,
            segments,
            seg_idx: start,
            file: Some(first),
            offset: segment::HEADER_SIZE,
            expected_lsn: segments[start],
            from,
            segment_size,
            max_record_size,
            buf: Vec::new(),
            done: false,
        }
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
    /// is no next segment (end of log). `expected_lsn` is **not** reset — the
    /// next segment's first record continues the dense LSN run (recovery
    /// validated cross-segment continuity, §8.1).
    fn advance_segment(&mut self) -> Result<bool> {
        self.seg_idx += 1;
        if self.seg_idx >= self.segments.len() {
            self.file = None;
            return Ok(false);
        }
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
                    return Some(Err(e));
                }
                Ok(ScanOutcome::CleanEnd) | Ok(ScanOutcome::Invalid) => {
                    // End of this segment's records. Follow the roll to the next
                    // segment, if any; otherwise the stream is done. (A sentinel,
                    // short read, or invalid record ends the live stream cleanly;
                    // torn-tail/corruption classification is a recovery concern.)
                    match self.advance_segment() {
                        Ok(true) => continue,
                        Ok(false) => {
                            self.done = true;
                            return None;
                        }
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e));
                        }
                    }
                }
                Ok(ScanOutcome::Record {
                    lsn,
                    payload_len,
                    framed_len,
                }) => {
                    if lsn != self.expected_lsn {
                        // A clean log has dense, in-order LSNs across segments; a
                        // break ends the valid records (defensive — recovery
                        // already validated continuity).
                        self.done = true;
                        return None;
                    }
                    self.offset += framed_len as u64;
                    self.expected_lsn = lsn.next();
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
