//! Streaming replay reader (§6, §6.1).
//!
//! [`Reader`] is a **lending** iterator: each yielded `&[u8]` borrows the
//! reader's reused buffer and is valid only until the next [`Reader::next`]
//! call. This is why it is not a `std::iter::Iterator` (§6.1) — the standard
//! trait cannot express an item tied to the per-call borrow. The reuse keeps
//! replay zero-copy and, after warm-up, zero-allocation (§14.7).
//!
//! **M2 scope:** a single active segment. Multi-segment following (sealed →
//! active) arrives with the roll machinery in M4.

use std::fs::File;

use crate::Lsn;
use crate::error::Result;
use crate::record::RECORD_HEADER_SIZE;
use crate::segment::{self, ScanOutcome};

/// Streaming reader over the log, starting at a caller-chosen LSN.
///
/// Holds an immutable borrow of the `Wal`'s active segment (so the writer's
/// `&mut self` methods are excluded for the reader's lifetime) plus a reusable
/// I/O buffer.
pub struct Reader<'w> {
    file: &'w File,
    offset: u64,
    expected_lsn: Lsn,
    from: Lsn,
    segment_size: u64,
    max_record_size: u32,
    buf: Vec<u8>,
    done: bool,
}

impl<'w> Reader<'w> {
    /// Build a reader over `file` (the active segment) whose first record's LSN
    /// is `base_lsn`, yielding records with `lsn >= from`.
    pub(crate) fn new(
        file: &'w File,
        base_lsn: Lsn,
        from: Lsn,
        segment_size: u64,
        max_record_size: u32,
    ) -> Reader<'w> {
        Reader {
            file,
            offset: segment::HEADER_SIZE,
            expected_lsn: base_lsn,
            from,
            segment_size,
            max_record_size,
            buf: Vec::new(),
            done: false,
        }
    }

    /// Yield the next record at or after `from`, or `None` at end of log.
    ///
    /// Lending-style: the returned borrow is tied to `&mut self` and is valid
    /// only until the following call (§6.1). Records before `from` are skipped.
    /// A non-record (sentinel / short read / invalid) or an LSN-continuity break
    /// ends the stream cleanly — torn-tail/corruption classification is a
    /// recovery concern (M3), not a live read.
    //
    // Deliberately not `std::iter::Iterator::next`: the lending borrow (item
    // tied to `&mut self`) cannot be expressed by the standard trait (§6.1).
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<(Lsn, &[u8])>> {
        // Copy the `Copy` fields so the per-iteration `&mut self.buf` borrow in
        // `read_record_at` does not conflict with reading `self.file`.
        let file = self.file;
        let segment_size = self.segment_size;
        let max_record_size = self.max_record_size;

        loop {
            if self.done {
                return None;
            }
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
                Ok(ScanOutcome::End) => {
                    self.done = true;
                    return None;
                }
                Ok(ScanOutcome::Record {
                    lsn,
                    payload_len,
                    framed_len,
                }) => {
                    if lsn != self.expected_lsn {
                        // A clean log has dense, in-order LSNs; a break is the
                        // end of valid records for M2's purposes.
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
