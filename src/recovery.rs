//! Intra-segment recovery (§8.2) — the milestone where this design's
//! correctness actually lives.
//!
//! [`recover_segment`] scans one segment's record run from offset 64, tracking
//! `expected_next_lsn`, and classifies the first invalid record it reaches
//! (§8.2 step 5):
//!
//! - **sealed segment** (`!is_active`): a sealed segment was fully synced before
//!   the next segment existed (§7.3), so it has **no torn tail** — any invalid
//!   record before the sentinel is fatal [`Corruption`](WalError::Corruption).
//!   No forward scan.
//! - **active segment**: a **bounded forward scan** (§8.2 step 5) decides
//!   torn-tail vs mid-log corruption. If a structurally valid record with the
//!   expected LSN exists within the bound, data after the gap was genuine and
//!   acknowledged ⇒ fatal [`TornMidLog`](WalError::TornMidLog) (D5). Otherwise
//!   it is a torn tail: truncate at the boundary, **durably zero `[X, EOF)`**
//!   (§8.2.1, [`segment::zero_to_eof`]) so nothing stale can be resurrected
//!   (D10), and report [`TailState::TruncatedAt`].
//!
//! Recovery never materializes payloads (§8.5) — it reads only LSNs and framed
//! sizes — and is a pure function of the on-disk bytes (§8.6). It is total and
//! never panics for any input (D11): every read is bounded by `segment_size`,
//! the forward scan is bounded by `max_record_size + 28`, and short physical
//! reads are tolerated by the scanner.
//!
//! **M3 scope:** a single active segment. The `is_active = false` path is wired
//! and unit-tested here, but multi-segment discovery, cross-segment continuity
//! (§8.1), and crash-during-roll handling (§8.4) are M4.

use std::fs::File;

use crate::Lsn;
use crate::error::{Result, WalError};
use crate::segment::{self, HEADER_SIZE, ScanOutcome};
use crate::wal::TailState;

/// Result of recovering one segment's record run (§8.2).
#[derive(Debug)]
pub(crate) struct SegmentRecovery {
    /// Highest valid LSN in the segment, or `base_lsn - 1` if it is empty
    /// (§8.1; an empty active segment after a crash-after-roll).
    pub(crate) max_lsn: Lsn,
    /// Offset just past the last valid record — where the next append writes,
    /// and the start of the end-of-records sentinel region.
    pub(crate) write_offset: u64,
    /// Whether the tail was clean or a torn tail was truncated and zeroed.
    pub(crate) tail_state: TailState,
}

/// The fixed parameters of one segment recovery, threaded through the scan and
/// its `classify`/forward-scan helpers (keeps their signatures small).
struct RecoverCtx {
    base_lsn: Lsn,
    /// Active segment ⇒ a torn tail is legitimate; sealed ⇒ any invalid record
    /// is fatal (§8.2 step 5).
    is_active: bool,
    segment_size: u64,
    max_record_size: u32,
}

/// Recover the record run of one segment (§8.2). `is_active` selects the
/// active-segment forward-scan/torn-tail path over the sealed-segment
/// fatal-on-any-invalid path (§8.2 step 5).
///
/// The caller must have validated the segment header (§5.2) and confirmed its
/// `base_lsn` before calling this; `base_lsn` is therefore `>= 1`, so the
/// empty-segment `base_lsn - 1` below cannot underflow.
pub(crate) fn recover_segment(
    file: &File,
    base_lsn: Lsn,
    is_active: bool,
    segment_size: u64,
    max_record_size: u32,
) -> Result<SegmentRecovery> {
    let ctx = RecoverCtx {
        base_lsn,
        is_active,
        segment_size,
        max_record_size,
    };
    let mut buf = Vec::new();
    let mut offset = HEADER_SIZE;
    let mut expected = base_lsn;
    // `base_lsn >= 1` (header-validated), so this does not underflow.
    let mut last_valid = Lsn(base_lsn.0 - 1);

    loop {
        match segment::read_record_at(file, offset, segment_size, max_record_size, &mut buf)? {
            ScanOutcome::Record {
                lsn, framed_len, ..
            } => {
                if lsn != expected {
                    // An LSN mismatch is an invalid record at this offset
                    // (§8.2 step 4) — classify it as tail vs corruption.
                    return classify(file, &ctx, offset, expected, last_valid, &mut buf);
                }
                // A valid, in-order record (§8.2 step 6): advance. No payload is
                // retained (§8.5).
                last_valid = lsn;
                offset += framed_len as u64;
                expected = lsn.next();
            }
            ScanOutcome::CleanEnd => {
                // End of records (§8.2 step 1): a clean tail, no zeroing needed.
                return Ok(SegmentRecovery {
                    max_lsn: last_valid,
                    write_offset: offset,
                    tail_state: TailState::Clean,
                });
            }
            ScanOutcome::Invalid => {
                return classify(file, &ctx, offset, expected, last_valid, &mut buf);
            }
        }
    }
}

/// Classify an invalid record at offset `x` (§8.2 step 5). On a torn tail this
/// also performs the durable physical invalidation (§8.2.1) before returning.
fn classify(
    file: &File,
    ctx: &RecoverCtx,
    x: u64,
    expected: Lsn,
    last_valid: Lsn,
    buf: &mut Vec<u8>,
) -> Result<SegmentRecovery> {
    if !ctx.is_active {
        // A sealed segment is fully synced before the next segment exists
        // (§7.3): it contains no torn tail, so any invalid record is fatal
        // corruption (§8.2 step 5). No forward scan.
        return Err(WalError::Corruption {
            segment: ctx.base_lsn,
            offset: x,
            detail: "invalid record in sealed segment",
        });
    }

    if forward_scan_finds_valid(file, ctx, x, expected, buf)? {
        // A genuine, acknowledged record exists after the gap: truncating would
        // silently drop it (D5). Fatal and loud, never silent truncation.
        return Err(WalError::TornMidLog {
            segment: ctx.base_lsn,
            offset: x,
        });
    }

    // Torn tail: durably invalidate `[x, EOF)` (§8.2.1) so no stale record can be
    // resurrected (D10), then report the truncation.
    segment::zero_to_eof(file, x, ctx.segment_size)?;
    Ok(SegmentRecovery {
        max_lsn: last_valid,
        write_offset: x,
        tail_state: TailState::TruncatedAt {
            segment_base: ctx.base_lsn,
            offset: x,
        },
    })
}

/// Bounded forward scan (§8.2 step 5): from `x + 8`, step forward in 8-byte
/// increments at most `max_record_size + 28` bytes (record header + max
/// padding), looking for a structurally valid record that **continues the log**.
///
/// A genuine acked record after the gap proves data past `x` was real (D5). The
/// condition is `lsn >= expected_next_lsn` (the corrected §8.2 step 5, v6.1):
/// when the record at `x` is itself a corrupted acked record, the next genuine
/// record is `expected_next_lsn + 1`, never `expected_next_lsn`, so the earlier
/// `==` form would never match and would silently misclassify mid-log corruption
/// as a torn tail (a D5 violation, inconsistent with §14.4e's "finds the **next**
/// valid record"). Every genuine continuation has `lsn >= expected_next_lsn`,
/// while a torn tail has no valid record ahead at all — the post-tail region is
/// durably zeroed (§8.2.1 / D10), so no stale record can appear within the bound.
///
/// The bound keeps recovery `O(segment)` with a small constant and immune to a
/// garbage-CRC scan DoS (D11). Every individual read is also bounded by
/// `segment_size` inside [`segment::read_record_at`].
fn forward_scan_finds_valid(
    file: &File,
    ctx: &RecoverCtx,
    x: u64,
    expected: Lsn,
    buf: &mut Vec<u8>,
) -> Result<bool> {
    let bound = u64::from(ctx.max_record_size) + 28;
    // Inclusive: candidate start offsets are `X+8 .. X+8+bound`, i.e. distance
    // ≤ `max_record_size + 28` from `X+8` — the largest a single record's frame
    // can be, so the next genuine record (if any) starts within this window.
    let end = x.saturating_add(8).saturating_add(bound);
    let mut p = x.saturating_add(8);
    while p <= end {
        if let ScanOutcome::Record { lsn, .. } =
            segment::read_record_at(file, p, ctx.segment_size, ctx.max_record_size, buf)?
        {
            if lsn >= expected {
                return Ok(true);
            }
        }
        p += 8;
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record;
    use std::os::unix::fs::FileExt;

    const SEGMENT_SIZE: u64 = 64 * 1024;
    const MAX_RECORD_SIZE: u32 = 4096;

    /// Build a fresh, pre-allocated, header-written segment at `base`, returning
    /// its open file. (Mirrors `segment::create` without the directory fsync,
    /// which is irrelevant to a single-segment recovery unit test.)
    fn fresh_segment(dir: &std::path::Path, base: Lsn) -> File {
        segment::create(dir, base, SEGMENT_SIZE).unwrap()
    }

    /// Append `payloads` as dense records starting at `base`, returning the
    /// offset just past the last one. Bypasses `Wal` so tests can then corrupt
    /// the bytes directly.
    fn write_dense(file: &File, base: Lsn, payloads: &[&[u8]]) -> u64 {
        let mut offset = HEADER_SIZE;
        let mut lsn = base;
        let mut buf = Vec::new();
        for p in payloads {
            buf.clear();
            let framed = record::encode_into(&mut buf, lsn, p);
            file.write_all_at(&buf, offset).unwrap();
            offset += framed as u64;
            lsn = lsn.next();
        }
        file.sync_data().unwrap();
        offset
    }

    #[test]
    fn empty_active_segment_is_clean_with_base_minus_one() {
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(101));
        let rec = recover_segment(&file, Lsn(101), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert_eq!(rec.max_lsn, Lsn(100));
        assert_eq!(rec.write_offset, HEADER_SIZE);
        assert_eq!(rec.tail_state, TailState::Clean);
    }

    #[test]
    fn clean_run_recovers_all_records_without_zeroing() {
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        let end = write_dense(&file, Lsn(1), &[b"a", b"bb", b"ccc"]);
        let rec = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert_eq!(rec.max_lsn, Lsn(3));
        assert_eq!(rec.write_offset, end);
        assert_eq!(rec.tail_state, TailState::Clean);
    }

    #[test]
    fn corrupt_last_record_is_a_torn_tail_and_is_zeroed() {
        // §14.4e (D4): corrupting the final (active-tail) record truncates the
        // tail at its offset and durably zeroes from there to EOF.
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        write_dense(&file, Lsn(1), &[b"first", b"second"]);
        // Offset of the 2nd record: header + framed("first").
        let x = HEADER_SIZE + record::framed_size(5) as u64;
        // Flip a payload byte of the last record ⇒ CRC failure.
        let mut byte = [0u8; 1];
        file.read_at(&mut byte, x + 20).unwrap();
        byte[0] ^= 0xFF;
        file.write_all_at(&byte, x + 20).unwrap();

        let rec = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert_eq!(rec.max_lsn, Lsn(1));
        assert_eq!(rec.write_offset, x);
        assert_eq!(
            rec.tail_state,
            TailState::TruncatedAt {
                segment_base: Lsn(1),
                offset: x
            }
        );
        // The region [x, segment_size) must now read as zero (durably).
        let mut tail = vec![0xAAu8; (SEGMENT_SIZE - x) as usize];
        file.read_at(&mut tail, x).unwrap();
        assert!(tail.iter().all(|&b| b == 0), "tail must be zeroed to EOF");
    }

    #[test]
    fn corrupt_middle_acked_record_is_fatal_tornmidlog() {
        // §14.4e (D5): corrupting an interior acked record — with a valid record
        // still after it — is fatal `TornMidLog`, never silent truncation. This
        // is the case a naive "first-invalid-is-the-tail" recovery would lose.
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        write_dense(&file, Lsn(1), &[b"one", b"two", b"three"]);
        // Corrupt the SECOND record (LSN 2); record 3 remains valid after it.
        let x = HEADER_SIZE + record::framed_size(3) as u64;
        let mut byte = [0u8; 1];
        file.read_at(&mut byte, x + 20).unwrap();
        byte[0] ^= 0xFF;
        file.write_all_at(&byte, x + 20).unwrap();

        let err = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap_err();
        assert!(
            matches!(err, WalError::TornMidLog { segment, offset } if segment == Lsn(1) && offset == x),
            "expected TornMidLog at {x}, got {err:?}"
        );
    }

    #[test]
    fn sealed_segment_invalid_record_is_fatal_corruption() {
        // §8.2 step 5: a sealed segment has no torn tail; any invalid record is
        // fatal `Corruption`, with no forward scan. (Exercises the M4 path now.)
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        write_dense(&file, Lsn(1), &[b"one", b"two", b"three"]);
        let x = HEADER_SIZE + record::framed_size(3) as u64;
        let mut byte = [0u8; 1];
        file.read_at(&mut byte, x + 20).unwrap();
        byte[0] ^= 0xFF;
        file.write_all_at(&byte, x + 20).unwrap();

        // is_active = false ⇒ fatal at the FIRST invalid record (no forward scan).
        let err = recover_segment(&file, Lsn(1), false, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap_err();
        assert!(
            matches!(err, WalError::Corruption { segment, offset, .. } if segment == Lsn(1) && offset == x),
            "expected Corruption at {x}, got {err:?}"
        );
    }

    #[test]
    fn sealed_torn_last_record_is_also_fatal() {
        // A sealed segment whose LAST record is corrupt is still fatal (it can
        // have no legitimate torn tail), unlike the same bytes as the active.
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        write_dense(&file, Lsn(1), &[b"first", b"second"]);
        let x = HEADER_SIZE + record::framed_size(5) as u64;
        let mut byte = [0u8; 1];
        file.read_at(&mut byte, x + 20).unwrap();
        byte[0] ^= 0xFF;
        file.write_all_at(&byte, x + 20).unwrap();

        assert!(matches!(
            recover_segment(&file, Lsn(1), false, SEGMENT_SIZE, MAX_RECORD_SIZE),
            Err(WalError::Corruption { .. })
        ));
        // The active interpretation of the very same bytes is a recoverable tail.
        assert!(matches!(
            recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE),
            Ok(SegmentRecovery {
                tail_state: TailState::TruncatedAt { .. },
                ..
            })
        ));
    }

    #[test]
    fn recovery_is_idempotent_after_torn_tail() {
        // D7: the first recovery truncates+zeroes; the second sees a genuinely
        // clean tail. Content (max_lsn) is stable; tail_state converges to Clean.
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        write_dense(&file, Lsn(1), &[b"first", b"second"]);
        let x = HEADER_SIZE + record::framed_size(5) as u64;
        let mut byte = [0u8; 1];
        file.read_at(&mut byte, x + 20).unwrap();
        byte[0] ^= 0xFF;
        file.write_all_at(&byte, x + 20).unwrap();

        let first = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert!(matches!(first.tail_state, TailState::TruncatedAt { .. }));

        for _ in 0..3 {
            let again =
                recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
            assert_eq!(again.max_lsn, Lsn(1));
            assert_eq!(again.write_offset, x);
            assert_eq!(again.tail_state, TailState::Clean);
        }
    }

    #[test]
    fn truncated_file_below_segment_size_recovers_without_panic() {
        // §14.4f / D11: a segment physically truncated mid-record recovers to the
        // valid prefix and re-extends the file to segment_size, never panicking.
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        let end = write_dense(&file, Lsn(1), &[b"alpha", b"beta", b"gamma"]);
        // Cut the file mid-way through the last record.
        file.set_len(end - 4).unwrap();

        let rec = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert_eq!(rec.max_lsn, Lsn(2));
        assert!(matches!(rec.tail_state, TailState::TruncatedAt { .. }));
        // Zeroing re-extended the file back to the pre-allocated size.
        assert_eq!(file.metadata().unwrap().len(), SEGMENT_SIZE);
    }

    #[test]
    fn zeroing_prevents_resurrection_of_stale_valid_record() {
        // §14.4g / D10: a stale but CRC-valid record whose LSN equals the
        // post-truncation `expected_next_lsn`, sitting beyond the truncation
        // point, MUST be erased by the zeroing of `[X, EOF)` (§8.2.1) and never
        // resurrected on a later recovery.
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));

        // r1 (LSN 1) is the only good record; r2's slot at `x` is torn (bad CRC).
        let x = write_dense(&file, Lsn(1), &[b"first"]);
        let mut torn = Vec::new();
        record::encode_into(&mut torn, Lsn(2), b"torn-r2");
        torn[0] ^= 0xFF; // corrupt the CRC ⇒ invalid record at `x`
        file.write_all_at(&torn, x).unwrap();

        // A stale, fully valid record with LSN == 2 (the post-truncation
        // expected) buried far beyond the bounded forward scan, so recovery
        // correctly sees a torn tail (not mid-log corruption) yet the stale
        // record still physically exists past `x`.
        let bound = u64::from(MAX_RECORD_SIZE) + 28;
        let stale_off = x + bound + 256;
        let mut stale = Vec::new();
        record::encode_into(&mut stale, Lsn(2), b"STALE-RESURRECTABLE");
        file.write_all_at(&stale, stale_off).unwrap();
        file.sync_data().unwrap();

        // Sanity: the buried record really is a valid LSN-2 record before recovery.
        let mut buf = Vec::new();
        assert!(matches!(
            segment::read_record_at(&file, stale_off, SEGMENT_SIZE, MAX_RECORD_SIZE, &mut buf),
            Ok(ScanOutcome::Record { lsn: Lsn(2), .. })
        ));

        // Recovery #1: torn tail at `x`, durable LSN 1, zero `[x, EOF)`.
        let rec = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert_eq!(rec.max_lsn, Lsn(1));
        assert!(matches!(rec.tail_state, TailState::TruncatedAt { offset, .. } if offset == x));

        // The stale record's bytes are now zero (erased), so it is no longer a
        // record at all.
        assert!(matches!(
            segment::read_record_at(&file, stale_off, SEGMENT_SIZE, MAX_RECORD_SIZE, &mut buf),
            Ok(ScanOutcome::CleanEnd)
        ));

        // Recovery #2: durable LSN stays 1 — the stale LSN-2 record is gone, not
        // resurrected.
        let again = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert_eq!(again.max_lsn, Lsn(1));
        assert_eq!(again.write_offset, x);
        assert_eq!(again.tail_state, TailState::Clean);
    }

    /// Write a valid record for `lsn`/`payload` at absolute `offset`.
    fn write_record_at(file: &File, offset: u64, lsn: Lsn, payload: &[u8]) {
        let mut buf = Vec::new();
        record::encode_into(&mut buf, lsn, payload);
        file.write_all_at(&buf, offset).unwrap();
    }

    /// Write a CRC-corrupt (invalid) record for `lsn` at absolute `offset`, so the
    /// scanner classifies it as a boundary. Returns its framed length.
    fn write_torn_at(file: &File, offset: u64, lsn: Lsn, payload: &[u8]) -> u64 {
        let mut buf = Vec::new();
        let framed = record::encode_into(&mut buf, lsn, payload);
        buf[0] ^= 0xFF; // corrupt the CRC field
        file.write_all_at(&buf, offset).unwrap();
        framed as u64
    }

    #[test]
    fn forward_scan_just_within_bound_is_fatal_tornmidlog() {
        // B2: a genuine continuation record whose start is at the *far edge* of
        // the bounded forward scan (`<= X+8 + (max_record_size+28)`) must still be
        // found ⇒ fatal `TornMidLog` (D5), not a silent truncation.
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        let x = write_dense(&file, Lsn(1), &[b"first"]); // expected at x = 2
        write_torn_at(&file, x, Lsn(2), b"z"); // invalid record at the boundary

        let bound = u64::from(MAX_RECORD_SIZE) + 28;
        let end = x + 8 + bound;
        let within = (end / 8) * 8; // largest 8-aligned start <= end (scan reaches it)
        assert!(within > x + 8 && within <= end);
        write_record_at(&file, within, Lsn(2), b"genuine-continuation");
        file.sync_data().unwrap();

        let err = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap_err();
        assert!(
            matches!(err, WalError::TornMidLog { offset, .. } if offset == x),
            "a continuation at the bound edge must be fatal, got {err:?}"
        );
    }

    #[test]
    fn forward_scan_just_beyond_bound_is_torn_tail() {
        // B2 (other side): the same continuation one 8-byte slot *beyond* the
        // bound is not reached ⇒ torn tail, truncate at X and zero it away (D4).
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        let x = write_dense(&file, Lsn(1), &[b"first"]);
        write_torn_at(&file, x, Lsn(2), b"z");

        let bound = u64::from(MAX_RECORD_SIZE) + 28;
        let end = x + 8 + bound;
        let beyond = (end / 8) * 8 + 8; // first 8-aligned start strictly > end
        assert!(beyond > end);
        write_record_at(&file, beyond, Lsn(2), b"genuine-continuation");
        file.sync_data().unwrap();

        let rec = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert_eq!(rec.max_lsn, Lsn(1));
        assert!(matches!(rec.tail_state, TailState::TruncatedAt { offset, .. } if offset == x));
        // The just-beyond record was zeroed away, so it can't be resurrected.
        let mut buf = Vec::new();
        assert!(matches!(
            segment::read_record_at(&file, beyond, SEGMENT_SIZE, MAX_RECORD_SIZE, &mut buf),
            Ok(ScanOutcome::CleanEnd)
        ));
    }

    #[test]
    fn unknown_rec_type_at_tail_is_treated_as_torn_tail() {
        // B3: a CRC-valid record with a reserved `rec_type` (2) is `Invalid`
        // (UnknownRecType) to the codec; at the tail, recovery truncates it as a
        // torn tail rather than surfacing it or panicking.
        let dir = tempfile::tempdir().unwrap();
        let file = fresh_segment(dir.path(), Lsn(1));
        let x = write_dense(&file, Lsn(1), &[b"first"]);

        // Craft a CRC-valid record at `x` but with rec_type = 2 (reserved).
        let mut buf = Vec::new();
        let framed = record::encode_into(&mut buf, Lsn(2), b"reserved-type");
        buf[16] = 2; // rec_type byte (0=sentinel, 1=Full, 2.. reserved)
        let crc = crate::crc::crc32c(&buf[4..framed]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        file.write_all_at(&buf, x).unwrap();
        file.sync_data().unwrap();

        let rec = recover_segment(&file, Lsn(1), true, SEGMENT_SIZE, MAX_RECORD_SIZE).unwrap();
        assert_eq!(rec.max_lsn, Lsn(1));
        assert!(matches!(rec.tail_state, TailState::TruncatedAt { offset, .. } if offset == x));
    }

    #[test]
    fn arbitrary_bytes_never_panic_and_terminate() {
        // Interim D11 coverage (the libFuzzer F1 target is M9): overwrite the
        // record region with deterministic pseudo-random bytes and assert
        // recovery returns Ok/Err without panicking, looping, or reading OOB.
        let dir = tempfile::tempdir().unwrap();
        for seed in 0u64..64 {
            // A distinct base per iteration ⇒ a distinct segment filename
            // (`create_new` would reject a reused name).
            let base = Lsn(seed + 1);
            let file = fresh_segment(dir.path(), base);
            // A small LCG fill — deterministic (§8.6), no external RNG dep.
            let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
            let mut bytes = vec![0u8; 4096];
            for b in &mut bytes {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                *b = (state >> 33) as u8;
            }
            file.write_all_at(&bytes, HEADER_SIZE).unwrap();
            // Must not panic; either classification or a fatal error is fine.
            let _ = recover_segment(&file, base, true, SEGMENT_SIZE, MAX_RECORD_SIZE);
        }
    }
}
