//! §14.4e — corruption / bit-flip injection, driven through the public
//! `open()` recovery path (M3).
//!
//! Each test writes a clean committed log via `Wal`, closes it, mutates raw
//! bytes on disk, then reopens and asserts the §8.2 classification:
//! - last-record (active) corruption ⇒ torn-tail truncation + zeroing,
//!   recoverable (D4);
//! - middle acked-record corruption ⇒ fatal `TornMidLog`, never silent
//!   truncation (D5);
//! - segment-header corruption ⇒ fatal `BadSegmentHeader`.
//!
//! The sealed-segment fatal path (§14.4e "any corruption in a sealed segment ⇒
//! FATAL") is exercised as a white-box unit test in `src/recovery.rs`, since the
//! M3 writer cannot yet produce a sealed segment (rolls are M4).

use std::path::Path;

mod common;
use common::*;

use open_wal::{Lsn, TailState, Wal, WalError};

/// `Wal::open`'s error, or panic if it unexpectedly succeeded. (`Wal` is not
/// `Debug`, so `Result::unwrap_err` is unavailable.)
fn open_err(dir: &Path) -> WalError {
    match Wal::open(dir, config()) {
        Ok(_) => panic!("expected recovery to fail, but open() succeeded"),
        Err(e) => e,
    }
}

#[test]
fn last_record_payload_corruption_is_truncated_and_recoverable() {
    // D4: corrupting the final record's payload truncates the tail at its offset.
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"first", b"second", b"third"];
    write_clean(dir.path(), payloads);

    let x = offset_of(payloads, 2);
    flip_byte(dir.path(), x + 20); // a payload byte of the 3rd record

    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(2));
    assert_eq!(
        report.tail_state,
        TailState::TruncatedAt {
            segment_base: Lsn(1),
            offset: x
        }
    );
    assert_eq!(replay(&wal), vec![b"first".to_vec(), b"second".to_vec()]);
}

#[test]
fn last_record_crc_field_corruption_is_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"alpha", b"beta"];
    write_clean(dir.path(), payloads);
    let x = offset_of(payloads, 1);
    flip_byte(dir.path(), x); // the CRC field of the last record

    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(1));
    assert!(matches!(report.tail_state, TailState::TruncatedAt { .. }));
    assert_eq!(replay(&wal), vec![b"alpha".to_vec()]);
}

#[test]
fn last_record_padding_corruption_is_truncated() {
    // §14.4e (vi): a padding byte is inside CRC coverage, so flipping it fails
    // the CRC and the last record is truncated — proving padding is covered.
    let dir = tempfile::tempdir().unwrap();
    // "ab" ⇒ payload 2, pad = (8 - (22 % 8)) % 8 = 2 padding bytes.
    let payloads: &[&[u8]] = &[b"xyz", b"ab"];
    write_clean(dir.path(), payloads);
    let x = offset_of(payloads, 1);
    // First padding byte: just past the 20-byte header + 2-byte payload.
    flip_byte(dir.path(), x + 20 + 2);

    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(1));
    assert!(matches!(report.tail_state, TailState::TruncatedAt { .. }));
    assert_eq!(replay(&wal), vec![b"xyz".to_vec()]);
}

#[test]
fn middle_record_corruption_is_fatal_tornmidlog() {
    // D5: corrupting an interior acked record (a valid record still follows it)
    // is a fatal `TornMidLog`, never silent truncation.
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"one", b"two", b"three", b"four"];
    write_clean(dir.path(), payloads);

    let x = offset_of(payloads, 1); // the 2nd record (LSN 2)
    flip_byte(dir.path(), x + 20);

    let err = open_err(dir.path());
    assert!(
        matches!(err, WalError::TornMidLog { segment, offset } if segment == Lsn(1) && offset == x),
        "expected TornMidLog at {x}, got {err:?}"
    );
}

#[test]
fn middle_record_length_field_corruption_is_fatal() {
    // Corrupting the `length` field (offset 4) of a middle record also fails CRC
    // and, with a valid record after it, is fatal.
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"one", b"two", b"three"];
    write_clean(dir.path(), payloads);
    let x = offset_of(payloads, 1);
    flip_byte(dir.path(), x + 4);

    assert!(matches!(open_err(dir.path()), WalError::TornMidLog { .. }));
}

#[test]
fn segment_header_corruption_is_fatal() {
    // §14.4e (v): a corrupt header is rejected outright (it is synced at
    // creation, so it is never a torn tail).
    let dir = tempfile::tempdir().unwrap();
    write_clean(dir.path(), &[b"a", b"b"]);
    flip_byte(dir.path(), 0); // the magic

    assert!(matches!(open_err(dir.path()), WalError::BadSegmentHeader));
}

#[test]
fn append_after_torn_tail_recovery_stays_dense() {
    // After a torn-tail recovery, the write offset resumes at the truncation
    // point: new appends neither overwrite the survivors nor leave a hole (D2).
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"keep1", b"keep2", b"torn"];
    write_clean(dir.path(), payloads);
    flip_byte(dir.path(), offset_of(payloads, 2) + 20);

    {
        let (mut wal, report) = Wal::open(dir.path(), config()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(2));
        assert_eq!(wal.append(b"new3").unwrap(), Lsn(3));
        assert_eq!(wal.commit().unwrap(), Lsn(3));
    }

    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(3));
    assert_eq!(report.tail_state, TailState::Clean); // zeroed tail is now clean
    assert_eq!(
        replay(&wal),
        vec![b"keep1".to_vec(), b"keep2".to_vec(), b"new3".to_vec()]
    );
}
