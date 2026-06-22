//! §14.4f — truncation / short-file injection, driven through `open()` (M3).
//!
//! A clean committed log is physically truncated (`set_len`) at various offsets;
//! recovery must yield a valid dense suffix, never panic (D4, D11), and
//! re-extend the segment to its pre-allocated size (the zeroing of `[X, EOF)`
//! writes back the tail blocks).

use std::path::Path;

mod common;
use common::*;

use open_wal::{Lsn, TailState, Wal, WalError};

fn truncate_to(dir: &Path, len: u64) {
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(seg_path(dir))
        .unwrap();
    f.set_len(len).unwrap();
    f.sync_all().unwrap();
}

/// Reopen, asserting the recovered prefix is `&payloads[..keep]`, the tail was
/// truncated, and the file was re-extended to the pre-allocated size.
fn assert_recovers_prefix(dir: &Path, payloads: &[&[u8]], keep: usize) {
    let (wal, report) = Wal::open(dir, config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(keep as u64));
    assert!(
        matches!(report.tail_state, TailState::TruncatedAt { .. }),
        "a physically truncated tail must be re-truncated + zeroed"
    );
    let want: Vec<Vec<u8>> = payloads[..keep].iter().map(|p| p.to_vec()).collect();
    assert_eq!(replay(&wal), want);
    assert_eq!(
        seg_path(dir).metadata().unwrap().len(),
        SEGMENT_SIZE,
        "zeroing must re-extend the segment to its pre-allocated size"
    );
}

#[test]
fn mid_header_truncation_is_bad_header_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    write_clean(dir.path(), &[b"a", b"b"]);
    truncate_to(dir.path(), 30); // below the 64-byte header
    assert!(matches!(
        Wal::open(dir.path(), config()),
        Err(WalError::BadSegmentHeader)
    ));
}

#[test]
fn mid_record_truncation_recovers_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"alpha", b"beta", b"gamma", b"delta"];
    write_clean(dir.path(), payloads);
    // Cut a few bytes into the 4th record (LSN 4) ⇒ keep 3.
    truncate_to(dir.path(), offset_of(payloads, 3) + 4);
    assert_recovers_prefix(dir.path(), payloads, 3);
}

#[test]
fn between_records_truncation_recovers_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"alpha", b"beta", b"gamma", b"delta"];
    write_clean(dir.path(), payloads);
    // Cut exactly at the start of the 3rd record (LSN 3) ⇒ keep 2.
    truncate_to(dir.path(), offset_of(payloads, 2));
    assert_recovers_prefix(dir.path(), payloads, 2);
}

#[test]
fn mid_padding_truncation_recovers_prefix() {
    let dir = tempfile::tempdir().unwrap();
    // "abc" ⇒ payload 3, framed 24, 1 padding byte at offset 23 of the record.
    let payloads: &[&[u8]] = &[b"alpha", b"beta", b"abc"];
    write_clean(dir.path(), payloads);
    // Cut inside the 3rd record's padding (its last byte) ⇒ keep 2.
    truncate_to(dir.path(), offset_of(payloads, 2) + 23);
    assert_recovers_prefix(dir.path(), payloads, 2);
}

#[test]
fn truncation_just_past_last_record_preserves_all() {
    // Cutting away the pre-allocated zero tail (right after the last record)
    // loses no committed record; recovery re-zeros + re-extends.
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"one", b"two", b"three"];
    write_clean(dir.path(), payloads);
    truncate_to(dir.path(), offset_of(payloads, 3)); // == end of last record
    assert_recovers_prefix(dir.path(), payloads, 3);
}

#[test]
fn recovered_truncation_is_idempotent_and_appendable() {
    // After recovery re-extends + zeroes, a second open is clean, and appends
    // resume densely (D7 + D2).
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"keep1", b"keep2", b"cut"];
    write_clean(dir.path(), payloads);
    truncate_to(dir.path(), offset_of(payloads, 2) + 3);

    {
        let (mut wal, _) = Wal::open(dir.path(), config()).unwrap();
        assert_eq!(wal.append(b"new3").unwrap(), Lsn(3));
        wal.commit().unwrap();
    }
    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(3));
    assert_eq!(report.tail_state, TailState::Clean);
    assert_eq!(
        replay(&wal),
        vec![b"keep1".to_vec(), b"keep2".to_vec(), b"new3".to_vec()]
    );
}
