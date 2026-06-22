//! §8.1 / §8.4 — multi-segment recovery, driven through the public `Wal` API.
//!
//! The writer is given a tiny `segment_size` so ordinary appends roll across
//! many segments; each test then reopens (optionally after mutating files on
//! disk) and asserts the §8 contract: a clean multi-segment log replays dense
//! (D2/D6); a torn tail in the active segment truncates and recovers; a deleted
//! *middle* segment is a fatal `ContiguityViolation` (D2); and a deleted
//! *prefix* (oldest segments) is accepted silently (§4 D2).

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

mod common;

use open_wal::{Lsn, TailState, Wal, WalConfig};

/// Tiny segments (448 usable bytes after the 64-byte header), so each ~200-byte
/// record pair fills a segment and the next forces a roll.
fn tiny() -> WalConfig {
    WalConfig {
        segment_size: 512,
        max_record_size: 256,
    }
}

fn seg(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.wal"))
}

/// Write `n` distinct 200-byte records (one commit) into a tiny-segment log,
/// returning the durable LSN. With two records per segment they land in segments
/// based at 1, 3, 5, …
fn write_n(dir: &Path, n: u8) -> u64 {
    let (mut wal, _) = Wal::open(dir, tiny()).unwrap();
    for i in 1..=n {
        let p = vec![i; 200];
        wal.append(&p).unwrap();
    }
    wal.commit().unwrap().0
}

/// Replay the whole log from `from`, returning the LSNs seen (payloads ignored).
fn replay_lsns(wal: &Wal, from: Lsn) -> Vec<u64> {
    let mut r = wal.reader_from(from).unwrap();
    let mut out = Vec::new();
    while let Some(item) = r.next() {
        out.push(item.unwrap().0.0);
    }
    out
}

#[test]
fn writer_produces_and_recovers_multi_segment_log() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(write_n(dir.path(), 5), 5);
    // Two records per segment ⇒ bases 1, 3, 5 all present.
    for b in [1u64, 3, 5] {
        assert!(seg(dir.path(), b).exists(), "segment {b} should exist");
    }

    let (wal, report) = Wal::open(dir.path(), tiny()).unwrap();
    assert_eq!(report.oldest_lsn, Lsn(1));
    assert_eq!(report.durable_lsn, Lsn(5));
    assert_eq!(report.segments_scanned, 3);
    assert_eq!(report.tail_state, TailState::Clean);
    assert_eq!(replay_lsns(&wal, Lsn(0)), vec![1, 2, 3, 4, 5]);
}

#[test]
fn torn_tail_in_active_segment_recovers_dense_prefix() {
    // The active segment (base 5) holds only record 5; corrupting its payload
    // makes it a torn tail ⇒ truncate + zero, durable falls back to 4 (D4).
    let dir = tempfile::tempdir().unwrap();
    write_n(dir.path(), 5);

    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(seg(dir.path(), 5))
        .unwrap();
    let mut b = [0u8; 1];
    f.read_at(&mut b, common::HEADER_SIZE + 20).unwrap(); // a payload byte of rec 5
    b[0] ^= 0xFF;
    f.write_all_at(&b, common::HEADER_SIZE + 20).unwrap();
    f.sync_all().unwrap();

    let (wal, report) = Wal::open(dir.path(), tiny()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(4));
    assert!(matches!(
        report.tail_state,
        TailState::TruncatedAt { segment_base, .. } if segment_base == Lsn(5)
    ));
    assert_eq!(replay_lsns(&wal, Lsn(0)), vec![1, 2, 3, 4]);
}

#[test]
fn deleting_a_middle_segment_is_a_fatal_gap() {
    // Removing an interior segment leaves an internal LSN gap between the prior
    // segment's max and the next segment's base ⇒ fatal `ContiguityViolation`
    // (D2), never a silent skip.
    let dir = tempfile::tempdir().unwrap();
    write_n(dir.path(), 5); // segs 1, 3, 5
    std::fs::remove_file(seg(dir.path(), 3)).unwrap();

    match Wal::open(dir.path(), tiny()) {
        Err(open_wal::WalError::ContiguityViolation) => {}
        Err(e) => panic!("expected ContiguityViolation, got {e:?}"),
        Ok(_) => panic!("expected ContiguityViolation, but open() succeeded"),
    }
}

#[test]
fn deleting_the_oldest_prefix_is_accepted_silently() {
    // A checkpointed-away prefix (here, simulated by deleting the oldest segment)
    // is fine for the writer's recovery (§4 D2): oldest_lsn advances, the
    // remaining suffix replays dense, and a reader from below it is a fatal gap.
    let dir = tempfile::tempdir().unwrap();
    write_n(dir.path(), 5); // segs 1, 3, 5
    std::fs::remove_file(seg(dir.path(), 1)).unwrap();

    let (wal, report) = Wal::open(dir.path(), tiny()).unwrap();
    assert_eq!(report.oldest_lsn, Lsn(3));
    assert_eq!(report.durable_lsn, Lsn(5));
    assert_eq!(replay_lsns(&wal, Lsn(3)), vec![3, 4, 5]);
    assert!(matches!(
        wal.reader_from(Lsn(2)),
        Err(open_wal::WalError::ContiguityViolation)
    ));
}

#[test]
fn append_continues_after_multi_segment_reopen() {
    // Reopening a multi-segment log and appending more rolls again; the full
    // sequence stays dense across the close/reopen (D2/D6).
    let dir = tempfile::tempdir().unwrap();
    write_n(dir.path(), 5);
    {
        let (mut wal, report) = Wal::open(dir.path(), tiny()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(5));
        wal.append(&[6u8; 200]).unwrap();
        wal.append(&[7u8; 200]).unwrap();
        assert_eq!(wal.commit().unwrap(), Lsn(7));
    }
    let (wal, report) = Wal::open(dir.path(), tiny()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(7));
    assert_eq!(replay_lsns(&wal, Lsn(0)), vec![1, 2, 3, 4, 5, 6, 7]);
}
