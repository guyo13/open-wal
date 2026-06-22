//! `reader_from` start-position behavior (B1): a mid-log start returns exactly
//! the suffix, and a start at/after the end returns nothing (never an error,
//! never earlier records).

mod common;
use common::*;

use open_wal::{Lsn, Wal};

fn collect_from(wal: &Wal, from: Lsn) -> Vec<(Lsn, Vec<u8>)> {
    let mut r = wal.reader_from(from).unwrap();
    let mut out = Vec::new();
    while let Some(item) = r.next() {
        let (lsn, payload) = item.unwrap();
        out.push((lsn, payload.to_vec()));
    }
    out
}

#[test]
fn reader_from_mid_log_returns_exact_suffix() {
    let dir = tempfile::tempdir().unwrap();
    write_clean(dir.path(), &[b"a", b"b", b"c", b"d", b"e"]);
    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(5));

    assert_eq!(
        collect_from(&wal, Lsn(3)),
        vec![
            (Lsn(3), b"c".to_vec()),
            (Lsn(4), b"d".to_vec()),
            (Lsn(5), b"e".to_vec()),
        ]
    );
    // The last record alone.
    assert_eq!(collect_from(&wal, Lsn(5)), vec![(Lsn(5), b"e".to_vec())]);
}

#[test]
fn reader_from_at_or_beyond_durable_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    write_clean(dir.path(), &[b"a", b"b"]);
    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(2));

    // One past the end, and far past the end — both yield nothing, no error.
    assert!(collect_from(&wal, Lsn(3)).is_empty());
    assert!(collect_from(&wal, Lsn(100)).is_empty());
}
