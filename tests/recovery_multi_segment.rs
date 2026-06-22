//! A1 — a directory with more than one segment must be **rejected**, not
//! silently recovered as only the active segment (which would present an
//! internal LSN gap, D2/D6). Real multi-segment recovery is M4.

mod common;
use common::*;

use open_wal::{Wal, WalError};

#[test]
fn multi_segment_directory_is_rejected_not_silently_misrecovered() {
    let dir = tempfile::tempdir().unwrap();
    write_clean(dir.path(), &[b"a", b"b", b"c"]);

    // Introduce a second, foreign segment file with a valid `{20 digits}.wal`
    // name. The guard fires on the *count* of segments before any per-segment
    // recovery, so the copy's contents don't matter — what matters is that
    // `open` refuses rather than recovering only one of them.
    let second = dir.path().join("00000000000000000065.wal");
    std::fs::copy(seg_path(dir.path()), &second).unwrap();

    match Wal::open(dir.path(), config()) {
        Err(WalError::Unsupported { .. }) => {}
        Ok(_) => panic!("multi-segment directory must be rejected, not silently recovered"),
        Err(e) => panic!("expected Unsupported, got {e:?}"),
    }
}

#[test]
fn single_segment_still_opens_after_the_guard() {
    // Sanity: the guard does not affect the normal single-segment path.
    let dir = tempfile::tempdir().unwrap();
    write_clean(dir.path(), &[b"x", b"y"]);
    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn.0, 2);
    assert_eq!(replay(&wal), vec![b"x".to_vec(), b"y".to_vec()]);
}
