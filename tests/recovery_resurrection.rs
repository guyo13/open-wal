//! §14.4g (logic part) — buried-garbage / resurrection, driven through
//! `open()` (M3).
//!
//! The headline byte-level hazard (a stale CRC-valid record whose LSN matches
//! the post-truncation `expected_next_lsn`) is covered as a white-box unit test
//! in `src/recovery.rs`. Here we exercise the "simpler case" through the public
//! API and assert the *mechanism* that defeats resurrection: after a torn-tail
//! recovery zeroes `[X, EOF)`, that region stays zero, so writing **shorter**
//! new records cannot expose a stale older record's tail (D10).
//!
//! The full strengthening — that the zeroed region survives a **power-loss
//! cycle** (LazyFS `clear-cache`) — needs FUSE and is the M3 gate (§14.4g
//! durability-of-zeroing), run separately.

mod common;
use common::*;

use open_wal::{Lsn, TailState, Wal, WalError};

#[test]
fn torn_tail_zeroes_old_record_bytes() {
    // The torn (last) record's bytes — and everything to EOF — read as zero
    // after recovery, so nothing stale can survive past the truncation point.
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"keep", b"OLD-LONG-RECORD-PAYLOAD"];
    write_clean(dir.path(), payloads);
    let x = offset_of(payloads, 1);
    let old_end = x + framed(payloads[1].len());
    // Corrupt the last record so it becomes a torn tail.
    flip_byte(dir.path(), x + 20);

    let (_wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(1));
    assert!(matches!(report.tail_state, TailState::TruncatedAt { .. }));

    // The whole old-record region (and beyond, to EOF) is now zero.
    assert!(
        read_range(dir.path(), x, SEGMENT_SIZE)
            .iter()
            .all(|&b| b == 0),
        "[X, EOF) must be zeroed by recovery"
    );
    // Belt and suspenders: the specific old-record span is gone.
    assert!(read_range(dir.path(), x, old_end).iter().all(|&b| b == 0));
}

#[test]
fn shorter_rewrite_after_torn_tail_leaves_no_buried_record() {
    // torn tail → recover (zeroes [X, EOF)) → write a SHORTER record → reopen.
    // The bytes beyond the new short record (where the old longer record's tail
    // used to live) are zero, and recovery yields exactly the dense suffix with
    // no resurrected/spurious record (D10).
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"r1", b"r2", b"OLD-LONG-THIRD-RECORD-PAYLOAD"];
    write_clean(dir.path(), payloads);
    let x = offset_of(payloads, 2);
    let old_end = x + framed(payloads[2].len());
    flip_byte(dir.path(), x + 20); // torn third record

    // Recover (truncates + zeroes), then append a much shorter third record.
    {
        let (mut wal, report) = Wal::open(dir.path(), config()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(2));
        assert_eq!(wal.append(b"x").unwrap(), Lsn(3)); // framed("x") = 24 bytes
        wal.commit().unwrap();
    }

    // Beyond the new short record, up to where the old long record ended, must
    // be zero — the old tail was erased, not merely overwritten at the front.
    let short_end = x + framed(1);
    assert!(
        read_range(dir.path(), short_end, old_end)
            .iter()
            .all(|&b| b == 0),
        "old record tail must remain zeroed after a shorter rewrite"
    );

    // Reopen: exactly r1, r2, and the new short r3 — no spurious 4th record.
    let (wal, report) = Wal::open(dir.path(), config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(3));
    assert_eq!(report.tail_state, TailState::Clean);
    assert_eq!(
        replay(&wal),
        vec![b"r1".to_vec(), b"r2".to_vec(), b"x".to_vec()]
    );
}

#[test]
fn repeated_torn_recovery_is_idempotent() {
    // D7: after the first (truncating) recovery, further opens are stable and
    // clean, with the same durable content.
    let dir = tempfile::tempdir().unwrap();
    let payloads: &[&[u8]] = &[b"a", b"b", b"c"];
    write_clean(dir.path(), payloads);
    flip_byte(dir.path(), offset_of(payloads, 2) + 20);

    let mut prev: Option<(Lsn, Vec<Vec<u8>>)> = None;
    for i in 0..4 {
        let (wal, report) = Wal::open(dir.path(), config()).unwrap();
        // The first open truncates; every open from the second on is clean.
        if i > 0 {
            assert_eq!(report.tail_state, TailState::Clean);
        }
        let snap = (report.durable_lsn, replay(&wal));
        if let Some(p) = &prev {
            assert_eq!(&snap, p, "recovery must be idempotent");
        }
        prev = Some(snap);
    }
    assert_eq!(prev.unwrap().0, Lsn(2));
}

#[test]
fn open_after_torn_tail_does_not_error() {
    // A torn tail is recoverable, not fatal (contrast with mid-log corruption).
    let dir = tempfile::tempdir().unwrap();
    write_clean(dir.path(), &[b"only"]);
    // Corrupt the single (last) record ⇒ empty durable suffix, but a clean open.
    flip_byte(dir.path(), HEADER_SIZE + 20);
    match Wal::open(dir.path(), config()) {
        Ok((_w, report)) => {
            assert_eq!(report.durable_lsn, Lsn(0));
            assert!(matches!(report.tail_state, TailState::TruncatedAt { .. }));
        }
        Err(e) => panic!("torn tail must recover, got {e:?}"),
    }
    // And it must not be a fatal error type if it had returned one.
    let _ = WalError::Poisoned;
}
