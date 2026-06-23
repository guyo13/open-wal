//! §14.2 P5 — checkpoint preservation property test.
//!
//! With tiny segments forcing many rolls, an arbitrary payload batch is committed
//! and then `checkpoint(up_to)` is called for an arbitrary `up_to`. The contract
//! (§9, D8): **no record `> up_to` is lost or made unreadable**, and after
//! reopening, the recovered suffix is dense from the new `oldest_lsn` up to the
//! original durable watermark. Records `≤ up_to` may be reclaimed (a prefix), but
//! never a record above it, and never an internal hole.

use proptest::prelude::*;

use open_wal::{Lsn, Wal, WalConfig};

/// 256-byte segments (192 usable after the 64-byte header) with a 165-byte max
/// record, so most records nearly fill a segment ⇒ many sealed segments to
/// checkpoint across a wide range of `up_to` boundaries.
fn tiny() -> WalConfig {
    WalConfig {
        segment_size: 256,
        max_record_size: 165,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn checkpoint_preserves_records_above_up_to(
        payloads in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..=165usize),
            1..40,
        ),
        // `up_to` spans below, within, and above the LSN range (+ a margin past
        // the end, which must still keep the active segment — never over-delete).
        up_to in 0u64..50,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = tiny();
        let n = payloads.len() as u64;

        // Write the whole batch durably, then checkpoint at the arbitrary up_to.
        {
            let (mut wal, _) = Wal::open(dir.path(), cfg).unwrap();
            for p in &payloads {
                wal.append(p).unwrap();
            }
            prop_assert_eq!(wal.commit().unwrap(), Lsn(n));
            wal.checkpoint(Lsn(up_to)).unwrap();
        }

        // Reopen (full recovery) and inspect the surviving suffix.
        let (wal, report) = Wal::open(dir.path(), cfg).unwrap();
        // The durable watermark never regresses: every record > up_to is retained,
        // so the top of the log is unchanged.
        prop_assert_eq!(report.durable_lsn, Lsn(n));
        // D8: nothing above up_to may have been reclaimed ⇒ oldest_lsn ≤ up_to+1
        // (and ≥ 1). Equivalently, no record > up_to was deleted.
        prop_assert!(report.oldest_lsn.0 >= 1);
        prop_assert!(
            report.oldest_lsn.0 <= up_to + 1,
            "checkpoint reclaimed a record > up_to: oldest_lsn={} up_to={}",
            report.oldest_lsn.0,
            up_to
        );

        // The recovered suffix is dense from oldest_lsn..=n, byte-identical.
        let mut r = wal.reader_from(Lsn(0)).unwrap();
        let mut expected = report.oldest_lsn.0;
        while let Some(item) = r.next() {
            let (lsn, got) = item.unwrap();
            prop_assert_eq!(lsn.0, expected, "recovered suffix must be dense");
            prop_assert_eq!(got, &payloads[(lsn.0 - 1) as usize][..]);
            expected += 1;
        }
        prop_assert_eq!(expected, n + 1, "suffix must reach the durable watermark");

        // A reader from below the new oldest is a fatal gap (§15.4), never a skip.
        if report.oldest_lsn.0 > 1 {
            prop_assert!(wal.reader_from(Lsn(report.oldest_lsn.0 - 1)).is_err());
        }
    }
}
