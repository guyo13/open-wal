//! §14.2 P4 + P7 — cross-segment + commit-time-split property tests.
//!
//! With a tiny `segment_size`, arbitrary payload batches (sizes from 0 to
//! `max_record_size`) and arbitrary commit groupings force many rolls and
//! commit batches that span ≥2 segments. After reopening, the full sequence must
//! reconstruct byte-for-byte and dense (D2/D6); that it reconstructs at all
//! proves no record was split across a segment boundary (§5.3) and that the split
//! loop terminated (no spin — §7.3). `durable_lsn` equals the record count.

use proptest::prelude::*;

use open_wal::{Lsn, Wal, WalConfig};

/// 256-byte segments (192 usable after the 64-byte header) with a 165-byte max
/// record (165 + 91 = 256), so most records nearly fill a segment and many
/// batches straddle boundaries — including the empty-prefix "next record does not
/// fit the remainder" case (P7).
fn tiny() -> WalConfig {
    WalConfig {
        segment_size: 256,
        max_record_size: 165,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn split_and_roll_roundtrip_is_dense(
        ops in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 0..=165usize), any::<bool>()),
            0..40,
        )
    ) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = tiny();

        // Write: append each payload, committing at the arbitrary group points,
        // then a final commit. The split/roll machinery runs inside commit.
        let payloads: Vec<Vec<u8>> = ops.iter().map(|(p, _)| p.clone()).collect();
        {
            let (mut wal, _) = Wal::open(dir.path(), cfg).unwrap();
            for (p, commit_after) in &ops {
                wal.append(p).unwrap();
                if *commit_after {
                    wal.commit().unwrap();
                }
            }
            let durable = wal.commit().unwrap();
            prop_assert_eq!(durable.0, payloads.len() as u64);
        }

        // Reopen (full multi-segment recovery) and replay the dense sequence.
        let (wal, report) = Wal::open(dir.path(), cfg).unwrap();
        prop_assert_eq!(report.oldest_lsn, Lsn(1));
        prop_assert_eq!(report.durable_lsn.0, payloads.len() as u64);

        let mut r = wal.reader_from(Lsn(0)).unwrap();
        let mut i = 0u64;
        while let Some(item) = r.next() {
            let (lsn, got) = item.unwrap();
            prop_assert_eq!(lsn.0, i + 1);
            prop_assert_eq!(got, &payloads[i as usize][..]);
            i += 1;
        }
        prop_assert_eq!(i, payloads.len() as u64);
    }
}
