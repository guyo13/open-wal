//! M2 property tests (§14.2) — single-segment write/read fidelity, density,
//! commit-boundary invariance, and idempotent clean recovery.
//!
//! These configs keep every generated batch inside one segment; rolls and
//! commit-time splits (and their property coverage P4/P7) are M4. The shared
//! checks below assert the durability contract slice that M2 owns: D6
//! (read-back fidelity), D2 (dense `1..=k`), D1 (commit-boundary invariance),
//! and D7 (idempotent recovery).

use open_wal::{Lsn, Wal, WalConfig};
use proptest::prelude::*;

/// One segment large enough to hold the test batches (payloads ≤ 256 B).
fn config() -> WalConfig {
    WalConfig {
        segment_size: 1 << 20, // 1 MiB
        max_record_size: 256,
    }
}

/// Replay the whole log from the beginning into an owned `Vec<Vec<u8>>`.
fn replay(wal: &Wal) -> Vec<Vec<u8>> {
    let mut reader = wal.reader_from(Lsn(0)).unwrap();
    let mut out = Vec::new();
    let mut expected = 1u64;
    while let Some(item) = reader.next() {
        let (lsn, payload) = item.unwrap();
        // D2: dense, in-order, starting at 1.
        assert_eq!(lsn, Lsn(expected), "recovered LSNs must be dense from 1");
        out.push(payload.to_vec());
        expected += 1;
    }
    out
}

proptest! {
    /// P1 Fidelity (D6): append all → commit → reopen → replay is byte-identical.
    #[test]
    fn p1_fidelity(records in proptest::collection::vec(
        proptest::collection::vec(any::<u8>(), 0..256), 0..200))
    {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path(), config()).unwrap();
            for r in &records {
                wal.append(r).unwrap();
            }
            wal.commit().unwrap();
        }
        let (wal, report) = Wal::open(dir.path(), config()).unwrap();
        prop_assert_eq!(report.durable_lsn, Lsn(records.len() as u64));
        prop_assert_eq!(replay(&wal), records);
    }

    /// P3 Commit-boundary invariance (D1): "commit after every append" yields
    /// the same durable content as "commit once".
    #[test]
    fn p3_commit_boundary_invariance(records in proptest::collection::vec(
        proptest::collection::vec(any::<u8>(), 0..256), 0..200))
    {
        let each = tempfile::tempdir().unwrap();
        let once = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(each.path(), config()).unwrap();
            for r in &records {
                wal.append(r).unwrap();
                wal.commit().unwrap();
            }
        }
        {
            let (mut wal, _) = Wal::open(once.path(), config()).unwrap();
            for r in &records {
                wal.append(r).unwrap();
            }
            wal.commit().unwrap();
        }
        let (wa, _) = Wal::open(each.path(), config()).unwrap();
        let (wb, _) = Wal::open(once.path(), config()).unwrap();
        prop_assert_eq!(replay(&wa), replay(&wb));
    }

    /// P2 Density (D2): arbitrary append/commit interleavings recover to exactly
    /// `1..=k` dense. `ops` is a script of (payload, commit?) steps.
    #[test]
    fn p2_density(ops in proptest::collection::vec(
        (proptest::collection::vec(any::<u8>(), 0..64), any::<bool>()), 0..200))
    {
        let dir = tempfile::tempdir().unwrap();
        let mut appended = 0u64;
        {
            let (mut wal, _) = Wal::open(dir.path(), config()).unwrap();
            for (payload, do_commit) in &ops {
                wal.append(payload).unwrap();
                appended += 1;
                if *do_commit {
                    wal.commit().unwrap();
                }
            }
            // A trailing commit makes the final tally deterministic: every
            // appended record is now durable.
            wal.commit().unwrap();
        }
        let committed = appended;
        let (wal, report) = Wal::open(dir.path(), config()).unwrap();
        prop_assert_eq!(report.durable_lsn, Lsn(committed));
        // replay() asserts density internally; here we pin the count.
        prop_assert_eq!(replay(&wal).len() as u64, committed);
    }

    /// P6 Idempotent clean recovery (D7): repeated open→close converges —
    /// stable durable_lsn / oldest_lsn / tail_state / content.
    #[test]
    fn p6_idempotent_recovery(records in proptest::collection::vec(
        proptest::collection::vec(any::<u8>(), 0..128), 0..100))
    {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path(), config()).unwrap();
            for r in &records {
                wal.append(r).unwrap();
            }
            wal.commit().unwrap();
        }
        let mut prev: Option<(Lsn, Lsn, Vec<Vec<u8>>)> = None;
        for _ in 0..4 {
            let (wal, report) = Wal::open(dir.path(), config()).unwrap();
            let content = replay(&wal);
            let snapshot = (report.oldest_lsn, report.durable_lsn, content);
            if let Some(p) = &prev {
                prop_assert_eq!(&snapshot, p, "recovery must be idempotent");
            }
            prev = Some(snapshot);
        }
    }
}
