//! v7.1 §14.2 — LSN-exhaustion property.
//!
//! A log cold-started near the top of the LSN space (seed in
//! `[u64::MAX − 8, u64::MAX]`), driven by arbitrary appends / commits /
//! checkpoints / clean reopens / crash-reopens over tiny segments (so rolls and
//! commit-time splits happen right at the end of the space), must:
//! - never assign or recover a duplicate LSN — every recovered run is dense and
//!   strictly increasing from `oldest_lsn` up to (at most) `u64::MAX` (D2/D6);
//! - report exhaustion as `WalError::LsnExhausted` exactly when the oracle's
//!   last LSN is `u64::MAX` — never a panic, never `Poisoned`, and the handle
//!   stays usable (commit, reads, checkpoint) afterwards (§12: not a poison);
//! - recover identically across reopens (D7).

use std::collections::BTreeMap;

use proptest::prelude::*;

use open_wal::{Lsn, Wal, WalConfig, WalError};

/// 256-byte segments (192 usable) with a 165-byte max record ⇒ rolls/splits.
fn tiny() -> WalConfig {
    WalConfig {
        segment_size: 256,
        max_record_size: 165,
    }
}

#[derive(Debug, Clone)]
enum Op {
    Append(Vec<u8>),
    Commit,
    Checkpoint(u64),
    Reopen,
    Crash,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => prop_oneof![
            Just(Vec::new()),
            Just(vec![0x5A; 165]),
            prop::collection::vec(any::<u8>(), 0..=165usize),
        ]
        .prop_map(Op::Append),
        3 => Just(Op::Commit),
        1 => (0u64..10).prop_map(Op::Checkpoint),
        1 => Just(Op::Reopen),
        1 => Just(Op::Crash),
    ]
}

/// Replay the whole log and check it is dense, strictly increasing and
/// byte-identical to the oracle's retained committed records.
fn check_log(wal: &Wal, committed: &BTreeMap<u64, Vec<u8>>) -> Result<(), TestCaseError> {
    let oldest = wal.oldest_lsn().0;
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let mut got = Vec::new();
    while let Some(item) = r.next() {
        let (lsn, p) = item.unwrap();
        got.push((lsn.0, p.to_vec()));
    }
    for w in got.windows(2) {
        prop_assert!(
            w[0].0 < w[1].0,
            "duplicate/non-increasing LSN: {:?}",
            (w[0].0, w[1].0)
        );
        prop_assert_eq!(w[0].0 + 1, w[1].0, "D2: gap");
    }
    if let Some(first) = got.first() {
        prop_assert_eq!(first.0, oldest);
    }
    let want: Vec<(u64, Vec<u8>)> = committed
        .range(oldest..)
        .map(|(l, p)| (*l, p.clone()))
        .collect();
    prop_assert_eq!(got, want, "D6: replay differs from the oracle");
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn exhaustion_is_loud_never_duplicates_never_poisons(
        back in 0u64..=8,
        ops in prop::collection::vec(op_strategy(), 1..40),
    ) {
        let seed = u64::MAX - back;
        let dir = tempfile::tempdir().unwrap();
        let cfg = tiny();
        let (mut wal, r) = Wal::options().seed(Lsn(seed)).open(dir.path(), cfg).unwrap();
        prop_assert_eq!(r.durable_lsn, Lsn(seed - 1));

        // Oracle. `last` = highest assigned LSN (durable or staged).
        let mut committed: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        let mut staged: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut last = seed - 1;

        for op in ops {
            match op {
                Op::Append(p) => match wal.append(&p) {
                    Ok(lsn) => {
                        prop_assert!(last < u64::MAX, "append succeeded past u64::MAX");
                        prop_assert_eq!(lsn.0, last + 1);
                        last += 1;
                        staged.push((last, p));
                    }
                    Err(WalError::LsnExhausted) => {
                        prop_assert_eq!(last, u64::MAX, "LsnExhausted before capacity");
                        prop_assert_eq!(wal.last_lsn(), Lsn(u64::MAX));
                    }
                    Err(e) => prop_assert!(false, "unexpected append error {:?}", e),
                },
                Op::Commit => {
                    // Never Poisoned, even after LsnExhausted.
                    prop_assert_eq!(wal.commit().unwrap(), Lsn(last));
                    committed.extend(staged.drain(..));
                    check_log(&wal, &committed)?;
                }
                Op::Checkpoint(k) => {
                    let up_to = (wal.oldest_lsn().0.saturating_add(k)).min(wal.durable_lsn().0);
                    wal.checkpoint(Lsn(up_to)).unwrap();
                    committed = committed.split_off(&wal.oldest_lsn().0);
                    check_log(&wal, &committed)?;
                }
                Op::Reopen | Op::Crash => {
                    if matches!(op, Op::Reopen) {
                        wal.commit().unwrap();
                        committed.extend(staged.drain(..));
                    } else {
                        last -= staged.len() as u64;
                        staged.clear();
                    }
                    let floor = wal.oldest_lsn();
                    drop(wal);
                    let (w, r) = Wal::open(dir.path(), cfg).unwrap();
                    wal = w;
                    prop_assert_eq!(r.oldest_lsn, floor, "D7");
                    prop_assert_eq!(r.durable_lsn, Lsn(last), "D1/D3");
                    prop_assert_eq!(wal.last_lsn(), Lsn(last));
                    check_log(&wal, &committed)?;
                }
            }
        }

        // Terminal: commit, then exhaust the space explicitly — always loud,
        // never a panic, never a poison; the log stays readable and reopens.
        wal.commit().unwrap();
        committed.extend(staged.drain(..));
        while last < u64::MAX {
            prop_assert_eq!(wal.append(b"fill").unwrap().0, last + 1);
            last += 1;
            committed.insert(last, b"fill".to_vec());
        }
        for _ in 0..2 {
            prop_assert!(matches!(wal.append(b"over"), Err(WalError::LsnExhausted)));
        }
        prop_assert_eq!(wal.commit().unwrap(), Lsn(u64::MAX));
        check_log(&wal, &committed)?;
        drop(wal);
        let (wal, r) = Wal::open(dir.path(), cfg).unwrap();
        prop_assert_eq!(r.durable_lsn, Lsn(u64::MAX));
        check_log(&wal, &committed)?;
    }
}
