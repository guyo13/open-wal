//! v7 §14.2 — seeded cold start + containing-segment `reader_from` properties.
//!
//! - **Seeded log (D2/D6/D7):** a log cold-started at an arbitrary seed `N`
//!   (`Wal::options().seed(N)`), driven through a random script of appends,
//!   commits (rolling and splitting across tiny segments), checkpoints, clean
//!   reopens and crash-reopens, is always dense from its `oldest_lsn` and
//!   byte-identical on replay. Every reopen passes a *different* seed, which
//!   must be ignored — the on-disk log is authoritative.
//! - **P-reader (D6, §8.5):** `reader_from(from)` for arbitrary `from` yields
//!   exactly the records `≥ from`, in order (a `from` below the floor is a fatal
//!   gap, §15.4). Under `--features fuzzing` it additionally asserts, via the
//!   `fuzzing::reader_open_segment` hook, that the reader opened the segment
//!   **containing** `from` (greatest base `≤ from`, derived independently from
//!   the directory listing) — not the oldest segment.

use std::collections::BTreeMap;
use std::path::Path;

use proptest::prelude::*;

use open_wal::{Lsn, Wal, WalConfig, WalError};

/// 256-byte segments (192 usable) with a 165-byte max record ⇒ frequent rolls
/// and commit-time splits.
fn tiny() -> WalConfig {
    WalConfig {
        segment_size: 256,
        max_record_size: 165,
    }
}

/// Seeds spanning the default, small, mid and very large origins.
fn seed_strategy() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(1u64),
        Just(2u64),
        2u64..10_000,
        (1u64 << 40)..(1u64 << 41),
        Just(u64::MAX / 4),
    ]
}

fn payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        Just(Vec::new()),
        Just(vec![0xA5; 165]),
        prop::collection::vec(any::<u8>(), 0..=165usize),
    ]
}

#[derive(Debug, Clone)]
enum Op {
    Append(Vec<u8>),
    Commit,
    /// Checkpoint at `oldest + k` (clamped to `durable` — the §9 caller rule).
    Checkpoint(u64),
    /// Commit, drop, reopen with this (ignored) seed.
    Reopen(u64),
    /// Drop without committing (lose the staged tail), reopen with this seed.
    Crash(u64),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => payload_strategy().prop_map(Op::Append),
        3 => Just(Op::Commit),
        1 => (0u64..12).prop_map(Op::Checkpoint),
        1 => seed_strategy().prop_map(Op::Reopen),
        1 => seed_strategy().prop_map(Op::Crash),
    ]
}

/// Sorted `base_lsn`s of the `*.wal` files in `dir` (independent of `Wal`).
fn bases_on_disk(dir: &Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            name.strip_suffix(".wal")?.parse().ok()
        })
        .collect();
    v.sort_unstable();
    v
}

/// The whole log, replayed from the beginning.
fn replay_all(wal: &Wal) -> Vec<(u64, Vec<u8>)> {
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let mut out = Vec::new();
    while let Some(item) = r.next() {
        let (lsn, p) = item.unwrap();
        out.push((lsn.0, p.to_vec()));
    }
    out
}

/// Assert the live log is dense from `oldest_lsn()` to `durable_lsn()` and
/// byte-identical to the oracle's retained records (D2/D6).
fn check_dense(
    wal: &Wal,
    committed: &BTreeMap<u64, Vec<u8>>,
) -> std::result::Result<(), TestCaseError> {
    let oldest = wal.oldest_lsn().0;
    let durable = wal.durable_lsn().0;
    let got = replay_all(wal);
    let want: Vec<(u64, Vec<u8>)> = committed
        .range(oldest..)
        .map(|(l, p)| (*l, p.clone()))
        .collect();
    prop_assert_eq!(got.len() as u64, durable + 1 - oldest, "D2: not dense");
    prop_assert_eq!(got, want, "D6: replay differs from the oracle");
    Ok(())
}

/// P-reader check for one `from` (D6 + §8.5 containment).
fn check_reader_from(
    wal: &Wal,
    dir: &Path,
    committed: &BTreeMap<u64, Vec<u8>>,
    from: u64,
) -> std::result::Result<(), TestCaseError> {
    let oldest = wal.oldest_lsn().0;
    if from != 0 && from < oldest {
        prop_assert!(
            matches!(
                wal.reader_from(Lsn(from)),
                Err(WalError::ContiguityViolation)
            ),
            "§15.4: from={} below oldest={} must be a fatal gap",
            from,
            oldest
        );
        return Ok(());
    }
    let mut r = wal.reader_from(Lsn(from)).unwrap();

    // §8.5: the reader opened the greatest base ≤ from (from == 0 or below the
    // first base ⇒ the oldest segment). Derived from the directory listing, not
    // from the `Wal`'s own segment list.
    let bases = bases_on_disk(dir);
    let effective = from.max(1);
    let want_base = bases
        .iter()
        .copied()
        .rfind(|&b| b <= effective)
        .unwrap_or(bases[0]);
    #[cfg(feature = "fuzzing")]
    prop_assert_eq!(
        open_wal::fuzzing::reader_open_segment(&r),
        Some(Lsn(want_base)),
        "reader_from({}) must open the containing segment (bases {:?})",
        from,
        bases
    );
    #[cfg(not(feature = "fuzzing"))]
    let _ = want_base;

    let mut got = Vec::new();
    while let Some(item) = r.next() {
        let (lsn, p) = item.unwrap();
        got.push((lsn.0, p.to_vec()));
    }
    let want: Vec<(u64, Vec<u8>)> = committed
        .range(effective.max(oldest)..)
        .map(|(l, p)| (*l, p.clone()))
        .collect();
    prop_assert_eq!(
        got,
        want,
        "D6: reader_from({}) yielded the wrong records",
        from
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn seeded_log_is_dense_and_identical_across_reopens(
        seed in seed_strategy(),
        ops in prop::collection::vec(op_strategy(), 1..60),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = tiny();

        let (mut wal, report) = Wal::options().seed(Lsn(seed)).open(dir.path(), cfg).unwrap();
        prop_assert_eq!(report.oldest_lsn, Lsn(seed));
        prop_assert_eq!(report.durable_lsn, Lsn(seed - 1));
        prop_assert_eq!(wal.oldest_lsn(), Lsn(seed));

        // Oracle: committed records (pruned below the floor on checkpoint), the
        // staged tail, and the next LSN to be assigned.
        let mut committed: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        let mut staged: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut next = seed;
        let mut max_ckpt: Option<u64> = None;

        for op in ops {
            match op {
                Op::Append(p) => {
                    prop_assert_eq!(wal.append(&p).unwrap(), Lsn(next));
                    staged.push((next, p));
                    next += 1;
                }
                Op::Commit => {
                    prop_assert_eq!(wal.commit().unwrap(), Lsn(next - 1));
                    committed.extend(staged.drain(..));
                }
                Op::Checkpoint(k) => {
                    let durable = wal.durable_lsn().0;
                    let up_to = (wal.oldest_lsn().0 + k).min(durable);
                    let before = wal.oldest_lsn().0;
                    wal.checkpoint(Lsn(up_to)).unwrap();
                    let after = wal.oldest_lsn().0;
                    // D8: monotone floor, never above up_to + 1.
                    prop_assert!(after >= before && after <= up_to.max(before - 1) + 1);
                    max_ckpt = Some(max_ckpt.map_or(up_to, |m: u64| m.max(up_to)));
                    committed = committed.split_off(&after);
                    check_dense(&wal, &committed)?;
                }
                Op::Reopen(other) | Op::Crash(other) => {
                    if matches!(op, Op::Reopen(_)) {
                        wal.commit().unwrap();
                        committed.extend(staged.drain(..));
                    } else {
                        // Process crash: the staged (never-written) tail is lost.
                        next -= staged.len() as u64;
                        staged.clear();
                    }
                    let floor = wal.oldest_lsn();
                    drop(wal);
                    // A different seed on a non-empty directory MUST be ignored.
                    let (w, r) = Wal::options().seed(Lsn(other)).open(dir.path(), cfg).unwrap();
                    wal = w;
                    prop_assert_eq!(r.oldest_lsn, floor, "D7: seed {} changed the floor", other);
                    prop_assert_eq!(wal.oldest_lsn(), r.oldest_lsn);
                    prop_assert_eq!(r.durable_lsn, Lsn(next - 1), "D1/D3");
                    prop_assert!(r.oldest_lsn.0 >= seed);
                    prop_assert!(r.oldest_lsn.0 <= max_ckpt.map_or(seed, |m| m + 1).max(seed));
                    check_dense(&wal, &committed)?;
                }
            }
        }

        wal.commit().unwrap();
        committed.extend(staged.drain(..));
        check_dense(&wal, &committed)?;
        // P-reader over the final log: every `from` from 0 to past the tip.
        let tip = wal.durable_lsn().0;
        for from in [0, 1, seed.saturating_sub(1), seed, tip, tip + 1, tip + 2] {
            check_reader_from(&wal, dir.path(), &committed, from)?;
        }
        for from in wal.oldest_lsn().0..=tip {
            check_reader_from(&wal, dir.path(), &committed, from)?;
        }
    }

    #[test]
    fn reader_from_yields_exactly_records_at_or_above_from(
        seed in seed_strategy(),
        payloads in prop::collection::vec(payload_strategy(), 1..40),
        ckpt in 0u64..45,
        from_off in 0u64..50,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (mut wal, _) = Wal::options().seed(Lsn(seed)).open(dir.path(), tiny()).unwrap();
        let mut committed = BTreeMap::new();
        for p in &payloads {
            let lsn = wal.append(p).unwrap();
            committed.insert(lsn.0, p.clone());
        }
        let tip = wal.commit().unwrap().0;
        wal.checkpoint(Lsn((seed + ckpt).min(tip))).unwrap();
        let committed = committed.split_off(&wal.oldest_lsn().0);
        // `from` relative to the seed, so it lands below the floor, in every
        // segment, at the tip and past it; plus the absolute "from the beginning".
        let from = seed - 1 + from_off;
        check_reader_from(&wal, dir.path(), &committed, from)?;
        check_reader_from(&wal, dir.path(), &committed, 0)?;
    }
}
