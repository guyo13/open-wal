//! v7.2 §14.2 — resumable reader position properties (§8.7).
//!
//! - **P9, resume equivalence.** A seeded random op-script (append / commit /
//!   roll-forcing sizes / checkpoint / reopen / crash / fabricated torn tail) on
//!   tiny segments. At random points a position is taken from a reader and
//!   checked **at capture time** against the test's own model of what it
//!   appended; later, after more ops, `reader_at(pos)` is compared with the
//!   model's records from `pos.next_lsn()` on *and* with
//!   `reader_from(pos.next_lsn())`. Comparing only the two readers would let a
//!   `position()` that is wrong but self-consistent pass.
//! - **P10, arbitrary positions never lie** (`--features fuzzing`, which exposes
//!   the only constructor). `ReaderPos` values built from arbitrary fields,
//!   biased near real ones, must make `reader_at` either fail or yield exactly
//!   the model's records from `next_lsn` — and never panic.
//!
//! Both are high-iteration-overridable: `PROPTEST_CASES=2000 cargo test --test
//! reader_position_property --features fuzzing`.

use std::collections::BTreeMap;
use std::os::unix::fs::FileExt;
use std::path::Path;

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use open_wal::{Lsn, Reader, ReaderPos, Wal, WalConfig, WalError};

/// 256-byte segments (192 usable) with a 165-byte max record ⇒ frequent rolls,
/// commit-time splits and empty-prefix rolls.
fn tiny() -> WalConfig {
    WalConfig {
        segment_size: 256,
        max_record_size: 165,
    }
}

/// `PROPTEST_CASES` if set, else the per-PR default.
fn config(default_cases: u32) -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default_cases);
    ProptestConfig::with_cases(cases)
}

/// The test's own §5.3 framing arithmetic.
fn framed(payload_len: usize) -> u64 {
    (20 + payload_len as u64).div_ceil(8) * 8
}

fn seed_strategy() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(1u64),
        2u64..10_000,
        (1u64 << 40)..(1u64 << 41),
        Just(u64::MAX / 4),
    ]
}

/// Random bytes (which never contain a CRC-valid record), biased to the sizes
/// that force rolls.
fn payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        1 => Just(Vec::new()),
        2 => prop::collection::vec(any::<u8>(), 165..=165usize),
        2 => prop::collection::vec(any::<u8>(), 60..=100usize),
        5 => prop::collection::vec(any::<u8>(), 0..=165usize),
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

/// Drain a reader into owned `(lsn, payload)` pairs; an `Err` item fails the case.
fn drain(mut r: Reader<'_>) -> Result<Vec<(u64, Vec<u8>)>, TestCaseError> {
    let mut out = Vec::new();
    while let Some(item) = r.next() {
        match item {
            Ok((lsn, p)) => out.push((lsn.0, p.to_vec())),
            Err(e) => return Err(TestCaseError::fail(format!("reader yielded Err: {e:?}"))),
        }
    }
    Ok(out)
}

/// The test's own model of the log.
struct Model {
    /// Committed records still expected on disk, pruned below the floor.
    committed: BTreeMap<u64, Vec<u8>>,
    /// Appended, not yet committed.
    staged: Vec<(u64, Vec<u8>)>,
    /// The next LSN `append` must assign.
    next: u64,
    /// The last committed LSN (`seed - 1` for an empty log).
    durable: u64,
}

impl Model {
    fn from(&self, lsn: u64) -> Vec<(u64, Vec<u8>)> {
        self.committed
            .range(lsn..)
            .map(|(l, p)| (*l, p.clone()))
            .collect()
    }
}

/// Resume `pos` and check the three-way agreement (P9): `reader_at` vs the
/// model vs `reader_from`, or both readers refusing with `ContiguityViolation`.
fn check_resume(wal: &Wal, dir: &Path, model: &Model, pos: ReaderPos) -> Result<(), TestCaseError> {
    let next = pos.next_lsn().0;
    // The floor, from the directory listing (not from the `Wal`).
    let oldest = bases_on_disk(dir)[0];
    if next < oldest {
        prop_assert!(
            matches!(wal.reader_at(pos), Err(WalError::ContiguityViolation)),
            "reader_at below the floor (next_lsn {} < oldest {}) must be ContiguityViolation",
            next,
            oldest
        );
        prop_assert!(matches!(
            wal.reader_from(Lsn(next)),
            Err(WalError::ContiguityViolation)
        ));
        return Ok(());
    }
    let at = match wal.reader_at(pos) {
        Ok(r) => drain(r)?,
        Err(e) => {
            return Err(TestCaseError::fail(format!(
                "reader_at refused a genuine position {pos:?}: {e:?}"
            )));
        }
    };
    let want = model.from(next);
    prop_assert_eq!(
        &at,
        &want,
        "reader_at({:?}) differs from the model's records from {}",
        pos,
        next
    );
    let from = drain(wal.reader_from(Lsn(next)).unwrap())?;
    prop_assert_eq!(&at, &from, "reader_at differs from reader_from({})", next);
    Ok(())
}

#[derive(Debug, Clone)]
enum Op {
    Append(Vec<u8>),
    Commit,
    /// Checkpoint at `oldest + k`, clamped to the model's durable LSN.
    Checkpoint(u64),
    /// Commit, drop, reopen.
    Reopen,
    /// Drop without committing (the staged tail is lost), reopen.
    Crash,
    /// Drop without committing, corrupt the last record of the active segment
    /// (a fabricated torn tail), reopen. Recovery truncates it.
    TornCrash,
    /// Open `reader_from(from)` (`from` picked by the selector), take `steps`
    /// records, capture the position.
    Capture(u64, usize),
    /// Resume the stored position picked by the selector.
    Resume(usize),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => payload_strategy().prop_map(Op::Append),
        4 => Just(Op::Commit),
        1 => (0u64..12).prop_map(Op::Checkpoint),
        1 => Just(Op::Reopen),
        1 => Just(Op::Crash),
        1 => Just(Op::TornCrash),
        4 => (any::<u64>(), 0usize..12).prop_map(|(a, b)| Op::Capture(a, b)),
        3 => any::<usize>().prop_map(Op::Resume),
    ]
}

/// Take a position from a fresh reader and check it at capture time against the
/// model (P9). Returns the captured position.
fn capture(
    wal: &Wal,
    dir: &Path,
    model: &Model,
    from_sel: u64,
    steps: usize,
) -> Result<ReaderPos, TestCaseError> {
    let bases = bases_on_disk(dir);
    let oldest = bases[0];
    // `from` anywhere in `oldest ..= durable + 2`.
    let from = oldest + from_sel % (model.durable + 3 - oldest);
    let mut r = wal.reader_from(Lsn(from)).unwrap();

    // Before any `next`: the containing segment's start. Its base is derived
    // from the directory listing — greatest base ≤ from.
    let containing = bases.iter().copied().rfind(|&b| b <= from).unwrap();
    let p = r
        .position()
        .ok_or_else(|| TestCaseError::fail("no position before the first next"))?;
    prop_assert_eq!(
        p.next_lsn().0,
        containing,
        "capture: before any next, next_lsn must be the containing segment's base (from {})",
        from
    );
    let mut captured = p;
    let mut last_yielded: Option<u64> = None;
    for _ in 0..steps {
        match r.next() {
            Some(item) => {
                let (lsn, payload) = item.unwrap();
                let want_lsn = last_yielded.map_or(from, |l| l + 1);
                prop_assert_eq!(lsn.0, want_lsn);
                prop_assert_eq!(Some(payload), model.committed.get(&lsn.0).map(|v| &v[..]));
                last_yielded = Some(lsn.0);
                captured = r
                    .position()
                    .ok_or_else(|| TestCaseError::fail("no position after a yielded record"))?;
                prop_assert_eq!(
                    captured.next_lsn().0,
                    lsn.0 + 1,
                    "capture: after yielding {}, next_lsn must be {}",
                    lsn.0,
                    lsn.0 + 1
                );
            }
            None => {
                // The model says the log ends at `durable`.
                prop_assert_eq!(last_yielded.unwrap_or(model.durable), model.durable);
                captured = r
                    .position()
                    .ok_or_else(|| TestCaseError::fail("no position at a clean end of log"))?;
                prop_assert_eq!(
                    captured.next_lsn().0,
                    model.durable + 1,
                    "capture: at the end of log, next_lsn must be durable + 1"
                );
                break;
            }
        }
    }
    Ok(captured)
}

/// Corrupt the last record of the active (highest-base) segment, whose offset
/// the test derives from its own model of the payload sizes. Returns the LSN it
/// corrupted, if the active segment held a record.
fn tear_last_record(dir: &Path, model: &Model) -> Option<u64> {
    let active = *bases_on_disk(dir).last().unwrap();
    if model.durable < active {
        return None; // the active segment is empty
    }
    let offset: u64 = 64
        + model
            .committed
            .range(active..model.durable)
            .map(|(_, p)| framed(p.len()))
            .sum::<u64>();
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join(format!("{active:020}.wal")))
        .unwrap();
    // Flip a byte of the record's CRC field (present even for an empty payload).
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, offset).unwrap();
    f.write_all_at(&[b[0] ^ 0x5A], offset).unwrap();
    f.sync_data().unwrap();
    Some(model.durable)
}

proptest! {
    #![proptest_config(config(192))]

    /// §14.2 P9 — resume equivalence.
    #[test]
    fn p9_resume_equivalence(
        seed in seed_strategy(),
        ops in prop::collection::vec(op_strategy(), 1..80),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = tiny();
        let (wal, _) = Wal::options().seed(Lsn(seed)).open(dir.path(), cfg).unwrap();
        let mut wal = Some(wal);
        let mut model = Model {
            committed: BTreeMap::new(),
            staged: Vec::new(),
            next: seed,
            durable: seed - 1,
        };
        let mut positions: Vec<ReaderPos> = Vec::new();

        for op in ops {
            let w = wal.as_mut().unwrap();
            match op {
                Op::Append(p) => {
                    prop_assert_eq!(w.append(&p).unwrap(), Lsn(model.next));
                    model.staged.push((model.next, p));
                    model.next += 1;
                }
                Op::Commit => {
                    w.commit().unwrap();
                    model.committed.extend(model.staged.drain(..));
                    model.durable = model.next - 1;
                }
                Op::Checkpoint(k) => {
                    let oldest = bases_on_disk(dir.path())[0];
                    let up_to = (oldest + k).min(model.durable);
                    w.checkpoint(Lsn(up_to)).unwrap();
                    let floor = bases_on_disk(dir.path())[0];
                    prop_assert!(floor <= up_to + 1, "D8");
                    model.committed = model.committed.split_off(&floor);
                }
                Op::Reopen | Op::Crash | Op::TornCrash => {
                    if matches!(op, Op::Reopen) {
                        w.commit().unwrap();
                        model.committed.extend(model.staged.drain(..));
                        model.durable = model.next - 1;
                    }
                    model.staged.clear();
                    drop(wal.take());
                    if matches!(op, Op::TornCrash) {
                        if let Some(torn) = tear_last_record(dir.path(), &model) {
                            model.committed.remove(&torn);
                            model.durable = torn - 1;
                        }
                    }
                    let (reopened, report) = Wal::open(dir.path(), cfg).unwrap();
                    prop_assert_eq!(report.durable_lsn.0, model.durable);
                    model.next = model.durable + 1;
                    // §8.7 Reopen: discard every position above durable + 1.
                    positions.retain(|p| p.next_lsn().0 <= model.durable + 1);
                    wal = Some(reopened);
                }
                Op::Capture(from_sel, steps) => {
                    let p = capture(w, dir.path(), &model, from_sel, steps)?;
                    positions.push(p);
                }
                Op::Resume(sel) => {
                    if !positions.is_empty() {
                        let p = positions[sel % positions.len()];
                        check_resume(w, dir.path(), &model, p)?;
                    }
                }
            }
        }

        // Every surviving position must still resume correctly at the end.
        let w = wal.as_ref().unwrap();
        for p in &positions {
            check_resume(w, dir.path(), &model, *p)?;
        }
    }
}

/// §14.2 P10 — arbitrary positions never lie. Needs the `fuzzing` feature for
/// the `ReaderPos` constructor (the public API has none).
#[cfg(feature = "fuzzing")]
mod p10 {
    use super::*;
    use open_wal::fuzzing::{reader_pos, reader_pos_parts};

    /// How one field of a real position is perturbed.
    #[derive(Debug, Clone)]
    enum Tweak {
        Keep,
        /// Add a small signed delta.
        Delta(i64),
        /// Take the same field from another real position (by selector).
        Other(usize),
        /// Any value at all.
        Arbitrary(u64),
    }

    fn offset_tweak() -> impl Strategy<Value = Tweak> {
        prop_oneof![
            4 => Just(Tweak::Keep),
            3 => prop_oneof![Just(1i64), Just(-1), Just(8), Just(-8), Just(16), Just(-24)]
                .prop_map(Tweak::Delta),
            // ± one record (or more): another real record boundary.
            4 => any::<usize>().prop_map(Tweak::Other),
            // Any 8-aligned offset in and around the segment (the zeroed tail,
            // the header, just past the end).
            3 => (0u64..36).prop_map(|k| Tweak::Arbitrary(k * 8)),
            1 => any::<u64>().prop_map(Tweak::Arbitrary),
        ]
    }

    fn lsn_tweak() -> impl Strategy<Value = Tweak> {
        prop_oneof![
            4 => Just(Tweak::Keep),
            4 => prop_oneof![Just(1i64), Just(-1), Just(2), Just(-2)].prop_map(Tweak::Delta),
            3 => any::<usize>().prop_map(Tweak::Other),
            1 => any::<u64>().prop_map(Tweak::Arbitrary),
        ]
    }

    fn base_tweak() -> impl Strategy<Value = Tweak> {
        prop_oneof![
            5 => Just(Tweak::Keep),
            // Another live base.
            3 => any::<usize>().prop_map(Tweak::Other),
            // A non-base next to a real one.
            2 => prop_oneof![Just(1i64), Just(-1)].prop_map(Tweak::Delta),
            1 => any::<u64>().prop_map(Tweak::Arbitrary),
        ]
    }

    fn apply(real: u64, tweak: &Tweak, others: &[u64]) -> u64 {
        match *tweak {
            Tweak::Keep => real,
            Tweak::Delta(d) => real.wrapping_add_signed(d),
            Tweak::Other(sel) => others[sel % others.len()],
            Tweak::Arbitrary(v) => v,
        }
    }

    #[derive(Debug, Clone)]
    enum Build {
        Append(Vec<u8>),
        Commit,
        Checkpoint(u64),
    }

    fn build_strategy() -> impl Strategy<Value = Build> {
        prop_oneof![
            8 => payload_strategy().prop_map(Build::Append),
            3 => Just(Build::Commit),
            1 => (0u64..12).prop_map(Build::Checkpoint),
        ]
    }

    proptest! {
        #![proptest_config(config(192))]

        #[test]
        fn p10_arbitrary_positions_never_lie(
            seed in prop_oneof![Just(1u64), 2u64..1000],
            build in prop::collection::vec(build_strategy(), 1..50),
            forgeries in prop::collection::vec(
                (any::<usize>(), base_tweak(), offset_tweak(), lsn_tweak()),
                1..80,
            ),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let (mut wal, _) = Wal::options().seed(Lsn(seed)).open(dir.path(), tiny()).unwrap();
            let mut model = Model {
                committed: BTreeMap::new(),
                staged: Vec::new(),
                next: seed,
                durable: seed - 1,
            };
            // The build may leave a staged (uncommitted) tail: staged records
            // are not in the file and must stay invisible to every reader.
            for b in build {
                match b {
                    Build::Append(p) => {
                        wal.append(&p).unwrap();
                        model.staged.push((model.next, p));
                        model.next += 1;
                    }
                    Build::Commit => {
                        wal.commit().unwrap();
                        model.committed.extend(model.staged.drain(..));
                        model.durable = model.next - 1;
                    }
                    Build::Checkpoint(k) => {
                        let oldest = bases_on_disk(dir.path())[0];
                        wal.checkpoint(Lsn((oldest + k).min(model.durable))).unwrap();
                        let floor = bases_on_disk(dir.path())[0];
                        model.committed = model.committed.split_off(&floor);
                    }
                }
            }
            let bases = bases_on_disk(dir.path());
            let oldest = bases[0];

            // Every real position of a full replay (generator input only).
            let mut real: Vec<(u64, u64, u64)> = Vec::new();
            {
                let mut r = wal.reader_from(Lsn(0)).unwrap();
                real.push(reader_pos_parts(&r.position().unwrap()));
                while let Some(item) = r.next() {
                    item.unwrap();
                    real.push(reader_pos_parts(&r.position().unwrap()));
                }
            }
            let offsets: Vec<u64> = real.iter().map(|p| p.1).collect();
            let lsns: Vec<u64> = real.iter().map(|p| p.2).collect();

            for (sel, bt, ot, lt) in &forgeries {
                let (b, o, n) = real[sel % real.len()];
                let forged = reader_pos(
                    apply(b, bt, &bases),
                    apply(o, ot, &offsets),
                    apply(n, lt, &lsns),
                );
                let next = forged.next_lsn().0;
                let untouched = matches!((bt, ot, lt), (Tweak::Keep, Tweak::Keep, Tweak::Keep));
                match wal.reader_at(forged) {
                    Err(WalError::ContiguityViolation) => {
                        prop_assert!(
                            next < oldest,
                            "ContiguityViolation for {:?} though next_lsn >= oldest {}",
                            forged,
                            oldest
                        );
                    }
                    Err(WalError::InvalidPosition) => {
                        prop_assert!(
                            next >= oldest,
                            "below the floor must be ContiguityViolation (rule 1 first): {:?}",
                            forged
                        );
                        prop_assert!(!untouched, "a real position was refused: {:?}", forged);
                    }
                    Err(e) => {
                        return Err(TestCaseError::fail(format!(
                            "unexpected error for {forged:?}: {e:?}"
                        )));
                    }
                    Ok(r) => {
                        // Accepted ⇒ it must tell the truth: exactly the
                        // model's records from next_lsn, which is also what
                        // reader_from(next_lsn) yields.
                        let got = drain(r)?;
                        prop_assert_eq!(
                            &got,
                            &model.from(next),
                            "reader_at accepted {:?} (real {:?}) but lied about the stream",
                            forged,
                            (b, o, n)
                        );
                        let from = drain(wal.reader_from(Lsn(next)).unwrap())?;
                        prop_assert_eq!(&got, &from);
                    }
                }
            }
        }
    }
}
