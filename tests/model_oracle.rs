//! §14.3 — stateful model/oracle harness (M6).
//!
//! Generates randomized op-scripts and drives them through the shared, proptest-
//! free executor in `mod model`, which checks the durability envelope
//! (D1/D2/D3/D6/D7/D8) against an independent in-memory oracle after every
//! crash/recover. Shrinking is on, so a failure prints a minimal `Vec<Op>`
//! reproducer.
//!
//! **Case count** is `PROPTEST_CASES`-overridable (§14.11): the in-file default is
//! modest for per-PR runs; nightly drives a high count, e.g.
//! `PROPTEST_CASES=20000 cargo test --test model_oracle`.
//!
//! The op-script type and executor live in `tests/model/mod.rs` so the M9
//! cargo-fuzz F4 target (§14.5) can drive the identical executor with zero
//! duplication (it has no proptest dependency).

mod model;

use model::Op;
use open_wal::WalConfig;
use proptest::prelude::*;

/// Tiny segments (192 usable after the 64-byte header) with a 165-byte max record
/// (165 + 91 = 256) force frequent rolls, commit-time splits, and the empty-prefix
/// "next record does not fit" boundary — the §14.3 stress points.
const SEGMENT_SIZE: u64 = 256;
const MAX_RECORD_SIZE: usize = 165;

fn tiny() -> WalConfig {
    WalConfig {
        segment_size: SEGMENT_SIZE,
        max_record_size: MAX_RECORD_SIZE as u32,
    }
}

/// Payloads biased to boundaries: empty, 1 byte, 8 bytes (alignment), exactly
/// `max_record_size`, plus uniformly random sizes across the whole range.
fn payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        1 => Just(Vec::<u8>::new()),
        1 => Just(vec![0u8]),
        1 => prop::collection::vec(any::<u8>(), 8..=8),
        1 => prop::collection::vec(any::<u8>(), MAX_RECORD_SIZE..=MAX_RECORD_SIZE),
        4 => prop::collection::vec(any::<u8>(), 0..=MAX_RECORD_SIZE),
    ]
}

/// Op mix: append-heavy with frequent commits, occasional checkpoints (raw `up_to`
/// mixing small values and `u64::MAX`, which clamps to exactly `durable_lsn`), and
/// crash/reopen restarts interleaved throughout.
fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => payload_strategy().prop_map(Op::Append),
        4 => Just(Op::Commit),
        2 => prop_oneof![0u64..=20, Just(u64::MAX)].prop_map(Op::Checkpoint),
        2 => Just(Op::CrashAndRecover),
        1 => Just(Op::Reopen),
    ]
}

proptest! {
    // Modest per-PR default; PROPTEST_CASES overrides for the nightly high-
    // iteration run (§14.11).
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// The real WAL refines the oracle across an arbitrary op-script with many
    /// rolls, splits, checkpoints, and crash/reopen cycles.
    #[test]
    fn model_matches_oracle(ops in prop::collection::vec(op_strategy(), 0..60)) {
        model::run(tiny(), &ops);
    }
}
