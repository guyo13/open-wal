//! F4 — operation-script fuzz (§14.5), driving the M6 stateful model/oracle.
//!
//! Decodes fuzzer bytes into a `WalConfig` + a randomized op-script and runs it
//! through the **same** proptest-free executor the M6 harness uses
//! (`tests/model/mod.rs::run`), included verbatim via `#[path]` — zero
//! duplication. `run` drives the real `Wal` against an independent in-memory
//! oracle and panics on any breach of the durability envelope
//! (D1/D2/D3/D6/D7/D8): recovered ⊇ committed, dense `oldest..=durable`,
//! byte-identical replay, no unauthorized reclamation, monotonic watermarks, D7
//! idempotence, the §15.4 below-oldest fatal gap.
//!
//! **Crash model (flag #2): the oracle's `CrashAndRecover` models a PROCESS
//! CRASH** — the page cache survives; it drops the handle *without* committing
//! and reopens, so exactly the un-committed staging buffer is lost (D3). It is
//! **NOT** power loss (un-synced data lost) and **NOT** a torn write — those are
//! the LazyFS suite (§14.4b/c) and the H1 power-pull. So a green F4 proves the
//! recovery **state machine** under crash, not durability under power loss; do
//! not over-read it.
//!
//! No change to `tests/model/` — the executor is reused exactly as the M6
//! proptest driver uses it.

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use open_wal::WalConfig;

#[path = "../../tests/model/mod.rs"]
mod model;

use model::Op;

/// Small, boundary-biased segment sizes that force rolls and commit-time splits
/// (the interesting recovery/checkpoint states), matching the M6 generator's tiny
/// configs. Each leaves ≥ 165 bytes of `max_record_size` headroom (`seg − 91`).
const SEGS: [u64; 4] = [256, 384, 512, 4096];

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(cfg) = arbitrary_config(&mut u) else {
        return;
    };
    let Ok(ops) = arbitrary_ops(&mut u, cfg.max_record_size) else {
        return;
    };
    // Drives the real Wal against the independent oracle; panics on any envelope
    // violation. Process-crash model only (see the module header).
    model::run(cfg, &ops);
});

/// A valid `WalConfig`: a small segment from [`SEGS`] and a `max_record_size`
/// reduced into `[0, segment_size − 91]` so §5.3 always holds (no `InvalidConfig`,
/// which would make the model's `expect("initial open")` a false positive).
fn arbitrary_config(u: &mut Unstructured) -> arbitrary::Result<WalConfig> {
    let seg = SEGS[usize::from(u8::arbitrary(u)?) % SEGS.len()];
    let max_hdr = (seg - 91) as u32;
    let max_record_size = u32::arbitrary(u)? % (max_hdr + 1);
    Ok(WalConfig {
        segment_size: seg,
        max_record_size,
    })
}

/// A bounded, weighted op-script. The weights mirror the M6 generator
/// (Append-heavy, with periodic Commit/Checkpoint and rarer crash/reopen). The
/// op count is bounded only to cap wall-clock per run (each `Commit` does a real
/// `fdatasync`); the executor itself handles any length.
fn arbitrary_ops(u: &mut Unstructured, max: u32) -> arbitrary::Result<Vec<Op>> {
    let n = u.int_in_range(0..=80u32)?;
    let mut ops = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let op = match u8::arbitrary(u)? % 16 {
            0..=7 => Op::Append(arbitrary_payload(u, max)?), // weight 8
            8..=11 => Op::Commit,                            // weight 4
            12..=13 => Op::Checkpoint(u64::arbitrary(u)?),   // weight 2 (run clamps ≤ durable)
            14 => Op::CrashAndRecover,                       // weight 1
            _ => Op::Reopen,                                 // weight 1
        };
        ops.push(op);
    }
    Ok(ops)
}

/// A payload whose length is **clamped to `max_record_size`** (so the model's
/// `append(..).expect("within max_record_size")` never spuriously fails) and
/// boundary-biased to 0 / 1 / 8 / max / random.
fn arbitrary_payload(u: &mut Unstructured, max: u32) -> arbitrary::Result<Vec<u8>> {
    let max = max as usize;
    let len = match u8::arbitrary(u)? % 8 {
        0 => 0,
        1 => 1.min(max),
        2 => 8.min(max),
        3 => max,
        _ if max == 0 => 0,
        _ => usize::from(u16::arbitrary(u)?) % (max + 1),
    };
    let mut v = vec![0u8; len];
    u.fill_buffer(&mut v)?;
    Ok(v)
}
