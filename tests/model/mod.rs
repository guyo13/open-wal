//! M6 — stateful model/oracle harness executor (§14.3).
//!
//! A single executor, [`run`], drives a randomized op-script against the real
//! [`Wal`] **and** an independent in-memory [`Oracle`], asserting the durability
//! envelope (D1/D2/D3/D6/D7/D8) after every crash/recover. Included via
//! `mod model;` by `tests/model_oracle.rs` (the proptest driver).
//!
//! **Crash model (§14.0):** this is the *state machine* model, never power loss.
//! A "crash" = abandon the handle (drop, no further `commit`) and reopen. Because
//! `append` is pure memory and only `commit` writes+`fdatasync`s, dropping loses
//! exactly the uncommitted staging buffer, so reopen must recover exactly the
//! committed suffix (D3). This harness does **not** simulate torn writes or power
//! loss — that is the LazyFS suite (§14.4b/c) and the deferred F4 fuzz (§14.5).
//!
//! **Independence:** the oracle is a pure model. It never inspects `Wal`
//! internals and never re-derives an answer from the implementation; its
//! committed map is the source of truth, checked *before* any resync. The only
//! values it adopts from the implementation are tail watermarks the contract
//! explicitly leaves to the implementation (never-acknowledged records past the
//! committed watermark) — and only after the core checks pass.
//!
//! **F4 reuse:** this module has **no proptest dependency** and uses plain
//! `assert!`/`panic!` (proptest shrinks on panic). The M9 cargo-fuzz F4 target
//! (§14.5) drives this *identical* [`run`] from fuzzer-decoded op-scripts with
//! zero duplication; a future LazyFS-backed crash variant can reuse it too (hence
//! the envelope is a refinement relation, not exact equality).

// Each test binary that includes this module uses all of it, but the fuzz target
// (M9) will not — keep it quiet for either consumer.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;

use open_wal::{Lsn, RecoveryReport, Wal, WalConfig};

/// One step of a randomized program. `Debug`/`Clone` so proptest prints a
/// minimal, re-runnable shrunk reproducer on failure.
#[derive(Debug, Clone)]
pub enum Op {
    /// Buffer a record (pure memory; durable only after a covering `Commit`).
    Append(Vec<u8>),
    /// Make all buffered records durable.
    Commit,
    /// `checkpoint(up_to)`; the executor clamps the raw value to `≤ durable_lsn`
    /// (the only constraint the test must respect — §9). `u64::MAX` therefore
    /// means "checkpoint right up to `durable_lsn`".
    Checkpoint(u64),
    /// Abrupt restart: drop the handle **without** committing (lose the staged
    /// tail), then reopen. Exercises D3.
    CrashAndRecover,
    /// Clean restart: commit the staged tail, drop, then reopen. Exercises D7
    /// idempotence (nothing should change across a no-mutation reopen).
    Reopen,
}

/// Pure in-memory model of the durable contract — independent of the WAL.
struct Oracle {
    /// The committed (durable) set, `lsn → payload`. Pruned below `oldest_lsn` to
    /// bound memory on long runs.
    committed: BTreeMap<u64, Vec<u8>>,
    /// Appended-but-uncommitted records, `(lsn, payload)`, in order.
    staged: Vec<(u64, Vec<u8>)>,
    /// `P`: oldest LSN still expected to be available (1 until the first
    /// checkpoint reclaims a prefix). Monotonic non-decreasing.
    oldest_lsn: u64,
    /// Committed watermark `k`: the highest committed LSN (0 = nothing committed).
    durable_lsn: u64,
    /// The LSN the next `append` will receive.
    next_lsn: u64,
    /// Highest `up_to` ever passed to `checkpoint` — bounds how far a prefix may
    /// legitimately have been reclaimed (D8).
    max_ckpt_up_to: u64,
}

impl Oracle {
    fn new() -> Oracle {
        Oracle {
            committed: BTreeMap::new(),
            staged: Vec::new(),
            oldest_lsn: 1,
            durable_lsn: 0,
            next_lsn: 1,
            max_ckpt_up_to: 0,
        }
    }

    /// Move the staged tail into the committed set, returning the new watermark.
    /// An empty staging buffer is a no-op that returns the prior watermark.
    fn commit_staged(&mut self) -> u64 {
        if let Some(&(last, _)) = self.staged.last() {
            for (lsn, payload) in self.staged.drain(..) {
                self.committed.insert(lsn, payload);
            }
            self.durable_lsn = last;
        }
        self.durable_lsn
    }
}

/// Run `ops` against a fresh WAL under `cfg`, asserting the §14.3 envelope after
/// every recovery (and a terminal recovery at the end). Panics on any violation;
/// the panic carries the failing invariant.
pub fn run(cfg: WalConfig, ops: &[Op]) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path();

    let mut oracle = Oracle::new();
    let (wal0, initial) = Wal::open(path, cfg).expect("initial open");
    let mut wal = Some(wal_check_initial(wal0, initial, &oracle));
    let mut prev_report = Some(initial);
    // "Has any Append/Commit/Checkpoint mutated state since the last open?" —
    // drives the D7 idempotence check (a no-mutation reopen must be a no-op).
    let mut dirty = false;

    for op in ops {
        match op {
            Op::Append(payload) => {
                let w = wal.as_mut().expect("live handle");
                let lsn = w.append(payload).expect("append within max_record_size");
                assert_eq!(
                    lsn.0, oracle.next_lsn,
                    "append assigned LSN {} but oracle expected {}",
                    lsn.0, oracle.next_lsn
                );
                oracle.staged.push((oracle.next_lsn, payload.clone()));
                oracle.next_lsn += 1;
                dirty = true;
            }
            Op::Commit => {
                let w = wal.as_mut().expect("live handle");
                let returned = w.commit().expect("commit").0;
                let staged_was_empty = oracle.staged.is_empty();
                let watermark = oracle.commit_staged();
                assert_eq!(
                    returned, watermark,
                    "commit returned watermark {returned} but oracle expected {watermark}"
                );
                if !staged_was_empty {
                    dirty = true;
                }
            }
            Op::Checkpoint(raw) => {
                let w = wal.as_mut().expect("live handle");
                // §9: up_to ≤ durable_lsn is the only constraint the test must
                // honor. `u64::MAX` clamps to exactly durable_lsn.
                let up_to = (*raw).min(w.durable_lsn().0);
                w.checkpoint(Lsn(up_to)).expect("checkpoint");
                oracle.max_ckpt_up_to = oracle.max_ckpt_up_to.max(up_to);
                live_probe(w, &oracle);
                dirty = true;
            }
            Op::CrashAndRecover => {
                // Abrupt: drop without committing ⇒ the staged tail is lost (D3).
                // The drop releases the flock so the reopen below can acquire it.
                drop(wal.take());
                oracle.staged.clear();
                oracle.next_lsn = oracle.durable_lsn + 1;
                let (w, r) = recover_and_check(path, cfg, &mut oracle, prev_report, dirty);
                wal = Some(w);
                prev_report = Some(r);
                dirty = false;
            }
            Op::Reopen => {
                // Clean: commit the staged tail first, then drop + reopen.
                {
                    let w = wal.as_mut().expect("live handle");
                    let returned = w.commit().expect("commit on reopen").0;
                    let staged_was_empty = oracle.staged.is_empty();
                    let watermark = oracle.commit_staged();
                    assert_eq!(returned, watermark, "reopen-commit watermark mismatch");
                    if !staged_was_empty {
                        dirty = true;
                    }
                }
                // Drop (releasing the flock) before reopening below.
                drop(wal.take());
                let (w, r) = recover_and_check(path, cfg, &mut oracle, prev_report, dirty);
                wal = Some(w);
                prev_report = Some(r);
                dirty = false;
            }
        }
    }

    // Terminal reopen (refinement #2): an op-script ending on a Checkpoint with
    // no following reopen would otherwise let a checkpoint over-deletion slip past
    // the live probe (which lacks an authoritative oldest_lsn). A final recovery
    // with the authoritative RecoveryReport.oldest_lsn closes that hole.
    drop(wal.take());
    oracle.staged.clear();
    oracle.next_lsn = oracle.durable_lsn + 1;
    recover_and_check(path, cfg, &mut oracle, prev_report, dirty);
}

/// Sanity-check the very first (cold-start) open and return the handle. A fresh
/// directory recovers to the empty log: `oldest_lsn == 1`, `durable_lsn == 0`.
fn wal_check_initial(wal: Wal, report: RecoveryReport, _oracle: &Oracle) -> Wal {
    assert_eq!(report.oldest_lsn, Lsn(1), "cold start oldest_lsn must be 1");
    assert_eq!(
        report.durable_lsn,
        Lsn(0),
        "cold start durable_lsn must be 0"
    );
    wal
}

/// Drop-then-reopen has already happened by the time this is called (the handle
/// is gone). Reopen, assert the full envelope against the oracle, resync the
/// oracle for continuation, and return the new handle + report.
fn recover_and_check(
    path: &Path,
    cfg: WalConfig,
    oracle: &mut Oracle,
    prev_report: Option<RecoveryReport>,
    dirty: bool,
) -> (Wal, RecoveryReport) {
    let (wal, report) = Wal::open(path, cfg).expect("reopen");

    // ---- Monotonicity + D7 idempotence (across reopens) ----
    if let Some(prev) = prev_report {
        assert!(
            report.durable_lsn.0 >= prev.durable_lsn.0,
            "durable_lsn regressed across reopen: {} < {}",
            report.durable_lsn.0,
            prev.durable_lsn.0
        );
        assert!(
            report.oldest_lsn.0 >= prev.oldest_lsn.0,
            "oldest_lsn regressed across reopen: {} < {}",
            report.oldest_lsn.0,
            prev.oldest_lsn.0
        );
        if !dirty {
            // D7: nothing mutated since the previous open ⇒ recovery is a no-op.
            assert_eq!(
                report.oldest_lsn, prev.oldest_lsn,
                "D7: oldest_lsn changed with no intervening mutation"
            );
            assert_eq!(
                report.durable_lsn, prev.durable_lsn,
                "D7: durable_lsn changed with no intervening mutation"
            );
        }
    }

    // ---- D8 oldest bounds: never reclaim past an authorized checkpoint ----
    assert!(report.oldest_lsn.0 >= 1, "oldest_lsn must be ≥ 1");
    assert!(
        report.oldest_lsn.0 <= oracle.max_ckpt_up_to + 1,
        "D8: oldest_lsn {} exceeds max_ckpt_up_to+1 {} (reclaimed an unauthorized record)",
        report.oldest_lsn.0,
        oracle.max_ckpt_up_to + 1
    );

    // ---- D1/D3 watermark superset ----
    assert!(
        report.durable_lsn.0 >= oracle.durable_lsn,
        "D1/D3: recovered durable_lsn {} < oracle committed watermark {}",
        report.durable_lsn.0,
        oracle.durable_lsn
    );
    // After recovery (no staged records) the highest assigned LSN equals the
    // durable watermark.
    assert_eq!(
        wal.last_lsn().0,
        report.durable_lsn.0,
        "post-recovery last_lsn must equal durable_lsn"
    );

    // ---- Replay the surviving suffix ----
    let replay = replay_all(&wal);

    // ---- D2 density: a contiguous run oldest_lsn..=durable_lsn ----
    if replay.is_empty() {
        assert_eq!(
            report.durable_lsn.0 + 1,
            report.oldest_lsn.0,
            "empty suffix ⇒ durable_lsn must be oldest_lsn-1"
        );
    } else {
        assert_eq!(
            replay[0].0, report.oldest_lsn.0,
            "D2: replay must start at oldest_lsn"
        );
        for pair in replay.windows(2) {
            assert_eq!(
                pair[1].0,
                pair[0].0 + 1,
                "D2: replay must be dense (hole between {} and {})",
                pair[0].0,
                pair[1].0
            );
        }
        assert_eq!(
            replay.last().unwrap().0,
            report.durable_lsn.0,
            "D2: replay must reach durable_lsn"
        );
    }

    let committed_watermark = oracle.durable_lsn;

    // ---- D1/D3/D8: every committed record is retained or authorized-reclaimed ----
    for (&lsn, payload) in &oracle.committed {
        if lsn >= report.oldest_lsn.0 {
            let got = lookup(&replay, lsn).unwrap_or_else(|| {
                panic!("D1/D3: committed record {lsn} (≥ oldest) missing from recovery")
            });
            assert_eq!(
                got,
                &payload[..],
                "D6: committed record {lsn} not byte-identical"
            );
        } else {
            // Reclaimed prefix — legal only if a checkpoint authorized it.
            assert!(
                lsn <= oracle.max_ckpt_up_to,
                "D1/D3/D8: committed record {lsn} lost (< oldest {} but > max_ckpt_up_to {})",
                report.oldest_lsn.0,
                oracle.max_ckpt_up_to
            );
        }
    }

    // ---- D6/D10: no resurrection; replayed records ≤ watermark match the oracle ----
    // (Records past the watermark are density-preserving extras — global density
    //  was already asserted, so a non-dense extra would have failed above.)
    for (lsn, payload) in &replay {
        if *lsn <= committed_watermark {
            let want = oracle.committed.get(lsn).unwrap_or_else(|| {
                panic!("D6/D10: replayed record {lsn} ≤ watermark not in committed oracle (resurrection?)")
            });
            assert_eq!(payload, want, "D6: replayed record {lsn} mismatches oracle");
        }
    }

    // ---- §15.4: a reader below the oldest available LSN is a fatal gap ----
    if report.oldest_lsn.0 > 1 {
        assert!(
            wal.reader_from(Lsn(report.oldest_lsn.0 - 1)).is_err(),
            "§15.4: reader_from below oldest_lsn must be a fatal gap, not a silent skip"
        );
    }

    // ---- Resync the oracle for continuation ----
    // Adopt density-preserving tail records beyond the committed watermark (none
    // in the state-machine model; defensive for a future power-loss variant).
    for (lsn, payload) in &replay {
        if *lsn > committed_watermark {
            oracle.committed.insert(*lsn, payload.clone());
        }
    }
    oracle.durable_lsn = report.durable_lsn.0;
    oracle.oldest_lsn = report.oldest_lsn.0;
    oracle.next_lsn = report.durable_lsn.0 + 1;
    oracle.staged.clear();
    oracle
        .committed
        .retain(|&lsn, _| lsn >= report.oldest_lsn.0);

    (wal, report)
}

/// A **live** post-checkpoint probe (no reopen). It cannot test the §15.4 gap
/// boundary — there is no authoritative live `oldest_lsn` and computing one would
/// require modeling segment-packing math, breaking the oracle's independence — so
/// it asserts only what `reader_from(0)` can establish without knowing the live
/// oldest: `durable_lsn` is unchanged, the visible suffix is dense and reaches the
/// committed watermark (the active segment is never deleted), and visible records
/// are byte-identical. The precise D8/§15.4 checks are anchored at the next
/// recovery in [`recover_and_check`].
fn live_probe(wal: &Wal, oracle: &Oracle) {
    assert_eq!(
        wal.durable_lsn().0,
        oracle.durable_lsn,
        "checkpoint must not change durable_lsn"
    );
    let replay = replay_all(wal);

    if oracle.durable_lsn == 0 {
        assert!(replay.is_empty(), "nothing committed ⇒ empty live replay");
        return;
    }

    for pair in replay.windows(2) {
        assert_eq!(
            pair[1].0,
            pair[0].0 + 1,
            "live: visible suffix must be dense (hole between {} and {})",
            pair[0].0,
            pair[1].0
        );
    }
    assert_eq!(
        replay
            .last()
            .expect("durable ≥ 1 ⇒ non-empty live replay")
            .0,
        oracle.durable_lsn,
        "live: visible suffix must reach durable_lsn (active segment never deleted)"
    );
    for (lsn, payload) in &replay {
        if *lsn <= oracle.durable_lsn {
            let want = oracle
                .committed
                .get(lsn)
                .unwrap_or_else(|| panic!("live: visible record {lsn} not in committed oracle"));
            assert_eq!(payload, want, "live: record {lsn} not byte-identical");
        }
    }
}

/// Replay the whole surviving log from the oldest available LSN into owned
/// `(lsn, payload)` pairs.
fn replay_all(wal: &Wal) -> Vec<(u64, Vec<u8>)> {
    let mut r = wal.reader_from(Lsn(0)).expect("reader_from(0)");
    let mut out = Vec::new();
    while let Some(item) = r.next() {
        let (lsn, payload) = item.expect("replay item");
        out.push((lsn.0, payload.to_vec()));
    }
    out
}

/// Find the payload for `lsn` in a replay (small, dense; linear scan is fine).
fn lookup(replay: &[(u64, Vec<u8>)], lsn: u64) -> Option<&[u8]> {
    replay
        .iter()
        .find(|(l, _)| *l == lsn)
        .map(|(_, p)| p.as_slice())
}
