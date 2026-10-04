//! RM2 gate — loom model-checking of the shipper ring (§15.7 of
//! `docs/replica_design_v1.md`).
//!
//! Run: `RUSTFLAGS="--cfg loom" cargo test -p open-wal-replica --test loom_ring --release`
//!
//! **This file drives the crate's production ring** (`open_wal_replica::ring`)
//! — the same `Producer`/`Consumer` code that ships, compiled against loom's
//! instrumented atomics, `UnsafeCell`, `Arc` and park/unpark via the
//! `cfg(loom)` swap in `src/sync.rs`. Nothing about the ring is re-implemented
//! here (§15.7.1).
//!
//! Model bounds (§15.7.3): 2–3 slots, 3–4 records, the producer on the model's
//! main thread plus 1 consumer thread (L1–L5) or 2 consumer threads (L6).
//! Payloads are `[lsn as u8; 4]`, so a torn or reused read is detectable by
//! value as well as by loom's race report.
//!
//! **Preemption bounds (documented per §15.7.3).** L1, L2, L3 and L5 run with
//! loom's default — no preemption bound, i.e. exhaustive. L4 and L6a are
//! exhaustive up to **3 preemptions** (`BOUNDED`) and L6b up to **2**
//! (`L6B_BOUND`): unbounded, L4 and L6a did not finish in 280 s+ and L6b at
//! bound 3 takes ~11 min (green, recorded in the RM2 status), and the models
//! are already at the spec's minimum size (2 slots / 3–4 records / 2
//! consumers), so they cannot shrink further. Bounded exploration is the
//! standard loom setting for models of this size, and every §15.7.5 mutation
//! assigned to these models was shown to fail *at these bounds*.
//! `LOOM_MAX_PREEMPTIONS` overrides the per-model setting (e.g. for a deeper
//! sweep off the per-PR path).
//!
//! # What loom does NOT prove (§15.7.6)
//! - It proves the **in-memory handoff** is race-free and ordering-correct
//!   within the bound. It does **not** prove the *durability* half of R1 — that
//!   `release` is only ever called with a `w` returned by `commit() → Ok(w)`.
//!   That is a logic invariant of the integrator's call order, covered by the
//!   RM7 LazyFS headline and its negative control (§15.5), not here.
//! - It does not cover the network, the wire codec, the WAL, or catch-up.
//! - It is exhaustive only for the modeled ring sizes and record counts; larger
//!   configurations are covered by the §15.4 oracle and §15.5 crash matrices.
//!
//! # Falsifiability (§15.7.5)
//! Each of the seven required mutations of `src/ring.rs` was applied, its model
//! run and shown to fail, then reverted — see the RM2 status in
//! `crates/open-wal-replica/CLAUDE.md` for the recorded results.
#![cfg(loom)]

use std::time::Duration;

use loom::thread;
use open_wal::Lsn;
use open_wal_replica::ring::{self, Consumer, ConsumerStatus, Producer, RingConfig};

/// Preemption bound for the models that are too large to explore unbounded.
const BOUNDED: Option<usize> = Some(3);

/// L6b (two consumers *and* slot reuse) is the largest model: ~1.45M
/// interleavings / ~11 min at bound 3 (run once, green — see the RM2 status),
/// so the per-PR gate runs it at bound 2.
const L6B_BOUND: Option<usize> = Some(2);

/// `loom::model` with an explicit preemption bound (`None` = unbounded), unless
/// `LOOM_MAX_PREEMPTIONS` is set in the environment.
fn model<F>(bound: Option<usize>, f: F)
where
    F: Fn() + Sync + Send + 'static,
{
    let mut b = loom::model::Builder::new();
    if b.preemption_bound.is_none() {
        b.preemption_bound = bound;
    }
    b.check(f);
}

fn pat(lsn: u64) -> [u8; 4] {
    [lsn as u8; 4]
}

/// One emitted record, plus the watermark of the drain pass that emitted it.
type Emitted = Vec<(u64, Vec<u8>, u64)>;

fn setup(slots: usize, consumers: usize) -> (Producer, Vec<Consumer>) {
    let (mut p, mut cs) = ring::ring(
        RingConfig {
            slots,
            slot_bytes: 4,
            consumers,
        },
        Lsn(1),
        Lsn(0),
    );
    for (id, c) in cs.iter_mut().enumerate() {
        p.attach(id, Lsn(1)).unwrap();
        assert_eq!(c.resume(), Some(Lsn(1)));
    }
    (p, cs)
}

/// How a model's consumer waits when nothing is shippable.
#[derive(Clone, Copy)]
enum Wait {
    /// Make a fixed number of concurrent drain passes (`yield_now` between
    /// them), then hand the consumer back to the model's main thread, which
    /// finishes the drain after `join` (see [`finish`]). No waker is
    /// registered. Used by every model except L5, so `released` (L1/L2) and
    /// the cursors / pins (L3/L4/L6) are the **only** synchronization under
    /// test:
    /// - *No waker*, because loom's `unpark` joins the unparker's causality
    ///   into the target thread immediately, even if it never parks — stronger
    ///   than `std`, where an unpark only orders memory for the `park` that
    ///   consumes its token. With a waker registered, `release()`'s unpark
    ///   hands loom a happens-before edge real hardware does not give a
    ///   consumer that is mid-drain, masking a broken `released` ordering (the
    ///   L1 Release→Relaxed mutations were not caught until these models
    ///   stopped registering wakers).
    /// - *Bounded passes, no spinning*, because a consumer spinning on
    ///   `released` lets loom feed it stale values indefinitely ("exceeded
    ///   maximum number of branches") — a model artifact, not a finding.
    Passes(usize),
    /// The production path: `Consumer::wait` (park until `release` unparks),
    /// until everything is emitted. L5 exercises it.
    Park,
}

type Outcome = (Emitted, ConsumerStatus, Consumer);

/// Drain once, stamping this pass's watermark on what it emitted (L2).
fn pass(c: &mut Consumer, out: &mut Emitted) -> ConsumerStatus {
    let d = c.drain(usize::MAX, |l, p| out.push((l.0, p.to_vec(), 0)));
    let n = out.len();
    for e in &mut out[n - d.emitted..] {
        e.2 = d.watermark.0;
    }
    d.status
}

/// Spawn a consumer that drains concurrently with the producer until it has
/// emitted `last`, fell behind, or (for `Passes`) ran out of passes.
fn spawn_consumer(mut c: Consumer, last: u64, wait: Wait) -> thread::JoinHandle<Outcome> {
    thread::spawn(move || {
        let mut out: Emitted = Vec::new();
        let mut passes = 0;
        loop {
            let status = pass(&mut c, &mut out);
            passes += 1;
            match status {
                ConsumerStatus::NeedsCatchUp { .. } => return (out, status, c),
                ConsumerStatus::Detached => unreachable!("attached by setup"),
                ConsumerStatus::Streaming => {}
            }
            if c.cursor().0 > last {
                return (out, status, c);
            }
            match wait {
                Wait::Passes(n) if passes >= n => return (out, status, c),
                Wait::Passes(_) => thread::yield_now(),
                Wait::Park => c.wait(Duration::from_secs(1)),
            }
        }
    })
}

/// After `join` (the producer is done): finish draining on the main thread.
/// The concurrent passes are where races and ordering bugs must show up; this
/// only completes the stream so the end state can be checked.
fn finish(h: thread::JoinHandle<Outcome>) -> (Emitted, ConsumerStatus) {
    let (mut out, mut status, mut c) = h.join().unwrap();
    if status == ConsumerStatus::Streaming {
        status = pass(&mut c, &mut out);
    }
    (out, status)
}

/// Every emitted record is genuine (payload matches the producer's log for
/// that LSN), never past its pass's watermark, and the LSNs are a dense prefix
/// starting at 1.
fn assert_consistent(out: &Emitted) {
    for (i, (lsn, payload, watermark)) in out.iter().enumerate() {
        assert_eq!(
            *lsn,
            i as u64 + 1,
            "emitted LSNs must be dense from 1: {out:?}"
        );
        assert_eq!(
            payload[..],
            pat(*lsn)[..],
            "torn/overwritten record emitted: {out:?}"
        );
        assert!(
            lsn <= watermark,
            "emitted {lsn} past its pass watermark {watermark}"
        );
    }
}

/// End state: either everything released was emitted, or the consumer was
/// (legitimately) evicted and emitted a dense prefix.
fn assert_end_state(out: &Emitted, status: ConsumerStatus, last: u64, overflow_possible: bool) {
    assert_consistent(out);
    match status {
        ConsumerStatus::Streaming => {
            assert_eq!(
                out.len() as u64,
                last,
                "a released record was never emitted: {out:?}"
            )
        }
        ConsumerStatus::NeedsCatchUp { next } => {
            assert!(overflow_possible, "NeedsCatchUp without overflow: {out:?}");
            assert_eq!(next.0, out.len() as u64 + 1);
        }
        ConsumerStatus::Detached => unreachable!(),
    }
}

/// L1 — visibility (the core R1 edge). The producer writes the slots, then
/// `release`s; a consumer that Acquire-observes `w` reads every record `≤ w`
/// fully written, with no `UnsafeCell` race. 3 slots / 3 records: no reuse.
#[test]
fn l1_visibility() {
    model(None, || {
        let (mut p, mut cs) = setup(3, 1);
        let h = spawn_consumer(cs.pop().unwrap(), 3, Wait::Passes(3));
        for l in 1..=3 {
            p.capture(Lsn(l), &pat(l));
        }
        p.release(Lsn(3));
        let (out, status) = finish(h);
        assert_end_state(&out, status, 3, false);
    });
}

/// L2 — never ahead. `released` advances (1 → 3) while the consumer may be
/// mid-drain; every record a pass emits is `≤` the watermark that pass loaded.
#[test]
fn l2_never_ahead_of_the_pass_watermark() {
    model(None, || {
        let (mut p, mut cs) = setup(3, 1);
        let h = spawn_consumer(cs.pop().unwrap(), 3, Wait::Passes(3));
        for l in 1..=3 {
            p.capture(Lsn(l), &pat(l));
        }
        p.release(Lsn(1));
        p.release(Lsn(3));
        let (out, status) = finish(h);
        assert_end_state(&out, status, 3, false);
    });
}

/// L3 — slot-reuse safety. 2 slots / 3 records: capturing 3 reuses the slot of
/// record 1, which the consumer may be reading at that moment. The producer
/// must Acquire the consumer's cursor before overwriting in place, or evict;
/// loom must find no race.
#[test]
fn l3_slot_reuse_only_after_min_cursor() {
    model(None, || {
        let (mut p, mut cs) = setup(2, 1);
        let h = spawn_consumer(cs.pop().unwrap(), 3, Wait::Passes(3));
        p.capture(Lsn(1), &pat(1));
        p.capture(Lsn(2), &pat(2));
        p.release(Lsn(2));
        p.capture(Lsn(3), &pat(3));
        p.release(Lsn(3));
        let (out, status) = finish(h);
        assert_end_state(&out, status, 3, true);
    });
}

/// L4 — eviction consistency. 2 slots / 4 records: the ring overflows; the
/// evicted slot's generation is bumped and the consumer whose cursor pointed at
/// it observes the mismatch and never emits the overwritten contents as the old
/// record (`assert_consistent` checks every emitted `(lsn, payload)`).
#[test]
fn l4_eviction_bumps_generation() {
    model(BOUNDED, || {
        let (mut p, mut cs) = setup(2, 1);
        let h = spawn_consumer(cs.pop().unwrap(), 4, Wait::Passes(3));
        p.capture(Lsn(1), &pat(1));
        p.capture(Lsn(2), &pat(2));
        p.release(Lsn(2));
        p.capture(Lsn(3), &pat(3));
        p.capture(Lsn(4), &pat(4));
        p.release(Lsn(4));
        let (out, status) = finish(h);
        assert_end_state(&out, status, 4, true);
        if let ConsumerStatus::NeedsCatchUp { .. } = status {
            assert!(p.needs_catch_up(0));
        }
    });
}

/// L5 — no lost wakeup. The consumer parks only when nothing is shippable; the
/// producer releases then unparks. Loom explores "consumer sees nothing →
/// producer releases + unparks → consumer parks". End-state assertion: every
/// released record was emitted. (A lost wakeup leaves the consumer parked with
/// nobody to wake it — loom reports that as a deadlock — and the end-state
/// assertion catches any consumer that gives up early.)
#[test]
fn l5_no_lost_wakeup() {
    model(None, || {
        let (mut p, mut cs) = setup(3, 1);
        let h = spawn_consumer(cs.pop().unwrap(), 3, Wait::Park);
        p.set_waker(0, h.thread().clone());
        p.capture(Lsn(1), &pat(1));
        p.release(Lsn(1));
        p.capture(Lsn(2), &pat(2));
        p.capture(Lsn(3), &pat(3));
        p.release(Lsn(3));
        let (out, status) = finish(h);
        assert_end_state(&out, status, 3, false);
    });
}

/// L6a — SPMC without reuse: two independent cursors both emit the full
/// released set, race-free.
#[test]
fn l6a_spmc_both_consumers_emit_everything() {
    model(BOUNDED, || {
        let (mut p, mut cs) = setup(3, 2);
        let h1 = spawn_consumer(cs.pop().unwrap(), 3, Wait::Passes(3));
        let h0 = spawn_consumer(cs.pop().unwrap(), 3, Wait::Passes(3));
        for l in 1..=3 {
            p.capture(Lsn(l), &pat(l));
        }
        p.release(Lsn(3));
        for h in [h0, h1] {
            let (out, status) = finish(h);
            assert_end_state(&out, status, 3, false);
        }
    });
}

/// L6b — SPMC with reuse: 2 slots / 3 records / 2 consumers. A slot is reused
/// in place only once **both** cursors passed it (the min); a slower consumer
/// mid-read is never overwritten. Each consumer emits the full set or a dense
/// prefix and `NeedsCatchUp`; loom must find no race.
#[test]
fn l6b_spmc_free_only_when_both_cursors_passed() {
    model(L6B_BOUND, || {
        let (mut p, mut cs) = setup(2, 2);
        let h1 = spawn_consumer(cs.pop().unwrap(), 3, Wait::Passes(3));
        let h0 = spawn_consumer(cs.pop().unwrap(), 3, Wait::Passes(3));
        p.capture(Lsn(1), &pat(1));
        p.capture(Lsn(2), &pat(2));
        p.release(Lsn(2));
        p.capture(Lsn(3), &pat(3));
        p.release(Lsn(3));
        for h in [h0, h1] {
            let (out, status) = finish(h);
            assert_end_state(&out, status, 3, true);
        }
    });
}
