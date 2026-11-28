//! RM2 — §15.2 P4 / §15.3 ring overflow at the ring level, with real threads.
//!
//! A tiny ring (2–6 slots) and a deliberately slow consumer make the producer
//! evict repeatedly, marking the consumer `NeedsCatchUp` again and again. Each
//! time, the consumer is "caught up" from an oracle copy of the log (standing in
//! for RM3's `reader_from` catch-up, which needs WAL v7) up to the point where
//! the writer thread re-attaches it, then resumes from the ring. The emitted
//! stream must be exactly `1..=N` — no record lost, none duplicated at any
//! ring/catch-up cutover — and byte-identical. The producer never waits for the
//! consumer (P4).
//!
//! `tiny_ring_marks_needs_catch_up_repeatedly` additionally passes **gates** to
//! [`run`]: LSNs at which the test itself sequences the two threads so that an
//! eviction is forced on any schedule (an unpaced producer can finish all its
//! captures before a late-scheduled consumer drains once, which gives exactly
//! one eviction). The eviction is still decided and reported by the ring.
#![cfg(not(loom))]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use open_wal::Lsn;
use open_wal_replica::ring::{self, AttachError, ConsumerStatus, RingConfig};
use proptest::prelude::*;

fn pat(lsn: u64, len: usize) -> Vec<u8> {
    (0..len).map(|i| (lsn as u8) ^ (i as u8)).collect()
}

/// How long a gate may wait before it fails the test loudly.
const GATE_TIMEOUT: Duration = Duration::from_secs(30);

/// `gates` (empty: no pacing, the producer never waits) are LSNs at which an
/// eviction is forced, using only the test's own counters:
/// 1. before capturing gate `g`, the producer waits (still serving re-attach
///    requests) until the consumer has emitted `1..g` and is back in the ring;
/// 2. the consumer, on emitting `g` from the ring, holds until the producer has
///    captured more than a ring's worth past `g`.
///
/// Either the ring evicted `g` before the consumer drained it, or the
/// consumer's cursor `g + 1` is evicted while it holds: one `NeedsCatchUp` per
/// gate, whatever the schedule. Needs `commit_every == 1` and gates more than
/// `slots + 2` apart and below `n − slots − 2`.
fn run(
    n: u64,
    slots: usize,
    commit_every: u64,
    consumer_delay_every: u64,
    gates: &[u64],
) -> (Vec<u64>, u64) {
    let gates: Arc<[u64]> = gates.into();
    let past_gate = slots as u64 + 2;
    // Test-side sequencing only: what the producer captured, and how much the
    // consumer has emitted while attached to the ring.
    let captured = Arc::new(AtomicU64::new(0));
    let progress = Arc::new(AtomicU64::new(0));
    let (c_gates, c_captured, c_progress) = (
        Arc::clone(&gates),
        Arc::clone(&captured),
        Arc::clone(&progress),
    );
    let (mut p, mut cs) = ring::ring(
        RingConfig {
            slots,
            slot_bytes: 8,
            consumers: 1,
        },
        Lsn(1),
        Lsn(0),
    );
    let mut c = cs.pop().unwrap();
    p.attach(0, Lsn(1)).unwrap();
    c.resume();
    // Consumer → writer: "I fell behind at `next`, re-attach me".
    let (req_tx, req_rx) = mpsc::channel::<u64>();
    // Writer → consumer: "re-attached at `at`" (records `next..at` come from
    // the log, i.e. the oracle).
    let (ack_tx, ack_rx) = mpsc::channel::<u64>();

    let consumer = std::thread::spawn(move || {
        let mut emitted: Vec<u64> = Vec::new();
        let mut catch_ups = 0;
        while emitted.len() < n as usize {
            let d = c.drain(usize::MAX, |l, payload| {
                assert_eq!(payload, &pat(l.0, (l.0 % 9) as usize)[..], "byte-identical");
                emitted.push(l.0);
                c_progress.store(emitted.len() as u64, Ordering::Release);
                if c_gates.contains(&l.0) {
                    let deadline = Instant::now() + GATE_TIMEOUT;
                    while c_captured.load(Ordering::Acquire) < l.0 + past_gate {
                        assert!(
                            Instant::now() < deadline,
                            "gate {}: the producer never got {past_gate} records ahead",
                            l.0
                        );
                        std::thread::yield_now();
                    }
                }
                if consumer_delay_every > 0 && l.0 % consumer_delay_every == 0 {
                    std::thread::sleep(Duration::from_micros(50));
                }
            });
            match d.status {
                ConsumerStatus::Streaming => {
                    if d.emitted == 0 {
                        c.wait(Duration::from_millis(5));
                    }
                }
                ConsumerStatus::NeedsCatchUp { next } => {
                    catch_ups += 1;
                    assert_eq!(next.0, emitted.len() as u64 + 1, "dense up to the eviction");
                    req_tx.send(next.0).unwrap();
                    let at = ack_rx.recv().unwrap();
                    // Simulated log catch-up: next..at from the source of truth.
                    for l in next.0..at {
                        emitted.push(l);
                    }
                    assert_eq!(c.resume(), Some(Lsn(at)), "joins the ring at the cutover");
                    c_progress.store(emitted.len() as u64, Ordering::Release);
                }
                ConsumerStatus::Detached => unreachable!(),
            }
        }
        (emitted, catch_ups)
    });
    p.set_waker(0, consumer.thread().clone());

    let mut pending: Option<u64> = None;
    let serve = |p: &mut ring::Producer, pending: &mut Option<u64>| {
        if pending.is_none() {
            *pending = req_rx.try_recv().ok();
        }
        if let Some(next) = *pending {
            // Writer-thread catch-up decision (§9): serve next..at from the log,
            // join the ring at `at` — never past the released watermark.
            let at = next.max(p.oldest_valid().0);
            match p.attach(0, Lsn(at)) {
                Ok(()) => {
                    *pending = None;
                    ack_tx.send(at).unwrap();
                    p.wake(0);
                }
                Err(AttachError::Ahead { .. }) => {} // retry after more releases
                Err(e) => panic!("attach({at}): {e:?}"),
            }
        }
    };
    for l in 1..=n {
        if gates.contains(&l) {
            // The consumer must be in the ring at cursor `l` before `l` exists.
            let deadline = Instant::now() + GATE_TIMEOUT;
            while progress.load(Ordering::Acquire) < l - 1 {
                serve(&mut p, &mut pending);
                assert!(
                    Instant::now() < deadline,
                    "gate {l}: the consumer never caught up to {} (at {})",
                    l - 1,
                    progress.load(Ordering::Acquire)
                );
                std::thread::yield_now();
            }
        }
        p.capture(Lsn(l), &pat(l, (l % 9) as usize));
        captured.store(l, Ordering::Release);
        if l % commit_every == 0 || l == n {
            p.release(Lsn(l));
        }
        serve(&mut p, &mut pending);
    }
    while !consumer.is_finished() {
        serve(&mut p, &mut pending);
        std::thread::yield_now();
    }
    let (emitted, catch_ups) = consumer.join().unwrap();
    (emitted, catch_ups)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]

    /// §15.2 P4 — tiny ring ⇒ repeated `NeedsCatchUp`; the combined
    /// ring + catch-up stream is exactly 1..=N, no duplicate at any cutover.
    #[test]
    fn p4_overflow_catchup_cutover_has_no_gap_or_duplicate(
        n in 200u64..3000,
        slots in 2usize..7,
        commit_every in 1u64..8,
        delay in prop_oneof![Just(0u64), 1u64..40],
    ) {
        let (emitted, _catch_ups) = run(n, slots, commit_every, delay, &[]);
        let want: Vec<u64> = (1..=n).collect();
        prop_assert_eq!(emitted, want);
    }
}

/// A slow consumer on a 2-slot ring is evicted many times over — at least
/// once per gate on any schedule (see [`run`]).
#[test]
fn tiny_ring_marks_needs_catch_up_repeatedly() {
    let (emitted, catch_ups) = run(5000, 2, 1, 3, &[1000, 2500, 4000]);
    assert_eq!(emitted, (1..=5000).collect::<Vec<_>>());
    eprintln!("2-slot ring, slow consumer: {catch_ups} NeedsCatchUp events over 5000 records");
    assert!(
        catch_ups >= 2,
        "expected repeated NeedsCatchUp, got {catch_ups}"
    );
}
