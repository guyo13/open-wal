//! RM1 — `Receiver` tests (§15.1 + §15.2 P1–P2 of `docs/replica_design_v1.md`).
//!
//! The receiver is driven by an in-process **fake primary** over a real
//! loopback socket (`common::FakePrimary`): it reads `HELLO`, sends `SERVING`
//! and `RECORD` frames — including deliberately broken streams — and observes
//! the replica's `ACK`/`ERR` frames.
//!
//! - **P1 mirroring (R2/R9):** an arbitrary primary op-script (append/commit,
//!   tiny segments ⇒ rolls) shipped over the wire yields a replica log that is
//!   byte-identical with identical LSNs.
//! - **P2 contiguity (R3):** gaps, duplicates, reorders, foreign LSNs and CRC
//!   corruption are **never appended**; the replica answers `ERR(Contiguity)` /
//!   `ERR(WireCrc)`, and after reconnecting from its `durable_lsn` it converges
//!   to the primary's log.
//! - **R4 ack honesty:** every `ACK` is a value `commit` returned, monotonic,
//!   never ahead of the replica's durable watermark, and survives a reopen.

mod common;

use std::time::Duration;

use common::{
    DurableTap, FakePrimary, Got, SMALL, payload, read_all, read_dir, spawn_receiver,
    spawn_receiver_tapped,
};
use open_wal::{Lsn, Wal};
use open_wal_replica::wire::{self, ErrCode, Frame};
use open_wal_replica::{ReceiverConfig, ReplError};
use proptest::prelude::*;

fn rx_cfg(batch_records: usize) -> ReceiverConfig {
    let mut c = ReceiverConfig::new(SMALL);
    c.batch_records = batch_records;
    c.batch_interval = Duration::from_millis(5);
    c.replica_id = 7;
    c
}

/// Primary op-script step.
#[derive(Clone, Debug)]
enum Op {
    Append(Vec<u8>),
    Commit,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => prop_oneof![
            Just(0usize),
            Just(1),
            Just(8),
            Just(SMALL.max_record_size as usize),
            0usize..=SMALL.max_record_size as usize,
        ]
        .prop_flat_map(|n| proptest::collection::vec(any::<u8>(), n))
        .prop_map(Op::Append),
        1 => Just(Op::Commit),
    ]
}

/// Run `ops` against a real primary WAL (tiny segments ⇒ rolls and commit-time
/// splits) and return its committed log as read back from disk.
fn primary_log(ops: &[Op]) -> Vec<(Lsn, Vec<u8>)> {
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(dir.path(), SMALL).unwrap();
    for op in ops {
        match op {
            Op::Append(p) => {
                wal.append(p).unwrap();
            }
            Op::Commit => {
                wal.commit().unwrap();
            }
        }
    }
    wal.commit().unwrap();
    read_all(&wal)
}

/// Ship `log` to a fresh replica in `chunk`-record writes and wait for the
/// final ACK. Returns the replica's ACK sequence.
fn ship_all(fp: &mut FakePrimary, log: &[(Lsn, Vec<u8>)], chunk: usize) -> Vec<u64> {
    ship_all_checked(fp, log, chunk, None)
}

/// [`ship_all`], optionally checking every ACK against the independent R4
/// oracle (the replica WAL's own `DurabilityObserver` tap).
fn ship_all_checked(
    fp: &mut FakePrimary,
    log: &[(Lsn, Vec<u8>)],
    chunk: usize,
    tap: Option<&DurableTap>,
) -> Vec<u64> {
    let d = fp.hello();
    fp.serving(d + 1);
    for c in log[d as usize..].chunks(chunk.max(1)) {
        fp.records(c);
    }
    match (log.last(), tap) {
        (Some((last, _)), Some(tap)) => fp.await_ack_tapped(last.0, tap),
        (Some((last, _)), None) => fp.await_ack(last.0),
        (None, _) => Vec::new(),
    }
}

fn assert_honest_acks(acks: &[u64], final_durable: u64) {
    assert!(
        acks.windows(2).all(|w| w[0] < w[1]),
        "ACKs must be strictly increasing (each one follows a commit that advanced durable_lsn): {acks:?}"
    );
    assert!(
        acks.iter().all(|&a| a <= final_durable),
        "ACK ahead of durable_lsn {final_durable}: {acks:?}"
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// §15.2 P1 — mirroring (R2) and byte fidelity (R9). Every ACK is also
    /// checked against the replica WAL's own durability tap at receipt (R4,
    /// independent of anything the receiver reports about itself).
    #[test]
    fn p1_replica_is_byte_identical_with_mirrored_lsns(
        ops in proptest::collection::vec(op_strategy(), 0..60),
        batch in 1usize..12,
        chunk in 1usize..20,
    ) {
        let log = primary_log(&ops);
        let rdir = tempfile::tempdir().unwrap();
        let (addr, h, tap) = spawn_receiver_tapped(rdir.path(), rx_cfg(batch), 1);
        let mut fp = FakePrimary::connect(addr);
        let acks = ship_all_checked(&mut fp, &log, chunk, Some(&tap));
        let rest = fp.finish();
        let (rx, results) = h.join().unwrap();
        prop_assert!(results[0].is_ok(), "{:?}", results[0]);
        prop_assert!(!rest.iter().any(|g| matches!(g, Got::Err(_))), "{rest:?}");

        let n = log.len() as u64;
        prop_assert_eq!(rx.durable_lsn(), Lsn(n));
        assert_honest_acks(&acks, n);
        prop_assert_eq!(read_all(rx.wal()), log.clone());
        // R4: an ACK is a durability claim — it survives a reopen (recovery).
        drop(rx);
        let (durable, reopened) = read_dir(rdir.path(), SMALL);
        prop_assert!(durable.0 >= acks.last().copied().unwrap_or(0));
        prop_assert_eq!(reopened, log);
    }
}

/// How the fake primary breaks the stream at position `k` (1-based LSN).
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// `1..k-1`, then `k+1` (record `k` missing).
    Gap,
    /// `1..k`, then `k` again.
    Dup,
    /// `1..k-1`, then `k+1` *before* `k`.
    Reorder,
    /// `1..k-1`, then an arbitrary foreign LSN.
    Foreign(u64),
    /// `1..k-1`, then an old LSN replayed (`1`).
    Stale,
    /// `1..k-1`, then record `k` with a corrupted CRC.
    BadCrc,
}

fn fault_strategy() -> impl Strategy<Value = Fault> {
    prop_oneof![
        Just(Fault::Gap),
        Just(Fault::Dup),
        Just(Fault::Reorder),
        prop_oneof![Just(0u64), Just(u64::MAX), any::<u64>()].prop_map(Fault::Foreign),
        Just(Fault::Stale),
        Just(Fault::BadCrc),
    ]
}

/// Send the faulty stream; returns `(records the replica may keep, ERR code)`.
fn send_faulty(fp: &mut FakePrimary, log: &[(Lsn, Vec<u8>)], k: usize, f: Fault) -> (u64, ErrCode) {
    let ok = |n: usize| log[..n].to_vec();
    let k1 = k - 1; // records 1..=k-1 are good
    match f {
        Fault::Gap | Fault::Reorder => {
            fp.records(&ok(k1));
            fp.records(&log[k..=k]); // lsn k+1 where k is expected
            (k1 as u64, ErrCode::Contiguity)
        }
        Fault::Dup => {
            fp.records(&ok(k));
            fp.records(&log[k - 1..k]); // lsn k again where k+1 is expected
            (k as u64, ErrCode::Contiguity)
        }
        Fault::Foreign(x) => {
            fp.records(&ok(k1));
            if x == k as u64 {
                // Not foreign after all — keep the property meaningful.
                fp.record(x.wrapping_add(7), b"foreign");
            } else {
                fp.record(x, b"foreign");
            }
            (k1 as u64, ErrCode::Contiguity)
        }
        Fault::Stale => {
            fp.records(&ok(k1));
            fp.records(&log[..1]); // lsn 1 replayed
            (k1 as u64, ErrCode::Contiguity)
        }
        Fault::BadCrc => {
            fp.records(&ok(k1));
            let (lsn, p) = &log[k - 1];
            let mut bytes = Vec::new();
            wire::encode_record(&mut bytes, *lsn, p);
            bytes[4 + 1 + 8] ^= 0x5A; // first byte of the crc field
            fp.send_raw(&bytes);
            (k1 as u64, ErrCode::WireCrc)
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    /// §15.2 P2 — contiguity (R3): a broken stream is never appended past the
    /// fault, the replica says `ERR(Contiguity|WireCrc)`, and after reconnecting
    /// from its `durable_lsn` it converges to the primary's log.
    #[test]
    fn p2_faults_are_never_appended_and_reconnect_converges(
        n in 2usize..40,
        k_frac in 0.0f64..1.0,
        fault in fault_strategy(),
        batch in 1usize..6,
    ) {
        let log: Vec<(Lsn, Vec<u8>)> =
            (1..=n as u64).map(|l| (Lsn(l), payload(l, (l as usize * 13) % 200))).collect();
        // k in 1..=n, but Gap/Reorder need a k+1 and Stale needs k ≥ 2.
        let lo = if matches!(fault, Fault::Stale) { 2 } else { 1 };
        let hi = if matches!(fault, Fault::Gap | Fault::Reorder) { n - 1 } else { n };
        let k = lo + ((hi - lo) as f64 * k_frac) as usize;

        let rdir = tempfile::tempdir().unwrap();
        let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(batch), 2);

        // Session 1: the faulty stream.
        let mut fp = FakePrimary::connect(addr);
        prop_assert_eq!(fp.hello(), 0);
        fp.serving(1);
        let (kept, code) = send_faulty(&mut fp, &log, k, fault);
        let rest = fp.finish();
        prop_assert!(rest.contains(&Got::Err(code)), "expected ERR({code:?}), got {rest:?}");
        // Every ACK the replica sent is ≤ what it may keep (never the bad record).
        let acks: Vec<u64> = rest.iter().filter_map(|g| match g { Got::Ack(a) => Some(*a), _ => None }).collect();
        prop_assert!(acks.iter().all(|&a| a <= kept), "acks {acks:?} kept {kept}");

        // Session 2: reconnect; the replica resumes from exactly `kept`.
        let mut fp = FakePrimary::connect(addr);
        let d = fp.hello();
        prop_assert_eq!(d, kept, "durable_lsn after the fault (bad record must not be appended)");
        fp.serving(d + 1);
        fp.records(&log[d as usize..]);
        fp.await_ack(n as u64);
        fp.finish();

        let (rx, results) = h.join().unwrap();
        match (&results[0], code) {
            (Err(ReplError::Contiguity { .. }), ErrCode::Contiguity) => {}
            (Err(ReplError::WireCrc), ErrCode::WireCrc) => {}
            (r, c) => prop_assert!(false, "session 1 returned {r:?}, expected {c:?}"),
        }
        prop_assert!(results[1].is_ok(), "{:?}", results[1]);
        prop_assert_eq!(read_all(rx.wal()), log);
    }
}

#[test]
fn serving_from_must_be_last_plus_one() {
    for bad_from in [0u64, 2, 3, u64::MAX] {
        let rdir = tempfile::tempdir().unwrap();
        let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(4), 1);
        let mut fp = FakePrimary::connect(addr);
        assert_eq!(fp.hello(), 0);
        fp.serving(bad_from);
        let rest = fp.finish();
        assert_eq!(rest, vec![Got::Err(ErrCode::Contiguity)], "from {bad_from}");
        let (rx, results) = h.join().unwrap();
        assert!(matches!(
            results[0],
            Err(ReplError::Contiguity {
                expected: Lsn(1),
                ..
            })
        ));
        assert_eq!(rx.last_lsn(), Lsn(0));
    }
}

#[test]
fn serving_mismatch_after_progress_appends_nothing() {
    let log: Vec<(Lsn, Vec<u8>)> = (1..=5).map(|l| (Lsn(l), payload(l, 10))).collect();
    let rdir = tempfile::tempdir().unwrap();
    let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(2), 2);
    let mut fp = FakePrimary::connect(addr);
    ship_all(&mut fp, &log, 5);
    fp.finish();
    // Reconnect and claim to serve from 7 (the replica is durable to 5).
    let mut fp = FakePrimary::connect(addr);
    assert_eq!(fp.hello(), 5);
    fp.serving(7);
    fp.record(7, b"x");
    assert_eq!(fp.finish(), vec![Got::Err(ErrCode::Contiguity)]);
    let (rx, results) = h.join().unwrap();
    assert!(results[0].is_ok());
    assert!(matches!(results[1], Err(ReplError::Contiguity { .. })));
    assert_eq!(rx.last_lsn(), Lsn(5));
    assert_eq!(read_all(rx.wal()), log);
}

#[test]
fn batch_interval_commits_a_partial_batch() {
    let rdir = tempfile::tempdir().unwrap();
    let mut cfg = rx_cfg(1000);
    cfg.batch_interval = Duration::from_millis(20);
    let (addr, h) = spawn_receiver(rdir.path(), cfg, 1);
    let mut fp = FakePrimary::connect(addr);
    fp.hello();
    fp.serving(1);
    fp.records(&[(Lsn(1), vec![1]), (Lsn(2), vec![2]), (Lsn(3), vec![3])]);
    // Without closing the connection: the interval alone must commit + ACK.
    assert_eq!(fp.await_ack(3), vec![3]);
    fp.finish();
    let (rx, results) = h.join().unwrap();
    assert!(results[0].is_ok());
    assert_eq!(rx.durable_lsn(), Lsn(3));
}

#[test]
fn heartbeat_is_answered_with_the_durable_watermark() {
    let rdir = tempfile::tempdir().unwrap();
    let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(1), 1);
    let mut fp = FakePrimary::connect(addr);
    fp.hello();
    fp.serving(1);
    fp.record(1, b"a");
    fp.await_ack(1);
    fp.send(&Frame::Heartbeat {
        durable_lsn: Lsn(1),
    });
    assert_eq!(fp.next(), Some(Got::Heartbeat(1)));
    fp.finish();
    assert!(h.join().unwrap().1[0].is_ok());
}

#[test]
fn reseed_required_is_fatal_for_run() {
    let rdir = tempfile::tempdir().unwrap();
    let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(1), 1);
    let mut fp = FakePrimary::connect(addr);
    fp.hello();
    fp.send(&Frame::ReseedRequired {
        primary_oldest: Lsn(100),
    });
    assert_eq!(fp.finish(), vec![]);
    let (_rx, results) = h.join().unwrap();
    assert!(matches!(
        results[0],
        Err(ReplError::ReseedRequired { oldest: Lsn(100) })
    ));
}

#[test]
fn unexpected_and_oversize_frames_are_protocol_errors() {
    // An ACK is never sent P→R.
    let rdir = tempfile::tempdir().unwrap();
    let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(1), 2);
    let mut fp = FakePrimary::connect(addr);
    fp.hello();
    fp.serving(1);
    fp.send(&Frame::Ack {
        durable_lsn: Lsn(1),
    });
    assert_eq!(fp.finish(), vec![Got::Err(ErrCode::Protocol)]);
    // A payload larger than the replica's max_record_size is rejected from the
    // frame header, never appended.
    let mut fp = FakePrimary::connect(addr);
    fp.hello();
    fp.serving(1);
    fp.record(1, &vec![0u8; SMALL.max_record_size as usize + 1]);
    assert_eq!(fp.finish(), vec![Got::Err(ErrCode::Protocol)]);
    let (rx, results) = h.join().unwrap();
    assert!(matches!(results[0], Err(ReplError::Protocol(_))));
    assert!(matches!(results[1], Err(ReplError::Protocol(_))));
    assert_eq!(rx.last_lsn(), Lsn(0));
}

#[test]
fn primary_err_frame_ends_the_session_without_echo() {
    let rdir = tempfile::tempdir().unwrap();
    let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(8), 1);
    let mut fp = FakePrimary::connect(addr);
    fp.hello();
    fp.serving(1);
    fp.record(1, b"kept");
    fp.send(&Frame::Err {
        code: ErrCode::Protocol,
        msg: "primary shutting down",
    });
    // The appended record is still committed + acked before the close; no ERR
    // is echoed back.
    assert_eq!(fp.finish(), vec![Got::Ack(1)]);
    let (rx, results) = h.join().unwrap();
    assert!(matches!(
        results[0],
        Err(ReplError::Remote {
            code: ErrCode::Protocol,
            ..
        })
    ));
    assert_eq!(rx.durable_lsn(), Lsn(1));
}

#[test]
fn watermarks_track_append_and_commit() {
    let rdir = tempfile::tempdir().unwrap();
    let mut cfg = rx_cfg(3);
    cfg.batch_interval = Duration::from_secs(30);
    let (addr, h) = spawn_receiver(rdir.path(), cfg, 1);
    let mut fp = FakePrimary::connect(addr);
    fp.hello();
    fp.serving(1);
    fp.records(&[(Lsn(1), vec![1]), (Lsn(2), vec![2]), (Lsn(3), vec![3])]);
    fp.await_ack(3);
    fp.finish();
    let (rx, _) = h.join().unwrap();
    let w = rx.watermarks();
    assert_eq!((w.durable_lsn(), w.last_lsn()), (Lsn(3), Lsn(3)));
}

#[test]
fn receiver_rejects_zero_batch_records() {
    let rdir = tempfile::tempdir().unwrap();
    assert!(open_wal_replica::Receiver::open(rdir.path(), rx_cfg(0)).is_err());
}

/// §7.1 — a replica-hosted consumer fed from the replica's own
/// `DurabilityObserver` sees only committed (durable) watermarks: monotonic,
/// covering every ACK, never ahead of what the replica made durable.
#[test]
fn replica_observer_sees_only_committed_watermarks() {
    use std::sync::{Arc, Mutex};

    struct Tap(Arc<Mutex<Vec<u64>>>);
    impl open_wal::DurabilityObserver for Tap {
        fn on_durable(&mut self, d: Lsn) {
            self.0.lock().unwrap().push(d.0);
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let rdir = tempfile::tempdir().unwrap();
    let mut rx =
        open_wal_replica::Receiver::open_with(rdir.path(), rx_cfg(4), Tap(Arc::clone(&seen)))
            .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let h = std::thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        let r = rx.serve_connection(s);
        (rx, r)
    });
    // 60 records of ~200 B over 4 KiB segments ⇒ rolls + split commits.
    let log: Vec<(Lsn, Vec<u8>)> = (1..=60).map(|l| (Lsn(l), payload(l, 200))).collect();
    let mut fp = FakePrimary::connect(addr);
    let acks = ship_all(&mut fp, &log, 7);
    fp.finish();
    let (rx, r) = h.join().unwrap();
    assert!(r.is_ok(), "{r:?}");
    let seen = seen.lock().unwrap().clone();
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
    assert_eq!(seen.last().copied(), Some(60));
    assert!(
        acks.iter().all(|a| seen.contains(a)),
        "acks {acks:?} ⊄ {seen:?}"
    );
    assert_eq!(read_all(rx.wal()), log);
}

/// WAL v7.1: sequencing uses `checked_next`. A replica whose log already ends
/// at `u64::MAX` cannot accept a `SERVING` (there is no next LSN) — it fails
/// with `LsnExhausted`, never a saturated/repeated LSN and never poison.
#[test]
fn exhausted_lsn_space_is_its_own_error_not_poison() {
    let rdir = tempfile::tempdir().unwrap();
    {
        let (mut wal, _) = Wal::options()
            .seed(Lsn(u64::MAX))
            .open(rdir.path(), SMALL)
            .unwrap();
        assert_eq!(wal.append(b"last").unwrap(), Lsn(u64::MAX));
        wal.commit().unwrap();
    }
    let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(1), 1);
    let mut fp = FakePrimary::connect(addr);
    assert_eq!(fp.hello(), u64::MAX);
    fp.serving(u64::MAX);
    fp.finish();
    let (rx, results) = h.join().unwrap();
    assert!(
        matches!(
            results[0],
            Err(ReplError::LsnExhausted {
                last: Lsn(u64::MAX)
            })
        ),
        "{:?}",
        results[0]
    );
    assert!(!rx.is_poisoned());
}

/// Review R3: dead-primary detection is ON by default. `None` would disable it
/// (a primary that vanishes without FIN/RST would wedge the receiver forever).
#[test]
fn default_idle_timeout_detects_a_dead_primary() {
    assert_eq!(
        ReceiverConfig::new(SMALL).idle_timeout,
        Some(Duration::from_secs(5))
    );
}

/// Review R3: a half-open primary (power loss / partition: no FIN, no RST)
/// must not wedge the replica. Under the **default** config the receiver gives
/// up on the silent connection after `idle_timeout` and serves the restarted
/// primary's new connection (R7).
#[test]
fn half_open_primary_does_not_wedge_the_replica() {
    let rdir = tempfile::tempdir().unwrap();
    let cfg = ReceiverConfig::new(SMALL); // the default idle_timeout under test
    let (addr, h) = spawn_receiver(rdir.path(), cfg, 2);
    // Primary #1 streams one record, sees the ACK, then goes silent with its
    // socket still open (kept alive in `_silent` until the end of the test).
    let mut silent = FakePrimary::connect(addr);
    silent.hello();
    silent.serving(1);
    silent.record(1, b"before the outage");
    silent.await_ack(1);
    // The restarted primary connects; it must be served (HELLO) within the idle
    // timeout plus slack — FakePrimary's own read timeout is 20 s, so a wedged
    // receiver makes `hello()` panic.
    let t = std::time::Instant::now();
    let mut restarted = FakePrimary::connect(addr);
    assert_eq!(
        restarted.hello(),
        1,
        "served from the replica's durable_lsn"
    );
    assert!(t.elapsed() < Duration::from_secs(15), "{:?}", t.elapsed());
    restarted.serving(2);
    restarted.record(2, b"after the restart");
    restarted.await_ack(2);
    restarted.finish();
    let (rx, results) = h.join().unwrap();
    assert!(
        matches!(&results[0], Err(ReplError::Io(e)) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)),
        "the silent session ends by idle timeout: {:?}",
        results[0]
    );
    assert!(results[1].is_ok(), "{:?}", results[1]);
    assert_eq!(rx.durable_lsn(), Lsn(2));
    drop(silent);
}

/// Review R5 / §7 step 5: EVERY poisoning commit is reported as
/// `ERR(Poisoned)` — including one that fails with `WalError::Io` (not just
/// `FsyncFailed`). Injected by planting the next segment's filename, so the
/// commit-time roll's `O_EXCL` create fails after the first segment synced.
#[test]
fn io_commit_failure_is_reported_as_err_poisoned() {
    let rdir = tempfile::tempdir().unwrap();
    // 100-byte payloads frame to 120 B; a 4 KiB segment (64 B header) holds 33,
    // so the commit-time roll creates the segment based at LSN 34.
    let log: Vec<(Lsn, Vec<u8>)> = (1..=40).map(|l| (Lsn(l), payload(l, 100))).collect();
    let (addr, h) = spawn_receiver(rdir.path(), rx_cfg(40), 2);
    std::fs::write(rdir.path().join(format!("{:020}.wal", 34)), b"planted").unwrap();
    let mut fp = FakePrimary::connect(addr);
    assert_eq!(fp.hello(), 0);
    fp.serving(1);
    fp.records(&log);
    let rest = fp.finish();
    assert!(
        rest.contains(&Got::Err(ErrCode::Poisoned)),
        "a poisoning commit must send ERR(Poisoned): {rest:?}"
    );
    // The poisoned receiver refuses the next connection with ERR(Poisoned) too.
    let mut fp = FakePrimary::connect(addr);
    assert_eq!(fp.next(), Some(Got::Err(ErrCode::Poisoned)));
    let (rx, results) = h.join().unwrap();
    assert!(rx.is_poisoned());
    assert!(
        matches!(results[0], Err(ReplError::Wal(_))),
        "{:?}",
        results[0]
    );
}

/// Review R5, the path that actually lost the ERR: the session ends on an
/// in-band fault (here a contiguity violation) while records are still pending,
/// and the end-of-session commit then fails with `WalError::Io` (the planted
/// next-segment file makes the roll's `O_EXCL` create fail). The poisoning
/// error replaces the pending one, so the primary must get `ERR(Poisoned)` —
/// not silence.
#[test]
fn poisoning_end_of_session_commit_still_sends_err_poisoned() {
    let rdir = tempfile::tempdir().unwrap();
    let log: Vec<(Lsn, Vec<u8>)> = (1..=40).map(|l| (Lsn(l), payload(l, 100))).collect();
    let mut cfg = rx_cfg(1000); // nothing commits until the session ends
    cfg.batch_interval = Duration::from_secs(60);
    let (addr, h) = spawn_receiver(rdir.path(), cfg, 1);
    std::fs::write(rdir.path().join(format!("{:020}.wal", 34)), b"planted").unwrap();
    let mut fp = FakePrimary::connect(addr);
    assert_eq!(fp.hello(), 0);
    fp.serving(1);
    // 40 good records and a gap, in one write: all are read before any commit.
    let mut bytes = Vec::new();
    for (lsn, p) in &log {
        wire::encode_record(&mut bytes, *lsn, p);
    }
    wire::encode_record(&mut bytes, Lsn(99), b"gap");
    fp.send_raw(&bytes);
    let rest = fp.finish();
    assert!(
        rest.contains(&Got::Err(ErrCode::Poisoned)),
        "a poisoning end-of-session commit must send ERR(Poisoned): {rest:?}"
    );
    let (rx, results) = h.join().unwrap();
    assert!(rx.is_poisoned());
    assert!(
        matches!(results[0], Err(ReplError::Wal(_))),
        "{:?}",
        results[0]
    );
}
