//! RM3 — catch-up / resume (§9, §9.1–§9.3 of `docs/replica_design_v1.md`;
//! kickoff C tests T1–T17).
//!
//! A real primary `Wal` + `Shipper` run on the test (writer) thread. Replicas
//! are either a real `Receiver` behind the forwarding [`Proxy`] (the wire-level
//! oracle) or a hand-driven [`Fake`] replica, whose view of the wire is the
//! oracle.
//!
//! **Oracles are independent of the shipper**: the proxy / the fake replica's
//! frames, the test's own record model ([`Prim::model`]), the replica WAL's
//! `DurableTap`, the receiver's session results, and the writer thread's
//! `/proc/thread-self/io` counters. `Shipper::request_pending`,
//! `replica_status` and `catch_up_events` are used for sequencing and
//! reachability only.
//!
//! **"A reader ran"** is proven structurally (the `Shipper` was created after
//! the prefix, so the ring cannot hold the cursor, or an overflow was induced)
//! or by `rchar > 0` for that `poll`.
#![cfg(not(loom))]

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::{
    Fake, FakeConn, First, IoCount, Proxied, SMALL, Saw, measure_io, payload, read_all,
    segment_bases, segment_path, wait_until,
};
use open_wal::{Lsn, Wal, WalConfig};
use open_wal_replica::wire::ErrCode;
use open_wal_replica::{ReceiverConfig, ReplError, ReplicaStatus, Shipper, ShipperConfig, ring};
use proptest::prelude::*;

const HB: Duration = Duration::from_millis(50);

fn ship_cfg(addrs: Vec<SocketAddr>) -> ShipperConfig {
    let mut c = ShipperConfig::new(addrs);
    c.ring_bytes = 64 * 256;
    c.slot_bytes = 256;
    c.heartbeat = HB;
    c.reconnect_backoff = Duration::from_millis(5);
    c.max_reconnect_backoff = Duration::from_millis(50);
    c.io_timeout = Duration::from_secs(5);
    c
}

fn rx_cfg(wal: WalConfig) -> ReceiverConfig {
    let mut c = ReceiverConfig::new(wal);
    c.batch_records = 8;
    c.batch_interval = Duration::from_millis(1);
    c
}

/// The primary: a WAL, optionally a shipper, and the test's own model of what
/// was appended (the byte-fidelity oracle).
struct Prim {
    dir: tempfile::TempDir,
    wal: Wal,
    sh: Option<Shipper>,
    /// `(lsn, payload)` of every record appended, in order.
    model: Vec<(u64, Vec<u8>)>,
}

impl Prim {
    fn open(cfg: WalConfig) -> Prim {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = Wal::open(dir.path(), cfg).unwrap();
        Prim {
            dir,
            wal,
            sh: None,
            model: Vec::new(),
        }
    }

    fn seeded(seed: u64, cfg: WalConfig) -> Prim {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = Wal::options()
            .seed(Lsn(seed))
            .open(dir.path(), cfg)
            .unwrap();
        Prim {
            dir,
            wal,
            sh: None,
            model: Vec::new(),
        }
    }

    fn start(&mut self, cfg: ShipperConfig) {
        self.sh = Some(Shipper::new(cfg, &self.wal).unwrap());
    }

    fn sh(&self) -> &Shipper {
        self.sh.as_ref().unwrap()
    }

    /// Append one record of `len` bytes (and capture it, if shipping).
    fn append(&mut self, len: usize) -> u64 {
        let next = self.wal.last_lsn().0 + 1;
        let p = payload(next, len);
        let lsn = self.wal.append(&p).unwrap();
        assert_eq!(lsn.0, next);
        if let Some(sh) = &mut self.sh {
            sh.capture(lsn, &p);
        }
        self.model.push((lsn.0, p));
        lsn.0
    }

    /// `commit()` without telling the shipper (durable but not released).
    fn commit_only(&mut self) -> Lsn {
        self.wal.commit().unwrap()
    }

    fn on_commit(&mut self, w: Lsn) {
        self.sh.as_mut().unwrap().on_commit(w);
    }

    fn try_poll(&mut self) -> Result<(), ReplError> {
        self.sh.as_mut().unwrap().poll(&self.wal)
    }

    fn poll(&mut self) {
        self.try_poll().unwrap();
    }

    /// The §9.1 cadence: `commit`, `on_commit`, `poll`.
    fn commit(&mut self) -> Lsn {
        let w = self.wal.commit().unwrap();
        if self.sh.is_some() {
            self.on_commit(w);
            self.poll();
        }
        w
    }

    /// Append `n` records of `len` bytes, committing every `batch`.
    fn fill(&mut self, n: u64, len: usize, batch: u64) {
        for i in 1..=n {
            self.append(len);
            if i % batch == 0 {
                self.commit();
            }
        }
        self.commit();
    }

    /// Poll (every ~1 ms) until `f` holds.
    fn poll_until(&mut self, secs: u64, what: &str, mut f: impl FnMut(&mut Prim) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            self.poll();
            if f(self) {
                return;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Wait (without polling) until replica `id` has a request posted.
    fn await_request(&self, id: usize) {
        wait_until(10, "a pending request", || self.sh().request_pending(id));
    }

    /// The model's records with `from ≤ lsn ≤ to`.
    fn expect(&self, from: u64, to: u64) -> Vec<(u64, Vec<u8>)> {
        self.model
            .iter()
            .filter(|(l, _)| (from..=to).contains(l))
            .cloned()
            .collect()
    }

    fn stop(&mut self) {
        if let Some(sh) = self.sh.take() {
            sh.shutdown();
        }
    }
}

/// Poll until the fake replica receives a frame that is not a heartbeat.
fn next_polling(p: &mut Prim, c: &mut FakeConn, secs: u64) -> Saw {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        p.poll();
        match c.next_data(Duration::from_millis(3)) {
            Saw::Timeout => assert!(Instant::now() < deadline, "no frame from the primary"),
            s => return s,
        }
    }
}

/// Answer replica `id`'s pending join with exactly ONE poll and return its
/// first frame. Unlike [`next_polling`] it does not keep polling, so the chunk
/// request the replica posts next is still pending afterwards — for tests that
/// must drive (or measure) the poll that answers it.
fn join(p: &mut Prim, id: usize, c: &mut FakeConn) -> Saw {
    p.await_request(id);
    p.poll();
    c.next_data(Duration::from_secs(5))
}

/// "This poll read the log": more than probe noise. (The probe's own cost
/// varies by a byte or two with the counters' digit count; the smallest log
/// read is a 20-byte record header.)
fn read_the_log(io: IoCount) -> bool {
    io.rchar > 16
}

/// Poll until the fake replica has received exactly the model's records
/// `from..=to`, in order, byte-identical.
fn expect_records(p: &mut Prim, c: &mut FakeConn, from: u64, to: u64) {
    for want in p.expect(from, to) {
        match next_polling(p, c, 20) {
            Saw::Record(l, bytes) => {
                assert_eq!(l, want.0, "dense: expected record {}", want.0);
                assert_eq!(bytes, want.1, "record {l} is not byte-identical");
            }
            other => panic!("expected RECORD {}, got {other:?}", want.0),
        }
    }
}

/// Keep polling for `window`; return every non-heartbeat frame that arrives.
fn quiet(p: &mut Prim, c: &mut FakeConn, window: Duration) -> Vec<Saw> {
    let deadline = Instant::now() + window;
    let mut out = Vec::new();
    while Instant::now() < deadline {
        p.poll();
        match c.next(Duration::from_millis(3)) {
            Saw::Timeout | Saw::Heartbeat(_) => {}
            Saw::Eof => {
                out.push(Saw::Eof);
                break;
            }
            s => out.push(s),
        }
    }
    out
}

fn no_inband_errors(results: &[Result<(), ReplError>]) {
    for r in results {
        if let Err(e) = r {
            assert!(
                e.err_code().is_none() && !matches!(e, ReplError::Remote { .. }),
                "replica session failed in-band: {e:?}"
            );
        }
    }
}

/// R4 sample (T7): `acked` first, then the replica WAL's own tap.
fn sample_r4(p: &Prim, id: usize, r: &Proxied) {
    let acked = p.sh().replica_acked_lsn(id).unwrap().0;
    let tap = r.server.tap.get();
    assert!(
        acked <= tap,
        "R4: acked {acked} > what the replica WAL certified durable ({tap})"
    );
}

// ---------------------------------------------------------------------------
// T1 — overflow → in-session catch-up → cutover (real Receiver, proxy oracle)
// ---------------------------------------------------------------------------

/// T1 (+ T7's R4 sampling). One overflow, induced by pausing the proxy while
/// the writer keeps committing; the ring (4096 slots) is far larger than
/// anything captured outside that pause, so no other overflow can happen. The
/// replica is then served the gap from the log in several chunks and cut over
/// to the ring — all inside ONE session — while the writer keeps appending and
/// committing.
#[test]
fn t1_overflow_falls_back_in_session_and_cuts_over() {
    const LEN: usize = 240;
    let wcfg = WalConfig {
        segment_size: 1024 * 1024,
        max_record_size: 256,
    };
    let rdir = tempfile::tempdir().unwrap();
    let r = Proxied::start(rdir.path(), rx_cfg(wcfg));
    let mut p = Prim::open(wcfg);
    let mut cfg = ship_cfg(vec![r.addr()]);
    cfg.ring_bytes = 4096 * 256;
    cfg.io_timeout = Duration::from_secs(60);
    cfg.catchup_chunk_records = 2000;
    p.start(cfg);

    // Phase A: streaming.
    p.fill(200, LEN, 20);
    let w = p.wal.durable_lsn();
    p.poll_until(20, "the replica to ack phase A", |p| {
        sample_r4(p, 0, &r);
        p.sh().replica_acked_lsn(0) == Some(w)
    });
    assert_eq!(p.sh().replica_status(0), Some(ReplicaStatus::Streaming));
    assert_eq!(p.sh().catch_up_events(0), 0);

    // Phase B: pause the proxy and write ~36 MB — far more than loopback
    // socket buffers hold — so the network thread blocks in write() and the
    // ring overflows past its cursor.
    r.proxy.pause();
    let before_pause = r.proxy.last().last_record.unwrap();
    for i in 0..150_000u64 {
        p.append(LEN);
        if i % 100 == 99 {
            p.commit();
            sample_r4(&p, 0, &r);
        }
    }
    p.commit();
    r.proxy.resume();

    // The network thread drains what it had, sees NeedsCatchUp and posts a
    // chunk request. Everything before the chunk's first LSN is on its socket
    // by then; once the proxy has forwarded it (quiescence), the chunk's first
    // LSN is the proxy's last record + 1.
    p.await_request(0);
    let mut last = r.proxy.last().last_record.unwrap();
    let mut stable_since = Instant::now();
    while stable_since.elapsed() < Duration::from_millis(200) {
        std::thread::sleep(Duration::from_millis(5));
        let now = r.proxy.last().last_record.unwrap();
        if now != last {
            last = now;
            stable_since = Instant::now();
        }
    }
    let chunk_first = last + 1;
    assert!(chunk_first > before_pause);
    assert!(
        chunk_first + 4096 < p.wal.durable_lsn().0,
        "structural: the ring (4096 slots) cannot hold the chunk's cursor"
    );
    let mut catchup_polls = 1;
    p.poll(); // the first chunk answer
    assert_eq!(p.sh().replica_status(0), Some(ReplicaStatus::CatchingUp));
    // R4 detection power: an ACK for a catch-up record must arrive while the
    // session is still catching up.
    wait_until(30, "a forwarded ACK for the first chunk", || {
        r.proxy.assert_clean();
        assert_eq!(r.proxy.session_count(), 1, "the session ended mid-catch-up");
        r.proxy.max_ack() >= chunk_first
    });
    sample_r4(&p, 0, &r);

    // Phase C: the writer keeps appending and committing while catch-up
    // proceeds (a few hundred records in total, ≪ the ring).
    //
    // After each commit + poll it lets the network thread act on that poll's
    // answer before it appends again: either the next chunk request is posted,
    // or the session cut over AND the replica's stream has reached everything
    // released. Without that, "exactly one overflow" would be a coin toss: the
    // cutover attaches wherever the chunk happens to end, possibly on the
    // ring's very oldest record, which the next capture evicts (a second,
    // legitimate, in-session fall-back — §9.2 — but not the single episode
    // this test's reachability check wants).
    let mut cut_over_while_committing = false;
    for _ in 0..20_000 {
        if p.sh().request_pending(0) {
            catchup_polls += 1;
        }
        for _ in 0..3 {
            p.append(LEN);
        }
        p.commit();
        sample_r4(&p, 0, &r);
        let released = p.sh().released().0;
        let settle = Instant::now() + Duration::from_secs(5);
        let mut streaming = false;
        while Instant::now() < settle {
            streaming = p.sh().replica_status(0) == Some(ReplicaStatus::Streaming)
                && r.proxy.last().last_record >= Some(released);
            if streaming || p.sh().request_pending(0) {
                break;
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        if streaming {
            cut_over_while_committing = true;
            break;
        }
    }
    assert!(
        cut_over_while_committing,
        "CatchingUp → Streaming must happen while the writer is committing"
    );
    assert!(
        catchup_polls >= 2,
        "catch-up must span several polls (got {catchup_polls})"
    );
    for _ in 0..50 {
        p.append(LEN);
        p.commit();
        sample_r4(&p, 0, &r);
    }
    let w = p.wal.durable_lsn();
    p.poll_until(60, "the replica to ack everything", |p| {
        sample_r4(p, 0, &r);
        p.sh().replica_acked_lsn(0) == Some(w)
    });
    let (status, episodes) = (p.sh().replica_status(0), p.sh().catch_up_events(0));
    p.stop();

    // Oracles (all independent of the shipper).
    r.proxy.assert_clean();
    assert_eq!(
        r.proxy.session_count(),
        1,
        "overflow must fall back in-session"
    );
    let log = r.proxy.last();
    assert_eq!(log.serving, Some((1, 1)));
    assert_eq!((log.first_record, log.last_record), (Some(1), Some(w.0)));
    let (rx, results) = r.server.stop();
    assert_eq!(
        results.len(),
        1,
        "the receiver served exactly one session: {results:?}"
    );
    no_inband_errors(&results);
    assert_eq!(rx.durable_lsn(), w, "acked == durable == the primary's");
    let got = read_all(rx.wal());
    assert_eq!(got.len(), p.model.len());
    for ((gl, gp), (ml, mp)) in got.iter().zip(&p.model) {
        assert_eq!(gl.0, *ml, "LSN mirroring");
        assert!(gp == mp, "record {ml} is not byte-identical");
    }
    // Reachability (shipper-reported; never the oracle): one episode, and the
    // replica ended up streaming.
    assert_eq!(status, Some(ReplicaStatus::Streaming));
    // `>= 1`, not `== 1`: a shipper counter is never an oracle, and a second
    // episode is legitimate (the cutover can land on the ring's oldest
    // record, which the next capture evicts — §9.2). That the fall-back was
    // in-session is the proxy's session count, asserted above.
    assert!(
        episodes >= 1,
        "the induced overflow must have caused a catch-up episode"
    );
}

// ---------------------------------------------------------------------------
// T2 — P5 + the step-1 boundary table
// ---------------------------------------------------------------------------

/// One T2 table row: the primary is seeded at 1000 and checkpointed so that
/// `P = oldest > 1001`; the fake replica says `HELLO{P + delta}`.
fn t2_row(in_ring: bool, delta: i64) {
    let mut p = Prim::seeded(1000, SMALL);
    let fake = Fake::new(256);
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.ring_bytes = 1024 * 256; // holds every record when created first
    if in_ring {
        p.start(cfg.clone());
    }
    p.fill(300, 64, 20);
    let last = p.wal.durable_lsn().0;
    p.wal.checkpoint(Lsn(1150)).unwrap();
    let oldest = p.wal.oldest_lsn().0;
    assert!(oldest > 1001 && oldest <= 1151, "P = {oldest}");
    assert_eq!(segment_bases(p.dir.path())[0], oldest, "P is on disk");
    if !in_ring {
        p.start(cfg); // created after the prefix: the ring cannot hold d + 1
    }
    let d = (oldest as i64 + delta) as u64;
    let mut c = fake.hello(d);
    let first = next_polling(&mut p, &mut c, 10);
    let what = format!("in_ring={in_ring} d=P{delta:+} (P={oldest})");
    match delta {
        -2 => {
            assert_eq!(first, Saw::Reseed { oldest }, "{what}");
            let rest = quiet(&mut p, &mut c, Duration::from_millis(150));
            assert!(
                rest.iter().all(|s| *s == Saw::Eof),
                "{what}: nothing may follow RESEED_REQUIRED, got {rest:?}"
            );
        }
        _ => {
            assert_eq!(
                first,
                Saw::Serving {
                    from: d + 1,
                    oldest
                },
                "{what}"
            );
            expect_records(&mut p, &mut c, d + 1, last);
        }
    }
    p.stop();
}

#[test]
fn t2_step1_boundary_table() {
    for in_ring in [true, false] {
        for delta in [-2, -1, 0] {
            t2_row(in_ring, delta);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// §15.2 P5 — catch-up after checkpoint (R6). A checkpoint past a lagging
    /// replica ⇒ the frame answering its next HELLO is
    /// `RESEED_REQUIRED{wal.oldest_lsn()}`, never a silent skip; a replica
    /// within retention is served exactly `d + 1..`, byte-identical.
    #[test]
    fn t2_p5_catch_up_after_checkpoint(
        n in 30u64..160,
        d_pick in 0u64..=1000,
        up_pick in 0u64..=1000,
        shipper_first in any::<bool>(),
    ) {
        let d = d_pick % (n + 1);
        let up_to = up_pick % (n + 1);
        let mut p = Prim::open(SMALL);
        let fake = Fake::new(256);
        let mut cfg = ship_cfg(vec![fake.addr()]);
        cfg.ring_bytes = 16 * 256;
        cfg.catchup_chunk_records = 7;
        if shipper_first {
            p.start(cfg.clone());
        }
        p.fill(n, 48, 9);
        p.wal.checkpoint(Lsn(up_to)).unwrap();
        let oldest = p.wal.oldest_lsn().0;
        if !shipper_first {
            p.start(cfg);
        }
        let mut c = fake.hello(d);
        let first = next_polling(&mut p, &mut c, 10);
        if d + 1 < oldest {
            prop_assert_eq!(first, Saw::Reseed { oldest });
        } else {
            prop_assert_eq!(first, Saw::Serving { from: d + 1, oldest });
            expect_records(&mut p, &mut c, d + 1, n);
        }
        p.stop();
    }
}

// ---------------------------------------------------------------------------
// T3 — step 2 boundaries, judged on raw frames
// ---------------------------------------------------------------------------

/// (i) `d == durable` is a fully caught-up replica: `SERVING{d + 1}`, then
/// exactly `d + 1..=d + N`. (ii) `d == durable + 1` ⇒ `ERR(Contiguity)` +
/// close, and nothing is ever served. Both with the shipper created before
/// the prefix (`d + 1` reachable through the ring) and after it.
#[test]
fn t3_i_ii_caught_up_is_served_and_one_ahead_is_rejected() {
    for shipper_first in [true, false] {
        // (i)
        let mut p = Prim::open(SMALL);
        let fake = Fake::new(256);
        if shipper_first {
            p.start(ship_cfg(vec![fake.addr()]));
        }
        p.fill(25, 40, 6);
        if !shipper_first {
            p.start(ship_cfg(vec![fake.addr()]));
        }
        let d = p.wal.durable_lsn().0;
        let mut c = fake.hello(d);
        assert_eq!(
            next_polling(&mut p, &mut c, 10),
            Saw::Serving {
                from: d + 1,
                oldest: 1
            },
            "(i) shipper_first={shipper_first}: d == durable must be served"
        );
        for _ in 0..3 {
            for _ in 0..4 {
                p.append(40);
            }
            p.commit();
        }
        expect_records(&mut p, &mut c, d + 1, d + 12);
        assert!(quiet(&mut p, &mut c, Duration::from_millis(60)).is_empty());
        p.stop();

        // (ii)
        let mut p = Prim::open(SMALL);
        let fake = Fake::new(256);
        if shipper_first {
            p.start(ship_cfg(vec![fake.addr()]));
        }
        p.fill(25, 40, 6);
        if !shipper_first {
            p.start(ship_cfg(vec![fake.addr()]));
        }
        let d = p.wal.durable_lsn().0 + 1;
        let mut c = fake.hello(d);
        match next_polling(&mut p, &mut c, 10) {
            Saw::Err(ErrCode::Contiguity, _) => {}
            other => panic!(
                "(ii) shipper_first={shipper_first}: expected ERR(Contiguity), got {other:?}"
            ),
        }
        // ≥ 2 more commits + polls: the record it claimed now exists, and it
        // must still get nothing on this connection.
        for _ in 0..3 {
            p.append(40);
            p.append(40);
            p.commit();
        }
        let rest = quiet(&mut p, &mut c, Duration::from_millis(100));
        assert!(
            rest.iter().all(|s| *s == Saw::Eof),
            "(ii): a rejected replica must receive nothing, got {rest:?}"
        );
        p.stop();
    }
}

/// (iii) A HELLO at `ring::MAX_LSN` is outside the LSN domain: the network
/// thread refuses it with `ERR(Protocol, "LSN domain exhausted")`.
#[test]
fn t3_iii_hello_at_max_lsn_is_a_protocol_error() {
    let mut p = Prim::open(SMALL);
    let fake = Fake::new(256);
    p.start(ship_cfg(vec![fake.addr()]));
    p.fill(5, 16, 5);
    for d in [ring::MAX_LSN, ring::MAX_LSN + 1, u64::MAX] {
        let mut c = fake.hello(d);
        match next_polling(&mut p, &mut c, 10) {
            Saw::Err(ErrCode::Protocol, msg) => {
                assert!(
                    msg.contains("LSN domain exhausted"),
                    "d={d}: refused for the wrong reason: {msg}"
                );
            }
            other => panic!("d={d}: expected ERR(Protocol), got {other:?}"),
        }
    }
    // The largest in-domain `d` is merely ahead of this primary (step 2).
    let mut c = fake.hello(ring::MAX_LSN - 1);
    match next_polling(&mut p, &mut c, 10) {
        Saw::Err(ErrCode::Contiguity, _) => {}
        other => panic!("expected ERR(Contiguity), got {other:?}"),
    }
    p.stop();
}

/// (iv) `poll` between `commit()` and `on_commit()`: a replica whose
/// `d == wal.durable_lsn() > released` is **served** (`SERVING{d + 1}`), never
/// rejected, and waits. With `heartbeat = 10 s`, the first record after the
/// release must still arrive within 1 s: the Wait re-post is driven by the
/// release, not by the heartbeat timer.
#[test]
fn t3_iv_durable_but_unreleased_waits_and_is_woken_by_the_release() {
    let mut p = Prim::open(SMALL);
    let fake = Fake::new(256);
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.heartbeat = Duration::from_secs(10);
    p.start(cfg);
    p.fill(10, 40, 5);
    for _ in 0..3 {
        p.append(40);
    }
    let w = p.commit_only();
    let d = w.0;
    assert!(p.sh().released().0 < d && d == p.wal.durable_lsn().0);
    let mut c = fake.hello(d);
    assert_eq!(
        next_polling(&mut p, &mut c, 10),
        Saw::Serving {
            from: d + 1,
            oldest: 1
        },
        "durable-but-unreleased must be served, not rejected"
    );
    p.on_commit(w);
    p.poll();
    let t = Instant::now();
    let got = loop {
        p.append(40);
        p.commit();
        match c.next(Duration::from_millis(8)) {
            Saw::Timeout => assert!(
                t.elapsed() < Duration::from_secs(1),
                "no RECORD within 1 s of the release: the Wait was not re-posted on release"
            ),
            s => break s,
        }
    };
    assert_eq!(got, Saw::Record(d + 1, p.expect(d + 1, d + 1)[0].1.clone()));
    assert!(t.elapsed() < Duration::from_secs(1));
    p.stop();
}

// ---------------------------------------------------------------------------
// T4 — R7 restart with two lagging replicas
// ---------------------------------------------------------------------------

/// T4 (+ T7's R4 sampling). The primary restarts with two replicas behind it;
/// the new shipper's ring starts after everything they lack (structural), so
/// both converge through catch-up.
#[test]
fn t4_restart_with_two_lagging_replicas_converges_through_catch_up() {
    let (ra, rb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = Proxied::start(ra.path(), rx_cfg(SMALL));
    let b = Proxied::start(rb.path(), rx_cfg(SMALL));
    let mut p = Prim::open(SMALL);
    let mut cfg = ship_cfg(vec![a.addr(), b.addr()]);
    cfg.catchup_chunk_records = 40;
    cfg.catchup_replicas_per_poll = 1;
    let both = |p: &mut Prim, secs: u64, a: &Proxied, b: &Proxied| {
        let w = p.wal.durable_lsn();
        p.poll_until(secs, "both replicas to ack", |p| {
            sample_r4(p, 0, a);
            sample_r4(p, 1, b);
            p.sh().replica_acked_lsn(0) == Some(w) && p.sh().replica_acked_lsn(1) == Some(w)
        });
    };

    // Life 1: ship 60 records to both.
    p.start(cfg.clone());
    p.fill(60, 100, 7);
    both(&mut p, 30, &a, &b);
    p.stop(); // "crash": only the WAL survives
    // Committed but never shipped, then the process restarts.
    p.fill(700, 100, 25);
    let Prim {
        dir, wal, model, ..
    } = p;
    drop(wal);
    let (wal, _) = Wal::open(dir.path(), SMALL).unwrap();
    let mut p = Prim {
        dir,
        wal,
        sh: None,
        model,
    };

    // Life 2: both replicas are 700 records behind a ring that starts at 761.
    p.start(cfg);
    let mut saw_catching_up = [false, false];
    for i in 0..400u64 {
        p.append(100);
        if i % 4 == 3 {
            p.commit();
            sample_r4(&p, 0, &a);
            sample_r4(&p, 1, &b);
        }
        for (id, saw) in saw_catching_up.iter_mut().enumerate() {
            *saw |= p.sh().replica_status(id) == Some(ReplicaStatus::CatchingUp);
        }
        std::thread::sleep(Duration::from_micros(200));
    }
    p.commit();
    both(&mut p, 60, &a, &b);
    let w = p.wal.durable_lsn();
    let statuses = [p.sh().replica_status(0), p.sh().replica_status(1)];
    p.stop();

    for r in [a, b] {
        r.proxy.assert_clean();
        assert_eq!(r.proxy.session_count(), 2, "one session per primary life");
        let log = r.proxy.last();
        assert_eq!(log.serving, Some((61, 1)), "resumed from the HELLO (R7)");
        assert_eq!((log.first_record, log.last_record), (Some(61), Some(w.0)));
        let (rx, results) = r.server.stop();
        assert_eq!(results.len(), 2, "{results:?}");
        no_inband_errors(&results);
        assert_eq!(rx.durable_lsn(), w);
        let got: Vec<(u64, Vec<u8>)> = read_all(rx.wal())
            .into_iter()
            .map(|(l, b)| (l.0, b))
            .collect();
        assert!(got == p.model, "replica log != the primary's records");
    }
    // Reachability (shipper-reported; never the oracle).
    assert_eq!(statuses, [Some(ReplicaStatus::Streaming); 2]);
    assert_eq!(
        saw_catching_up,
        [true, true],
        "both replicas must have caught up from the log"
    );
}

// ---------------------------------------------------------------------------
// T5 — writer stall (Linux): the §9.1 per-poll bounds, measured
// ---------------------------------------------------------------------------

const T5_SEGMENT: u64 = 8 * 1024 * 1024;
const T5_CHUNK: usize = 1024 * 1024;
const T5_SLACK: u64 = 4096;

/// The framed size of a record in a segment file: 20-byte header + payload,
/// padded to 8.
fn framed(len: usize) -> u64 {
    ((20 + len as u64) + 7) & !7
}

struct Stall {
    first: IoCount,
    prefix: u64,
    later_max: IoCount,
    later_polls: u64,
    join_poll: IoCount,
}

/// Catch a fake replica up over a ≥ 64 MiB lag of `len`-byte records, starting
/// ≥ 4 MiB into an 8 MiB segment, measuring every `poll`.
fn t5_run(len: usize, chunk_records: usize) -> Stall {
    let wcfg = WalConfig {
        segment_size: T5_SEGMENT,
        max_record_size: 4096,
    };
    let f = framed(len);
    let mut p = Prim::open(wcfg);
    let total = (80 * 1024 * 1024) / f;
    for i in 0..total {
        let lsn = p.wal.last_lsn().0 + 1;
        // Not modelled record by record (millions of them): payload(lsn, len)
        // is recomputed at the receiving end.
        p.wal.append(&payload(lsn, len)).unwrap();
        if i % 4096 == 4095 {
            p.wal.commit().unwrap();
        }
    }
    let last = p.wal.commit().unwrap().0;
    // `d` mid-segment: ≥ 4 MiB into the SECOND segment.
    let bases = segment_bases(p.dir.path());
    let base = bases[1];
    let in_seg = (5 * 1024 * 1024) / f;
    let cursor = base + in_seg;
    assert!(cursor < bases[2], "the cursor stays inside segment 2");
    let prefix = in_seg * f;
    assert!(prefix >= 4 * 1024 * 1024);
    assert!((last - cursor + 1) * f >= 64 * 1024 * 1024, "lag ≥ 64 MiB");

    let fake = Fake::new(4096);
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.catchup_chunk_bytes = T5_CHUNK;
    cfg.catchup_chunk_records = chunk_records;
    cfg.io_timeout = Duration::from_secs(30);
    // Created after the whole log: nothing is in the ring (structural), and no
    // append happens during the measurement, so no overflow fall-back starts a
    // second episode.
    p.start(cfg);
    let mut c = fake.hello(cursor - 1);

    // A drain thread: the fake replica reads promptly and checks the stream.
    let drain = std::thread::spawn(move || {
        let mut next = cursor;
        let mut serving = None;
        while next <= last {
            match c.next(Duration::from_secs(30)) {
                Saw::Serving { from, .. } => serving = Some(from),
                Saw::Record(l, bytes) => {
                    assert_eq!(l, next, "dense");
                    assert!(bytes == payload(l, len), "record {l} is not byte-identical");
                    next += 1;
                    if l % 4096 == 0 {
                        c.ack(l);
                    }
                }
                Saw::Heartbeat(_) => {}
                other => panic!("unexpected {other:?} at {next}"),
            }
        }
        (serving, c)
    });

    // The poll that answers the JOIN reads nothing.
    p.await_request(0);
    let ((), join_poll) = measure_io(|| p.poll());
    let mut first = None;
    let mut later_max = IoCount::default();
    let mut later_polls = 0;
    let deadline = Instant::now() + Duration::from_secs(300);
    while p.sh().replica_status(0) != Some(ReplicaStatus::Streaming) {
        assert!(Instant::now() < deadline, "catch-up did not finish");
        if !p.sh().request_pending(0) {
            std::thread::yield_now();
            continue;
        }
        let ((), io) = measure_io(|| p.poll());
        if !read_the_log(io) {
            continue; // Attached at the cutover: no reader
        }
        if first.is_none() {
            first = Some(io);
        } else {
            later_max.rchar = later_max.rchar.max(io.rchar);
            later_max.syscr = later_max.syscr.max(io.syscr);
            later_polls += 1;
        }
    }
    let (serving, _c) = drain.join().unwrap();
    assert_eq!(serving, Some(cursor));
    assert_eq!(p.sh().catch_up_events(0), 1, "one episode");
    p.stop();
    Stall {
        first: first.expect("a reading poll"),
        prefix,
        later_max,
        later_polls,
        join_poll,
    }
}

fn t5_assert(s: &Stall, len: usize, chunk_records: usize) {
    let max_framed = framed(4096);
    eprintln!(
        "T5 len={len}: join poll {:?}; first reading poll {:?} (prefix {} B); {} later reading \
         polls, max {:?}; C + one framed record = {}",
        s.join_poll,
        s.first,
        s.prefix,
        s.later_polls,
        s.later_max,
        T5_CHUNK as u64 + framed(len)
    );
    assert!(
        !read_the_log(s.join_poll),
        "the poll that answers the join must read nothing ({:?})",
        s.join_poll
    );
    // First reading poll of the episode: one containing-segment prefix scan
    // plus one chunk — and the skip really happened (positive control).
    assert!(
        s.first.rchar <= T5_SEGMENT + T5_CHUNK as u64 + max_framed + T5_SLACK,
        "first reading poll read {} B",
        s.first.rchar
    );
    assert!(
        s.first.rchar >= s.prefix,
        "positive control: the first poll must have skipped the {} B prefix (read {})",
        s.prefix,
        s.first.rchar
    );
    // Every later reading poll: one chunk, no prefix.
    assert!(s.later_polls >= 16, "the lag spans many chunks");
    assert!(
        s.later_max.rchar <= T5_CHUNK as u64 + 2 * max_framed + T5_SLACK,
        "a later reading poll read {} B (bound: C + 2·max framed + 4 KiB)",
        s.later_max.rchar
    );
    assert!(
        s.later_max.syscr <= 2 * chunk_records as u64 + 64,
        "a later reading poll made {} read syscalls (bound: 2·{chunk_records} + slack)",
        s.later_max.syscr
    );
}

fn skip_without_io_probe(name: &str) -> bool {
    if common::io_probe_available() {
        return false;
    }
    eprintln!(
        "\n*** {name}: SKIPPED — /proc/thread-self/io is unavailable (not Linux, or no \
         CONFIG_TASK_IO_ACCOUNTING). The writer-stall bounds were NOT exercised. ***\n"
    );
    true
}

/// T5, 4 KiB records: the byte budget binds.
#[test]
fn t5_writer_stall_is_bounded_per_poll_4k_records() {
    if skip_without_io_probe("t5 (4 KiB records)") {
        return;
    }
    let s = t5_run(4096, 16_384);
    t5_assert(&s, 4096, 16_384);
}

/// T5, 0-byte payloads: two `pread`s per record, so the record cap binds.
#[test]
fn t5_writer_stall_is_bounded_per_poll_empty_records() {
    if skip_without_io_probe("t5 (0-byte records)") {
        return;
    }
    let s = t5_run(0, 8192);
    t5_assert(&s, 0, 8192);
}

/// T5 companions, each `rchar ≈ 0`: a poll with nothing pending, and every
/// poll in a ≥ 2-heartbeat window where a chunk request waits
/// (`released < cursor ≤ durable`, no `on_commit`).
#[test]
fn t5_companions_polls_that_must_not_read() {
    if skip_without_io_probe("t5 companions") {
        return;
    }
    let mut p = Prim::open(SMALL);
    let fake = Fake::new(256);
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.ring_bytes = 4 * 256; // 4 slots
    p.start(cfg);
    p.fill(40, 64, 8);
    let d = p.wal.durable_lsn().0;
    let mut c = fake.hello(d);
    assert_eq!(
        next_polling(&mut p, &mut c, 10),
        Saw::Serving {
            from: d + 1,
            oldest: 1
        }
    );
    p.poll_until(5, "streaming", |p| {
        p.sh().replica_status(0) == Some(ReplicaStatus::Streaming)
    });
    // Nothing pending.
    for _ in 0..5 {
        let ((), io) = measure_io(|| p.poll());
        assert!(
            !read_the_log(io),
            "a poll with nothing pending read the log"
        );
    }
    // 30 records captured into a 4-slot ring and committed WITHOUT on_commit:
    // the ring evicts the replica's cursor (d + 1 > released), so it falls back
    // and posts Chunk{d + 1} with released < cursor ≤ durable.
    for _ in 0..30 {
        p.append(64);
    }
    let w = p.commit_only();
    let (cursor, released, durable) = (d + 1, p.sh().released().0, p.wal.durable_lsn().0);
    assert!(
        released < cursor && cursor <= durable,
        "released < cursor ≤ durable"
    );
    p.await_request(0);
    let until = Instant::now() + 3 * HB;
    let mut polls = 0;
    while Instant::now() < until {
        let ((), io) = measure_io(|| p.poll());
        assert!(
            !read_the_log(io),
            "a chunk request above `released` must wait without opening a reader ({io:?})"
        );
        polls += 1;
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(polls > 10);
    let got = c.drain(Duration::from_millis(20));
    assert!(
        got.iter().all(|s| matches!(s, Saw::Heartbeat(_))),
        "R1: nothing above `released` may be served, got {got:?}"
    );
    // The release unblocks it; the records come from the log.
    p.on_commit(w);
    expect_records(&mut p, &mut c, d + 1, w.0);
    p.stop();
}

// ---------------------------------------------------------------------------
// T6 — mid-catch-up checkpoint
// ---------------------------------------------------------------------------

/// T6. A checkpoint deletes the records a replica is in the middle of catching
/// up on. The session ends with a FIN (no ERR, nothing skipped), and the
/// replica's next HELLO is answered `RESEED_REQUIRED{oldest}` — every time.
#[test]
fn t6_checkpoint_mid_catch_up_ends_the_session_and_demands_a_reseed() {
    const K: u64 = 4;
    const CHUNK: u64 = 5;
    let rdir = tempfile::tempdir().unwrap();
    let r = Proxied::start(rdir.path(), rx_cfg(SMALL));
    let mut p = Prim::open(SMALL);
    p.fill(600, 64, 30);
    let mut cfg = ship_cfg(vec![r.addr()]);
    cfg.ring_bytes = 8 * 256;
    cfg.catchup_chunk_records = CHUNK as usize;
    p.start(cfg);

    // The test thread drives `poll`: one for the join, then K chunks.
    p.await_request(0);
    p.poll();
    for k in 1..=K {
        p.await_request(0);
        p.poll();
        wait_until(10, "the chunk to be forwarded", || {
            r.proxy.last().last_record == Some(k * CHUNK)
        });
    }
    let c = K * CHUNK + 1; // c − 1 is the last LSN shipped
    p.await_request(0); // Chunk{c, pos} is pending
    p.wal.checkpoint(Lsn(500)).unwrap();
    let oldest = p.wal.oldest_lsn().0;
    assert!(oldest > c, "precondition: oldest {oldest} > c {c}");
    assert!(
        p.sh().released().0 + 1 - oldest > 8 + CHUNK,
        "precondition: the ring cannot reach the cursor"
    );
    assert_eq!(r.proxy.session_count(), 1);

    // The next poll answers BelowOldest; the shipper reconnects at once and
    // keeps reconnecting, and every HELLO gets RESEED_REQUIRED.
    p.poll_until(20, "three sessions answered RESEED_REQUIRED", |_| {
        r.proxy.session_count() >= 4 && r.proxy.sessions()[1..4].iter().all(|s| s.first.is_some())
    });
    p.stop();

    r.proxy.assert_clean(); // dense, no ERR in either direction, ACK ≤ tap
    let logs = r.proxy.sessions();
    let s1 = &logs[0];
    assert_eq!(s1.serving, Some((1, 1)));
    assert_eq!(
        (s1.first_record, s1.last_record, s1.records),
        (Some(1), Some(c - 1), c - 1),
        "session 1 is exactly d + 1..=c − 1: no RECORD ≥ c"
    );
    assert!(s1.primary_eof, "session 1 must end with a FIN");
    assert!(s1.errs.is_empty());
    for s in &logs[1..4] {
        assert_eq!(s.hello, Some(c - 1));
        assert_eq!(
            s.first,
            Some(First::Reseed { oldest }),
            "the frame answering the next HELLO must be RESEED_REQUIRED"
        );
        assert_eq!(s.records, 0);
    }
    let (rx, results) = r.server.stop();
    assert!(results.len() >= 4, "{results:?}");
    assert!(
        matches!(results[0], Ok(())),
        "session 1 must end cleanly, got {:?}",
        results[0]
    );
    for res in &results[1..] {
        // (The last one may be the shutdown racing a HELLO.)
        match res {
            Err(ReplError::ReseedRequired { oldest: o }) => assert_eq!(o.0, oldest),
            Err(ReplError::Io(_)) | Ok(()) if std::ptr::eq(res, results.last().unwrap()) => {}
            other => panic!("every later session must be ReseedRequired, got {other:?}"),
        }
    }
    let got: Vec<(u64, Vec<u8>)> = read_all(rx.wal())
        .into_iter()
        .map(|(l, b)| (l.0, b))
        .collect();
    assert!(
        got == p.expect(1, c - 1),
        "the replica log must be exactly d + 1..=c − 1"
    );
}

/// T6b (review of #59, Required 2). After BelowOldest the primary half-closes
/// and waits for the replica's EOF. A peer that never closes and keeps sending
/// ACKs must not pin the network thread: the drain has a deadline
/// (`io_timeout`), after which the session is torn down — so the next HELLO
/// is answered `RESEED_REQUIRED`, and `Shipper::shutdown` returns promptly even
/// while the peer is still ACKing.
fn t6b_run(check_shutdown: bool) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    const IO: Duration = Duration::from_millis(400);
    let mut p = Prim::open(SMALL);
    p.fill(600, 64, 30);
    let fake = Fake::new(256);
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.ring_bytes = 8 * 256;
    cfg.catchup_chunk_records = 5;
    cfg.io_timeout = IO;
    p.start(cfg);
    let mut c = fake.hello(0);
    assert_eq!(join(&mut p, 0, &mut c), Saw::Serving { from: 1, oldest: 1 });
    p.await_request(0);
    p.poll(); // one chunk: 1..=5
    assert_eq!(c.drain_records(Duration::from_millis(100)).len(), 5);
    p.await_request(0);
    p.wal.checkpoint(Lsn(500)).unwrap();
    let oldest = p.wal.oldest_lsn().0;
    assert!(oldest > 6);
    p.poll(); // BelowOldest: the primary sends a FIN
    assert_eq!(c.next_data(Duration::from_secs(5)), Saw::Eof, "the FIN");

    // The peer does not close; it ACKs every 20 ms.
    let stop = Arc::new(AtomicBool::new(false));
    let st = Arc::clone(&stop);
    let acker = std::thread::spawn(move || {
        while !st.load(Ordering::Acquire) {
            c.ack(1);
            std::thread::sleep(Duration::from_millis(20));
        }
        drop(c);
    });
    let bound = IO + Duration::from_secs(3);
    let outcome: Result<(), String> = if check_shutdown {
        let sh = p.sh.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let t = Instant::now();
        std::thread::spawn(move || {
            sh.shutdown();
            let _ = tx.send(());
        });
        rx.recv_timeout(bound)
            .map_err(|_| format!("Shipper::shutdown did not return within {bound:?}"))
            .map(|()| eprintln!("T6b: shutdown returned after {:?}", t.elapsed()))
    } else {
        let t = Instant::now();
        match fake.try_hello(5, bound) {
            None => Err(format!(
                "no new session within {bound:?}: the peer's ACKs pinned the old one"
            )),
            Some(mut c2) => match next_polling(&mut p, &mut c2, 10) {
                Saw::Reseed { oldest: o } if o == oldest => {
                    eprintln!("T6b: RESEED_REQUIRED after {:?}", t.elapsed());
                    Ok(())
                }
                other => Err(format!(
                    "expected RESEED_REQUIRED{{{oldest}}}, got {other:?}"
                )),
            },
        }
    };
    // Release the old connection before failing, so a failure is a failure
    // and not a hang in `Drop`.
    stop.store(true, Ordering::Release);
    acker.join().unwrap();
    if let Err(e) = outcome {
        panic!("{e}");
    }
    p.stop();
}

#[test]
fn t6b_a_peer_that_keeps_acking_after_the_fin_still_gets_reseed_required() {
    t6b_run(false);
}

#[test]
fn t6b_a_peer_that_keeps_acking_after_the_fin_cannot_hang_shutdown() {
    t6b_run(true);
}

// ---------------------------------------------------------------------------
// T8a — the serve ceiling is `released`
// ---------------------------------------------------------------------------

/// T8a. `1..=R` is committed before the shipper exists, so the ring starts at
/// `R + 1` and the lagging replica MUST be served from the log. Records above
/// `R` are then appended, captured and **committed without `on_commit`**: they
/// are durable and in the log, but not released. Catch-up must stop at `R`.
#[test]
fn t8a_catch_up_stops_at_the_released_watermark() {
    const R: u64 = 40;
    let mut p = Prim::open(SMALL);
    p.fill(R, 64, 10);
    let fake = Fake::new(256);
    p.start(ship_cfg(vec![fake.addr()]));
    let mut c = fake.hello(10);
    assert_eq!(
        join(&mut p, 0, &mut c),
        Saw::Serving {
            from: 11,
            oldest: 1
        }
    );
    for _ in 0..10 {
        p.append(64);
    }
    let w = p.commit_only(); // durable to R + 10; released stays R
    assert_eq!((p.sh().released().0, p.wal.durable_lsn().0), (R, R + 10));

    p.await_request(0); // Chunk{11}: the lag + one more record fits one chunk
    if common::io_probe_available() {
        let ((), io) = measure_io(|| p.poll());
        assert!(read_the_log(io), "that poll must have read the log");
    } else {
        p.poll();
    }
    expect_records(&mut p, &mut c, 11, R);
    let extra = quiet(&mut p, &mut c, Duration::from_millis(200));
    assert!(
        extra.is_empty(),
        "R1: nothing above the released watermark {R} may be served, got {extra:?}"
    );
    // The release makes them shippable.
    p.on_commit(w);
    expect_records(&mut p, &mut c, R + 1, R + 10);
    p.stop();
}

// ---------------------------------------------------------------------------
// T9 / T16 — an unreadable committed record
// ---------------------------------------------------------------------------

const T9_LEN: usize = 100;

/// A primary with 400 committed 100-byte records in 4 KiB segments, one of
/// which — sealed, and far below anything a ring could hold — has a flipped
/// payload byte. Returns the corrupt LSN and its segment's last LSN.
fn corrupt_primary() -> (Prim, u64, u64) {
    let mut p = Prim::open(SMALL);
    p.fill(400, T9_LEN, 20);
    let bases = segment_bases(p.dir.path());
    let (base, next_base) = (bases[2], bases[3]);
    let bad = base + 10;
    assert!(bad + 5 < next_base);
    // After open (recovery would refuse it): flip one payload byte on disk.
    let path = segment_path(p.dir.path(), base);
    let mut bytes = std::fs::read(&path).unwrap();
    let off = 64 + (bad - base) * framed(T9_LEN) + 20 + 3;
    bytes[off as usize] ^= 0x40;
    std::fs::write(&path, &bytes).unwrap();
    (p, bad, next_base - 1)
}

/// T9. Replica C is streaming; replica A's catch-up runs into the unreadable
/// record; replica B then asks for a chunk above it in the same segment.
#[test]
fn t9_unreadable_committed_record_is_loud_sticky_and_local() {
    let (mut p, bad, seg_last) = corrupt_primary();
    let last = p.wal.durable_lsn().0;
    // C already holds the whole log (fed by a throwaway fake primary — this
    // primary could not serve it the damaged prefix), so it streams.
    let cdir = tempfile::tempdir().unwrap();
    let c = feed_prefix(cdir.path(), &p, last);
    let (fa, fb) = (Fake::new(256), Fake::new(256));
    let mut cfg = ship_cfg(vec![fa.addr(), fb.addr(), c.addr()]);
    cfg.catchup_chunk_records = 1000;
    // A and B do not come back by themselves within the test.
    cfg.reconnect_backoff = Duration::from_secs(30);
    cfg.max_reconnect_backoff = Duration::from_secs(30);
    p.start(cfg);
    p.poll_until(20, "C to stream", |p| {
        p.sh().replica_status(2) == Some(ReplicaStatus::Streaming)
    });
    assert_eq!(c.proxy.session_count(), 1);

    // A: catch-up from below the bad record.
    let a_from = bad - 30;
    let mut a = fa.hello(a_from - 1);
    assert_eq!(
        join(&mut p, 0, &mut a),
        Saw::Serving {
            from: a_from,
            oldest: 1
        }
    );
    p.await_request(0);
    let (res, io) = if common::io_probe_available() {
        let (r, io) = measure_io(|| p.try_poll());
        (r, Some(io))
    } else {
        (p.try_poll(), None)
    };
    match res {
        Err(ReplError::PrimaryLogUnreadable { lsn }) => assert_eq!(lsn.0, bad),
        other => panic!("poll must return PrimaryLogUnreadable{{{bad}}}, got {other:?}"),
    }
    if let Some(io) = io {
        assert!(read_the_log(io), "a reader ran");
    }
    wait_until(10, "A to be Failed", || {
        p.sh().replica_status(0) == Some(ReplicaStatus::Failed)
    });
    let a_frames = a.drain(Duration::from_millis(200));
    assert!(
        a_frames
            .iter()
            .all(|s| !matches!(s, Saw::Record(l, _) if *l >= bad)),
        "nothing ≥ {bad} may reach A's wire: {a_frames:?}"
    );
    assert_eq!(a_frames.last(), Some(&Saw::Eof), "A's session must end");

    // B: its cursor is above the bad record, in the same segment. The sticky
    // failure answers it without reading.
    let b_from = bad + 3;
    assert!(b_from <= seg_last);
    let mut b = fb.hello(b_from - 1);
    assert_eq!(
        join(&mut p, 1, &mut b),
        Saw::Serving {
            from: b_from,
            oldest: 1
        }
    );
    p.await_request(1);
    if common::io_probe_available() {
        let (res, io) = measure_io(|| p.try_poll());
        assert!(
            matches!(res, Err(ReplError::PrimaryLogUnreadable { lsn }) if lsn.0 == bad),
            "the sticky failure must be reported for B's request too: {res:?}"
        );
        assert!(
            !read_the_log(io),
            "the sticky ReadFailed must not read (rchar {})",
            io.rchar
        );
    } else {
        assert!(p.try_poll().is_err());
    }
    let b_frames = b.drain(Duration::from_millis(200));
    assert!(
        b_frames.iter().all(|s| !matches!(s, Saw::Record(..))),
        "B must get no record: {b_frames:?}"
    );
    // The sticky state alone is not an error.
    assert!(p.try_poll().is_ok());

    // C is unaffected: same session, and it keeps receiving new commits.
    for _ in 0..5 {
        for _ in 0..4 {
            p.append(T9_LEN);
        }
        p.commit();
    }
    let w = p.wal.durable_lsn();
    p.poll_until(20, "C to ack the new commits", |p| {
        p.sh().replica_acked_lsn(2) == Some(w)
    });
    assert_eq!(c.proxy.session_count(), 1, "C's session must survive");
    assert_eq!(p.sh().replica_status(2), Some(ReplicaStatus::Streaming));
    p.stop();
    c.proxy.assert_clean();
    assert_eq!(c.proxy.last().last_record, Some(w.0));
}

/// A real receiver (behind a proxy) whose WAL already holds the model's
/// records `1..=last`, written by a throwaway fake primary.
fn feed_prefix(dir: &std::path::Path, p: &Prim, last: u64) -> Proxied {
    let (addr, h) = common::spawn_receiver(dir, rx_cfg(SMALL), 1);
    let mut fp = common::FakePrimary::connect(addr);
    assert_eq!(fp.hello(), 0);
    fp.serving(1);
    let recs: Vec<(Lsn, Vec<u8>)> = p
        .expect(1, last)
        .into_iter()
        .map(|(l, b)| (Lsn(l), b))
        .collect();
    fp.records(&recs);
    fp.await_ack(last);
    drop(fp.finish());
    let (rx, _) = h.join().unwrap();
    drop(rx);
    Proxied::start(dir, rx_cfg(SMALL))
}

/// T9, non-sticky variant. An I/O error on one answer (the segment file is
/// renamed away) fails that answer only; once it is back, the next request
/// reads normally.
#[test]
fn t9_an_io_error_fails_one_answer_and_is_not_sticky() {
    let mut p = Prim::open(SMALL);
    p.fill(300, T9_LEN, 20);
    let fake = Fake::new(256);
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.catchup_chunk_records = 20;
    p.start(cfg);
    let last = p.wal.durable_lsn().0;
    let bases = segment_bases(p.dir.path());
    let from = bases[2] + 4;
    let path = segment_path(p.dir.path(), bases[2]);
    let away = path.with_extension("away");

    let mut c = fake.hello(from - 1);
    assert_eq!(join(&mut p, 0, &mut c), Saw::Serving { from, oldest: 1 });
    p.await_request(0);
    std::fs::rename(&path, &away).unwrap();
    match p.try_poll() {
        Err(ReplError::PrimaryLogUnreadable { lsn }) => assert_eq!(lsn.0, from),
        other => panic!("expected PrimaryLogUnreadable{{{from}}}, got {other:?}"),
    }
    std::fs::rename(&away, &path).unwrap();
    let frames = c.drain(Duration::from_millis(300));
    assert!(frames.iter().all(|s| !matches!(s, Saw::Record(..))));
    assert_eq!(frames.last(), Some(&Saw::Eof), "that session ends");

    // The replica reconnects (bounded by its backoff) and is served normally.
    let mut c = fake.hello(from - 1);
    assert_eq!(join(&mut p, 0, &mut c), Saw::Serving { from, oldest: 1 });
    expect_records(&mut p, &mut c, from, last);
    p.stop();
}

/// T16. `poll` finishes its pass: one answer failing (A's chunk hits the
/// unreadable record) does not leave the other requests of that pass
/// unanswered. Two shapes, each in both id orders:
/// - B's **join** is pending together with A's chunk request ⇒ B gets
///   `SERVING` within 1 s of that ONE `poll`, and A gets no record;
/// - B's **chunk** request is pending together with A's ⇒ both sessions get
///   their answer from that ONE `poll` (both end: the failure disables
///   catch-up reads).
#[test]
fn t16_poll_finishes_its_pass_after_an_error() {
    for b_is_join in [true, false] {
        for a_first in [true, false] {
            let (mut p, bad, _) = corrupt_primary();
            let (fa, fb) = (Fake::new(256), Fake::new(256));
            let (ia, ib) = if a_first { (0, 1) } else { (1, 0) };
            let mut addrs = vec![fa.addr(); 2];
            addrs[ib] = fb.addr();
            let mut cfg = ship_cfg(addrs);
            cfg.reconnect_backoff = Duration::from_secs(30);
            cfg.max_reconnect_backoff = Duration::from_secs(30);
            cfg.catchup_chunk_records = 1000;
            p.start(cfg);
            let what = format!("b_is_join={b_is_join} a_first={a_first}");
            let a_serving = Saw::Serving {
                from: bad - 19,
                oldest: 1,
            };
            let b_serving = Saw::Serving {
                from: bad - 9,
                oldest: 1,
            };

            let mut a = fa.hello(bad - 20);
            let mut b;
            if b_is_join {
                // A is joined first; then B's join and A's chunk are pending.
                assert_eq!(join(&mut p, ia, &mut a), a_serving, "{what}");
                b = fb.hello(bad - 10);
            } else {
                // Both joins are answered by one (non-reading) poll; then both
                // chunk requests are pending.
                b = fb.hello(bad - 10);
                p.await_request(ia);
                p.await_request(ib);
                p.poll();
                assert_eq!(a.next_data(Duration::from_secs(5)), a_serving, "{what}");
                assert_eq!(b.next_data(Duration::from_secs(5)), b_serving, "{what}");
            }
            p.await_request(ia);
            p.await_request(ib);
            let res = p.try_poll(); // the ONE poll
            assert!(
                matches!(res, Err(ReplError::PrimaryLogUnreadable { lsn }) if lsn.0 == bad),
                "{what}: the poll must report the failure: {res:?}"
            );
            // No further poll: both answers must already be on their way.
            if b_is_join {
                assert_eq!(
                    b.next_data(Duration::from_secs(1)),
                    b_serving,
                    "{what}: B's join must be answered by the same poll"
                );
            } else {
                let b_frames = b.drain(Duration::from_secs(1));
                assert_eq!(
                    b_frames.last(),
                    Some(&Saw::Eof),
                    "{what}: B's chunk request must be answered by the same poll: {b_frames:?}"
                );
                assert!(b_frames.iter().all(|s| !matches!(s, Saw::Record(..))));
            }
            let a_frames = a.drain(Duration::from_secs(1));
            assert_eq!(
                a_frames.last(),
                Some(&Saw::Eof),
                "{what}: A's chunk request must be answered by the same poll: {a_frames:?}"
            );
            assert!(
                a_frames.iter().all(|s| !matches!(s, Saw::Record(..))),
                "{what}: A must get no record: {a_frames:?}"
            );
            p.stop();
        }
    }
}

// ---------------------------------------------------------------------------
// T10 — cadence under load
// ---------------------------------------------------------------------------

/// T10. A tight commit loop that calls `poll(&wal)` **only** after each
/// `on_commit` (the normative cadence). A fresh real `Receiver` must reach
/// `SERVING` within its `idle_timeout` and converge.
#[test]
fn t10_join_is_decided_under_a_tight_commit_loop() {
    let idle = Duration::from_secs(1);
    let rdir = tempfile::tempdir().unwrap();
    let mut rc = rx_cfg(SMALL);
    rc.idle_timeout = Some(idle);
    let r = Proxied::start(rdir.path(), rc);
    let mut p = Prim::open(SMALL);
    let mut cfg = ship_cfg(vec![r.addr()]);
    cfg.ring_bytes = 256 * 256;
    p.start(cfg);
    let t = Instant::now();
    let mut hello_at = None;
    let mut serving_after = None;
    while t.elapsed() < Duration::from_secs(3) {
        p.append(32);
        p.commit(); // commit + on_commit + poll — and nothing else
        if r.proxy.session_count() > 0 {
            let log = r.proxy.sessions()[0].clone();
            if log.hello.is_some() && hello_at.is_none() {
                hello_at = Some(Instant::now());
            }
            if log.serving.is_some() && serving_after.is_none() {
                serving_after = Some(hello_at.unwrap().elapsed());
            }
        }
    }
    let after = serving_after.expect("the replica never reached SERVING under load");
    assert!(
        after < idle,
        "SERVING took {after:?}, past the receiver's idle_timeout"
    );
    let w = p.wal.durable_lsn();
    p.poll_until(30, "the replica to converge", |p| {
        p.sh().replica_acked_lsn(0) == Some(w)
    });
    p.stop();
    r.proxy.assert_clean();
    assert_eq!(r.proxy.session_count(), 1);
    let (rx, results) = r.server.stop();
    assert_eq!(results.len(), 1);
    no_inband_errors(&results);
    assert_eq!(rx.durable_lsn(), w);
}

// ---------------------------------------------------------------------------
// T11 — teardown liveness
// ---------------------------------------------------------------------------

/// T11 *(liveness)*. The socket is cut mid-catch-up, several times, with
/// polls in between; each new session resumes from the replica's HELLO and the
/// last one streams to convergence. The proxy counts exactly the induced
/// sessions.
#[test]
fn t11_teardown_mid_catch_up_then_the_next_session_streams() {
    const CUTS: usize = 3;
    let rdir = tempfile::tempdir().unwrap();
    let r = Proxied::start(rdir.path(), rx_cfg(SMALL));
    let mut p = Prim::open(SMALL);
    p.fill(900, 64, 30);
    let mut cfg = ship_cfg(vec![r.addr()]);
    cfg.catchup_chunk_records = 10;
    p.start(cfg);
    for cut in 1..=CUTS {
        // A few chunks into this session's catch-up…
        p.poll_until(20, "catch-up to be under way", |p| {
            r.proxy.session_count() == cut
                && r.proxy.last().records >= 30
                && p.sh().replica_status(0) == Some(ReplicaStatus::CatchingUp)
        });
        r.proxy.cut_current();
        // …then keep polling across the teardown and the reconnect.
        p.poll_until(20, "the reconnect", |_| r.proxy.session_count() == cut + 1);
    }
    let w = p.wal.durable_lsn();
    p.poll_until(60, "the last session to converge", |p| {
        p.sh().replica_acked_lsn(0) == Some(w)
    });
    for _ in 0..5 {
        p.append(64);
        p.commit();
    }
    let w = p.wal.durable_lsn();
    p.poll_until(20, "streaming after the cutover", |p| {
        p.sh().replica_acked_lsn(0) == Some(w)
    });
    assert_eq!(p.sh().replica_status(0), Some(ReplicaStatus::Streaming));
    p.stop();
    assert_eq!(
        r.proxy.session_count(),
        CUTS + 1,
        "exactly the induced sessions"
    );
    // Density and SERVING honesty hold in every session; the cut sessions end
    // on a transport error, never an in-band one.
    r.proxy.assert_clean();
    let (rx, results) = r.server.stop();
    assert_eq!(results.len(), CUTS + 1);
    no_inband_errors(&results);
    let got: Vec<(u64, Vec<u8>)> = read_all(rx.wal())
        .into_iter()
        .map(|(l, b)| (l.0, b))
        .collect();
    assert!(got == p.model);
}

// ---------------------------------------------------------------------------
// T12 — the LSN domain
// ---------------------------------------------------------------------------

#[test]
fn t12_shipper_new_enforces_the_lsn_domain() {
    let fake = Fake::new(256);
    // Seed MAX_LSN + 1 ⇒ last_lsn == MAX_LSN: the next record could not be
    // captured.
    let p = Prim::seeded(ring::MAX_LSN + 1, SMALL);
    assert!(matches!(
        Shipper::new(ship_cfg(vec![fake.addr()]), &p.wal),
        Err(ReplError::LsnDomainExhausted)
    ));
    // A WAL at u64::MAX keeps its own error (checked first).
    let mut p = Prim::seeded(u64::MAX, SMALL);
    p.append(8);
    p.commit_only();
    assert!(matches!(
        Shipper::new(ship_cfg(vec![fake.addr()]), &p.wal),
        Err(ReplError::LsnExhausted {
            last: Lsn(u64::MAX)
        })
    ));
    // Seed MAX_LSN ⇒ accepted.
    let p = Prim::seeded(ring::MAX_LSN, SMALL);
    Shipper::new(ship_cfg(vec![fake.addr()]), &p.wal)
        .unwrap()
        .shutdown();
}

/// Seeded at `MAX_LSN`: exactly the record `MAX_LSN` is shipped; the next
/// capture makes the shipper terminal — `ERR(Protocol)`, `poll` ⇒
/// `Err(LsnDomainExhausted)`, and nothing above `MAX_LSN` is ever sent.
#[test]
fn t12_crossing_the_lsn_domain_is_terminal() {
    let m = ring::MAX_LSN;
    let mut p = Prim::seeded(m, SMALL);
    let fake = Fake::new(256);
    p.start(ship_cfg(vec![fake.addr()]));
    let mut c = fake.hello(m - 1);
    assert_eq!(
        next_polling(&mut p, &mut c, 10),
        Saw::Serving { from: m, oldest: m }
    );
    assert_eq!(p.append(16), m);
    p.commit();
    expect_records(&mut p, &mut c, m, m);
    // The WAL itself goes on; the shipper cannot.
    assert_eq!(p.append(16), m + 1);
    let w = p.commit_only();
    p.on_commit(w);
    assert!(matches!(p.try_poll(), Err(ReplError::LsnDomainExhausted)));
    let rest = c.drain(Duration::from_millis(300));
    assert!(
        rest.iter().all(|s| !matches!(s, Saw::Record(..))),
        "nothing above MAX_LSN may be sent: {rest:?}"
    );
    assert!(
        rest.iter()
            .any(|s| matches!(s, Saw::Err(ErrCode::Protocol, msg) if msg.contains("LSN domain"))),
        "the session must end with ERR(Protocol): {rest:?}"
    );
    wait_until(5, "Failed", || {
        p.sh().replica_status(0) == Some(ReplicaStatus::Failed)
    });
    // It stays terminal: a reconnecting replica is refused, poll keeps failing.
    let mut c = fake.hello(m);
    assert!(matches!(
        c.next_data(Duration::from_secs(5)),
        Saw::Err(ErrCode::Protocol, _)
    ));
    assert!(matches!(p.try_poll(), Err(ReplError::LsnDomainExhausted)));
    p.stop();
}

/// The in-flight variant: a replica's chunk request is pending when the
/// capture crosses the bound. It is answered Terminal — no read, no panic,
/// no record.
#[test]
fn t12_a_chunk_request_in_flight_at_the_crossing_gets_terminal() {
    let m = ring::MAX_LSN;
    let mut p = Prim::seeded(m - 6, SMALL);
    p.fill(5, 16, 5); // m−6 ..= m−2, committed before the shipper exists
    let fake = Fake::new(256);
    p.start(ship_cfg(vec![fake.addr()]));
    let mut c = fake.hello(m - 7);
    assert_eq!(
        join(&mut p, 0, &mut c),
        Saw::Serving {
            from: m - 6,
            oldest: m - 6
        }
    );
    p.await_request(0); // Chunk{m − 6} is in flight; do not poll
    assert_eq!(p.append(16), m - 1);
    assert_eq!(p.append(16), m);
    assert_eq!(p.append(16), m + 1); // crosses: terminal
    let w = p.commit_only();
    p.on_commit(w);
    let (res, io) = if common::io_probe_available() {
        let (r, io) = measure_io(|| p.try_poll());
        (r, Some(io))
    } else {
        (p.try_poll(), None)
    };
    assert!(matches!(res, Err(ReplError::LsnDomainExhausted)), "{res:?}");
    if let Some(io) = io {
        assert!(
            !read_the_log(io),
            "a Terminal answer must not read ({io:?})"
        );
    }
    let rest = c.drain(Duration::from_millis(300));
    assert!(
        rest.iter().all(|s| !matches!(s, Saw::Record(..))),
        "a terminal shipper serves nothing: {rest:?}"
    );
    assert!(
        rest.iter()
            .any(|s| matches!(s, Saw::Err(ErrCode::Protocol, _))),
        "{rest:?}"
    );
    p.stop();
}

/// The released ceiling is the ring's **clamped** watermark, never the raw
/// `on_commit` argument. Here a record is committed but not captured (so the
/// raw `w` is one above the last capture, as it is at the LSN-domain
/// crossing): catch-up must stop at the last captured-and-released record and
/// cut over — a raw-`w` ceiling would read past it and reach the "impossible"
/// `Ahead`.
#[test]
fn t12_the_ceiling_is_the_clamped_released_watermark() {
    let mut p = Prim::open(SMALL);
    p.fill(30, 40, 10);
    let fake = Fake::new(256);
    p.start(ship_cfg(vec![fake.addr()]));
    let mut c = fake.hello(5);
    assert_eq!(join(&mut p, 0, &mut c), Saw::Serving { from: 6, oldest: 1 });
    p.await_request(0);
    p.append(40); // 31: captured
    // 32: appended and committed, but never captured.
    let lsn = p.wal.append(&payload(32, 40)).unwrap();
    p.model.push((lsn.0, payload(32, 40)));
    let w = p.commit_only();
    assert_eq!(w.0, 32);
    p.on_commit(w); // raw w = 32; the ring clamps to 31
    assert_eq!(p.sh().released().0, 31);
    expect_records(&mut p, &mut c, 6, 31);
    // …and it cuts over at 32 (a ceiling above the clamp would leave the
    // cursor "ahead" of the ring, waiting).
    p.poll_until(5, "the cutover", |p| {
        p.sh().replica_status(0) == Some(ReplicaStatus::Streaming)
    });
    p.stop();
}

// ---------------------------------------------------------------------------
// T13 — reader cap and round-robin
// ---------------------------------------------------------------------------

/// T13. Two replicas lag far beyond a chunk; the cap is one reader per `poll`.
/// Each round both have a chunk request pending and ONE `poll` runs: exactly
/// one of them receives records, and they alternate.
#[test]
fn t13_one_reader_per_poll_round_robin() {
    const CHUNK: usize = 10;
    const ROUNDS: usize = 8;
    let mut p = Prim::open(SMALL);
    p.fill(600, 64, 30);
    let fakes = [Fake::new(256), Fake::new(256)];
    let mut cfg = ship_cfg(vec![fakes[0].addr(), fakes[1].addr()]);
    cfg.catchup_chunk_records = CHUNK;
    cfg.catchup_replicas_per_poll = 1;
    p.start(cfg);
    let mut conns: Vec<FakeConn> = fakes.iter().map(|f| f.hello(0)).collect();
    // One poll answers both joins (a join never reads).
    p.await_request(0);
    p.await_request(1);
    p.poll();
    for c in &mut conns {
        assert_eq!(
            c.next_data(Duration::from_secs(5)),
            Saw::Serving { from: 1, oldest: 1 }
        );
    }
    let mut next = [1u64, 1u64];
    let settle = |conns: &mut Vec<FakeConn>, next: &mut [u64; 2]| -> [usize; 2] {
        let mut got = [0, 0];
        for (id, c) in conns.iter_mut().enumerate() {
            for (l, bytes) in c.drain_records(Duration::from_millis(40)) {
                assert_eq!(l, next[id], "replica {id}: dense");
                assert!(bytes == payload(l, 64));
                next[id] += 1;
                got[id] += 1;
            }
        }
        got
    };
    let mut winners = Vec::new();
    for round in 0..ROUNDS {
        p.await_request(0);
        p.await_request(1);
        if common::io_probe_available() {
            let ((), io) = measure_io(|| p.poll());
            assert!(
                read_the_log(io) && io.rchar <= 4096 + (CHUNK as u64 + 2) * framed(64) + 4096,
                "round {round}: one answer's worth of reading, got {io:?}"
            );
        } else {
            p.poll();
        }
        let got = settle(&mut conns, &mut next);
        let winner = match got {
            [n, 0] if n == CHUNK => 0,
            [0, n] if n == CHUNK => 1,
            other => {
                panic!("round {round}: exactly one replica must get one chunk, got {other:?}")
            }
        };
        winners.push(winner);
    }
    for pair in winners.windows(2) {
        assert_ne!(pair[0], pair[1], "the replicas must alternate: {winners:?}");
    }
    p.stop();
}

// ---------------------------------------------------------------------------
// T15 — heartbeat rules
// ---------------------------------------------------------------------------

fn t15_receiver(dir: &std::path::Path) -> Proxied {
    let mut rc = rx_cfg(SMALL);
    rc.idle_timeout = Some(3 * HB);
    Proxied::start(dir, rc)
}

/// T15(a). Nothing is sent before `SERVING`: with polls withheld for two
/// heartbeats after the HELLO, the first primary→replica frame is still
/// `SERVING`, and the session survives.
#[test]
fn t15a_nothing_is_sent_before_serving() {
    let rdir = tempfile::tempdir().unwrap();
    let r = t15_receiver(rdir.path());
    let mut p = Prim::open(SMALL);
    p.fill(5, 32, 5);
    p.start(ship_cfg(vec![r.addr()]));
    p.await_request(0);
    wait_until(5, "the HELLO", || r.proxy.last().hello.is_some());
    std::thread::sleep(2 * HB);
    assert_eq!(
        r.proxy.last().first,
        None,
        "nothing may be sent before SERVING"
    );
    let w = p.wal.durable_lsn();
    p.poll_until(20, "convergence", |p| {
        p.sh().replica_acked_lsn(0) == Some(w)
    });
    p.stop();
    r.proxy.assert_clean();
    assert_eq!(r.proxy.session_count(), 1);
    assert!(matches!(
        r.proxy.last().first,
        Some(First::Serving { from: 1, .. })
    ));
}

/// T15(b). After `SERVING`, a chunk request waits unanswered (no polls) for
/// five heartbeats: the session-level timer keeps heartbeating, so the
/// receiver (idle_timeout = 3 heartbeats) keeps the session.
#[test]
fn t15b_heartbeats_flow_while_a_chunk_request_awaits_its_answer() {
    let rdir = tempfile::tempdir().unwrap();
    let r = t15_receiver(rdir.path());
    let mut p = Prim::open(SMALL);
    p.fill(200, 64, 20);
    let mut cfg = ship_cfg(vec![r.addr()]);
    cfg.catchup_chunk_records = 50;
    p.start(cfg);
    p.await_request(0);
    p.poll(); // the join: SERVING, then Chunk{1} is posted
    wait_until(5, "SERVING", || r.proxy.last().serving.is_some());
    p.await_request(0);
    let before = r.proxy.last().heartbeats;
    std::thread::sleep(5 * HB); // no poll
    assert!(
        p.sh().request_pending(0),
        "the chunk request is still waiting"
    );
    let beats = r.proxy.last().heartbeats - before;
    assert!(
        beats >= 3,
        "expected heartbeats while awaiting an answer, saw {beats}"
    );
    assert_eq!(r.proxy.session_count(), 1, "the session must survive");
    let w = p.wal.durable_lsn();
    p.poll_until(20, "convergence", |p| {
        p.sh().replica_acked_lsn(0) == Some(w)
    });
    p.stop();
    r.proxy.assert_clean();
    assert_eq!(r.proxy.session_count(), 1);
    let (_rx, results) = r.server.stop();
    assert_eq!(results.len(), 1);
    no_inband_errors(&results);
}

/// T15(c). A Wait loop that is woken more often than once per heartbeat — by
/// releases that do not reach its cursor — still heartbeats: the timer belongs
/// to the session, not to each wait. (A fake replica: a real `Receiver` can
/// never be durable above the primary's released watermark for this long.)
#[test]
fn t15c_the_heartbeat_timer_spans_a_wait_loop() {
    let mut p = Prim::open(SMALL);
    let fake = Fake::new(256);
    p.start(ship_cfg(vec![fake.addr()]));
    p.fill(10, 32, 5);
    // 40 single-record commits the shipper is not told about yet.
    let mut ws = Vec::new();
    for _ in 0..40 {
        p.append(32);
        ws.push(p.commit_only());
    }
    let d = p.wal.durable_lsn().0;
    let mut c = fake.hello(d);
    assert_eq!(
        next_polling(&mut p, &mut c, 10),
        Saw::Serving {
            from: d + 1,
            oldest: 1
        }
    );
    // Release them one by one, four per heartbeat, for five heartbeats: every
    // release wakes the waiting network thread, none reaches its cursor.
    let mut beats = Vec::new();
    let t = Instant::now();
    for w in ws.iter().take(20) {
        p.on_commit(*w);
        p.poll();
        for s in c.drain(HB / 4) {
            match s {
                Saw::Heartbeat(_) => beats.push(t.elapsed()),
                other => panic!("a waiting replica must get only heartbeats, got {other:?}"),
            }
        }
    }
    assert!(t.elapsed() >= 5 * HB);
    assert!(
        beats.len() >= 3,
        "expected ≥ 3 heartbeats across the wait loop, saw {beats:?}"
    );
    p.stop();
}

// ---------------------------------------------------------------------------
// T17 — at least one record per chunk
// ---------------------------------------------------------------------------

/// T17. A byte budget smaller than one record (64 B, and 0 B) still makes
/// progress: exactly one record per chunk, and the replica converges.
#[test]
fn t17_a_chunk_always_holds_at_least_one_record() {
    let wcfg = WalConfig {
        segment_size: 64 * 1024,
        max_record_size: 1024,
    };
    for budget in [64usize, 0] {
        let mut p = Prim::open(wcfg);
        p.fill(24, 1024, 6);
        let fake = Fake::new(1024);
        let mut cfg = ship_cfg(vec![fake.addr()]);
        cfg.slot_bytes = 1024;
        cfg.ring_bytes = 8 * 1024;
        cfg.catchup_chunk_bytes = budget;
        p.start(cfg);
        let mut c = fake.hello(0);
        assert_eq!(join(&mut p, 0, &mut c), Saw::Serving { from: 1, oldest: 1 });
        for lsn in 1..=24u64 {
            p.await_request(0);
            p.poll(); // one chunk
            let got = c.drain_records(Duration::from_millis(30));
            assert_eq!(
                got,
                p.expect(lsn, lsn),
                "budget {budget}: exactly one record per chunk (at lsn {lsn})"
            );
        }
        p.poll_until(5, "the cutover", |p| {
            p.sh().replica_status(0) == Some(ReplicaStatus::Streaming)
        });
        p.append(1024);
        p.commit();
        expect_records(&mut p, &mut c, 25, 25);
        p.stop();
    }
}

#[test]
fn t17_shipper_new_rejects_a_zero_cap_and_zero_records() {
    let p = Prim::open(SMALL);
    let fake = Fake::new(256);
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.catchup_replicas_per_poll = 0;
    assert!(matches!(
        Shipper::new(cfg, &p.wal),
        Err(ReplError::Protocol(_))
    ));
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.catchup_chunk_records = 0;
    assert!(matches!(
        Shipper::new(cfg, &p.wal),
        Err(ReplError::Protocol(_))
    ));
    // A zero BYTE budget is legal: a chunk always takes one record.
    let mut cfg = ship_cfg(vec![fake.addr()]);
    cfg.catchup_chunk_bytes = 0;
    Shipper::new(cfg, &p.wal).unwrap().shutdown();
}
