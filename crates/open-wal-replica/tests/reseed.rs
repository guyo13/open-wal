//! RM4 — re-seed (§10, §10.1, §10.2 of `docs/replica_design_v1.md`; kickoff E
//! tests E1–E10).
//!
//! **Oracles are independent of the code under test**: the replica's own WAL
//! (read back with a plain `Wal`), the directory on disk, the forwarding
//! [`Proxy`]'s per-session log, a hand-driven [`Fake`] replica's view of the
//! wire, the test's own record model ([`Prim::model`]) and — for E9 — the
//! process's syscall trace. `Shipper::request_pending` / `replica_status` are
//! used for sequencing only; `retention_floor`, `replica_needs_reseed` and
//! `replica_acked_lsn` are the *subject* of E6 and inputs of E7's checkpoint
//! policy, never a stand-in for the wire.
#![cfg(not(loom))]

mod common;

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{
    DurableTap, Fake, FakeConn, FakePrimary, First, Proxy, RunOutcome, SMALL, Saw, SeededServer,
    payload, read_all, read_dir, segment_bases, segment_path, stale_dirs, stale_path, wait_until,
};
use open_wal::{DurabilityObserver, Lsn, NullObserver, Wal, WalConfig, WalError};
use open_wal_replica::wire::ErrCode;
use open_wal_replica::{
    Receiver, ReceiverConfig, ReplError, ReplicaStatus, Shipper, ShipperConfig,
};

const HB: Duration = Duration::from_millis(50);
/// 100-byte payloads: 120 framed bytes, 33 records per 4 KiB segment.
const LEN: usize = 100;

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

fn rx_cfg() -> ReceiverConfig {
    let mut c = ReceiverConfig::new(SMALL);
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
    /// `(lsn, payload)` of every record appended, in order (LSNs from 1).
    model: Vec<(Lsn, Vec<u8>)>,
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

    fn start(&mut self, cfg: ShipperConfig) {
        self.sh = Some(Shipper::new(cfg, &self.wal).unwrap());
    }

    /// Shut the shipper down (the replicas see their sessions end).
    fn stop(&mut self) {
        if let Some(sh) = self.sh.take() {
            sh.shutdown();
        }
    }

    fn sh(&self) -> &Shipper {
        self.sh.as_ref().unwrap()
    }

    fn append(&mut self, len: usize) -> u64 {
        let next = self.wal.last_lsn().0 + 1;
        let p = payload(next, len);
        let lsn = self.wal.append(&p).unwrap();
        assert_eq!(lsn.0, next);
        if let Some(sh) = &mut self.sh {
            sh.capture(lsn, &p);
        }
        self.model.push((lsn, p));
        lsn.0
    }

    fn poll(&mut self) {
        if let Some(sh) = &mut self.sh {
            sh.poll(&self.wal).unwrap();
        }
    }

    /// The §9.1 cadence: `commit`, `on_commit`, `poll`.
    fn commit(&mut self) -> Lsn {
        let w = self.wal.commit().unwrap();
        if let Some(sh) = &mut self.sh {
            sh.on_commit(w);
            sh.poll(&self.wal).unwrap();
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

    /// Append (8 records per commit) until the log has rolled `n` more
    /// segments; `after_roll` runs after every commit that rolled.
    fn roll(&mut self, n: usize, after_roll: &mut dyn FnMut(&mut Prim)) {
        let top = |p: &Prim| *segment_bases(p.dir.path()).last().unwrap();
        let mut rolled = 0;
        let mut last = top(self);
        while rolled < n {
            for _ in 0..8 {
                self.append(LEN);
            }
            self.commit();
            let now = top(self);
            if now != last {
                last = now;
                rolled += 1;
                after_roll(self);
            }
        }
    }

    fn durable(&self) -> u64 {
        self.wal.durable_lsn().0
    }

    fn oldest(&self) -> u64 {
        self.wal.oldest_lsn().0
    }

    /// The model's records with `from ≤ lsn ≤ to`.
    fn expect(&self, from: u64, to: u64) -> Vec<(Lsn, Vec<u8>)> {
        self.model
            .iter()
            .filter(|(l, _)| (from..=to).contains(&l.0))
            .cloned()
            .collect()
    }

    fn begin_reseed(&mut self, id: usize, s: u64) -> Result<(), ReplError> {
        self.sh
            .as_mut()
            .unwrap()
            .begin_reseed(&self.wal, id, Lsn(s))
    }

    fn end_reseed(&mut self, id: usize) {
        self.sh.as_mut().unwrap().end_reseed(id);
    }

    fn floor(&self) -> Option<u64> {
        self.sh().retention_floor().map(|l| l.0)
    }

    fn acked(&self, id: usize) -> u64 {
        self.sh().replica_acked_lsn(id).unwrap().0
    }

    /// Wait (without polling) until replica `id` has a request posted.
    fn await_request(&self, id: usize) {
        wait_until(10, "a pending request", || self.sh().request_pending(id));
    }

    /// Keep polling until `f` holds.
    fn poll_until(&mut self, secs: u64, what: &str, mut f: impl FnMut(&Prim) -> bool) {
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
}

/// Poll the primary until the receiver's `run` returns by itself.
fn join_polling(p: &mut Prim, srv: SeededServer) -> RunOutcome {
    p.poll_until(20, "Receiver::run to return", |_| srv.finished());
    srv.join()
}

/// Poll until replica 0 has acked `end`. A `run` that ends first is a failure
/// (e.g. the re-seeded replica was answered `RESEED_REQUIRED` again).
fn converge(p: &mut Prim, srv: SeededServer, end: u64) -> (SeededServer, ()) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        p.poll();
        if p.acked(0) == end && srv.tap.get() == end {
            return (srv, ());
        }
        if srv.finished() {
            let (_, res) = srv.join();
            panic!("the re-seeded replica never converged: its run ended with {res:?}");
        }
        assert!(
            Instant::now() < deadline,
            "timed out: the replica never converged (acked {}, want {end})",
            p.acked(0)
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// One replica behind the forwarding proxy, on a fixed address.
struct Site {
    _root: tempfile::TempDir,
    /// The replica's WAL directory (`<root>/replica`; may not exist yet).
    dir: PathBuf,
    listener: Arc<TcpListener>,
    tap: DurableTap,
    proxy: Proxy,
}

impl Site {
    fn new() -> Site {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("replica");
        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").unwrap());
        let tap = DurableTap::default();
        let proxy = Proxy::start(
            listener.local_addr().unwrap(),
            tap.clone(),
            SMALL.max_record_size,
        );
        Site {
            _root: root,
            dir,
            listener,
            tap,
            proxy,
        }
    }

    fn start(&self, installed: Option<u64>) -> SeededServer {
        SeededServer::start_on(
            Arc::clone(&self.listener),
            &self.dir,
            rx_cfg(),
            installed.map(Lsn),
            self.tap.clone(),
        )
        .expect("open_seeded")
    }
}

/// How E1's replica got to need a re-seed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Start {
    /// It received a prefix, disconnected, and the primary checkpointed past it.
    Prefix,
    /// Variant A: a new replica; its directory does not exist.
    Missing,
    /// Variant B: a new replica that was first opened without a seed (it holds
    /// an empty log at base 1) and was answered `RESEED_REQUIRED`.
    EmptyBase1,
}

/// The old log as the replica's own WAL reported it before the re-seed.
type OldLog = (Lsn, Vec<(Lsn, Vec<u8>)>);

/// Bring a primary and a replica site to the point where the replica needs a
/// re-seed: the primary's log no longer starts at 1, its shipper is running,
/// and (unless `Missing`) the replica's `Receiver::run` has returned
/// `ReseedRequired{oldest}` with the primary's `oldest_lsn`.
fn needs_reseed(start: Start) -> (Prim, Site, Option<OldLog>) {
    let mut p = Prim::open(SMALL);
    let site = Site::new();
    let mut old = None;
    let mut srv = None;
    if start == Start::Prefix {
        let s = site.start(None);
        p.start(ship_cfg(vec![site.proxy.addr]));
        p.fill(40, LEN, 8);
        p.poll_until(20, "the prefix to be acked", |p| p.acked(0) == 40);
        // The replica disconnects (here: the primary's shipper goes away; a
        // new one is started below — the primary keeps no shipping state, R7).
        p.stop();
        srv = Some(s);
    }
    // The primary appends, rolls and checkpoints past the replica.
    p.fill(200, LEN, 8);
    let d = p.wal.durable_lsn();
    p.wal.checkpoint(d).unwrap();
    assert!(p.oldest() > 41, "the checkpoint must pass the replica");
    if start == Start::EmptyBase1 {
        srv = Some(site.start(None));
    }
    p.start(ship_cfg(vec![site.proxy.addr]));
    if let Some(srv) = srv {
        assert_eq!(srv.report.set_aside, None);
        assert_eq!(srv.observer_calls, 1);
        let (rx, res) = join_polling(&mut p, srv);
        match res {
            Err(ReplError::ReseedRequired { oldest }) => assert_eq!(
                oldest,
                p.wal.oldest_lsn(),
                "ReseedRequired must carry the primary's oldest_lsn"
            ),
            other => panic!("run must end with ReseedRequired, got {other:?}"),
        }
        let log = (rx.durable_lsn(), read_all(rx.wal()));
        let want = if start == Start::Prefix { 40 } else { 0 };
        assert_eq!(log.0, Lsn(want));
        assert_eq!(log.1, p.expect(1, want));
        old = Some(log);
        // §10.1 step 1: drop the receiver.
        drop(rx);
    }
    (p, site, old)
}

/// The sessions whose `HELLO.d ≥ s` and in which the primary said anything.
fn answered_at_or_after(proxy: &Proxy, s: u64) -> Vec<common::SessionLog> {
    proxy
        .sessions()
        .into_iter()
        .filter(|l| l.hello.is_some_and(|d| d >= s) && l.first.is_some())
        .collect()
}

/// The proxy's verdict on a re-seed at `s`: exactly one answered session at or
/// after `s`, with `HELLO.d == s` and `SERVING.from == s + 1`; no
/// `RESEED_REQUIRED` at or after `s`; no `ERR` anywhere.
fn assert_wire_after_reseed(proxy: &Proxy, s: u64) {
    let all = proxy.sessions();
    for l in &all {
        assert!(l.errs.is_empty(), "ERR on the wire: {l:?}");
        if l.hello.is_some_and(|d| d >= s) {
            assert!(
                !matches!(l.first, Some(First::Reseed { .. })),
                "a session with HELLO.d >= s ({s}) was answered RESEED_REQUIRED: {l:?}"
            );
        }
    }
    let after = answered_at_or_after(proxy, s);
    assert_eq!(
        after.len(),
        1,
        "exactly one session after the re-seed (no second HELLO, no BelowOldest FIN): {after:#?}"
    );
    assert_eq!(
        after[0].hello,
        Some(s),
        "the re-seeded HELLO carries d == s"
    );
    assert_eq!(
        after[0].serving.map(|(from, _)| from),
        Some(s + 1),
        "the re-seeded replica is served from s + 1"
    );
    assert_eq!(after[0].first_record, Some(s + 1));
    proxy.assert_clean();
}

// ---------------------------------------------------------------------------
// E1 — re-seed end to end
// ---------------------------------------------------------------------------

fn e1_run(start: Start) {
    let (mut p, site, old) = needs_reseed(start);

    // §10.2: the primary registers the snapshot LSN before anything else.
    let s = p.durable();
    p.begin_reseed(0, s).unwrap();
    // ... and keeps appending while the snapshot travels.
    p.fill(100, LEN, 8);

    // §10.1 steps 2–3: the application installs the snapshot durably (its own
    // business — here the value `s`), then opens the receiver seeded at it.
    let srv = site.start(Some(s));
    p.fill(30, LEN, 8);
    let end = p.durable();
    let (srv, ()) = converge(&mut p, srv, end);
    p.stop();
    let (report, calls) = (srv.report.clone(), srv.observer_calls);
    let (rx, _) = srv.stop();

    // From the replica's own WAL.
    assert_eq!(
        rx.wal().oldest_lsn().0,
        s + 1,
        "the new log starts at s + 1"
    );
    assert_eq!(rx.durable_lsn().0, end);
    assert_eq!(
        read_all(rx.wal()),
        p.expect(s + 1, end),
        "records s+1..=durable are byte-identical to what the primary appended"
    );
    drop(rx);

    // From the proxy.
    assert_wire_after_reseed(&site.proxy, s);

    // From disk.
    match (start, old) {
        (Start::Missing, _) => {
            assert_eq!(report.set_aside, None);
            assert_eq!(stale_dirs(&site.dir), Vec::<PathBuf>::new());
            assert_eq!(calls, 1, "one WAL open, one observer");
        }
        (_, Some(old)) => {
            let stale = stale_path(&site.dir, 0);
            assert_eq!(report.set_aside, Some(stale.clone()));
            assert_eq!(stale_dirs(&site.dir), vec![stale.clone()]);
            assert_eq!(calls, 2, "set-aside: two WAL opens, two observers");
            // A plain `Wal::open` of the set-aside directory yields the old
            // log, with its old durable_lsn, byte-identical.
            let (wal, _) = Wal::open(&stale, SMALL).unwrap();
            assert_eq!(wal.oldest_lsn(), Lsn(1));
            assert_eq!((wal.durable_lsn(), read_all(&wal)), old);
        }
        (_, None) => unreachable!(),
    }
    site.proxy.stop();
}

#[test]
fn e1_reseed_end_to_end() {
    e1_run(Start::Prefix);
}

#[test]
fn e1_variant_a_new_replica_on_a_missing_directory() {
    e1_run(Start::Missing);
}

#[test]
fn e1_variant_b_new_replica_first_opened_without_a_seed() {
    e1_run(Start::EmptyBase1);
}

// ---------------------------------------------------------------------------
// E2 — the keep / set-aside boundary
// ---------------------------------------------------------------------------

/// A log `1..=d` at `dir` (`d == 0`: an empty log at base 1); returns its records.
fn build_log(dir: &Path, d: u64) -> Vec<(Lsn, Vec<u8>)> {
    let (mut wal, _) = Wal::open(dir, SMALL).unwrap();
    for l in 1..=d {
        assert_eq!(wal.append(&payload(l, LEN)).unwrap(), Lsn(l));
    }
    assert_eq!(wal.commit().unwrap(), Lsn(d));
    read_all(&wal)
}

/// `open_seeded` with a counting `NullObserver` factory.
fn open_counted(
    dir: &Path,
    installed: Option<u64>,
) -> Result<(Receiver, Option<PathBuf>, usize), ReplError> {
    let mut calls = 0;
    let (rx, report) = Receiver::open_seeded(dir, rx_cfg(), installed.map(Lsn), || {
        calls += 1;
        NullObserver
    })?;
    Ok((rx, report.set_aside, calls))
}

/// The LSN the next append on this receiver's WAL gets.
fn first_append(rx: Receiver) -> u64 {
    let mut wal = rx.into_wal();
    wal.append(b"probe").unwrap().0
}

#[derive(Clone, Copy, Debug)]
enum Before {
    /// A log `1..=d` with records (or the empty base-1 log for `d == 0`).
    Log(u64),
    Missing,
    EmptyDir,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Want {
    Kept,
    /// Cold start at the seed, nothing set aside.
    Seeded,
    SetAside,
    /// Empty directory, no seed: starts at 1.
    StartsAtOne,
}

#[test]
fn e2_keep_set_aside_boundary_table() {
    let s = 20u64;
    let rows: &[(&str, Before, Option<u64>, Want)] = &[
        ("d < s", Before::Log(s - 1), Some(s), Want::SetAside),
        ("d == s", Before::Log(s), Some(s), Want::Kept),
        ("d > s", Before::Log(s + 1), Some(s), Want::Kept),
        ("None", Before::Log(s - 5), None, Want::Kept),
        ("missing, Some(s)", Before::Missing, Some(s), Want::Seeded),
        ("empty, Some(s)", Before::EmptyDir, Some(s), Want::Seeded),
        ("empty, None", Before::EmptyDir, None, Want::StartsAtOne),
        (
            "empty base-1 log, Some(s)",
            Before::Log(0),
            Some(s),
            Want::SetAside,
        ),
    ];
    for &(name, before, installed, want) in rows {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("wal");
        let (d, records) = match before {
            Before::Log(d) => (d, build_log(&dir, d)),
            Before::Missing => (0, Vec::new()),
            Before::EmptyDir => {
                std::fs::create_dir(&dir).unwrap();
                (0, Vec::new())
            }
        };
        let (rx, set_aside, calls) =
            open_counted(&dir, installed).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let (oldest, durable) = (rx.wal().oldest_lsn().0, rx.durable_lsn().0);
        let now = read_all(rx.wal());
        let stale = stale_dirs(&dir);
        match want {
            Want::Kept => {
                // "Kept" from data: the records are still readable, nothing moved.
                assert_eq!(now, records, "{name}: the log's records must survive");
                assert_eq!(now.len() as u64, d, "{name}");
                assert_eq!((oldest, durable), (1, d), "{name}");
                assert_eq!(stale, Vec::<PathBuf>::new(), "{name}: nothing set aside");
                assert_eq!(set_aside, None, "{name}");
                assert_eq!(calls, 1, "{name}: one open, one observer (no probe open)");
                assert_eq!(first_append(rx), d + 1, "{name}");
            }
            Want::Seeded => {
                assert_eq!((oldest, durable), (s + 1, s), "{name}");
                assert!(now.is_empty(), "{name}");
                assert_eq!(stale, Vec::<PathBuf>::new(), "{name}: nothing set aside");
                assert_eq!(set_aside, None, "{name}");
                assert_eq!(calls, 1, "{name}");
                assert_eq!(first_append(rx), s + 1, "{name}: first appended LSN");
            }
            Want::StartsAtOne => {
                assert_eq!((oldest, durable), (1, 0), "{name}");
                assert_eq!(stale, Vec::<PathBuf>::new(), "{name}");
                assert_eq!(set_aside, None, "{name}");
                assert_eq!(calls, 1, "{name}");
                assert_eq!(first_append(rx), 1, "{name}");
            }
            Want::SetAside => {
                assert_eq!((oldest, durable), (s + 1, s), "{name}");
                assert!(now.is_empty(), "{name}");
                let moved = stale_path(&dir, 0);
                assert_eq!(set_aside, Some(moved.clone()), "{name}");
                assert_eq!(stale, vec![moved.clone()], "{name}");
                assert_eq!(calls, 2, "{name}: two opens, two observers");
                assert_eq!(first_append(rx), s + 1, "{name}: first appended LSN");
                let (old_d, old) = read_dir(&moved, SMALL);
                assert_eq!(
                    (old_d.0, old),
                    (d, records),
                    "{name}: the stale log is intact"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// E3 — every §10.1 crash state converges
// ---------------------------------------------------------------------------

/// A completed seeded log (`seed = s + 1`) with `extra` records after `s`.
fn build_seeded(dir: &Path, s: u64, extra: u64) -> Wal {
    let (mut wal, _) = Wal::options().seed(Lsn(s + 1)).open(dir, SMALL).unwrap();
    for l in s + 1..=s + extra {
        assert_eq!(wal.append(&payload(l, LEN)).unwrap(), Lsn(l));
    }
    wal.commit().unwrap();
    wal
}

#[test]
fn e3_every_crash_state_converges() {
    let s = 20u64;
    // (name, .stale.* directories expected, builder). The builder returns
    // what must be unchanged: Some((oldest, records)) for (g) and (h).
    type Kept = Option<(u64, Vec<(Lsn, Vec<u8>)>)>;
    type Build = Box<dyn Fn(&Path) -> Kept>;
    let zero_len = |base: u64| -> Build {
        Box::new(move |dir: &Path| {
            std::fs::create_dir(dir).unwrap();
            std::fs::write(segment_path(dir, base), b"").unwrap();
            None
        })
    };
    let states: Vec<(&str, usize, Build)> = vec![
        (
            "(a) old log intact",
            1,
            Box::new(|dir| {
                build_log(dir, 10);
                None
            }),
        ),
        (
            "(b) dir absent, .stale.0 present",
            1,
            Box::new(|dir| {
                build_log(dir, 10);
                std::fs::rename(dir, stale_path(dir, 0)).unwrap();
                None
            }),
        ),
        (
            "(c) dir empty",
            0,
            Box::new(|dir| {
                std::fs::create_dir(dir).unwrap();
                None
            }),
        ),
        (
            "(d) dir holding only LOCK",
            0,
            Box::new(|dir| {
                std::fs::create_dir(dir).unwrap();
                std::fs::write(dir.join("LOCK"), b"").unwrap();
                None
            }),
        ),
        ("(e1) zero-length segment named s + 1", 0, zero_len(s + 1)),
        ("(e2) zero-length segment named 1", 0, zero_len(1)),
        (
            "(e3) zero-length segment, unrelated base",
            0,
            zero_len(7777),
        ),
        (
            "(f) full-size all-zero segment",
            0,
            Box::new(move |dir| {
                std::fs::create_dir(dir).unwrap();
                std::fs::write(dir.join("LOCK"), b"").unwrap();
                let zeros = vec![0u8; SMALL.segment_size as usize];
                std::fs::write(segment_path(dir, s + 1), zeros).unwrap();
                None
            }),
        ),
        (
            "(g) seeded log with records after s",
            0,
            Box::new(move |dir| {
                let wal = build_seeded(dir, s, 15);
                Some((wal.oldest_lsn().0, read_all(&wal)))
            }),
        ),
        (
            "(h) seeded log, checkpointed",
            0,
            Box::new(move |dir| {
                let mut wal = build_seeded(dir, s, 120);
                wal.checkpoint(Lsn(s + 80)).unwrap();
                assert!(wal.oldest_lsn().0 > s + 1, "(h) must have checkpointed");
                Some((wal.oldest_lsn().0, read_all(&wal)))
            }),
        ),
        (
            "(i) empty base-1 log",
            1,
            Box::new(|dir| {
                build_log(dir, 0);
                None
            }),
        ),
    ];
    for (name, want_stale, build) in states {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("wal");
        let kept = build(&dir);
        for call in 1..=2 {
            let (rx, _, _) = open_counted(&dir, Some(s))
                .unwrap_or_else(|e| panic!("{name}, call {call}: {e:?}"));
            assert!(
                rx.durable_lsn().0 >= s,
                "{name}, call {call}: durable_lsn {} < s",
                rx.durable_lsn()
            );
            match &kept {
                Some((oldest, records)) => {
                    assert_eq!(rx.wal().oldest_lsn().0, *oldest, "{name}, call {call}");
                    assert_eq!(&read_all(rx.wal()), records, "{name}, call {call}");
                }
                None => {
                    assert_eq!(
                        rx.wal().oldest_lsn().0,
                        s + 1,
                        "{name}, call {call}: the log must start at s + 1"
                    );
                    assert_eq!(rx.durable_lsn().0, s, "{name}, call {call}");
                }
            }
            assert_eq!(
                stale_dirs(&dir).len(),
                want_stale,
                "{name}, call {call}: .stale.* directories"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// E4 — set-aside never clobbers
// ---------------------------------------------------------------------------

#[test]
fn e4_set_aside_never_clobbers() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("wal");
    let old = build_log(&dir, 10);
    for k in 0..2 {
        let taken = stale_path(&dir, k);
        std::fs::create_dir(&taken).unwrap();
        std::fs::write(taken.join("marker"), format!("marker {k}")).unwrap();
    }
    let (rx, set_aside, _) = open_counted(&dir, Some(20)).expect("open_seeded");
    assert_eq!(set_aside, Some(stale_path(&dir, 2)), "smallest unused k");
    assert_eq!(rx.wal().oldest_lsn(), Lsn(21));
    for k in 0..2 {
        let marker = std::fs::read_to_string(stale_path(&dir, k).join("marker"));
        assert_eq!(
            marker.ok(),
            Some(format!("marker {k}")),
            ".stale.{k} was clobbered"
        );
    }
    assert_eq!(read_dir(&stale_path(&dir, 2), SMALL), (Lsn(10), old));
    assert_eq!(stale_dirs(&dir).len(), 3);
}

// ---------------------------------------------------------------------------
// E5 — an unrecoverable log is never set aside
// ---------------------------------------------------------------------------

fn segment_bytes(dir: &Path) -> Vec<(u64, Vec<u8>)> {
    segment_bases(dir)
        .into_iter()
        .map(|b| (b, std::fs::read(segment_path(dir, b)).unwrap()))
        .collect()
}

#[test]
fn e5_an_unrecoverable_log_is_never_set_aside() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("wal");
    build_log(&dir, 100);
    let bases = segment_bases(&dir);
    assert!(bases.len() >= 3, "the log must have sealed segments");
    // Mid-log corruption (WAL D5): one payload byte of the first record of a
    // sealed segment; valid records follow it.
    let path = segment_path(&dir, bases[0]);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[64 + 20 + 3] ^= 0x40;
    std::fs::write(&path, &bytes).unwrap();
    assert!(
        Wal::open(&dir, SMALL).is_err(),
        "the log must be unrecoverable"
    );
    let before = segment_bytes(&dir);

    let mut calls = 0;
    let res = Receiver::open_seeded(&dir, rx_cfg(), Some(Lsn(500)), || {
        calls += 1;
        NullObserver
    });
    match res {
        Err(ReplError::Wal(WalError::Corruption { .. } | WalError::TornMidLog { .. })) => {}
        Err(e) => panic!("expected the WAL's own open error, got {e:?}"),
        Ok(_) => panic!("an unrecoverable log was opened (or replaced)"),
    }
    assert_eq!(calls, 1);
    assert_eq!(
        stale_dirs(&dir),
        Vec::<PathBuf>::new(),
        "an unrecoverable log must never be set aside"
    );
    assert!(
        segment_bytes(&dir) == before,
        "the segment files must be byte-for-byte unchanged"
    );
}

// ---------------------------------------------------------------------------
// E6 — begin_reseed validation, the hold lifecycle, retention_floor
// ---------------------------------------------------------------------------

/// Small records: 40 framed bytes, ~100 per 4 KiB segment.
const TINY: usize = 16;

/// A primary with `n` records whose log starts above 1 (`oldest` in
/// `(1, n/2]`), and a shipper for `fakes`.
fn e6_primary(n: u64, fakes: &[&Fake]) -> Prim {
    let mut p = Prim::open(SMALL);
    p.fill(n, TINY, 10);
    p.wal.checkpoint(Lsn(n / 2)).unwrap();
    assert!(
        p.oldest() > 2 && p.oldest() <= n / 2,
        "oldest = {}",
        p.oldest()
    );
    p.start(ship_cfg(fakes.iter().map(|f| f.addr()).collect()));
    p
}

/// Answer replica `id`'s pending join with exactly ONE poll (the caller may
/// assert right after it returns), then return the replica's first frame.
fn join_once(
    p: &mut Prim,
    id: usize,
    c: &mut FakeConn,
    right_after_poll: impl FnOnce(&Prim),
) -> Saw {
    p.await_request(id);
    p.poll();
    right_after_poll(p);
    c.next_data(Duration::from_secs(5))
}

/// Poll until the fake replica has received the record `lsn` (dense from
/// wherever it is).
fn read_through(p: &mut Prim, c: &mut FakeConn, lsn: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        p.poll();
        match c.next_data(Duration::from_millis(3)) {
            Saw::Record(l, _) if l == lsn => return,
            Saw::Record(..) => {}
            Saw::Timeout => assert!(Instant::now() < deadline, "record {lsn} never arrived"),
            other => panic!("waiting for record {lsn}, got {other:?}"),
        }
    }
}

/// Drop a fake replica's connection and wait until the primary noticed.
fn hang_up(p: &Prim, id: usize, c: FakeConn) {
    drop(c);
    wait_until(10, "the session to end", || {
        p.sh().replica_status(id) == Some(ReplicaStatus::Connecting)
    });
}

#[test]
fn e6_begin_reseed_validation_table() {
    let fake = Fake::new(SMALL.max_record_size);
    let mut p = e6_primary(300, &[&fake]);
    let (oldest, durable) = (p.oldest(), p.durable());
    let invalid = |r: Result<(), ReplError>, s: u64| match r {
        Err(ReplError::InvalidSnapshotLsn {
            s: es,
            oldest: eo,
            durable: ed,
        }) => assert_eq!((es.0, eo.0, ed.0), (s, oldest, durable)),
        other => panic!("s = {s}: expected InvalidSnapshotLsn, got {other:?}"),
    };
    // One replica that never said HELLO: the floor is its acked, 0. A hold
    // recorded by a failed call would change it.
    assert_eq!(p.floor(), Some(0));

    match p.begin_reseed(1, durable) {
        Err(ReplError::UnknownReplica { id: 1 }) => {}
        other => panic!("unknown id: expected UnknownReplica, got {other:?}"),
    }
    assert_eq!(p.floor(), Some(0), "a failed begin_reseed records nothing");

    invalid(p.begin_reseed(0, durable + 1), durable + 1);
    assert_eq!(p.floor(), Some(0), "a failed begin_reseed records nothing");

    invalid(p.begin_reseed(0, oldest - 2), oldest - 2);
    assert_eq!(p.floor(), Some(0), "a failed begin_reseed records nothing");

    p.begin_reseed(0, durable).expect("s == durable is valid");
    assert_eq!(p.floor(), Some(durable));

    // Failures leave the existing hold alone, too.
    invalid(p.begin_reseed(0, durable + 1), durable + 1);
    invalid(p.begin_reseed(0, oldest - 2), oldest - 2);
    assert!(matches!(
        p.begin_reseed(7, oldest),
        Err(ReplError::UnknownReplica { id: 7 })
    ));
    assert_eq!(p.floor(), Some(durable));

    p.begin_reseed(0, oldest - 1)
        .expect("s + 1 == oldest is valid");
    assert_eq!(
        p.floor(),
        Some(oldest - 1),
        "a second hold replaces the first"
    );

    assert_eq!(p.sh().replica_needs_reseed(0), Some(false));
    assert_eq!(p.sh().replica_needs_reseed(1), None);
    p.end_reseed(1); // unknown id: a no-op
    assert_eq!(p.floor(), Some(oldest - 1));
}

#[test]
fn e6_retention_floor_table() {
    let (fa, fb) = (
        Fake::new(SMALL.max_record_size),
        Fake::new(SMALL.max_record_size),
    );
    let mut p = e6_primary(300, &[&fa, &fb]);
    let oldest = p.oldest();
    assert_eq!(
        p.floor(),
        Some(0),
        "never-connected replicas count as 0 (R7)"
    );

    // Both streaming: the minimum of their acked.
    let mut a = fa.hello(300);
    assert_eq!(
        join_once(&mut p, 0, &mut a, |_| {}),
        Saw::Serving { from: 301, oldest }
    );
    let mut b = fb.hello(290);
    assert_eq!(
        join_once(&mut p, 1, &mut b, |_| {}),
        Saw::Serving { from: 291, oldest }
    );
    read_through(&mut p, &mut b, 300);
    b.ack(295);
    wait_until(10, "B's ack", || p.acked(1) == 295);
    assert_eq!((p.acked(0), p.acked(1)), (300, 295));
    assert_eq!(p.floor(), Some(295), "both streaming: min of their acked");
    assert_eq!(p.sh().min_replica_acked_lsn(), Some(Lsn(295)));

    // A is answered RESEED_REQUIRED and has no hold: it is excluded.
    hang_up(&p, 0, a);
    let mut a = fa.hello(0);
    let first = join_once(&mut p, 0, &mut a, |p| {
        assert_eq!(
            p.sh().replica_needs_reseed(0),
            Some(true),
            "needs_reseed must be set as soon as the poll returns"
        );
        assert_eq!(p.sh().replica_needs_reseed(1), Some(false));
    });
    assert_eq!(first, Saw::Reseed { oldest });
    // The network thread stored acked[A] = 0 before it sent that frame.
    assert_eq!(p.acked(0), 0);
    assert_eq!(
        p.floor(),
        Some(295),
        "a replica that needs a re-seed and has no hold is excluded"
    );
    assert_eq!(p.sh().min_replica_acked_lsn(), Some(Lsn(0)), "unchanged");

    // A with a hold: its term is the hold.
    p.begin_reseed(0, 280).unwrap();
    assert_eq!(p.floor(), Some(280), "min(hold, other acked)");
    p.begin_reseed(0, 285).unwrap();
    assert_eq!(
        p.floor(),
        Some(285),
        "a second begin_reseed replaces the hold"
    );
    p.begin_reseed(0, 298).unwrap();
    assert_eq!(p.floor(), Some(295), "min(hold, other acked)");
    p.begin_reseed(0, 285).unwrap();

    // B needs a re-seed too.
    hang_up(&p, 1, b);
    let mut b = fb.hello(1);
    assert_eq!(join_once(&mut p, 1, &mut b, |_| {}), Saw::Reseed { oldest });
    assert_eq!(p.floor(), Some(285), "only A's hold is left");
    p.begin_reseed(1, 270).unwrap();
    assert_eq!(p.floor(), Some(270));
    p.end_reseed(1);
    assert_eq!(
        p.floor(),
        Some(285),
        "end_reseed clears only that replica's hold"
    );
    p.end_reseed(0);
    assert_eq!(p.floor(), None, "both excluded");

    // Served again: back in the floor at its HELLO value, at once.
    drop(a);
    let mut a = fa.hello(300);
    let first = join_once(&mut p, 0, &mut a, |p| {
        assert_eq!(p.sh().replica_needs_reseed(0), Some(false));
        assert_eq!(
            p.floor(),
            Some(300),
            "a served replica counts again, at its HELLO value"
        );
    });
    assert_eq!(first, Saw::Serving { from: 301, oldest });
    assert_eq!(p.sh().replica_needs_reseed(1), Some(true));
}

#[test]
fn e6_holds_are_not_persisted() {
    let fake = Fake::new(SMALL.max_record_size);
    let mut p = Prim::open(SMALL);
    p.fill(300, TINY, 10);
    p.start(ship_cfg(vec![fake.addr()]));
    p.begin_reseed(0, 50).unwrap();
    assert_eq!(p.floor(), Some(50));

    // The primary restarts: reopen the WAL, a fresh Shipper (R7).
    p.stop();
    let Prim { dir, wal, .. } = p;
    drop(wal);
    let (wal, _) = Wal::open(dir.path(), SMALL).unwrap();
    let mut p = Prim {
        dir,
        wal,
        sh: None,
        model: Vec::new(),
    };
    p.start(ship_cfg(vec![fake.addr()]));
    assert_eq!(p.floor(), Some(0), "a fresh Shipper has no hold");

    // Nothing held the log: a checkpoint passes s, and the re-seed that was
    // in flight can no longer be registered.
    p.wal.checkpoint(Lsn(200)).unwrap();
    assert!(p.oldest() > 51);
    match p.begin_reseed(0, 50) {
        Err(ReplError::InvalidSnapshotLsn { s, oldest, durable }) => {
            assert_eq!((s.0, oldest.0, durable.0), (50, p.oldest(), 300));
        }
        other => panic!("expected InvalidSnapshotLsn, got {other:?}"),
    }
    assert_eq!(p.floor(), Some(0));
}

#[test]
fn e6_hold_release() {
    let (fa, fb) = (
        Fake::new(SMALL.max_record_size),
        Fake::new(SMALL.max_record_size),
    );
    let mut p = e6_primary(300, &[&fa, &fb]);
    let oldest = p.oldest();
    assert!(oldest <= 151);
    p.begin_reseed(0, 200).unwrap();
    p.begin_reseed(1, 250).unwrap(); // B never connects: its hold must stay
    assert_eq!(p.floor(), Some(200));

    // A join with d < s that is served does not clear the hold.
    let mut a = fa.hello(150);
    let first = join_once(&mut p, 0, &mut a, |p| {
        assert_eq!(p.acked(0), 150);
        assert_eq!(
            p.floor(),
            Some(200),
            "a served join with d < s keeps the hold"
        );
    });
    assert_eq!(first, Saw::Serving { from: 151, oldest });

    // A session end does not.
    hang_up(&p, 0, a);
    p.poll();
    assert_eq!(p.floor(), Some(200), "a session end keeps the hold");

    // A RESEED_REQUIRED answer does not.
    let mut a = fa.hello(0);
    let first = join_once(&mut p, 0, &mut a, |p| {
        assert_eq!(p.sh().replica_needs_reseed(0), Some(true));
        assert_eq!(p.floor(), Some(200), "RESEED_REQUIRED keeps the hold");
    });
    assert_eq!(first, Saw::Reseed { oldest });
    drop(a);

    // A rejected join does not.
    let mut a = fa.hello(305);
    let first = join_once(&mut p, 0, &mut a, |p| {
        assert_eq!(p.floor(), Some(200), "a rejected join keeps the hold");
    });
    assert!(
        matches!(first, Saw::Err(ErrCode::Contiguity, _)),
        "d > durable must be rejected, got {first:?}"
    );
    drop(a);

    // A join with d >= s answered Serve clears that replica's hold, and no
    // other: the floor is min(acked A = 210, hold B = 250).
    let mut a = fa.hello(210);
    let first = join_once(&mut p, 0, &mut a, |p| {
        assert_eq!(p.sh().replica_needs_reseed(0), Some(false));
        assert_eq!(
            p.floor(),
            Some(210),
            "Serve with d >= s releases A's hold (and only A's)"
        );
    });
    assert_eq!(first, Saw::Serving { from: 211, oldest });

    // The boundary d == s releases too: afterwards the floor follows A's acks.
    hang_up(&p, 0, a);
    p.begin_reseed(0, 220).unwrap();
    assert_eq!(p.floor(), Some(220));
    let mut a = fa.hello(220);
    assert_eq!(
        join_once(&mut p, 0, &mut a, |_| {}),
        Saw::Serving { from: 221, oldest }
    );
    read_through(&mut p, &mut a, 240);
    a.ack(230);
    wait_until(10, "A's ack", || p.acked(0) == 230);
    assert_eq!(p.floor(), Some(230), "d == s released the hold");

    // B's hold is still there, and end_reseed drops it.
    p.end_reseed(1);
    assert_eq!(
        p.floor(),
        Some(0),
        "B never said HELLO: its term is 0 again"
    );
}

#[test]
fn e6_handover_has_no_unprotected_window() {
    let fake = Fake::new(SMALL.max_record_size);
    let mut p = Prim::open(SMALL);
    p.fill(1000, TINY, 50);
    p.start(ship_cfg(vec![fake.addr()]));
    assert!(p.oldest() <= 501);

    // An earlier session: the replica acked 1000.
    let mut c = fake.hello(900);
    assert_eq!(
        join_once(&mut p, 0, &mut c, |_| {}),
        Saw::Serving {
            from: 901,
            oldest: 1
        }
    );
    read_through(&mut p, &mut c, 1000);
    c.ack(1000);
    wait_until(10, "the ack of 1000", || p.acked(0) == 1000);
    hang_up(&p, 0, c);

    // It lost its disk and is re-seeded at 500.
    p.begin_reseed(0, 500).unwrap();
    assert_eq!(p.floor(), Some(500));
    let mut c = fake.hello(500);
    p.await_request(0);
    p.poll();
    // Immediately after the poll, with no wait: the hold is gone and `acked`
    // already says 500 — a checkpoint in this loop iteration is bounded by it.
    let (acked, floor) = (p.acked(0), p.floor());
    assert_eq!(
        acked, 500,
        "the writer must store acked = d when it answers Serve"
    );
    assert_eq!(floor, Some(500), "no unprotected window at the handover");
    assert_eq!(
        c.next_data(Duration::from_secs(5)),
        Saw::Serving {
            from: 501,
            oldest: 1
        }
    );
}

// ---------------------------------------------------------------------------
// E7 — the hold prevents the loop (end to end), with its negative control
// ---------------------------------------------------------------------------

fn e7_run(use_holds: bool) {
    let (mut p, site, _old) = needs_reseed(Start::Prefix);
    let s = p.durable();
    p.begin_reseed(0, s).unwrap();

    // A policy that would otherwise checkpoint everything:
    // `up_to = min(durable, floor)`. The control computes the floor itself,
    // from needs_reseed and acked — i.e. it ignores the holds.
    let policy = move |p: &mut Prim| {
        let floor = if use_holds {
            p.sh().retention_floor()
        } else if p.sh().replica_needs_reseed(0) == Some(true) {
            None
        } else {
            p.sh().replica_acked_lsn(0)
        };
        let durable = p.wal.durable_lsn();
        let up_to = floor.map_or(durable, |f| f.min(durable));
        p.wal.checkpoint(up_to).unwrap();
    };

    // The re-seed is in flight: the primary rolls five segments and
    // checkpoints after each.
    p.roll(5, &mut |p| {
        policy(p);
        if use_holds {
            assert!(
                p.oldest() <= s + 1,
                "the hold must keep s + 1 = {} (oldest is {})",
                s + 1,
                p.oldest()
            );
        }
    });

    if !use_holds {
        // Anti-vacuity: the policy really deleted s + 1.
        assert!(
            p.oldest() > s + 1,
            "the control must have checkpointed past s + 1"
        );
        let srv = site.start(Some(s));
        let (_rx, res) = join_polling(&mut p, srv);
        assert!(
            matches!(res, Err(ReplError::ReseedRequired { .. })),
            "the control's re-seeded replica must be told to re-seed again, got {res:?}"
        );
        let looped = site
            .proxy
            .sessions()
            .into_iter()
            .any(|l| l.hello == Some(s) && matches!(l.first, Some(First::Reseed { .. })));
        assert!(
            looped,
            "the session with HELLO.d == s must be answered RESEED_REQUIRED"
        );
        drop(site.listener);
        p.stop();
        return;
    }

    let srv = site.start(Some(s));
    // Five more segments while the re-seeded replica catches up.
    let proxy = &site.proxy;
    p.roll(5, &mut |p| {
        policy(p);
        // Read AFTER the policy ran: if nothing was answered yet, the replica
        // cannot have acked anything above s, so the policy was bound by s.
        if answered_at_or_after(proxy, s).is_empty() {
            assert!(
                p.oldest() <= s + 1,
                "s + 1 must be retained until the replica is served"
            );
        }
    });
    let end = p.durable();
    let (srv, ()) = converge(&mut p, srv, end);
    // The policy is live, not inert: once the replica caught up it reclaims.
    policy(&mut p);
    assert!(
        p.oldest() > s + 1,
        "the policy must reclaim after the catch-up"
    );
    p.stop();
    let (rx, _) = srv.stop();
    assert_eq!(rx.wal().oldest_lsn().0, s + 1);
    assert_eq!(
        read_all(rx.wal()),
        p.expect(s + 1, end),
        "the replica converges byte-identical"
    );
    assert_wire_after_reseed(&site.proxy, s);
    site.proxy.stop();
}

#[test]
fn e7_the_hold_prevents_the_reseed_loop() {
    e7_run(true);
}

#[test]
fn e7_negative_control_a_policy_that_ignores_holds_loops() {
    e7_run(false);
}

// ---------------------------------------------------------------------------
// E8 — a snapshot ahead of the primary is never served
// ---------------------------------------------------------------------------

#[test]
fn e8_a_snapshot_ahead_of_the_primary_is_never_served() {
    let mut p = Prim::open(SMALL);
    p.fill(50, LEN, 8);
    let site = Site::new();
    // A provider bug: a snapshot at 80 with the primary durable only to 50,
    // and no begin_reseed.
    let s = 80u64;
    let srv = site.start(Some(s));
    p.start(ship_cfg(vec![site.proxy.addr]));
    match p.begin_reseed(0, s) {
        Err(ReplError::InvalidSnapshotLsn { .. }) => {}
        other => panic!("begin_reseed must refuse s > durable, got {other:?}"),
    }
    // Let it be refused at least twice (the shipper reconnects under backoff).
    let refused = |proxy: &Proxy| {
        proxy
            .sessions()
            .into_iter()
            .filter(|l| l.hello == Some(s) && l.errs.contains(&(true, ErrCode::Contiguity)))
            .count()
    };
    p.poll_until(20, "two refused sessions", |_| refused(&site.proxy) >= 2);
    p.fill(10, LEN, 8); // still below s
    p.stop();
    let (rx, _) = srv.stop();

    // The proxy records a primary ERR as a violation, so read the session
    // logs directly instead of asserting a clean run.
    let sessions = site.proxy.sessions();
    for l in sessions.iter().filter(|l| l.first.is_some()) {
        assert_eq!(l.hello, Some(s));
        assert_eq!(l.first, Some(First::Err(ErrCode::Contiguity)), "{l:?}");
        assert_eq!(l.serving, None, "never served: {l:?}");
        assert_eq!(l.records, 0, "no record may be sent: {l:?}");
    }
    assert_eq!(
        rx.wal().last_lsn(),
        Lsn(s),
        "the replica received no record"
    );
    assert_eq!(rx.wal().oldest_lsn(), Lsn(s + 1));
    site.proxy.stop();
}

// ---------------------------------------------------------------------------
// E9 — the directory operations are durable and ordered (strace targets)
// ---------------------------------------------------------------------------
// Run by `scripts/replica/reseed-dirops.sh`, which traces each target and
// checks the order of the syscalls. Plain `cargo test` skips them.

fn e9_dir() -> PathBuf {
    let parent = std::env::var_os("RESEED_E9_DIR")
        .expect("RESEED_E9_DIR: run via scripts/replica/reseed-dirops.sh");
    PathBuf::from(parent).join("wal")
}

#[test]
#[ignore = "strace target: run via scripts/replica/reseed-dirops.sh"]
fn e9_target_set_aside() {
    let dir = e9_dir();
    build_log(&dir, 5);
    let (rx, set_aside, calls) = open_counted(&dir, Some(20)).unwrap();
    assert_eq!(set_aside, Some(stale_path(&dir, 0)));
    assert_eq!((rx.wal().oldest_lsn(), calls), (Lsn(21), 2));
}

#[test]
#[ignore = "strace target: run via scripts/replica/reseed-dirops.sh"]
fn e9_target_missing_dir() {
    let dir = e9_dir();
    assert!(!dir.exists());
    let (rx, set_aside, calls) = open_counted(&dir, Some(20)).unwrap();
    assert_eq!(set_aside, None);
    assert_eq!((rx.wal().oldest_lsn(), calls), (Lsn(21), 1));
}

// ---------------------------------------------------------------------------
// E10 — observer on a seeded replica
// ---------------------------------------------------------------------------

/// Records every `on_durable` value, in order, and feeds a [`DurableTap`].
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<u64>>>, DurableTap);

impl Recorder {
    fn seen(&self) -> Vec<u64> {
        self.0.lock().unwrap().clone()
    }
}

impl DurabilityObserver for Recorder {
    fn on_durable(&mut self, d: Lsn) {
        self.0.lock().unwrap().push(d.0);
        self.1.on_durable(d);
    }
}

fn e10_run(prepare: impl FnOnce(&Path), want_calls: usize) {
    let s = 40u64;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("wal");
    prepare(&dir);
    // Deliberately NOT pre-seeded.
    let rec = Recorder::default();
    let tap = rec.1.clone();
    let mut calls = 0;
    let (mut rx, _) = Receiver::open_seeded(&dir, rx_cfg(), Some(Lsn(s)), || {
        calls += 1;
        rec.clone()
    })
    .unwrap();
    assert_eq!(calls, want_calls);
    // The crate does not call the observer; neither does the WAL at open.
    assert_eq!(rec.seen(), Vec::<u64>::new(), "no observer call at open");
    assert_eq!(tap.get(), 0, "the tap stays at 0 after a seeded cold start");
    assert_eq!(
        rx.durable_lsn(),
        Lsn(s),
        "the starting watermark is durable_lsn()"
    );

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let h = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let res = rx.serve_connection(stream);
        (rx, res)
    });
    let mut prim = FakePrimary::connect(addr);
    assert_eq!(prim.hello(), s, "HELLO carries d == s");
    prim.serving(s + 1);
    assert_eq!(
        rec.seen(),
        Vec::<u64>::new(),
        "nothing before the first commit"
    );
    prim.record(s + 1, &payload(s + 1, LEN));
    assert_eq!(prim.await_ack(s + 1), vec![s + 1]);
    // The ACK is sent after `commit` returned, and the observer fires inside it.
    let seen = rec.seen();
    assert_eq!(
        seen.first().copied(),
        Some(s + 1),
        "the observer's first value comes from the first commit: {seen:?}"
    );
    assert!(tap.get() > s);
    drop(prim.finish());
    let (rx, res) = h.join().unwrap();
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(rx.wal().oldest_lsn(), Lsn(s + 1));
}

#[test]
fn e10_observer_on_a_seeded_replica() {
    // A cold start on a missing directory: one open.
    e10_run(|_| {}, 1);
    // The set-aside path: two opens, and still no observer call at open.
    e10_run(
        |dir| {
            build_log(dir, 10);
        },
        2,
    );
}

/// `tap` seeding is a real precondition of the proxy oracle, not decoration:
/// the tap of a seeded replica reads 0 until its first commit while its
/// heartbeats already carry `s`.
#[test]
fn e10_tap_is_not_advanced_by_open() {
    let root = tempfile::tempdir().unwrap();
    let tap = DurableTap::default();
    let (rx, _) = Receiver::open_seeded(&root.path().join("wal"), rx_cfg(), Some(Lsn(9)), || {
        tap.clone()
    })
    .unwrap();
    assert_eq!(
        (rx.durable_lsn(), tap.0.load(Ordering::Acquire)),
        (Lsn(9), 0)
    );
}
