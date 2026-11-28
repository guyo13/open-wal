//! Shared helpers for the replica integration tests: tiny WAL configs that
//! force segment rolls, log readers, and an in-process **fake primary** that
//! speaks the §6 wire protocol over a real loopback socket.
#![allow(dead_code)] // each test binary uses a different subset

use std::io::Write;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use open_wal::{Lsn, Wal, WalConfig};
use open_wal_replica::wire::{self, Frame, FrameReader, Limits, PROTO_VERSION};
use open_wal_replica::{Receiver, ReceiverConfig, ReplError};

/// 4 KiB segments with 256-byte records: a few dozen records force rolls and
/// commit-time splits on both primary and replica.
pub const SMALL: WalConfig = WalConfig {
    segment_size: 4096,
    max_record_size: 256,
};

/// Every `(lsn, payload)` in `wal`, oldest first.
pub fn read_all<O: open_wal::DurabilityObserver>(wal: &Wal<O>) -> Vec<(Lsn, Vec<u8>)> {
    let mut r = wal.reader_from(Lsn(0)).expect("reader_from");
    let mut out = Vec::new();
    while let Some(rec) = r.next() {
        let (lsn, p) = rec.expect("replay");
        out.push((lsn, p.to_vec()));
    }
    out
}

/// Every `(lsn, payload)` in the WAL at `dir` (opened, read, closed).
pub fn read_dir(dir: &Path, cfg: WalConfig) -> (Lsn, Vec<(Lsn, Vec<u8>)>) {
    let (wal, _) = Wal::open(dir, cfg).expect("open");
    (wal.durable_lsn(), read_all(&wal))
}

/// Deterministic, LSN-derived payload of `len` bytes.
pub fn payload(lsn: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (lsn as u8).wrapping_mul(31).wrapping_add(i as u8))
        .collect()
}

/// A spawned receiver thread: yields the receiver and each session's result.
pub type ReceiverThread = JoinHandle<(Receiver, Vec<Result<(), ReplError>>)>;

/// The **independent R4 oracle**: a `DurabilityObserver` on the replica WAL.
/// The WAL fires it inside `commit`, after the `fdatasync` — so its value is
/// what the replica WAL itself certifies durable, not anything the receiver
/// reports about itself. Every ACK a primary sees must be `≤` it at receipt.
#[derive(Clone, Default)]
pub struct DurableTap(pub Arc<AtomicU64>);

impl DurableTap {
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

impl open_wal::DurabilityObserver for DurableTap {
    fn on_durable(&mut self, d: Lsn) {
        self.0.fetch_max(d.0, Ordering::AcqRel);
    }
}

/// Like [`spawn_receiver`], but the replica WAL carries a [`DurableTap`].
pub fn spawn_receiver_tapped(
    dir: &Path,
    cfg: ReceiverConfig,
    connections: usize,
) -> (SocketAddr, TappedReceiverThread, DurableTap) {
    let tap = DurableTap::default();
    let mut rx = Receiver::open_with(dir, cfg, tap.clone()).expect("receiver open");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let h = std::thread::spawn(move || {
        let mut results = Vec::new();
        for _ in 0..connections {
            let (s, _) = listener.accept().expect("accept");
            results.push(rx.serve_connection(s));
        }
        (rx, results)
    });
    (addr, h, tap)
}

/// A spawned tapped receiver thread.
pub type TappedReceiverThread = JoinHandle<(Receiver<DurableTap>, Vec<Result<(), ReplError>>)>;

/// A receiver thread: accepts `connections` connections on a fresh loopback
/// listener and serves each, returning the receiver and each session result.
pub fn spawn_receiver(
    dir: &Path,
    cfg: ReceiverConfig,
    connections: usize,
) -> (SocketAddr, ReceiverThread) {
    let mut rx = Receiver::open(dir, cfg).expect("receiver open");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let h = std::thread::spawn(move || {
        let mut results = Vec::new();
        for _ in 0..connections {
            let (s, _) = listener.accept().expect("accept");
            results.push(rx.serve_connection(s));
        }
        (rx, results)
    });
    (addr, h)
}

/// One owned frame as observed by the fake primary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Got {
    Hello(u64),
    Ack(u64),
    Heartbeat(u64),
    Err(wire::ErrCode),
    Other(String),
}

/// The primary side of one connection, driven by hand.
pub struct FakePrimary {
    stream: TcpStream,
    rd: FrameReader<TcpStream>,
    buf: Vec<u8>,
}

impl FakePrimary {
    pub fn connect(addr: SocketAddr) -> FakePrimary {
        let stream = TcpStream::connect(addr).expect("connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let rd = FrameReader::new(stream.try_clone().unwrap(), Limits::new(1 << 20));
        FakePrimary {
            stream,
            rd,
            buf: Vec::new(),
        }
    }

    pub fn next(&mut self) -> Option<Got> {
        match self.rd.read_frame() {
            Ok(Frame::Hello {
                proto_ver,
                durable_lsn,
                ..
            }) => {
                assert_eq!(proto_ver, PROTO_VERSION);
                Some(Got::Hello(durable_lsn.0))
            }
            Ok(Frame::Ack { durable_lsn }) => Some(Got::Ack(durable_lsn.0)),
            Ok(Frame::Heartbeat { durable_lsn }) => Some(Got::Heartbeat(durable_lsn.0)),
            Ok(Frame::Err { code, .. }) => Some(Got::Err(code)),
            Ok(other) => Some(Got::Other(format!("{other:?}"))),
            Err(_) => None,
        }
    }

    /// Read the replica's HELLO and return its `durable_lsn`.
    pub fn hello(&mut self) -> u64 {
        match self.next() {
            Some(Got::Hello(d)) => d,
            other => panic!("expected HELLO, got {other:?}"),
        }
    }

    pub fn send(&mut self, f: &Frame<'_>) {
        self.buf.clear();
        wire::encode_frame(f, &mut self.buf);
        self.stream.write_all(&self.buf).expect("send");
    }

    pub fn send_raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).expect("send raw");
    }

    pub fn serving(&mut self, from: u64) {
        self.send(&Frame::Serving {
            from_lsn: Lsn(from),
            primary_oldest: Lsn(1),
        });
    }

    pub fn record(&mut self, lsn: u64, payload: &[u8]) {
        self.send(&Frame::Record {
            lsn: Lsn(lsn),
            payload,
        });
    }

    /// Send a batch of records in one `write` (exercises frame reassembly).
    pub fn records(&mut self, recs: &[(Lsn, Vec<u8>)]) {
        self.buf.clear();
        for (lsn, p) in recs {
            wire::encode_record(&mut self.buf, *lsn, p);
        }
        self.stream.write_all(&self.buf).expect("send records");
    }

    /// Read until an ACK of `target` arrives; returns every ACK value seen.
    pub fn await_ack(&mut self, target: u64) -> Vec<u64> {
        let mut acks = Vec::new();
        loop {
            match self.next() {
                Some(Got::Ack(a)) => {
                    acks.push(a);
                    if a >= target {
                        return acks;
                    }
                }
                Some(Got::Heartbeat(_)) => {}
                other => panic!("waiting for ACK {target}, got {other:?} after {acks:?}"),
            }
        }
    }

    /// Like [`await_ack`](Self::await_ack), checking R4 against the
    /// independent oracle: every ACK must be `≤` the replica WAL's own durable
    /// watermark (the tap) at the moment the ACK is received.
    pub fn await_ack_tapped(&mut self, target: u64, tap: &DurableTap) -> Vec<u64> {
        let mut acks = Vec::new();
        loop {
            match self.next() {
                Some(Got::Ack(a)) => {
                    let durable = tap.get();
                    assert!(
                        a <= durable,
                        "R4: ACK {a} received while the replica WAL is durable only to {durable}"
                    );
                    acks.push(a);
                    if a >= target {
                        return acks;
                    }
                }
                Some(Got::Heartbeat(_)) => {}
                other => panic!("waiting for ACK {target}, got {other:?} after {acks:?}"),
            }
        }
    }

    /// Close our sending half and read everything the replica still sends.
    pub fn finish(mut self) -> Vec<Got> {
        let _ = self.stream.shutdown(Shutdown::Write);
        let mut rest = Vec::new();
        while let Some(g) = self.next() {
            rest.push(g);
        }
        rest
    }
}

/// A real `Receiver` serving connections in a loop on its own thread until
/// [`ReplicaServer::stop`].
pub struct ReplicaServer {
    pub addr: SocketAddr,
    pub watermarks: std::sync::Arc<open_wal_replica::ReplicaWatermarks>,
    /// Independent R4 oracle (see [`DurableTap`]).
    pub tap: DurableTap,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: TappedReceiverThread,
}

impl ReplicaServer {
    pub fn start(dir: &Path, cfg: ReceiverConfig) -> ReplicaServer {
        Self::start_on(TcpListener::bind("127.0.0.1:0").expect("bind"), dir, cfg)
    }

    pub fn start_on(listener: TcpListener, dir: &Path, cfg: ReceiverConfig) -> ReplicaServer {
        use std::sync::atomic::{AtomicBool, Ordering};
        let tap = DurableTap::default();
        let mut rx = Receiver::open_with(dir, cfg, tap.clone()).expect("receiver open");
        let addr = listener.local_addr().unwrap();
        let watermarks = rx.watermarks();
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let st = std::sync::Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut results = Vec::new();
            loop {
                let (s, _) = listener.accept().expect("accept");
                if st.load(Ordering::Acquire) {
                    return (rx, results);
                }
                results.push(rx.serve_connection(s));
            }
        });
        ReplicaServer {
            addr,
            watermarks,
            tap,
            stop,
            handle,
        }
    }

    /// Stop accepting (the current connection must already be closed by the
    /// primary) and return the receiver plus every session's result.
    pub fn stop(self) -> (Receiver<DurableTap>, Vec<Result<(), ReplError>>) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        let _ = TcpStream::connect(self.addr);
        self.handle.join().expect("receiver thread")
    }
}

/// Spin (sleeping briefly) until `f` holds or `secs` elapse; panics on timeout.
pub fn wait_until(secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

// ===========================================================================
// RM3 shared infrastructure (kickoff C): independent oracles.
// ===========================================================================

use std::io::Read;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use open_wal_replica::wire::{Decoded, ErrCode};

/// The first primary→replica frame of a proxied session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum First {
    Serving { from: u64, oldest: u64 },
    Reseed { oldest: u64 },
    Record(u64),
    Heartbeat,
    Err(ErrCode),
    Other,
}

/// What the proxy saw on one session (one primary connection).
#[derive(Clone, Debug, Default)]
pub struct SessionLog {
    /// The replica's `HELLO.durable_lsn`.
    pub hello: Option<u64>,
    /// The first primary→replica frame.
    pub first: Option<First>,
    /// `SERVING{from, primary_oldest}`.
    pub serving: Option<(u64, u64)>,
    pub first_record: Option<u64>,
    pub last_record: Option<u64>,
    pub records: u64,
    /// Primary→replica heartbeats.
    pub heartbeats: u64,
    /// `ERR` frames, either direction (`true` = from the primary).
    pub errs: Vec<(bool, ErrCode)>,
    /// The primary closed its sending half cleanly (FIN).
    pub primary_eof: bool,
    /// Highest ACK / replica heartbeat forwarded to the primary.
    pub max_ack: u64,
}

struct ProxyInner {
    upstream: SocketAddr,
    tap: DurableTap,
    max_payload: u32,
    paused: AtomicBool,
    stop: AtomicBool,
    sessions: Mutex<Vec<SessionLog>>,
    conns: Mutex<Vec<(TcpStream, TcpStream)>>,
    violations: Mutex<Vec<String>>,
    max_ack: AtomicU64,
}

impl ProxyInner {
    fn violate(&self, session: usize, what: String) {
        self.violations
            .lock()
            .unwrap()
            .push(format!("session {session}: {what}"));
    }

    fn log<R>(&self, session: usize, f: impl FnOnce(&mut SessionLog) -> R) -> R {
        f(&mut self.sessions.lock().unwrap()[session])
    }
}

/// A loopback **forwarding proxy** between the `Shipper` and the real
/// `Receiver` — the wire-level oracle, independent of both ends.
///
/// It forwards raw bytes and tee-parses them (a parse error is a violation),
/// propagates half-closes, and counts sessions. Per session it checks
/// `SERVING.from == HELLO.d + 1`, dense `RECORD` LSNs starting at
/// `SERVING.from`, no `ERR` in either direction, and every `ACK` (and replica
/// heartbeat) `≤` the replica WAL's [`DurableTap`] **at forwarding time**. It
/// can pause primary→replica forwarding (backpressure the shipper), wait for a
/// forwarded ACK, and cut the current session's sockets.
pub struct Proxy {
    pub addr: SocketAddr,
    inner: Arc<ProxyInner>,
    handle: Option<JoinHandle<()>>,
}

impl Proxy {
    pub fn start(upstream: SocketAddr, tap: DurableTap, max_payload: u32) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").expect("proxy bind");
        let addr = listener.local_addr().unwrap();
        let inner = Arc::new(ProxyInner {
            upstream,
            tap,
            max_payload,
            paused: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            sessions: Mutex::new(Vec::new()),
            conns: Mutex::new(Vec::new()),
            violations: Mutex::new(Vec::new()),
            max_ack: AtomicU64::new(0),
        });
        let inn = Arc::clone(&inner);
        let handle = std::thread::spawn(move || {
            loop {
                let (p, _) = listener.accept().expect("proxy accept");
                if inn.stop.load(Ordering::Acquire) {
                    return;
                }
                let Ok(r) = TcpStream::connect(inn.upstream) else {
                    continue; // receiver gone: drop the primary's connection
                };
                let _ = p.set_nodelay(true);
                let _ = r.set_nodelay(true);
                let session = {
                    let mut s = inn.sessions.lock().unwrap();
                    s.push(SessionLog::default());
                    s.len() - 1
                };
                inn.conns
                    .lock()
                    .unwrap()
                    .push((p.try_clone().unwrap(), r.try_clone().unwrap()));
                let (p2, r2) = (p.try_clone().unwrap(), r.try_clone().unwrap());
                let (a, b) = (Arc::clone(&inn), Arc::clone(&inn));
                std::thread::spawn(move || pump(&a, session, p, r, true));
                std::thread::spawn(move || pump(&b, session, r2, p2, false));
            }
        });
        Proxy {
            addr,
            inner,
            handle: Some(handle),
        }
    }

    /// Stop forwarding primary→replica bytes (the shipper's writes back up).
    pub fn pause(&self) {
        self.inner.paused.store(true, Ordering::Release);
    }

    pub fn resume(&self) {
        self.inner.paused.store(false, Ordering::Release);
    }

    pub fn session_count(&self) -> usize {
        self.inner.sessions.lock().unwrap().len()
    }

    pub fn sessions(&self) -> Vec<SessionLog> {
        self.inner.sessions.lock().unwrap().clone()
    }

    /// The latest session's log (panics if none yet).
    pub fn last(&self) -> SessionLog {
        self.sessions().last().cloned().expect("no session yet")
    }

    pub fn violations(&self) -> Vec<String> {
        self.inner.violations.lock().unwrap().clone()
    }

    /// No parse error, no `ERR` frame, dense records, honest `SERVING`, and
    /// every ACK `≤` the tap at forwarding time.
    pub fn assert_clean(&self) {
        let v = self.violations();
        assert!(v.is_empty(), "proxy oracle violations: {v:#?}");
    }

    /// Highest ACK forwarded to the primary so far (any session).
    pub fn max_ack(&self) -> u64 {
        self.inner.max_ack.load(Ordering::Acquire)
    }

    /// Wait until an ACK `≥ lsn` has been forwarded to the primary.
    pub fn wait_ack(&self, lsn: u64, secs: u64) {
        wait_until(secs, "a forwarded ACK", || self.max_ack() >= lsn);
    }

    /// Cut the latest session: both sockets are shut down in both directions.
    pub fn cut_current(&self) {
        if let Some((p, r)) = self.inner.conns.lock().unwrap().last() {
            let _ = p.shutdown(Shutdown::Both);
            let _ = r.shutdown(Shutdown::Both);
        }
    }

    pub fn stop(mut self) {
        self.inner.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.addr);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Forward `from → to`, tee-parsing every frame. `p2r`: primary→replica.
fn pump(inner: &ProxyInner, session: usize, mut from: TcpStream, mut to: TcpStream, p2r: bool) {
    let limits = Limits::new(if p2r { inner.max_payload } else { 0 });
    let mut parse: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if p2r {
            while inner.paused.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let n = match from.read(&mut buf) {
            Ok(0) => {
                if p2r {
                    inner.log(session, |l| l.primary_eof = true);
                }
                let _ = to.shutdown(Shutdown::Write); // propagate the half-close
                return;
            }
            Ok(n) => n,
            Err(_) => {
                let _ = to.shutdown(Shutdown::Both);
                return;
            }
        };
        if p2r {
            // Bytes read while a pause began are held, not forwarded.
            while inner.paused.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        parse.extend_from_slice(&buf[..n]);
        let mut off = 0;
        loop {
            match wire::decode_frame(&parse[off..], &limits) {
                Ok(Decoded::Frame(f, used)) => {
                    if p2r {
                        observe_p2r(inner, session, &f);
                    } else {
                        observe_r2p(inner, session, &f);
                    }
                    off += used;
                }
                Ok(Decoded::Incomplete { .. }) => break,
                Err(e) => {
                    inner.violate(session, format!("unparseable frame (p2r={p2r}): {e:?}"));
                    let _ = to.shutdown(Shutdown::Both);
                    let _ = from.shutdown(Shutdown::Both);
                    return;
                }
            }
        }
        parse.drain(..off);
        if to.write_all(&buf[..n]).is_err() {
            let _ = from.shutdown(Shutdown::Both);
            return;
        }
    }
}

fn observe_p2r(inner: &ProxyInner, session: usize, f: &Frame<'_>) {
    let first = match *f {
        Frame::Serving {
            from_lsn,
            primary_oldest,
        } => First::Serving {
            from: from_lsn.0,
            oldest: primary_oldest.0,
        },
        Frame::ReseedRequired { primary_oldest } => First::Reseed {
            oldest: primary_oldest.0,
        },
        Frame::Record { lsn, .. } => First::Record(lsn.0),
        Frame::Heartbeat { .. } => First::Heartbeat,
        Frame::Err { code, .. } => First::Err(code),
        _ => First::Other,
    };
    let mut bad = None;
    inner.log(session, |l| {
        l.first.get_or_insert(first);
        match first {
            First::Serving { from, oldest } => {
                if l.serving.is_some() {
                    bad = Some("a second SERVING".to_string());
                }
                if l.hello.map(|d| d + 1) != Some(from) {
                    bad = Some(format!(
                        "SERVING.from {from} != HELLO.d + 1 ({:?})",
                        l.hello
                    ));
                }
                l.serving = Some((from, oldest));
            }
            First::Record(lsn) => {
                let want = match (l.last_record, l.serving) {
                    (Some(last), _) => Some(last + 1),
                    (None, Some((from, _))) => Some(from),
                    (None, None) => None,
                };
                if want != Some(lsn) {
                    bad = Some(format!("RECORD {lsn} is not dense (expected {want:?})"));
                }
                l.first_record.get_or_insert(lsn);
                l.last_record = Some(lsn);
                l.records += 1;
            }
            First::Heartbeat => l.heartbeats += 1,
            First::Err(code) => {
                l.errs.push((true, code));
                bad = Some(format!("ERR({code:?}) from the primary"));
            }
            First::Reseed { .. } => {}
            First::Other => bad = Some("unexpected primary→replica frame".to_string()),
        }
    });
    if let Some(b) = bad {
        inner.violate(session, b);
    }
}

fn observe_r2p(inner: &ProxyInner, session: usize, f: &Frame<'_>) {
    match *f {
        Frame::Hello { durable_lsn, .. } => inner.log(session, |l| l.hello = Some(durable_lsn.0)),
        Frame::Ack { durable_lsn } | Frame::Heartbeat { durable_lsn } => {
            // R4, independent oracle: the replica WAL's own observer, read at
            // forwarding time (before the primary can see the ACK).
            let durable = inner.tap.get();
            if durable_lsn.0 > durable {
                inner.violate(
                    session,
                    format!(
                        "R4: ACK {} forwarded while the replica WAL is durable only to {durable}",
                        durable_lsn.0
                    ),
                );
            }
            inner.log(session, |l| l.max_ack = l.max_ack.max(durable_lsn.0));
            if matches!(f, Frame::Ack { .. }) {
                inner.max_ack.fetch_max(durable_lsn.0, Ordering::AcqRel);
            }
        }
        Frame::Err { code, .. } => {
            inner.log(session, |l| l.errs.push((false, code)));
            inner.violate(session, format!("ERR({code:?}) from the replica"));
        }
        _ => inner.violate(session, "unexpected replica→primary frame".to_string()),
    }
}

/// A real `Receiver` behind a [`Proxy`]: point the shipper at `proxy.addr`.
pub struct Proxied {
    pub server: ReplicaServer,
    pub proxy: Proxy,
}

impl Proxied {
    pub fn start(dir: &Path, cfg: ReceiverConfig) -> Proxied {
        let max = cfg.wal_config.max_record_size;
        let server = ReplicaServer::start(dir, cfg);
        let proxy = Proxy::start(server.addr, server.tap.clone(), max);
        Proxied { server, proxy }
    }

    pub fn addr(&self) -> SocketAddr {
        self.proxy.addr
    }
}

// ---- /proc/thread-self/io probe (Linux): the calling thread's read volume ----

/// `rchar` / `syscr` of the calling thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoCount {
    pub rchar: u64,
    pub syscr: u64,
}

/// Read `/proc/thread-self/io`. `None` off Linux or without
/// `CONFIG_TASK_IO_ACCOUNTING`.
pub fn thread_io() -> Option<IoCount> {
    let s = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    let field = |k: &str| {
        s.lines()
            .find_map(|l| l.strip_prefix(k))
            .and_then(|v| v.trim().parse::<u64>().ok())
    };
    Some(IoCount {
        rchar: field("rchar:")?,
        syscr: field("syscr:")?,
    })
}

/// Whether the probe works here. Tests that need it print a loud skip.
pub fn io_probe_available() -> bool {
    thread_io().is_some()
}

/// What one probe read itself adds to the counters (measured).
pub fn io_probe_self_cost() -> IoCount {
    let mut worst = IoCount::default();
    for _ in 0..4 {
        let a = thread_io().expect("io probe");
        let b = thread_io().expect("io probe");
        worst.rchar = worst.rchar.max(b.rchar - a.rchar);
        worst.syscr = worst.syscr.max(b.syscr - a.syscr);
    }
    worst
}

/// Run `f` and return the calling thread's `rchar`/`syscr` delta across it,
/// minus the probe's measured self-cost.
pub fn measure_io<T>(f: impl FnOnce() -> T) -> (T, IoCount) {
    let cost = io_probe_self_cost();
    let a = thread_io().expect("io probe");
    let out = f();
    let b = thread_io().expect("io probe");
    (
        out,
        IoCount {
            rchar: (b.rchar - a.rchar).saturating_sub(cost.rchar),
            syscr: (b.syscr - a.syscr).saturating_sub(cost.syscr),
        },
    )
}

// ---- a hand-driven fake replica (the wire is the oracle) ----

/// One frame as a fake replica saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Saw {
    Serving {
        from: u64,
        oldest: u64,
    },
    Reseed {
        oldest: u64,
    },
    Record(u64, Vec<u8>),
    Heartbeat(u64),
    Err(ErrCode, String),
    /// The primary closed the connection.
    Eof,
    /// Nothing arrived within the timeout.
    Timeout,
}

/// A listener a `Shipper` connects to, driven by hand.
pub struct Fake {
    listener: TcpListener,
    max_payload: u32,
}

impl Fake {
    pub fn new(max_payload: u32) -> Fake {
        Fake {
            listener: TcpListener::bind("127.0.0.1:0").unwrap(),
            max_payload,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// Accept the primary's next connection and send `HELLO{durable}`.
    pub fn hello(&self, durable: u64) -> FakeConn {
        self.listener.set_nonblocking(false).unwrap();
        let (stream, _) = self.listener.accept().unwrap();
        let mut c = FakeConn {
            rd: FrameReader::new(stream.try_clone().unwrap(), Limits::new(self.max_payload)),
            stream,
        };
        c.send(&Frame::Hello {
            proto_ver: PROTO_VERSION,
            replica_id: 9,
            durable_lsn: Lsn(durable),
        });
        c
    }

    /// Like [`hello`](Self::hello), but gives up after `timeout` if the primary
    /// does not connect.
    pub fn try_hello(&self, durable: u64, timeout: Duration) -> Option<FakeConn> {
        self.listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + timeout;
        let stream = loop {
            match self.listener.accept() {
                Ok((s, _)) => break s,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(_) => {
                    self.listener.set_nonblocking(false).unwrap();
                    return None;
                }
            }
        };
        self.listener.set_nonblocking(false).unwrap();
        stream.set_nonblocking(false).unwrap();
        let mut c = FakeConn {
            rd: FrameReader::new(stream.try_clone().unwrap(), Limits::new(self.max_payload)),
            stream,
        };
        c.send(&Frame::Hello {
            proto_ver: PROTO_VERSION,
            replica_id: 9,
            durable_lsn: Lsn(durable),
        });
        Some(c)
    }

    /// How many further connections the primary has opened (non-blocking):
    /// an independent count of reconnects.
    pub fn pending_connections(&self) -> usize {
        self.listener.set_nonblocking(true).unwrap();
        let mut n = 0;
        let mut held = Vec::new();
        while let Ok((s, _)) = self.listener.accept() {
            held.push(s);
            n += 1;
        }
        self.listener.set_nonblocking(false).unwrap();
        n
    }
}

pub struct FakeConn {
    stream: TcpStream,
    rd: FrameReader<TcpStream>,
}

impl FakeConn {
    pub fn send(&mut self, f: &Frame<'_>) {
        let mut b = Vec::new();
        wire::encode_frame(f, &mut b);
        let _ = self.stream.write_all(&b);
    }

    pub fn ack(&mut self, lsn: u64) {
        self.send(&Frame::Ack {
            durable_lsn: Lsn(lsn),
        });
    }

    /// The next frame, or `Timeout` / `Eof`.
    pub fn next(&mut self, timeout: Duration) -> Saw {
        self.stream
            .set_read_timeout(Some(timeout.max(Duration::from_millis(1))))
            .unwrap();
        match self.rd.read_frame() {
            Ok(Frame::Serving {
                from_lsn,
                primary_oldest,
            }) => Saw::Serving {
                from: from_lsn.0,
                oldest: primary_oldest.0,
            },
            Ok(Frame::ReseedRequired { primary_oldest }) => Saw::Reseed {
                oldest: primary_oldest.0,
            },
            Ok(Frame::Record { lsn, payload }) => Saw::Record(lsn.0, payload.to_vec()),
            Ok(Frame::Heartbeat { durable_lsn }) => Saw::Heartbeat(durable_lsn.0),
            Ok(Frame::Err { code, msg }) => Saw::Err(code, msg.to_owned()),
            Ok(other) => panic!("fake replica: unexpected {other:?}"),
            Err(ReplError::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Saw::Timeout
            }
            Err(_) => Saw::Eof,
        }
    }

    /// The next frame that is not a heartbeat.
    pub fn next_data(&mut self, timeout: Duration) -> Saw {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.next(left) {
                Saw::Heartbeat(_) if Instant::now() < deadline => {}
                Saw::Heartbeat(_) => return Saw::Timeout,
                other => return other,
            }
        }
    }

    /// Everything that arrives within `window` (heartbeats included), stopping
    /// early at EOF.
    pub fn drain(&mut self, window: Duration) -> Vec<Saw> {
        let deadline = Instant::now() + window;
        let mut out = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return out;
            }
            match self.next(left) {
                Saw::Timeout => return out,
                Saw::Eof => {
                    out.push(Saw::Eof);
                    return out;
                }
                s => out.push(s),
            }
        }
    }

    /// Like [`drain`](Self::drain), keeping only `RECORD` LSNs + payloads.
    pub fn drain_records(&mut self, window: Duration) -> Vec<(u64, Vec<u8>)> {
        self.drain(window)
            .into_iter()
            .filter_map(|s| match s {
                Saw::Record(l, p) => Some((l, p)),
                _ => None,
            })
            .collect()
    }
}

/// The sorted base LSNs of the segment files in `dir` — read from the
/// directory itself (independent of the `Wal` handle).
pub fn segment_bases(dir: &Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().into_string().ok()?;
            name.strip_suffix(".wal")?.parse::<u64>().ok()
        })
        .collect();
    v.sort_unstable();
    v
}

/// The path of the segment file with base LSN `base`.
pub fn segment_path(dir: &Path, base: u64) -> std::path::PathBuf {
    dir.join(format!("{base:020}.wal"))
}

// ===========================================================================
// RM4 shared infrastructure (kickoff E): a real `Receiver` opened with
// `Receiver::open_seeded` and driven by `Receiver::run`.
// ===========================================================================

use open_wal_replica::SeedReport;

/// What a [`SeededServer`]'s thread returns: the receiver and what `run`
/// ended with.
pub type RunOutcome = (Receiver<DurableTap>, Result<(), ReplError>);

/// A real `Receiver`, opened with `Receiver::open_seeded`, running
/// **`Receiver::run`** on its own thread — so it stops by itself on a fatal
/// error (`ReseedRequired`, a poisoned WAL), exactly as an integrator's replica
/// does. (`ReplicaServer` loops `serve_connection` and never stops.)
///
/// The listener is shared (`Arc`) so that a re-seeded receiver can be started
/// on the **same address**: the proxy's upstream and the shipper's replica
/// address are fixed for a whole test.
///
/// The [`DurableTap`] is the caller's and is shared across restarts. The WAL
/// does not call its observer at open, and a receiver answers heartbeats with
/// its recovered `durable_lsn` (after a seeded cold start: `s`), so the tap is
/// seeded here — with the opened WAL's own `durable_lsn()` — before the thread
/// (and with it any proxied session) starts.
pub struct SeededServer {
    pub addr: SocketAddr,
    pub tap: DurableTap,
    /// What `open_seeded` reported.
    pub report: SeedReport,
    /// How many times `open_seeded` called the observer factory.
    pub observer_calls: usize,
    pub watermarks: Arc<open_wal_replica::ReplicaWatermarks>,
    listener: Arc<TcpListener>,
    handle: JoinHandle<RunOutcome>,
}

impl SeededServer {
    pub fn start_on(
        listener: Arc<TcpListener>,
        dir: &Path,
        cfg: ReceiverConfig,
        installed: Option<Lsn>,
        tap: DurableTap,
    ) -> Result<SeededServer, ReplError> {
        let mut observer_calls = 0;
        let (mut rx, report) = Receiver::open_seeded(dir, cfg, installed, || {
            observer_calls += 1;
            tap.clone()
        })?;
        tap.0.fetch_max(rx.wal().durable_lsn().0, Ordering::AcqRel);
        let addr = listener.local_addr().unwrap();
        let watermarks = rx.watermarks();
        let l = Arc::clone(&listener);
        let handle = std::thread::spawn(move || {
            let res = rx.run(&l);
            (rx, res)
        });
        Ok(SeededServer {
            addr,
            tap,
            report,
            observer_calls,
            watermarks,
            listener,
            handle,
        })
    }

    /// `run` has returned.
    pub fn finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Wait for `run` to return by itself (e.g. on `ReseedRequired`).
    pub fn join(self) -> RunOutcome {
        self.handle.join().expect("receiver thread")
    }

    /// Make `run` return: its next `accept` fails with `WouldBlock` (the
    /// result is then `Err(Io)`, unless `run` had already ended). The current
    /// connection must already be closed by the primary.
    pub fn stop(self) -> RunOutcome {
        self.listener.set_nonblocking(true).unwrap();
        // Wake an `accept` that is already blocked.
        drop(TcpStream::connect(self.addr));
        let out = self.handle.join().expect("receiver thread");
        self.listener.set_nonblocking(false).unwrap();
        out
    }
}

/// The `<dir>.stale.<k>` directories next to `dir`, sorted by name.
pub fn stale_dirs(dir: &Path) -> Vec<std::path::PathBuf> {
    let prefix = format!("{}.stale.", dir.file_name().unwrap().to_str().unwrap());
    let parent = match dir.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let mut v: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&prefix))
        })
        .collect();
    v.sort();
    v
}

/// `<dir>.stale.<k>`.
pub fn stale_path(dir: &Path, k: u32) -> std::path::PathBuf {
    let mut n = dir.file_name().unwrap().to_os_string();
    n.push(format!(".stale.{k}"));
    dir.with_file_name(n)
}
