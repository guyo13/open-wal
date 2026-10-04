//! Shared helpers for the replica integration tests: tiny WAL configs that
//! force segment rolls, log readers, and an in-process **fake primary** that
//! speaks the §6 wire protocol over a real loopback socket.
#![allow(dead_code)] // each test binary uses a different subset

use std::io::Write;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
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
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: JoinHandle<(Receiver, Vec<Result<(), ReplError>>)>,
}

impl ReplicaServer {
    pub fn start(dir: &Path, cfg: ReceiverConfig) -> ReplicaServer {
        Self::start_on(TcpListener::bind("127.0.0.1:0").expect("bind"), dir, cfg)
    }

    pub fn start_on(listener: TcpListener, dir: &Path, cfg: ReceiverConfig) -> ReplicaServer {
        use std::sync::atomic::{AtomicBool, Ordering};
        let mut rx = Receiver::open(dir, cfg).expect("receiver open");
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
            stop,
            handle,
        }
    }

    /// Stop accepting (the current connection must already be closed by the
    /// primary) and return the receiver plus every session's result.
    pub fn stop(self) -> (Receiver, Vec<Result<(), ReplError>>) {
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
