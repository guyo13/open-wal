//! The primary side: [`Shipper`] (§8 and §4.2 of `docs/replica_design_v1.md`).
//!
//! The integrator calls, **on the WAL writer thread**:
//! - [`capture`](Shipper::capture)`(lsn, payload)` right after each
//!   `wal.append` — one bounded memcpy into the ring, no I/O, never blocks;
//! - [`on_commit`](Shipper::on_commit)`(w)` right after each
//!   `wal.commit() → Ok(w)` — releases records `≤ w` (R1) and wakes the network
//!   threads. It sends nothing. **Never** call it with `last_lsn`, and never
//!   after `commit` returned `Err` (the WAL is poisoned; captured-but-unreleased
//!   records are discarded — see [`ring::Producer::capture`]).
//! - [`poll`](Shipper::poll)`()` when idle — serves replicas waiting to join
//!   (also done inside `on_commit`). Pure memory, never blocks.
//!
//! Everything that can block runs on **one network thread per replica** (plus a
//! companion thread reading that replica's `ACK`s): connect with backoff, read
//! `HELLO`, ask the writer thread to join the replica's ring cursor at
//! `durable_lsn + 1` (R7 — the primary persists no shipping position), send
//! `SERVING`, then drain released records into `RECORD` frames, heartbeat when
//! idle, and record acks. A slow or dead replica only ever stalls its own
//! thread; the ring overflows past it ([`ReplicaStatus::NeedsCatchUp`]) rather
//! than slowing the writer or the other replicas.
//!
//! **RM2 scope:** a replica whose resume point is no longer in the ring is
//! marked `NeedsCatchUp` and held (heartbeats only); serving it from the log
//! (§9 catch-up) is RM3, which needs WAL v7. `SERVING.primary_oldest` is sent as
//! `Lsn(0)` ("not reported") until RM3 can read `wal.oldest_lsn()`.

use std::io::Write;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

use open_wal::{DurabilityObserver, Lsn, Wal};

use crate::error::{ReplError, Result};
use crate::ring::{self, AttachError, Consumer, ConsumerStatus, Producer, RingConfig};
use crate::wire::{self, ErrCode, Frame, FrameReader, Limits, PROTO_VERSION};

/// Index of a replica in [`ShipperConfig::replicas`].
pub type ReplicaId = usize;

/// Shipper configuration (§14).
#[derive(Clone, Debug)]
pub struct ShipperConfig {
    /// Replica addresses; the primary connects to each (the replica's
    /// [`Receiver`](crate::Receiver) listens). Index = [`ReplicaId`].
    pub replicas: Vec<SocketAddr>,
    /// Total ring payload capacity in bytes (§8.1). The ring has
    /// `max(2, ring_bytes / slot_bytes)` fixed slots.
    pub ring_bytes: usize,
    /// Preallocated bytes per ring slot. Size it to the typical payload; a
    /// larger payload grows its slot once (the §8.1 fallback allocation).
    pub slot_bytes: usize,
    /// Idle keepalive period; also the bound on how long a network thread
    /// sleeps between checks.
    pub heartbeat: Duration,
    /// First reconnect delay; doubles per failed attempt.
    pub reconnect_backoff: Duration,
    /// Cap on the reconnect delay.
    pub max_reconnect_backoff: Duration,
    /// Connect / write / handshake timeout. A replica that stops reading
    /// stalls only its own network thread, for at most this long per write.
    pub io_timeout: Duration,
    /// Records copied out of the ring per network write.
    pub send_batch: usize,
}

impl ShipperConfig {
    /// Defaults around `replicas`: a 64 MiB ring of 4 KiB slots, 500 ms
    /// heartbeat, 50 ms → 5 s reconnect backoff, 5 s I/O timeout, 256-record
    /// send batches.
    #[must_use]
    pub fn new(replicas: Vec<SocketAddr>) -> ShipperConfig {
        ShipperConfig {
            replicas,
            ring_bytes: 64 * 1024 * 1024,
            slot_bytes: 4096,
            heartbeat: Duration::from_millis(500),
            reconnect_backoff: Duration::from_millis(50),
            max_reconnect_backoff: Duration::from_secs(5),
            io_timeout: Duration::from_secs(5),
            send_batch: 256,
        }
    }
}

/// A replica connection's state, as seen by the primary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplicaStatus {
    /// Not connected (connecting, or backing off).
    Connecting = 0,
    /// Connected; `HELLO` received; waiting for the writer thread to join it.
    Joining = 1,
    /// Receiving the live stream from the ring.
    Streaming = 2,
    /// Its resume point is no longer in the ring (overflow, §8.3, or it
    /// connected too far behind). Needs catch-up from the log (§9, RM3).
    NeedsCatchUp = 3,
    /// Its `durable_lsn` is ahead of the primary's (§9 step 2): a former
    /// primary or a bug. Refused with `ERR(Contiguity)`; it must be re-seeded.
    Rejected = 4,
}

impl ReplicaStatus {
    fn from_u8(v: u8) -> ReplicaStatus {
        match v {
            1 => ReplicaStatus::Joining,
            2 => ReplicaStatus::Streaming,
            3 => ReplicaStatus::NeedsCatchUp,
            4 => ReplicaStatus::Rejected,
            _ => ReplicaStatus::Connecting,
        }
    }
}

const JOIN_PENDING: u8 = 0;
const JOIN_OK: u8 = 1;
const JOIN_BEHIND: u8 = 2;
const JOIN_AHEAD: u8 = 3;

/// Per-replica state shared between the writer thread and its network thread.
#[derive(Default)]
struct ReplicaShared {
    /// Highest `ACK`/`HEARTBEAT` watermark this replica reported (R4).
    acked: AtomicU64,
    status: AtomicU8,
    /// `from` LSN of a pending join request (0 = none).
    join_req: AtomicU64,
    join_resp: AtomicU8,
    /// Times this replica was found `NeedsCatchUp` (observability).
    catch_ups: AtomicU64,
}

struct Net {
    shutdown: AtomicBool,
    join_pending: AtomicBool,
    replicas: Box<[ReplicaShared]>,
}

/// The primary's replication shipper (§8).
pub struct Shipper {
    producer: Producer,
    net: Arc<Net>,
    threads: Vec<JoinHandle<()>>,
}

impl Shipper {
    /// Start shipping `wal`'s future records to `cfg.replicas`. Call on the
    /// writer thread with the WAL open (after recovery): the ring starts at
    /// `wal.last_lsn() + 1` with `wal.durable_lsn()` already released. Spawns
    /// one network thread per replica.
    pub fn new<O: DurabilityObserver>(cfg: ShipperConfig, wal: &Wal<O>) -> Result<Shipper> {
        let n = cfg.replicas.len();
        if n == 0 || n > ring::MAX_CONSUMERS {
            return Err(ReplError::Protocol("shipper needs 1..=255 replicas"));
        }
        if cfg.slot_bytes == 0 || cfg.send_batch == 0 {
            return Err(ReplError::Protocol("slot_bytes and send_batch must be > 0"));
        }
        let ring_cfg = RingConfig {
            slots: (cfg.ring_bytes / cfg.slot_bytes).max(2),
            slot_bytes: cfg.slot_bytes,
            consumers: n,
        };
        let (mut producer, consumers) =
            ring::ring(ring_cfg, wal.last_lsn().next(), wal.durable_lsn());
        let net = Arc::new(Net {
            shutdown: AtomicBool::new(false),
            join_pending: AtomicBool::new(false),
            replicas: (0..n).map(|_| ReplicaShared::default()).collect(),
        });
        let mut threads = Vec::with_capacity(n);
        for (id, consumer) in consumers.into_iter().enumerate() {
            let net = Arc::clone(&net);
            let cfg = cfg.clone();
            let h = thread::Builder::new()
                .name(format!("wal-ship-{id}"))
                .spawn(move || replica_thread(id, cfg, consumer, &net))?;
            producer.set_waker(id, h.thread().clone());
            threads.push(h);
        }
        Ok(Shipper {
            producer,
            net,
            threads,
        })
    }

    /// Capture a just-appended record (§8.1). Writer thread; never blocks.
    pub fn capture(&mut self, lsn: Lsn, payload: &[u8]) {
        self.producer.capture(lsn, payload);
    }

    /// Release records `≤ w` after `commit() → Ok(w)` (§8.2) and serve pending
    /// joins. Writer thread; never blocks, sends nothing.
    pub fn on_commit(&mut self, w: Lsn) {
        self.producer.release(w);
        self.poll();
    }

    /// Serve replicas waiting to join (writer thread; pure memory). Call it
    /// periodically when the writer is idle, so a replica that connects while
    /// no commits happen is still joined promptly.
    pub fn poll(&mut self) {
        if !self.net.join_pending.swap(false, Ordering::AcqRel) {
            return;
        }
        for (id, r) in self.net.replicas.iter().enumerate() {
            let from = r.join_req.swap(0, Ordering::AcqRel);
            if from == 0 {
                continue;
            }
            let resp = match self.producer.attach(id, Lsn(from)) {
                Ok(()) => JOIN_OK,
                Err(AttachError::Ahead { .. }) => JOIN_AHEAD,
                Err(AttachError::Behind { .. } | AttachError::AlreadyAttached) => JOIN_BEHIND,
            };
            r.join_resp.store(resp, Ordering::Release);
            self.producer.wake(id);
        }
    }

    /// The minimum acked LSN over **all** configured replicas — the input to
    /// the integrator's checkpoint policy (§11). A replica that has not acked
    /// anything since this shipper started counts as `Lsn(0)` (most
    /// conservative; the primary persists no replica state, R7).
    #[must_use]
    pub fn min_replica_acked_lsn(&self) -> Option<Lsn> {
        self.net
            .replicas
            .iter()
            .map(|r| Lsn(r.acked.load(Ordering::Acquire)))
            .min()
    }

    /// The highest durable LSN replica `id` has acked (`Lsn(0)` = none yet), or
    /// `None` for an unknown id (§13 sync surface).
    #[must_use]
    pub fn replica_acked_lsn(&self, id: ReplicaId) -> Option<Lsn> {
        self.net
            .replicas
            .get(id)
            .map(|r| Lsn(r.acked.load(Ordering::Acquire)))
    }

    /// Replica `id`'s connection state, or `None` for an unknown id.
    #[must_use]
    pub fn replica_status(&self, id: ReplicaId) -> Option<ReplicaStatus> {
        self.net
            .replicas
            .get(id)
            .map(|r| ReplicaStatus::from_u8(r.status.load(Ordering::Acquire)))
    }

    /// How many times replica `id` was found `NeedsCatchUp` (ring overflow or
    /// a too-old resume point).
    #[must_use]
    pub fn catch_up_events(&self, id: ReplicaId) -> u64 {
        self.net.replicas[id].catch_ups.load(Ordering::Acquire)
    }

    /// The released (shippable) watermark.
    #[must_use]
    pub fn released(&self) -> Lsn {
        self.producer.released()
    }

    /// Stop all network threads and wait for them (bounded by `io_timeout`).
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.net.shutdown.store(true, Ordering::Release);
        for id in 0..self.threads.len() {
            self.producer.wake(id);
        }
        for h in self.threads.drain(..) {
            let _ = h.join();
        }
    }
}

impl Drop for Shipper {
    fn drop(&mut self) {
        self.stop();
    }
}

fn set_status(r: &ReplicaShared, s: ReplicaStatus) {
    r.status.store(s as u8, Ordering::Release);
}

/// Sleep up to `d`, waking early on shutdown (or any unpark).
fn nap(net: &Net, d: Duration) {
    let until = Instant::now() + d;
    while !net.shutdown.load(Ordering::Acquire) {
        let now = Instant::now();
        if now >= until {
            return;
        }
        thread::park_timeout(until - now);
    }
}

/// One replica's network thread: connect, serve, back off, repeat.
fn replica_thread(id: ReplicaId, cfg: ShipperConfig, mut consumer: Consumer, net: &Net) {
    let r = &net.replicas[id];
    let mut backoff = cfg.reconnect_backoff;
    while !net.shutdown.load(Ordering::Acquire) {
        set_status(r, ReplicaStatus::Connecting);
        let outcome = session(&cfg, cfg.replicas[id], &mut consumer, net, r);
        consumer.detach();
        match outcome {
            // Fell behind mid-stream: reconnect at once — the replica's new
            // resume point may still be in the ring.
            Ok(SessionEnd::FellBehind) => backoff = cfg.reconnect_backoff,
            Ok(SessionEnd::Streamed) => {
                backoff = cfg.reconnect_backoff;
                nap(net, backoff);
            }
            Ok(SessionEnd::Refused) | Err(_) => {
                nap(net, backoff);
                backoff = (backoff * 2).min(cfg.max_reconnect_backoff);
            }
        }
    }
    consumer.detach();
}

enum SessionEnd {
    /// Streamed, then the connection ended.
    Streamed,
    /// Detected `NeedsCatchUp` mid-stream; closed so the replica re-HELLOs.
    FellBehind,
    /// Never streamed (rejected, or behind the ring).
    Refused,
}

fn send(mut stream: &TcpStream, f: &Frame<'_>) -> Result<()> {
    let mut buf = Vec::with_capacity(32);
    wire::encode_frame(f, &mut buf);
    stream.write_all(&buf)?;
    Ok(())
}

fn session(
    cfg: &ShipperConfig,
    addr: SocketAddr,
    consumer: &mut Consumer,
    net: &Net,
    r: &ReplicaShared,
) -> Result<SessionEnd> {
    let stream = TcpStream::connect_timeout(&addr, cfg.io_timeout)?;
    stream.set_nodelay(true)?;
    stream.set_write_timeout(Some(cfg.io_timeout))?;
    stream.set_read_timeout(Some(cfg.io_timeout))?;
    // Replica→primary frames are small (no RECORDs); Limits(0) bounds them.
    let mut rd = FrameReader::new(stream.try_clone()?, Limits::new(0));
    let durable = match rd.read_frame()? {
        Frame::Hello {
            proto_ver,
            durable_lsn,
            ..
        } => {
            if proto_ver != PROTO_VERSION {
                let _ = send(
                    &stream,
                    &Frame::Err {
                        code: ErrCode::Protocol,
                        msg: "unsupported protocol version",
                    },
                );
                return Err(ReplError::Protocol("unsupported protocol version"));
            }
            durable_lsn
        }
        Frame::Err { code, msg } => {
            return Err(ReplError::Remote {
                code,
                msg: msg.to_owned(),
            });
        }
        _ => {
            let _ = send(
                &stream,
                &Frame::Err {
                    code: ErrCode::Protocol,
                    msg: "expected HELLO",
                },
            );
            return Err(ReplError::Protocol("expected HELLO"));
        }
    };

    // R7: resume from the replica's own durable_lsn. The writer thread decides
    // (it owns the ring's producer side): join, behind (catch-up), or ahead.
    set_status(r, ReplicaStatus::Joining);
    let from = durable.next();
    r.join_resp.store(JOIN_PENDING, Ordering::Relaxed);
    r.join_req.store(from.0, Ordering::Release);
    net.join_pending.store(true, Ordering::Release);
    let decision = loop {
        if net.shutdown.load(Ordering::Acquire) {
            return Ok(SessionEnd::Refused);
        }
        match r.join_resp.load(Ordering::Acquire) {
            JOIN_PENDING => thread::park_timeout(cfg.heartbeat),
            d => break d,
        }
    };
    match decision {
        JOIN_AHEAD => {
            set_status(r, ReplicaStatus::Rejected);
            let _ = send(
                &stream,
                &Frame::Err {
                    code: ErrCode::Contiguity,
                    msg: "replica durable_lsn is ahead of the primary: \
                          a former primary must be wiped and re-seeded (§12.2)",
                },
            );
            let _ = stream.shutdown(Shutdown::Both);
            return Ok(SessionEnd::Refused);
        }
        JOIN_BEHIND => {
            set_status(r, ReplicaStatus::NeedsCatchUp);
            r.catch_ups.fetch_add(1, Ordering::AcqRel);
            hold(cfg, &stream, rd, net, r, durable);
            return Ok(SessionEnd::Refused);
        }
        _ => {}
    }
    if consumer.resume() != Some(from) {
        return Err(ReplError::Protocol("ring attach did not take effect"));
    }
    send(
        &stream,
        &Frame::Serving {
            from_lsn: from,
            primary_oldest: Lsn(0),
        },
    )?;
    set_status(r, ReplicaStatus::Streaming);
    stream.set_read_timeout(Some(ack_timeout(cfg)))?;

    let dead = AtomicBool::new(false);
    let sent = AtomicU64::new(durable.0);
    let me = thread::current();
    let end = thread::scope(|s| {
        s.spawn(|| ack_reader(rd, r, &dead, &sent, &me));
        let end = stream_records(cfg, &stream, consumer, net, r, &dead, &sent);
        let _ = stream.shutdown(Shutdown::Both); // unblocks the ack reader
        end
    });
    if let Ok(SessionEnd::FellBehind) = end {
        set_status(r, ReplicaStatus::NeedsCatchUp);
        r.catch_ups.fetch_add(1, Ordering::AcqRel);
    }
    end
}

fn ack_timeout(cfg: &ShipperConfig) -> Duration {
    // The replica answers every heartbeat, so a live link yields a frame at
    // least every `heartbeat`.
    (cfg.heartbeat * 3).max(cfg.io_timeout)
}

/// Drain the ring into `RECORD` frames until the connection dies, shutdown, or
/// the replica falls behind.
fn stream_records(
    cfg: &ShipperConfig,
    stream: &TcpStream,
    consumer: &mut Consumer,
    net: &Net,
    _r: &ReplicaShared,
    dead: &AtomicBool,
    sent: &AtomicU64,
) -> Result<SessionEnd> {
    let mut out = Vec::with_capacity(cfg.send_batch * (cfg.slot_bytes + 17));
    let mut last_send = Instant::now();
    let mut w = stream;
    loop {
        if net.shutdown.load(Ordering::Acquire) || dead.load(Ordering::Acquire) {
            return Ok(SessionEnd::Streamed);
        }
        out.clear();
        let mut last = None;
        // The emit closure runs while the record is pinned: encode (CRC +
        // memcpy) and return — never block in it.
        let d = consumer.drain(cfg.send_batch, |lsn, p| {
            wire::encode_record(&mut out, lsn, p);
            last = Some(lsn);
        });
        if !out.is_empty() {
            w.write_all(&out)?;
            last_send = Instant::now();
            if let Some(l) = last {
                sent.store(l.0, Ordering::Release);
            }
        }
        match d.status {
            ConsumerStatus::Streaming => {}
            ConsumerStatus::NeedsCatchUp { .. } => return Ok(SessionEnd::FellBehind),
            ConsumerStatus::Detached => return Err(ReplError::Protocol("consumer detached")),
        }
        if d.emitted == 0 {
            let idle = last_send.elapsed();
            if idle >= cfg.heartbeat {
                send(
                    stream,
                    &Frame::Heartbeat {
                        durable_lsn: d.watermark,
                    },
                )?;
                last_send = Instant::now();
                consumer.wait(cfg.heartbeat);
            } else {
                consumer.wait(cfg.heartbeat - idle);
            }
        }
    }
}

/// A replica the ring cannot serve (RM3 catch-up pending): keep the connection
/// alive with heartbeats so it is not reconnect-stormed, until it closes or
/// shutdown.
fn hold(
    cfg: &ShipperConfig,
    stream: &TcpStream,
    rd: FrameReader<TcpStream>,
    net: &Net,
    r: &ReplicaShared,
    durable: Lsn,
) {
    let _ = stream.set_read_timeout(Some(ack_timeout(cfg)));
    let dead = AtomicBool::new(false);
    let sent = AtomicU64::new(durable.0);
    let me = thread::current();
    thread::scope(|s| {
        s.spawn(|| ack_reader(rd, r, &dead, &sent, &me));
        while !net.shutdown.load(Ordering::Acquire) && !dead.load(Ordering::Acquire) {
            if send(
                stream,
                &Frame::Heartbeat {
                    durable_lsn: durable,
                },
            )
            .is_err()
            {
                break;
            }
            nap(net, cfg.heartbeat);
        }
        let _ = stream.shutdown(Shutdown::Both);
    });
}

/// Read the replica's `ACK`/`HEARTBEAT` frames into `acked` (monotonic). An
/// ack beyond what was sent on this connection is a protocol violation (R4: a
/// replica can only be durable to what it received) and ends the connection.
fn ack_reader(
    mut rd: FrameReader<TcpStream>,
    r: &ReplicaShared,
    dead: &AtomicBool,
    sent: &AtomicU64,
    sender: &Thread,
) {
    // Anything else (ERR, an unexpected frame, EOF, timeout) ends the session.
    while let Ok(Frame::Ack { durable_lsn } | Frame::Heartbeat { durable_lsn }) = rd.read_frame() {
        if durable_lsn.0 > sent.load(Ordering::Acquire) {
            break;
        }
        r.acked.fetch_max(durable_lsn.0, Ordering::AcqRel);
    }
    dead.store(true, Ordering::Release);
    let _ = rd.get_ref().shutdown(Shutdown::Both);
    sender.unpark();
}
