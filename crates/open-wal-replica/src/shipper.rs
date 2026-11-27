//! The primary side: [`Shipper`] (§8, §9 and §4.2 of `docs/replica_design_v1.md`).
//!
//! The integrator calls, **on the WAL writer thread**:
//! - [`capture`](Shipper::capture)`(lsn, payload)` right after each
//!   `wal.append` — one bounded memcpy into the ring, no I/O, never blocks;
//! - [`on_commit`](Shipper::on_commit)`(w)` right after each
//!   `wal.commit() → Ok(w)` — releases records `≤ w` (R1) and wakes the network
//!   threads. It sends nothing and decides nothing. **Never** call it with
//!   `last_lsn`, and never after `commit` returned `Err` (the WAL is poisoned;
//!   captured-but-unreleased records are discarded — see
//!   [`ring::Producer::capture`]).
//! - [`poll`](Shipper::poll)`(&wal)` **after every `on_commit`, and at least
//!   once per `heartbeat` when no commits occur** (§9.1 cadence). It answers
//!   the requests the network threads posted: join decisions (§9 steps 1–3,
//!   which need `wal.oldest_lsn()` / `wal.durable_lsn()`) and catch-up chunks
//!   (§9.1, the only place this crate reads the log). A `poll` with nothing
//!   pending is one atomic swap.
//!
//! Everything that can block runs on **one network thread per replica** (plus a
//! companion thread reading that replica's `ACK`s): connect with backoff, read
//! `HELLO`, post a join request, send `SERVING`, then send `RECORD`s — from the
//! ring when it holds the replica's cursor, otherwise from the log in bounded
//! chunks the writer copies for it (§9.1) until it can cut over to the ring
//! (§9.2). A replica the ring overflows past falls back to catch-up **inside
//! its session**. A slow or dead replica only ever stalls its own thread.
//!
//! # Writer ↔ network handshake (§9.3)
//! The writer keeps no per-session state. Each replica has one request
//! [`Slot`]; the network thread posts, the writer answers in a later `poll`,
//! and `Producer::attach` is called **only** while answering. The slot's
//! sequences live in [`crate::slot`] (model-checked by loom, L8 / L8b) and are
//! called verbatim here.
//!
//! # Integrator obligations
//! - **Cadence** (above). The join decision is bounded by one commit or one
//!   heartbeat; that bound must stay well under the receiver's `idle_timeout`,
//!   because nothing the receiver accepts can be sent before `SERVING`. While
//!   [`catching_up`](Shipper::catching_up)` > 0`, poll more often: catch-up
//!   advances about one chunk per opened reader per `poll`.
//! - **Writer stall.** A reading `poll` costs at most one containing-segment
//!   prefix scan (first chunk of an episode only) plus one chunk, for at most
//!   `catchup_replicas_per_poll` replicas. Size segments by record count when
//!   payloads are small (§9.1).
//! - **After an in-process WAL reopen** (§8.2): before the next `append`, call
//!   `shipper.on_commit(wal.commit()?)`.

use std::io::Write;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

use open_wal::{DurabilityObserver, Lsn, Reader, ReaderPos, Wal, WalError};

use crate::error::{ReplError, Result};
use crate::ring::{self, AttachError, Consumer, ConsumerStatus, Producer, RingConfig};
use crate::slot::{Answer, Awaited, ChunkEnd, Idle, Request, Serve, Slot};
use crate::wire::{self, ErrCode, Frame, FrameReader, Limits, PROTO_VERSION};

/// Index of a replica in [`ShipperConfig::replicas`].
pub type ReplicaId = usize;

/// What one record costs against `catchup_chunk_bytes`, beyond its payload
/// (§9.1): the WAL's 20-byte header and padding on the read side, and the
/// 17-byte `RECORD` frame on the wire.
const CHUNK_RECORD_OVERHEAD: usize = 32;

/// Chunk-buffer framing: `lsn: u64 LE | len: u32 LE | payload`.
const CHUNK_HEADER: usize = 12;

const LSN_DOMAIN_MSG: &str = "LSN domain exhausted";

/// Shipper configuration (§14).
#[derive(Clone, Debug)]
pub struct ShipperConfig {
    /// Replica addresses; the primary connects to each (the replica's
    /// [`Receiver`](crate::Receiver) listens). Index = [`ReplicaId`].
    pub replicas: Vec<SocketAddr>,
    /// Total ring payload capacity in bytes (§8.1). The ring has
    /// `max(2, ring_bytes / slot_bytes)` fixed slots. Size it to hold the
    /// largest commit batch plus what the primary captures while one catch-up
    /// chunk is on the wire (§9.2); an undersized ring only degrades to
    /// serving from the log.
    pub ring_bytes: usize,
    /// Preallocated bytes per ring slot. Size it to the typical payload; a
    /// larger payload grows its slot once (the §8.1 fallback allocation).
    pub slot_bytes: usize,
    /// Idle keepalive period; also the bound on how long a network thread
    /// sleeps between checks, and the longest the integrator may go without
    /// calling [`Shipper::poll`].
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
    /// Catch-up chunk budget in bytes (§9.1), counting each record as
    /// `payload + 32`. Checked before each read, so a chunk may exceed it by
    /// at most one record; a chunk always holds at least one record.
    pub catchup_chunk_bytes: usize,
    /// Catch-up chunk budget in records (≥ 1). Reading costs two `pread`s per
    /// record, so a log of tiny payloads is bounded by this, not by the bytes.
    pub catchup_chunk_records: usize,
    /// How many replicas one [`Shipper::poll`] opens a log reader for (≥ 1),
    /// round-robin. Bounds the per-`poll` writer stall regardless of how many
    /// replicas lag.
    pub catchup_replicas_per_poll: usize,
}

impl ShipperConfig {
    /// Defaults around `replicas`: a 64 MiB ring of 4 KiB slots, 500 ms
    /// heartbeat, 50 ms → 5 s reconnect backoff, 5 s I/O timeout, 256-record
    /// send batches, catch-up chunks of 16 MiB / 16 384 records, one catch-up
    /// reader per `poll`.
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
            catchup_chunk_bytes: 16 * 1024 * 1024,
            catchup_chunk_records: 16_384,
            catchup_replicas_per_poll: 1,
        }
    }
}

/// A replica connection's state, as seen by the primary (§9.3):
/// `Connecting → Joining → CatchingUp ⇄ Streaming`, plus three states that end
/// a session and stay visible until the replica's next `HELLO`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplicaStatus {
    /// Not connected (connecting, or backing off).
    Connecting = 0,
    /// Connected; `HELLO` received; waiting for the writer thread's `poll` to
    /// decide the join.
    Joining = 1,
    /// Receiving the live stream from the ring.
    Streaming = 2,
    /// Being served from the log in chunks (§9.1), or waiting for the released
    /// watermark to reach its cursor. Poll more often while any replica is
    /// here ([`Shipper::catching_up`]).
    CatchingUp = 3,
    /// Its `durable_lsn` is ahead of the primary's (§9 step 2): a former
    /// primary or a bug. Refused with `ERR(Contiguity)`; it must be re-seeded.
    Rejected = 4,
    /// The records it needs were checkpointed away (§9 step 1, R6): answered
    /// `RESEED_REQUIRED`.
    NeedsReseed = 5,
    /// Its session ended on a primary-side failure: an unreadable committed
    /// record (§9.1) or the LSN domain (§9.2).
    Failed = 6,
}

impl ReplicaStatus {
    fn from_u8(v: u8) -> ReplicaStatus {
        match v {
            1 => ReplicaStatus::Joining,
            2 => ReplicaStatus::Streaming,
            3 => ReplicaStatus::CatchingUp,
            4 => ReplicaStatus::Rejected,
            5 => ReplicaStatus::NeedsReseed,
            6 => ReplicaStatus::Failed,
            _ => ReplicaStatus::Connecting,
        }
    }

    /// The three states that outlive their session (until the next `HELLO`).
    fn ends_session(self) -> bool {
        matches!(
            self,
            ReplicaStatus::Rejected | ReplicaStatus::NeedsReseed | ReplicaStatus::Failed
        )
    }
}

/// Per-replica state shared between the writer thread and its network thread.
#[derive(Default)]
struct ReplicaShared {
    /// This replica's durable watermark as last reported (R4): set to the
    /// HELLO's `durable_lsn` when a session is accepted (so a wiped / re-seeded
    /// replica's lower watermark replaces a stale one), then raised by each
    /// `ACK`/`HEARTBEAT` within the session. Never above what was sent.
    acked: AtomicU64,
    status: AtomicU8,
    /// Entries into `CatchingUp` (observability).
    catch_ups: AtomicU64,
    /// The §9.3 request slot.
    slot: Slot,
}

struct Net {
    shutdown: AtomicBool,
    /// Set by a network thread after it posted a request; swapped by `poll`.
    pending: AtomicBool,
    /// The shipper left the ring's LSN domain (§9.2).
    terminal: AtomicBool,
    replicas: Box<[ReplicaShared]>,
}

/// The sending-side contiguity check (§9.2; R3 on the primary side, defence
/// in depth). The network thread keeps `next = d + 1` for the session and
/// passes **every** record it is about to encode through this — whether the
/// record came from a catch-up chunk or from the ring. `lsn == next` ⇒ the
/// value `next` takes afterwards; anything else ⇒ `Contiguity`, and the
/// record is never sent.
fn send_check(next: Lsn, lsn: Lsn) -> Result<Lsn> {
    if lsn != next {
        return Err(ReplError::Contiguity {
            expected: next,
            got: lsn,
        });
    }
    lsn.checked_next()
        .ok_or(ReplError::LsnExhausted { last: lsn })
}

/// Append one record to a chunk buffer.
fn chunk_push(buf: &mut Vec<u8>, lsn: Lsn, payload: &[u8]) {
    buf.extend_from_slice(&lsn.0.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
}

/// Split the first record off a chunk buffer.
fn chunk_pop(buf: &[u8]) -> Option<(Lsn, &[u8], &[u8])> {
    let (head, rest) = buf.split_at_checked(CHUNK_HEADER)?;
    let lsn = u64::from_le_bytes(head[..8].try_into().ok()?);
    let len = u32::from_le_bytes(head[8..].try_into().ok()?) as usize;
    let (payload, rest) = rest.split_at_checked(len)?;
    Some((Lsn(lsn), payload, rest))
}

/// What one catch-up read produced.
enum ReadOutcome {
    /// `buf` holds `cursor..=last`; `pos` is the reader's position after it.
    Chunk { last: Lsn, pos: Option<ReaderPos> },
    /// The reader could not be opened: checkpointed away (R6).
    BelowOldest,
    /// A committed record could not be read. `sticky` for data-level failures
    /// (`None` / wrong LSN), not for I/O errors.
    Failed { lsn: Lsn, sticky: bool },
}

/// Bookkeeping for one `poll` pass.
#[derive(Default)]
struct Pass {
    /// Readers opened so far (the §9.1 cap).
    readers: usize,
    first_deferred: Option<ReplicaId>,
    last_reader: Option<ReplicaId>,
    /// The first ReadFailed answered in this pass.
    read_failed: Option<Lsn>,
}

/// The writer thread's private half: the ring's producer and the catch-up
/// state. Only `poll`'s `Slot::answer` closures call [`Writer::decide`] — the
/// one place `Producer::attach` is reached from.
struct Writer {
    producer: Producer,
    chunk_bytes: usize,
    chunk_records: usize,
    readers_per_poll: usize,
    /// §9.2: a capture crossed `ring::MAX_LSN`.
    terminal: bool,
    /// §9.1: the first data-level read failure; disables catch-up reads.
    sticky_read_failed: Option<Lsn>,
    position_fallbacks: u64,
}

impl Writer {
    /// Answer one request (inside `Slot::answer`).
    fn decide<O: DurabilityObserver>(
        &mut self,
        id: ReplicaId,
        req: &Request,
        buf: &mut Vec<u8>,
        wal: &Wal<O>,
        pass: &mut Pass,
    ) -> Answer {
        if self.terminal {
            return Answer::Terminal;
        }
        match *req {
            Request::Join { d } => self.join(id, d, wal),
            Request::Chunk { cursor, pos } => self.chunk(id, cursor, pos, buf, wal, pass),
        }
    }

    /// §9 steps 1–3. Never opens a reader.
    fn join<O: DurabilityObserver>(&mut self, id: ReplicaId, d: Lsn, wal: &Wal<O>) -> Answer {
        // The network thread refuses `d ≥ MAX_LSN` itself; never trust it here.
        let Some(cursor) = d.checked_next().filter(|c| c.0 <= ring::MAX_LSN) else {
            return Answer::Internal;
        };
        // Read at decision time (never cached): the integrator may have
        // checkpointed since the last join.
        let oldest = wal.oldest_lsn();
        if cursor < oldest {
            return Answer::Reseed { oldest };
        }
        // Step 2 — the ONLY source of this rejection. `d == durable` is a
        // fully caught-up replica and is served.
        if d > wal.durable_lsn() {
            return Answer::Reject;
        }
        let how = match self.producer.attach(id, cursor) {
            Ok(()) => Serve::Attached,
            // `released < d ≤ wal.durable_lsn()`: between `commit()` and
            // `on_commit()`, or after a failed split commit. Wait — never
            // reject (the serve ceiling is `released`, §9).
            Err(AttachError::Ahead { .. }) => Serve::Wait,
            Err(AttachError::Behind { .. }) => {
                if cursor > self.producer.released() {
                    Serve::Wait
                } else {
                    Serve::NeedChunk
                }
            }
            Err(AttachError::AlreadyAttached) => return self.already_attached(),
        };
        Answer::Serve { oldest, how }
    }

    /// §9.1 answers 1–3.
    fn chunk<O: DurabilityObserver>(
        &mut self,
        id: ReplicaId,
        cursor: Lsn,
        pos: Option<ReaderPos>,
        buf: &mut Vec<u8>,
        wal: &Wal<O>,
        pass: &mut Pass,
    ) -> Answer {
        // Answer 1: the ring holds the cursor.
        match self.producer.attach(id, cursor) {
            Ok(()) => return Answer::Attached { cursor },
            Err(AttachError::AlreadyAttached) => return self.already_attached(),
            Err(AttachError::Ahead { .. } | AttachError::Behind { .. }) => {}
        }
        // The serve ceiling (R1): the clamped released watermark — never
        // `wal.durable_lsn()`, never the raw `on_commit` argument, never the
        // reader's end.
        let released = self.producer.released();
        // Answer 2: nothing to serve yet. No reader is opened.
        if cursor > released {
            return Answer::Wait;
        }
        // Answer 3.
        if let Some(lsn) = self.sticky_read_failed {
            pass.read_failed.get_or_insert(lsn);
            return Answer::ReadFailed { lsn };
        }
        if pass.readers >= self.readers_per_poll {
            pass.first_deferred.get_or_insert(id);
            return Answer::Deferred;
        }
        pass.readers += 1;
        pass.last_reader = Some(id);
        match self.read(wal, cursor, pos, released, buf) {
            ReadOutcome::BelowOldest => Answer::BelowOldest,
            ReadOutcome::Failed { lsn, sticky } => {
                buf.clear();
                if sticky {
                    self.sticky_read_failed.get_or_insert(lsn);
                }
                pass.read_failed.get_or_insert(lsn);
                Answer::ReadFailed { lsn }
            }
            ReadOutcome::Chunk { last, pos } => {
                let Some(next) = last.checked_next() else {
                    buf.clear();
                    return Answer::Internal; // unreachable: last ≤ released ≤ MAX_LSN
                };
                let end = match self.producer.attach(id, next) {
                    Ok(()) => ChunkEnd::Attached,
                    Err(AttachError::Behind { .. }) => ChunkEnd::More(pos),
                    Err(AttachError::Ahead { .. }) => {
                        // Cannot occur: `last ≤ released` ⇒ `next ≤ released + 1`.
                        debug_assert!(false, "catch-up read past the released ceiling");
                        ChunkEnd::More(pos)
                    }
                    Err(AttachError::AlreadyAttached) => {
                        buf.clear();
                        return self.already_attached();
                    }
                };
                Answer::Chunk { next, end }
            }
        }
    }

    /// The network thread always detaches before it posts, so this is an
    /// internal bug: end the session loudly.
    fn already_attached(&self) -> Answer {
        debug_assert!(false, "request posted while the consumer is attached");
        Answer::Internal
    }

    /// Open one reader at `cursor` and copy one chunk into `buf` (§9.1).
    fn read<O: DurabilityObserver>(
        &mut self,
        wal: &Wal<O>,
        cursor: Lsn,
        pos: Option<ReaderPos>,
        released: Lsn,
        buf: &mut Vec<u8>,
    ) -> ReadOutcome {
        // A position is used only if it is exactly at the cursor: that is what
        // makes a stale one (kept across a cutover and a ring fall-back)
        // harmless.
        if let Some(p) = pos.filter(|p| p.next_lsn() == cursor) {
            match wal.reader_at(p) {
                Ok(mut reader) => match self.copy(&mut reader, cursor, released, buf) {
                    Ok((last, pos)) => return ReadOutcome::Chunk { last, pos },
                    // A reader started by `reader_at` never causes a failure
                    // by itself: discard what it produced and retry once with
                    // `reader_from`; only that result can become ReadFailed.
                    Err(_) => {
                        buf.clear();
                        self.position_fallbacks += 1;
                    }
                },
                Err(WalError::ContiguityViolation) => return ReadOutcome::BelowOldest,
                // `InvalidPosition`, or e.g. an I/O error opening the segment.
                Err(_) => self.position_fallbacks += 1,
            }
        }
        match wal.reader_from(cursor) {
            Ok(mut reader) => match self.copy(&mut reader, cursor, released, buf) {
                Ok((last, pos)) => ReadOutcome::Chunk { last, pos },
                Err((lsn, sticky)) => ReadOutcome::Failed { lsn, sticky },
            },
            Err(WalError::ContiguityViolation) => ReadOutcome::BelowOldest,
            Err(_) => ReadOutcome::Failed {
                lsn: cursor,
                sticky: false,
            },
        }
    }

    /// Copy records `cursor..` from `reader` into `buf` while `lsn ≤ released`
    /// and both budgets have room — always at least one. Every record read is
    /// shipped: whether to read the next one is decided from its LSN alone
    /// (`last + 1`), before reading it. `Err((lsn, data_level))` if the record
    /// `lsn ≤ released` could not be read.
    fn copy(
        &self,
        reader: &mut Reader<'_>,
        cursor: Lsn,
        released: Lsn,
        buf: &mut Vec<u8>,
    ) -> std::result::Result<(Lsn, Option<ReaderPos>), (Lsn, bool)> {
        debug_assert!(cursor <= released);
        let mut expect = cursor;
        let mut last = cursor;
        let mut bytes = 0usize;
        let mut records = 0usize;
        loop {
            if records > 0
                && (expect > released || bytes >= self.chunk_bytes || records >= self.chunk_records)
            {
                break;
            }
            match reader.next() {
                Some(Ok((lsn, payload))) if lsn == expect => {
                    chunk_push(buf, lsn, payload);
                    bytes = bytes.saturating_add(payload.len() + CHUNK_RECORD_OVERHEAD);
                    records += 1;
                    last = lsn;
                    match lsn.checked_next() {
                        Some(n) => expect = n,
                        None => break,
                    }
                }
                // `Reader::next` ends with `None` on an invalid record or an
                // LSN break: below the ceiling that is an unreadable committed
                // record, never an empty chunk. Data-level ⇒ sticky.
                Some(Ok(_)) | None => return Err((expect, true)),
                // An I/O error (e.g. EMFILE on the segment open): not sticky.
                Some(Err(_)) => return Err((expect, false)),
            }
        }
        Ok((last, reader.position()))
    }
}

/// The primary's replication shipper (§8, §9).
pub struct Shipper {
    w: Writer,
    net: Arc<Net>,
    threads: Vec<JoinHandle<()>>,
    /// Where the next `poll` pass starts (round-robin, §9.1).
    start: ReplicaId,
}

impl Shipper {
    /// Start shipping `wal`'s future records to `cfg.replicas`. Call on the
    /// writer thread with the WAL open (after recovery): the ring starts at
    /// `wal.last_lsn() + 1` with `wal.durable_lsn()` already released. Spawns
    /// one network thread per replica.
    ///
    /// Errors: an invalid configuration; `LsnExhausted` for a WAL at
    /// `u64::MAX`; `LsnDomainExhausted` when `wal.last_lsn() ≥ ring::MAX_LSN`
    /// (the next record could not be captured, §9.2).
    pub fn new<O: DurabilityObserver>(cfg: ShipperConfig, wal: &Wal<O>) -> Result<Shipper> {
        let n = cfg.replicas.len();
        if n == 0 || n > ring::MAX_CONSUMERS {
            return Err(ReplError::Protocol("shipper needs 1..=255 replicas"));
        }
        if cfg.slot_bytes == 0 || cfg.send_batch == 0 {
            return Err(ReplError::Protocol("slot_bytes and send_batch must be > 0"));
        }
        if cfg.catchup_chunk_records == 0 || cfg.catchup_replicas_per_poll == 0 {
            return Err(ReplError::Protocol(
                "catchup_chunk_records and catchup_replicas_per_poll must be > 0",
            ));
        }
        let ring_cfg = RingConfig {
            slots: (cfg.ring_bytes / cfg.slot_bytes).max(2),
            slot_bytes: cfg.slot_bytes,
            consumers: n,
        };
        let last = wal.last_lsn();
        // `checked_next` first, so a WAL at `u64::MAX` keeps its own error.
        let next = last
            .checked_next()
            .ok_or(ReplError::LsnExhausted { last })?;
        if last.0 >= ring::MAX_LSN {
            return Err(ReplError::LsnDomainExhausted);
        }
        let (mut producer, consumers) = ring::ring(ring_cfg, next, wal.durable_lsn());
        let net = Arc::new(Net {
            shutdown: AtomicBool::new(false),
            pending: AtomicBool::new(false),
            terminal: AtomicBool::new(false),
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
            w: Writer {
                producer,
                chunk_bytes: cfg.catchup_chunk_bytes,
                chunk_records: cfg.catchup_chunk_records,
                readers_per_poll: cfg.catchup_replicas_per_poll,
                terminal: false,
                sticky_read_failed: None,
                position_fallbacks: 0,
            },
            net,
            threads,
            start: 0,
        })
    }

    /// Capture a just-appended record (§8.1). Writer thread; never blocks.
    ///
    /// An `lsn` past `ring::MAX_LSN` makes the shipper **terminal** (§9.2):
    /// the ring is not called, every session ends with `ERR(Protocol)`, and
    /// `poll` returns `Err(LsnDomainExhausted)`.
    pub fn capture(&mut self, lsn: Lsn, payload: &[u8]) {
        if self.w.terminal {
            return;
        }
        if lsn.0 > ring::MAX_LSN {
            self.w.terminal = true;
            self.net.terminal.store(true, Ordering::Release);
            for id in 0..self.threads.len() {
                self.w.producer.wake(id);
            }
            return;
        }
        self.w.producer.capture(lsn, payload);
    }

    /// Release records `≤ w` after `commit() → Ok(w)` (§8.2): one watermark
    /// store plus one wake per replica. Writer thread; never blocks, sends
    /// nothing, decides nothing — call [`poll`](Shipper::poll) right after.
    pub fn on_commit(&mut self, w: Lsn) {
        self.w.producer.release(w);
    }

    /// Answer the network threads' pending requests (§9, §9.1): join
    /// decisions and catch-up chunks. Writer thread. **Call it after every
    /// `on_commit`, and at least once per `heartbeat` when idle.**
    ///
    /// - Every pending join is answered before any log reader is opened, and
    ///   a join never reads.
    /// - At most `catchup_replicas_per_poll` readers are opened, starting at a
    ///   rotating replica; the rest are deferred to a later `poll`.
    /// - The pass always completes; errors are returned after it:
    ///   `Err(PrimaryLogUnreadable{lsn})` iff this pass answered a chunk
    ///   request `ReadFailed`, `Err(LsnDomainExhausted)` once terminal.
    ///
    /// `wal` may be a poisoned handle: nothing above the released watermark is
    /// ever served (§9.1).
    pub fn poll<O: DurabilityObserver>(&mut self, wal: &Wal<O>) -> Result<()> {
        if !self.net.pending.swap(false, Ordering::AcqRel) {
            return self.idle_result();
        }
        let n = self.net.replicas.len();
        let start = self.start % n;
        let mut pass = Pass::default();
        let w = &mut self.w;
        // Joins first: `SERVING` latency never includes a log read.
        for k in 0..n {
            let id = (start + k) % n;
            let slot = &self.net.replicas[id].slot;
            if slot.pending_join() {
                slot.answer(|req, buf| w.decide(id, req, buf, wal, &mut pass));
            }
        }
        for k in 0..n {
            let id = (start + k) % n;
            self.net.replicas[id]
                .slot
                .answer(|req, buf| w.decide(id, req, buf, wal, &mut pass));
        }
        // A replica deferred by the cap is answered first next time.
        self.start = pass
            .first_deferred
            .or(pass.last_reader.map(|id| id + 1))
            .unwrap_or(start)
            % n;
        if let Some(lsn) = pass.read_failed {
            return Err(ReplError::PrimaryLogUnreadable { lsn });
        }
        self.idle_result()
    }

    fn idle_result(&self) -> Result<()> {
        if self.w.terminal {
            Err(ReplError::LsnDomainExhausted)
        } else {
            Ok(())
        }
    }

    /// How many replicas are in [`ReplicaStatus::CatchingUp`]. While this is
    /// non-zero an otherwise idle integrator should `poll` more often: each
    /// `poll` advances catch-up by about one chunk per opened reader.
    #[must_use]
    pub fn catching_up(&self) -> usize {
        self.net
            .replicas
            .iter()
            .filter(|r| r.status.load(Ordering::Acquire) == ReplicaStatus::CatchingUp as u8)
            .count()
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

    /// How many times replica `id` entered `CatchingUp` (a join the ring could
    /// not serve, or a ring overflow mid-stream).
    #[must_use]
    pub fn catch_up_events(&self, id: ReplicaId) -> u64 {
        self.net.replicas[id].catch_ups.load(Ordering::Acquire)
    }

    /// The released (shippable) watermark: the serve ceiling.
    #[must_use]
    pub fn released(&self) -> Lsn {
        self.w.producer.released()
    }

    /// Whether replica `id` has a request posted that no `poll` has taken yet.
    /// A read of the slot's state word: a reachability aid for tests, not a
    /// fault hook and never an oracle.
    #[doc(hidden)]
    #[must_use]
    pub fn request_pending(&self, id: ReplicaId) -> bool {
        self.net.replicas.get(id).is_some_and(|r| r.slot.pending())
    }

    /// How many times a chunk answer fell back from `reader_at(pos)` to
    /// `reader_from(cursor)` (§9.1). Observability; never an oracle.
    #[doc(hidden)]
    #[must_use]
    pub fn position_fallbacks(&self) -> u64 {
        self.w.position_fallbacks
    }

    /// Stop all network threads and wait for them (bounded by `io_timeout`).
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.net.shutdown.store(true, Ordering::Release);
        for (id, r) in self.net.replicas.iter().enumerate() {
            r.slot.shutdown();
            self.w.producer.wake(id);
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

fn enter_catching_up(r: &ReplicaShared) {
    set_status(r, ReplicaStatus::CatchingUp);
    r.catch_ups.fetch_add(1, Ordering::AcqRel);
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

/// What a network thread needs for one replica.
struct Ctx<'a> {
    cfg: &'a ShipperConfig,
    addr: SocketAddr,
    net: &'a Net,
    r: &'a ReplicaShared,
}

/// One replica's network thread: connect, serve, back off, repeat.
fn replica_thread(id: ReplicaId, cfg: ShipperConfig, mut consumer: Consumer, net: &Net) {
    let r = &net.replicas[id];
    // Registered once, before the first post (§9.3).
    r.slot.register_waker(thread::current());
    let ctx = Ctx {
        cfg: &cfg,
        addr: cfg.replicas[id],
        net,
        r,
    };
    let mut backoff = cfg.reconnect_backoff;
    // The chunk buffer this thread owns; swapped with the slot's on each answer.
    let mut chunk = Vec::new();
    while !net.shutdown.load(Ordering::Acquire) {
        // Rejected / NeedsReseed / Failed stay visible until the next HELLO.
        if !ReplicaStatus::from_u8(r.status.load(Ordering::Acquire)).ends_session() {
            set_status(r, ReplicaStatus::Connecting);
        }
        let mut served = false;
        let outcome = session(&ctx, &mut consumer, &mut chunk, &mut served);
        // §9.3: a session never ends with a request outstanding. Every path
        // out of `session` already left the slot IDLE (shutdown aside); this
        // is the same sequence once more — an IDLE slot means detach + return.
        r.slot.abandon(&mut consumer);
        match outcome {
            // Below-oldest mid-stream: reconnect at once; the next HELLO is
            // answered RESEED_REQUIRED (R6).
            Ok(SessionEnd::Reconnect) => backoff = cfg.reconnect_backoff,
            // A session that was served and then ended is a normal
            // disconnect: reconnect promptly.
            Ok(SessionEnd::Closed) | Err(_) if served => {
                backoff = cfg.reconnect_backoff;
                nap(net, backoff);
            }
            _ => {
                nap(net, backoff);
                backoff = (backoff * 2).min(cfg.max_reconnect_backoff);
            }
        }
    }
    consumer.detach();
}

enum SessionEnd {
    /// The connection ended (or shutdown).
    Closed,
    /// Closed by us with a FIN; reconnect immediately (BelowOldest).
    Reconnect,
    /// Refused or failed: back off.
    Refused,
}

fn send(mut stream: &TcpStream, f: &Frame<'_>) -> Result<()> {
    let mut buf = Vec::with_capacity(32);
    wire::encode_frame(f, &mut buf);
    stream.write_all(&buf)?;
    Ok(())
}

fn send_err(stream: &TcpStream, code: ErrCode, msg: &str) {
    let _ = send(stream, &Frame::Err { code, msg });
}

/// How the served part of a session ended.
enum Served {
    /// Transport end / shutdown.
    Closed,
    /// BelowOldest: FIN, reconnect at once.
    BelowOldest,
    /// End with `ERR(code, msg)` and status `Failed`.
    Failed(ErrCode, String),
}

fn session(
    ctx: &Ctx<'_>,
    consumer: &mut Consumer,
    chunk: &mut Vec<u8>,
    served: &mut bool,
) -> Result<SessionEnd> {
    let Ctx { cfg, net, r, .. } = *ctx;
    let stream = TcpStream::connect_timeout(&ctx.addr, cfg.io_timeout)?;
    stream.set_nodelay(true)?;
    stream.set_write_timeout(Some(cfg.io_timeout))?;
    stream.set_read_timeout(Some(cfg.io_timeout))?;
    // Replica→primary frames are small (no RECORDs); Limits(0) bounds them.
    let mut rd = FrameReader::new(stream.try_clone()?, Limits::new(0));
    let d = match rd.read_frame()? {
        Frame::Hello {
            proto_ver,
            durable_lsn,
            ..
        } => {
            if proto_ver != PROTO_VERSION {
                send_err(&stream, ErrCode::Protocol, "unsupported protocol version");
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
            send_err(&stream, ErrCode::Protocol, "expected HELLO");
            return Err(ReplError::Protocol("expected HELLO"));
        }
    };

    // §9: a HELLO at or past `ring::MAX_LSN` lies outside the LSN domain —
    // refused here, no join is posted. (`checked_next`, never the saturating
    // `next`: `d + 1` exists for every `d` below the bound.)
    let from = match d.checked_next() {
        Some(from) if d.0 < ring::MAX_LSN => from,
        _ => {
            send_err(&stream, ErrCode::Protocol, LSN_DOMAIN_MSG);
            return Ok(SessionEnd::Refused);
        }
    };
    if net.terminal.load(Ordering::Acquire) {
        set_status(r, ReplicaStatus::Failed);
        send_err(&stream, ErrCode::Protocol, LSN_DOMAIN_MSG);
        return Ok(SessionEnd::Refused);
    }

    // R7: resume from the replica's own durable_lsn. The writer thread decides
    // inside `poll(&wal)`. Nothing is sent before SERVING: the receiver
    // accepts nothing else first.
    set_status(r, ReplicaStatus::Joining);
    consumer.detach();
    r.slot.post(Request::Join { d });
    net.pending.store(true, Ordering::Release);
    let answer = match r
        .slot
        .await_answer(consumer, chunk, |_| Idle::Park(cfg.heartbeat))
    {
        Awaited::Answer(a) => a,
        Awaited::Abandoned | Awaited::Shutdown => return Ok(SessionEnd::Closed),
    };
    let (oldest, how) = match answer {
        Answer::Serve { oldest, how } => (oldest, how),
        Answer::Reseed { oldest } => {
            set_status(r, ReplicaStatus::NeedsReseed);
            let _ = send(
                &stream,
                &Frame::ReseedRequired {
                    primary_oldest: oldest,
                },
            );
            let _ = stream.shutdown(Shutdown::Both);
            return Ok(SessionEnd::Refused);
        }
        Answer::Reject => {
            set_status(r, ReplicaStatus::Rejected);
            send_err(
                &stream,
                ErrCode::Contiguity,
                "replica durable_lsn is ahead of the primary: \
                 a former primary must be wiped and re-seeded (§12.2)",
            );
            let _ = stream.shutdown(Shutdown::Both);
            return Ok(SessionEnd::Refused);
        }
        Answer::Terminal => {
            set_status(r, ReplicaStatus::Failed);
            send_err(&stream, ErrCode::Protocol, LSN_DOMAIN_MSG);
            let _ = stream.shutdown(Shutdown::Both);
            return Ok(SessionEnd::Refused);
        }
        _ => {
            send_err(
                &stream,
                ErrCode::Protocol,
                "internal error deciding the join",
            );
            return Err(ReplError::Protocol("unexpected join answer"));
        }
    };

    // Accepted. `acked` describes the replica connected NOW (review R4, §9.2
    // retention): after a wipe / re-seed its HELLO carries a lower
    // durable_lsn, which must replace the stale watermark.
    r.acked.store(d.0, Ordering::Release);
    send(
        &stream,
        &Frame::Serving {
            from_lsn: from,
            primary_oldest: oldest,
        },
    )?;
    *served = true;
    stream.set_read_timeout(Some(ack_timeout(cfg)))?;

    let dead = AtomicBool::new(false);
    let sent = AtomicU64::new(d.0);
    let me = thread::current();
    let end = thread::scope(|s| {
        // R4: the ACK reader runs from SERVING through catch-up and streaming.
        s.spawn(|| ack_reader(rd, r, &dead, &sent, &me));
        let mut sess = Serving {
            ctx,
            stream: &stream,
            dead: &dead,
            sent: &sent,
            next: from,
            pos: None,
            last_send: Instant::now(),
            out: Vec::with_capacity(cfg.send_batch * (cfg.slot_bytes + 17)),
        };
        let end = sess.run(consumer, chunk, how);
        match &end {
            Ok(Served::BelowOldest) => {
                // A FIN and no ERR: the replica sees a clean end, commits and
                // acks what it has, and closes; the ack reader then sees EOF
                // (the scope waits for it, bounded by its read timeout).
                let _ = stream.shutdown(Shutdown::Write);
            }
            Ok(Served::Closed) => {
                let _ = stream.shutdown(Shutdown::Both);
            }
            Ok(Served::Failed(code, msg)) => {
                set_status(r, ReplicaStatus::Failed);
                send_err(&stream, *code, msg);
                let _ = stream.shutdown(Shutdown::Both);
            }
            Err(e) => {
                // In-band faults (the sending-side contiguity check, a cursor
                // disagreement) are reported, never a silent EOF.
                if let Some(code) = e.err_code() {
                    send_err(&stream, code, &e.to_string());
                }
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
        end
    });
    match end? {
        Served::Closed => Ok(SessionEnd::Closed),
        Served::BelowOldest => Ok(SessionEnd::Reconnect),
        Served::Failed(..) => {
            *served = false; // back off: the failure is ours and may persist
            Ok(SessionEnd::Refused)
        }
    }
}

fn ack_timeout(cfg: &ShipperConfig) -> Duration {
    // The replica answers every heartbeat, so a live link yields a frame at
    // least every `heartbeat`.
    (cfg.heartbeat * 3).max(cfg.io_timeout)
}

/// What the network thread does next in a served session.
enum Mode {
    /// Drain the ring.
    Stream,
    /// Post `Chunk{next, pos}` and await the answer.
    Post,
    /// A Wait answer: re-post once `released ≥ next`.
    Waiting,
}

/// The served part of one session (after `SERVING`).
struct Serving<'a> {
    ctx: &'a Ctx<'a>,
    stream: &'a TcpStream,
    dead: &'a AtomicBool,
    /// R4 bound for the ack reader: the last LSN handed to the socket.
    sent: &'a AtomicU64,
    /// The sending-side check's cursor (§9.2): the next LSN to put on the wire.
    next: Lsn,
    /// The latest reader position a Chunk answer returned; kept for the rest
    /// of the session and sent with every chunk request (§9.1).
    pos: Option<ReaderPos>,
    /// The session-level heartbeat timer: when a frame was last sent.
    last_send: Instant,
    out: Vec<u8>,
}

impl Serving<'_> {
    fn run(&mut self, consumer: &mut Consumer, chunk: &mut Vec<u8>, how: Serve) -> Result<Served> {
        let Ctx { cfg, net, r, .. } = *self.ctx;
        let mut mode = match how {
            Serve::Attached => {
                self.resume_at(consumer, self.next)?;
                set_status(r, ReplicaStatus::Streaming);
                Mode::Stream
            }
            Serve::Wait => {
                enter_catching_up(r);
                Mode::Waiting
            }
            Serve::NeedChunk => {
                enter_catching_up(r);
                Mode::Post
            }
        };
        loop {
            if net.shutdown.load(Ordering::Acquire) || self.dead.load(Ordering::Acquire) {
                return Ok(Served::Closed);
            }
            if net.terminal.load(Ordering::Acquire) {
                return Ok(Served::Failed(ErrCode::Protocol, LSN_DOMAIN_MSG.into()));
            }
            match mode {
                Mode::Stream => {
                    if self.stream_pass(consumer)? {
                        // Overflow: fall back to catch-up inside the session.
                        enter_catching_up(r);
                        mode = Mode::Post;
                    }
                }
                Mode::Waiting => {
                    // A condition re-checked before every park, not an event:
                    // a release wake can coalesce with the DONE wake.
                    if consumer.released() >= self.next {
                        mode = Mode::Post;
                        continue;
                    }
                    let nap = self.heartbeat_if_due(consumer.released())?;
                    thread::park_timeout(nap);
                }
                Mode::Post => {
                    // Always detach before posting (§9.3).
                    consumer.detach();
                    r.slot.post(Request::Chunk {
                        cursor: self.next,
                        pos: self.pos,
                    });
                    net.pending.store(true, Ordering::Release);
                    let (stream, dead, last_send) = (self.stream, self.dead, &mut self.last_send);
                    let awaited = r.slot.await_answer(consumer, chunk, |c| {
                        if dead.load(Ordering::Acquire) {
                            return Idle::Abandon;
                        }
                        // One session-level timer across every post-SERVING
                        // wait, this one included.
                        let idle = last_send.elapsed();
                        if idle < cfg.heartbeat {
                            return Idle::Park(cfg.heartbeat - idle);
                        }
                        let hb = Frame::Heartbeat {
                            durable_lsn: c.released(),
                        };
                        if send(stream, &hb).is_err() {
                            return Idle::Abandon;
                        }
                        *last_send = Instant::now();
                        Idle::Park(cfg.heartbeat)
                    });
                    let answer = match awaited {
                        Awaited::Answer(a) => a,
                        Awaited::Abandoned | Awaited::Shutdown => return Ok(Served::Closed),
                    };
                    mode = match answer {
                        Answer::Attached { cursor } => {
                            if cursor != self.next {
                                return Err(ReplError::Contiguity {
                                    expected: self.next,
                                    got: cursor,
                                });
                            }
                            self.resume_at(consumer, cursor)?;
                            set_status(r, ReplicaStatus::Streaming);
                            Mode::Stream
                        }
                        Answer::Wait => Mode::Waiting,
                        Answer::Deferred => Mode::Post,
                        Answer::Chunk { next, end } => {
                            self.send_chunk(chunk)?;
                            if self.next != next {
                                return Err(ReplError::Contiguity {
                                    expected: next,
                                    got: self.next,
                                });
                            }
                            match end {
                                ChunkEnd::Attached => {
                                    self.resume_at(consumer, next)?;
                                    set_status(r, ReplicaStatus::Streaming);
                                    Mode::Stream
                                }
                                ChunkEnd::More(pos) => {
                                    self.pos = pos;
                                    Mode::Post
                                }
                            }
                        }
                        Answer::BelowOldest => return Ok(Served::BelowOldest),
                        Answer::ReadFailed { lsn } => {
                            return Ok(Served::Failed(
                                ErrCode::Poisoned,
                                format!("primary log unreadable at lsn {lsn}"),
                            ));
                        }
                        Answer::Terminal => {
                            return Ok(Served::Failed(ErrCode::Protocol, LSN_DOMAIN_MSG.into()));
                        }
                        Answer::Internal
                        | Answer::Reseed { .. }
                        | Answer::Reject
                        | Answer::Serve { .. } => {
                            return Ok(Served::Failed(
                                ErrCode::Protocol,
                                "internal error serving the replica".into(),
                            ));
                        }
                    };
                }
            }
        }
    }

    /// Pick up an attach the writer made. `resume()` MUST return the answered
    /// cursor (§9.2); otherwise the session ends with `ERR(Contiguity)`.
    fn resume_at(&self, consumer: &mut Consumer, cursor: Lsn) -> Result<()> {
        match consumer.resume() {
            Some(c) if c == cursor => Ok(()),
            got => Err(ReplError::Contiguity {
                expected: cursor,
                got: got.unwrap_or(Lsn(0)),
            }),
        }
    }

    /// Send `HEARTBEAT{released}` if the session has been silent for a
    /// heartbeat; return how long the caller may park.
    fn heartbeat_if_due(&mut self, released: Lsn) -> Result<Duration> {
        let hb = self.ctx.cfg.heartbeat;
        let idle = self.last_send.elapsed();
        if idle < hb {
            return Ok(hb - idle);
        }
        send(
            self.stream,
            &Frame::Heartbeat {
                durable_lsn: released,
            },
        )?;
        self.last_send = Instant::now();
        Ok(hb)
    }

    /// Publish the R4 bound, then write `out`. `sent` is stored BEFORE the
    /// write (review R1): the replica can receive, commit and ACK the front of
    /// a batch while `write_all` is still blocked; every byte it can receive
    /// is already in `out`, so `sent` stays a sound upper bound.
    fn flush(&mut self, last: Lsn) -> Result<()> {
        self.sent.store(last.0, Ordering::Release);
        let mut w = self.stream;
        w.write_all(&self.out)?;
        self.out.clear();
        self.last_send = Instant::now();
        Ok(())
    }

    /// Send a catch-up chunk's records, each through the sending-side check.
    fn send_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        let batch = self.ctx.cfg.send_batch;
        self.out.clear();
        let mut rest = chunk;
        let mut in_batch = 0;
        let mut last = None;
        while !rest.is_empty() {
            let Some((lsn, payload, tail)) = chunk_pop(rest) else {
                return Err(ReplError::Protocol("malformed catch-up chunk"));
            };
            rest = tail;
            // A record with lsn ≠ next is never encoded, never sent.
            self.next = send_check(self.next, lsn)?;
            wire::encode_record(&mut self.out, lsn, payload);
            last = Some(lsn);
            in_batch += 1;
            if in_batch == batch {
                self.flush(lsn)?;
                in_batch = 0;
                last = None;
            }
        }
        if let Some(l) = last {
            self.flush(l)?;
        }
        Ok(())
    }

    /// One drain of the ring into `RECORD` frames. `Ok(true)` if the ring
    /// evicted the cursor (`NeedsCatchUp`): the consumer is detached and the
    /// caller posts a chunk request at `self.next`.
    fn stream_pass(&mut self, consumer: &mut Consumer) -> Result<bool> {
        let cfg = self.ctx.cfg;
        self.out.clear();
        let mut next = self.next;
        let mut last = None;
        let mut bad = None;
        let out = &mut self.out;
        // The emit closure runs while the record is pinned: check, encode
        // (CRC + memcpy) and return — never block in it.
        let d = consumer.drain(cfg.send_batch, |lsn, p| {
            if bad.is_some() {
                return;
            }
            match send_check(next, lsn) {
                Ok(n) => {
                    wire::encode_record(out, lsn, p);
                    last = Some(lsn);
                    next = n;
                }
                Err(e) => bad = Some(e),
            }
        });
        self.next = next;
        if let Some(l) = last {
            self.flush(l)?;
        }
        if let Some(e) = bad {
            return Err(e);
        }
        match d.status {
            ConsumerStatus::Streaming => {}
            ConsumerStatus::NeedsCatchUp { next } => {
                if next != self.next {
                    return Err(ReplError::Contiguity {
                        expected: self.next,
                        got: next,
                    });
                }
                return Ok(true);
            }
            ConsumerStatus::Detached => return Err(ReplError::Protocol("consumer detached")),
        }
        if d.emitted == 0 {
            let nap = self.heartbeat_if_due(d.watermark)?;
            consumer.wait(nap);
        }
        Ok(false)
    }
}

/// Read the replica's `ACK`/`HEARTBEAT` frames into `acked` — monotonic
/// **within a session**; the session start resets it to the HELLO's
/// `durable_lsn` (see `session`). An ack beyond what was sent on this
/// connection is a protocol violation (R4: a replica can only be durable to
/// what it received) and ends the connection.
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
