//! The replica side: [`Receiver`] (§7 of `docs/replica_design_v1.md`).
//!
//! A receiver owns its **own** [`Wal`] in its own directory and turns the
//! primary's `RECORD` stream into durable local records:
//!
//! 1. send `HELLO{durable_lsn}` (its resume point, R7);
//! 2. accept `SERVING{from}` only if `from == last_lsn + 1` (R3);
//! 3. per `RECORD`: **CRC** (verified by the decoder, R9) → **contiguity**
//!    `lsn == last_lsn + 1` (R3, hard check — never skip, never best-effort) →
//!    `append` → the returned LSN MUST equal the shipped LSN (R2);
//! 4. group-commit every `batch_records` records or `batch_interval`, and after
//!    `commit() → Ok(w)` send `ACK{w}` — the durable watermark, never
//!    `last_lsn` (R4).
//!
//! Any violation closes the connection with an `ERR` frame; nothing past the
//! offending record is appended. Records appended *before* it are valid (the
//! primary only ships records `≤` its durable watermark, R1), so they are
//! committed and acked before the connection closes — the next `HELLO` then
//! carries `durable_lsn == last_lsn`, and the primary resumes exactly after it.
//!
//! A failed `commit` **poisons** the replica WAL (WAL §12): the receiver sends
//! `ERR(Poisoned)`, closes, and refuses every later connection. The process
//! must drop the `Receiver` and [`open`](Receiver::open) it again (recovery)
//! before reconnecting.
//!
//! **Replica-hosted consumers (§7.1).** A replica may host downstream consumers
//! (read models, subscribers) off its *own* [`DurabilityObserver`]: open it with
//! [`Receiver::open_with`]. The observer fires only after the replica's
//! `commit() → Ok` — so a consumer fed from it acts only on records durable on
//! at least two nodes (here, and by R1 on the primary), never on `append`. (Once
//! WAL v7 lands, a re-seeded replica opens via `Wal::options().seed(..)
//! .observer(o)`; that is RM4.)
//!
//! The replica's `WalConfig` MUST use the primary's `segment_size` and
//! `max_record_size` (§7 step 1). The protocol does not carry them; a primary
//! with a larger `max_record_size` is rejected per-frame (`ERR(Protocol)`).

use std::io::{self, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use open_wal::{DurabilityObserver, Lsn, NullObserver, Wal, WalConfig, WalError};

use crate::error::{ReplError, Result};
use crate::wire::{self, Frame, FrameReader, Limits, PROTO_VERSION};

/// Receiver configuration (§14).
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    /// The replica WAL's configuration. `segment_size` and `max_record_size`
    /// MUST equal the primary's.
    pub wal_config: WalConfig,
    /// This replica's identity, sent in `HELLO` (informational for the primary).
    pub replica_id: u64,
    /// Group-commit after this many appended records (≥ 1).
    pub batch_records: usize,
    /// Group-commit when the oldest uncommitted record is this old, even if the
    /// batch is not full.
    pub batch_interval: Duration,
    /// Close the connection if the primary sends nothing for this long while
    /// nothing is pending (`None` = wait forever). The primary heartbeats when
    /// idle, so a value of a few heartbeat periods detects a dead primary.
    pub idle_timeout: Option<Duration>,
}

impl ReceiverConfig {
    /// Defaults around `wal_config`: batches of up to 256 records or 2 ms, no
    /// idle timeout, `replica_id` 0.
    #[must_use]
    pub fn new(wal_config: WalConfig) -> ReceiverConfig {
        ReceiverConfig {
            wal_config,
            replica_id: 0,
            batch_records: 256,
            batch_interval: Duration::from_millis(2),
            idle_timeout: None,
        }
    }
}

/// The replica's watermarks, observable from other threads (monitoring, tests).
/// Updated by the receiver after each `append` (`last`) and each successful
/// `commit` (`durable`).
#[derive(Debug, Default)]
pub struct ReplicaWatermarks {
    durable: AtomicU64,
    last: AtomicU64,
}

impl ReplicaWatermarks {
    /// The replica WAL's `durable_lsn` as of its last successful commit.
    #[must_use]
    pub fn durable_lsn(&self) -> Lsn {
        Lsn(self.durable.load(Ordering::Acquire))
    }

    /// The replica WAL's `last_lsn` (durable or still buffered).
    #[must_use]
    pub fn last_lsn(&self) -> Lsn {
        Lsn(self.last.load(Ordering::Acquire))
    }
}

/// The replica: an `open-wal` fed by the primary's record stream (§7).
///
/// Generic over the replica WAL's [`DurabilityObserver`] (default: none) so a
/// replica can host downstream consumers off its own durable watermark (§7.1).
pub struct Receiver<O: DurabilityObserver = NullObserver> {
    wal: Wal<O>,
    cfg: ReceiverConfig,
    poisoned: bool,
    watermarks: Arc<ReplicaWatermarks>,
    out: Vec<u8>,
}

impl Receiver<NullObserver> {
    /// Open (and recover) the replica WAL in `dir`. Its recovered `durable_lsn`
    /// is the resume point sent in the next `HELLO`.
    pub fn open(dir: &Path, cfg: ReceiverConfig) -> Result<Receiver<NullObserver>> {
        Receiver::open_with(dir, cfg, NullObserver)
    }
}

impl<O: DurabilityObserver> Receiver<O> {
    /// Like [`open`](Receiver::open), with `observer` attached to the replica
    /// WAL (§7.1): it is notified of each durable advance after the replica's
    /// `commit() → Ok`, on the receiver's thread, under the WAL's observer
    /// contract (cheap, non-blocking, no I/O, no panic).
    pub fn open_with(dir: &Path, cfg: ReceiverConfig, observer: O) -> Result<Receiver<O>> {
        if cfg.batch_records == 0 {
            return Err(ReplError::Wal(WalError::InvalidConfig));
        }
        let (wal, _report) = Wal::open_with(dir, cfg.wal_config, observer)?;
        let watermarks = Arc::new(ReplicaWatermarks {
            durable: AtomicU64::new(wal.durable_lsn().0),
            last: AtomicU64::new(wal.last_lsn().0),
        });
        Ok(Receiver {
            wal,
            cfg,
            poisoned: false,
            watermarks,
            out: Vec::with_capacity(64),
        })
    }

    /// The replica WAL's durable watermark.
    #[must_use]
    pub fn durable_lsn(&self) -> Lsn {
        self.wal.durable_lsn()
    }

    /// The replica WAL's last assigned LSN.
    #[must_use]
    pub fn last_lsn(&self) -> Lsn {
        self.wal.last_lsn()
    }

    /// A shared handle to the replica's watermarks, readable from any thread.
    #[must_use]
    pub fn watermarks(&self) -> Arc<ReplicaWatermarks> {
        Arc::clone(&self.watermarks)
    }

    /// True once a `commit` failed: the WAL is poisoned and this receiver
    /// refuses all further connections until reopened.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// The replica WAL (e.g. to replay it, or to checkpoint it per the
    /// replica integrator's own snapshot policy, §7 step 6).
    #[must_use]
    pub fn wal(&self) -> &Wal<O> {
        &self.wal
    }

    /// Mutable access to the replica WAL, e.g. for `checkpoint`. Must not be
    /// used to `append` — that would break LSN mirroring (R2).
    pub fn wal_mut(&mut self) -> &mut Wal<O> {
        &mut self.wal
    }

    /// Consume the receiver, returning its WAL.
    #[must_use]
    pub fn into_wal(self) -> Wal<O> {
        self.wal
    }

    /// Accept connections from `listener` and serve each in turn. Returns on
    /// the first **fatal** error — the WAL is poisoned or the primary demands a
    /// re-seed — or an `accept` failure. Connection-level errors (contiguity,
    /// CRC, protocol, transport) just end that connection; the primary
    /// reconnects and the next `HELLO` resumes from `durable_lsn`.
    pub fn run(&mut self, listener: &TcpListener) -> Result<()> {
        loop {
            let (stream, _) = listener.accept()?;
            match self.serve_connection(stream) {
                Ok(()) => {}
                Err(e @ (ReplError::Wal(_) | ReplError::ReseedRequired { .. })) => return Err(e),
                Err(_) => {}
            }
        }
    }

    /// Serve one primary connection to completion (§7 steps 2–5).
    ///
    /// Returns `Ok(())` when the primary closes the connection cleanly. Every
    /// error ends the connection; before it closes, every record appended so far
    /// is committed and acked, and an in-band error is reported to the peer as
    /// an `ERR` frame. `Err(Wal(_))` means the WAL is poisoned (reopen needed).
    pub fn serve_connection(&mut self, stream: TcpStream) -> Result<()> {
        if self.poisoned {
            let _ = self.send_err(&stream, &ReplError::Wal(WalError::Poisoned));
            let _ = stream.shutdown(Shutdown::Both);
            return Err(ReplError::Wal(WalError::Poisoned));
        }
        let _ = stream.set_nodelay(true);
        let res = self.session(&stream);
        // Make everything appended durable before closing, so the next HELLO's
        // durable_lsn equals last_lsn and the primary resumes exactly after it.
        let res = match (res, self.commit_and_ack(&stream)) {
            (_, Err(poison)) => Err(poison),
            (r, Ok(())) => r,
        };
        if let Err(e) = &res {
            let _ = self.send_err(&stream, e);
        }
        let _ = stream.shutdown(Shutdown::Both);
        match res {
            // A clean EOF from the primary ends the session normally.
            Err(ReplError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(()),
            other => other,
        }
    }

    fn session(&mut self, stream: &TcpStream) -> Result<()> {
        self.send(
            stream,
            &Frame::Hello {
                proto_ver: PROTO_VERSION,
                replica_id: self.cfg.replica_id,
                durable_lsn: self.wal.durable_lsn(),
            },
        )?;
        let limits = Limits::new(self.cfg.wal_config.max_record_size);
        let mut rd = FrameReader::new(stream, limits);

        set_timeout(stream, self.cfg.idle_timeout)?;
        match rd.read_frame()? {
            Frame::Serving { from_lsn, .. } => {
                let expected = self.wal.last_lsn().next();
                if from_lsn != expected {
                    return Err(ReplError::Contiguity {
                        expected,
                        got: from_lsn,
                    });
                }
            }
            Frame::ReseedRequired { primary_oldest } => {
                return Err(ReplError::ReseedRequired {
                    oldest: primary_oldest,
                });
            }
            Frame::Err { code, msg } => {
                return Err(ReplError::Remote {
                    code,
                    msg: msg.to_owned(),
                });
            }
            _ => return Err(ReplError::Protocol("expected SERVING after HELLO")),
        }

        let mut pending = 0usize;
        let mut batch_deadline: Option<Instant> = None;
        loop {
            let timeout = match batch_deadline {
                None => self.cfg.idle_timeout,
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        self.commit_and_ack(stream)?;
                        pending = 0;
                        batch_deadline = None;
                        continue;
                    }
                    Some(deadline - now)
                }
            };
            set_timeout(stream, timeout)?;
            match rd.read_frame() {
                Ok(Frame::Record { lsn, payload }) => {
                    // R9 (CRC) was verified by the decoder. R3: hard contiguity
                    // check BEFORE append — a gap, duplicate or reorder is never
                    // appended.
                    let expected = self.wal.last_lsn().next();
                    if lsn != expected {
                        return Err(ReplError::Contiguity { expected, got: lsn });
                    }
                    let got = self.append(payload)?;
                    // R2: LSN mirroring. Guaranteed by the check above unless the
                    // WAL itself misbehaves; never continue past a mismatch.
                    if got != lsn {
                        return Err(ReplError::Contiguity { expected: lsn, got });
                    }
                    pending += 1;
                    batch_deadline.get_or_insert_with(|| Instant::now() + self.cfg.batch_interval);
                    if pending >= self.cfg.batch_records {
                        self.commit_and_ack(stream)?;
                        pending = 0;
                        batch_deadline = None;
                    }
                }
                Ok(Frame::Heartbeat { .. }) => {
                    let durable_lsn = self.wal.durable_lsn();
                    self.send(stream, &Frame::Heartbeat { durable_lsn })?;
                }
                Ok(Frame::Err { code, msg }) => {
                    return Err(ReplError::Remote {
                        code,
                        msg: msg.to_owned(),
                    });
                }
                Ok(_) => return Err(ReplError::Protocol("unexpected frame from primary")),
                Err(ReplError::Io(e)) if is_timeout(&e) && pending > 0 => {
                    // batch_interval elapsed: loop around and commit.
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn append(&mut self, payload: &[u8]) -> Result<Lsn> {
        let lsn = self.wal.append(payload)?;
        self.watermarks.last.store(lsn.0, Ordering::Release);
        Ok(lsn)
    }

    /// Commit everything appended and, if the durable watermark advanced, send
    /// `ACK{w}` with the value `commit` returned (R4 — never `last_lsn`). A
    /// commit failure poisons the receiver.
    fn commit_and_ack(&mut self, stream: &TcpStream) -> Result<()> {
        if self.wal.last_lsn() == self.wal.durable_lsn() {
            return Ok(());
        }
        let w = match self.wal.commit() {
            Ok(w) => w,
            Err(e) => {
                self.poisoned = true;
                return Err(ReplError::Wal(e));
            }
        };
        self.watermarks.durable.store(w.0, Ordering::Release);
        // A failed ACK send is a transport problem, not a durability one: the
        // records are durable regardless, and the next HELLO reports them.
        let _ = self.send(stream, &Frame::Ack { durable_lsn: w });
        Ok(())
    }

    fn send(&mut self, mut stream: &TcpStream, frame: &Frame<'_>) -> Result<()> {
        self.out.clear();
        wire::encode_frame(frame, &mut self.out);
        stream.write_all(&self.out)?;
        Ok(())
    }

    fn send_err(&mut self, stream: &TcpStream, e: &ReplError) -> Result<()> {
        match e.err_code() {
            Some(code) => {
                let msg = e.to_string();
                self.send(stream, &Frame::Err { code, msg: &msg })
            }
            None => Ok(()),
        }
    }
}

fn set_timeout(stream: &TcpStream, t: Option<Duration>) -> Result<()> {
    // `set_read_timeout(Some(0))` is an error; clamp to the smallest real wait.
    let t = t.map(|d| d.max(Duration::from_micros(1)));
    stream.set_read_timeout(t)?;
    Ok(())
}

fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}
