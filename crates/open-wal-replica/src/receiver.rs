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
//! at least two nodes (here, and by R1 on the primary), never on `append`.
//!
//! **Re-seed (§10.1).** A replica that was answered `RESEED_REQUIRED`, or that
//! is new on a primary whose log no longer starts at 1, is restarted from an
//! application snapshot at LSN `s` with [`Receiver::open_seeded`]: a log that
//! predates the snapshot is **set aside** (renamed, never deleted) and a new
//! one is cold-started at `s + 1`. The crate never produces, transports or
//! installs the snapshot itself.
//!
//! The replica's `WalConfig` MUST use the primary's `segment_size` and
//! `max_record_size` (§7 step 1). The protocol does not carry them; a primary
//! with a larger `max_record_size` is rejected per-frame (`ERR(Protocol)`).

use std::io::{self, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use open_wal::{DurabilityObserver, Lsn, NullObserver, Wal, WalConfig, WalError};

use crate::error::{ReplError, Result};
use crate::wire::{self, ErrCode, Frame, FrameReader, Limits, PROTO_VERSION};

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
    /// nothing is pending. The primary heartbeats when idle, so a few
    /// heartbeat periods detects a primary that died without FIN/RST (power
    /// loss, partition) — without it the receiver would block in `read`
    /// forever and never serve the restarted primary's new connection (R7).
    /// Default 5 s (≥ 3× the shipper's default 500 ms heartbeat). **`None`
    /// disables dead-primary detection.**
    pub idle_timeout: Option<Duration>,
}

impl ReceiverConfig {
    /// Defaults around `wal_config`: batches of up to 256 records or 2 ms, a
    /// 5 s idle timeout (dead-primary detection), `replica_id` 0.
    #[must_use]
    pub fn new(wal_config: WalConfig) -> ReceiverConfig {
        ReceiverConfig {
            wal_config,
            replica_id: 0,
            batch_records: 256,
            batch_interval: Duration::from_millis(2),
            idle_timeout: Some(Duration::from_secs(5)),
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

/// What [`Receiver::open_seeded`] did to the directory (§10.1).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SeedReport {
    /// The directory a stale log was moved to (`<dir>.stale.<k>`), if one was
    /// set aside by this call. The crate never deletes or reopens it: it still
    /// holds the replica's old durable prefix, and removing it is the
    /// operator's decision.
    pub set_aside: Option<PathBuf>,
}

impl<O: DurabilityObserver> Receiver<O> {
    /// Like [`open`](Receiver::open), with `observer` attached to the replica
    /// WAL (§7.1): it is notified of each durable advance after the replica's
    /// `commit() → Ok`, on the receiver's thread, under the WAL's observer
    /// contract (cheap, non-blocking, no I/O, no panic).
    pub fn open_with(dir: &Path, cfg: ReceiverConfig, observer: O) -> Result<Receiver<O>> {
        check_cfg(&cfg)?;
        let (wal, _report) = Wal::open_with(dir, cfg.wal_config, observer)?;
        Ok(Receiver::from_wal(wal, cfg))
    }

    /// Open the replica WAL for an application that may have been re-seeded
    /// from a snapshot (§10.1).
    ///
    /// `installed` is the LSN of the most recent re-seed snapshot the
    /// application has **durably** installed, read from the application's own
    /// durable state; `None` if it has never been re-seeded. Pass the same
    /// value on every start.
    ///
    /// - `None` ⇒ exactly [`open_with`](Receiver::open_with)`(dir, cfg,
    ///   observer())`.
    /// - `Some(s)` ⇒ the WAL is opened with the cold-start seed `s + 1` (which
    ///   the WAL ignores when `dir` already holds a log). A log with
    ///   `durable_lsn ≥ s` is kept — every normal restart. A log with
    ///   `durable_lsn < s` **predates the snapshot**: it is closed, the whole
    ///   directory is renamed to `<dir>.stale.<k>` (smallest unused `k`; never
    ///   deleted, never overwritten), and a new log is cold-started at `s + 1`
    ///   (`oldest_lsn = s + 1`, `durable_lsn = s`). The rule is stateless, so a
    ///   crash anywhere in it converges when the same call is made again.
    ///
    /// `observer` is a factory because the set-aside path opens the WAL twice
    /// and a `Wal` consumes its observer: it is called exactly once per WAL
    /// open (once normally, twice when a log is set aside). The crate never
    /// calls the observer itself, and neither does the WAL at open: a consumer
    /// that needs a starting watermark reads [`durable_lsn`](Receiver::durable_lsn)
    /// (after a seeded cold start it is `s`).
    ///
    /// # Requirements
    /// - `dir` MUST be dedicated to the WAL (the set-aside renames the whole
    ///   directory): keep the application's `installed` record and snapshot
    ///   files elsewhere. It must not be a mount point; its parent must exist
    ///   and be writable. A bare relative `dir` has parent `.`.
    /// - The snapshot (including `s` itself) MUST be durably installed before
    ///   this call: afterwards the log starts at `s + 1`.
    /// - This process is the only one creating `<dir>.stale.*`.
    /// - A former primary (§12.2) is **not** covered: its log may have
    ///   `durable_lsn ≥ s` and would be kept. Wipe it by hand first.
    ///
    /// # Errors
    /// A WAL open error is returned unchanged and nothing is set aside (a log
    /// that cannot be recovered has an unknown `durable_lsn`; the operator
    /// decides). A failed rename / directory creation / directory fsync is
    /// [`ReplError::Io`]. `s == u64::MAX` is [`ReplError::LsnExhausted`].
    pub fn open_seeded(
        dir: &Path,
        cfg: ReceiverConfig,
        installed: Option<Lsn>,
        mut observer: impl FnMut() -> O,
    ) -> Result<(Receiver<O>, SeedReport)> {
        let Some(s) = installed else {
            let rx = Receiver::open_with(dir, cfg, observer())?;
            return Ok((rx, SeedReport::default()));
        };
        check_cfg(&cfg)?;
        let seed = s
            .checked_next()
            .ok_or(ReplError::LsnExhausted { last: s })?;
        let parent = parent_of(dir)?;
        if !exists(dir)? {
            // The WAL's own `create_dir_all` does not fsync the parent: the new
            // log's directory entry would not be durable while the replica
            // already acks records in it.
            create_dir_durably(dir, parent)?;
        }
        let (wal, _report) = Wal::options()
            .seed(seed)
            .observer(observer())
            .open(dir, cfg.wal_config)?;
        if wal.durable_lsn() >= s {
            return Ok((Receiver::from_wal(wal, cfg), SeedReport::default()));
        }
        // The log predates the snapshot. Drop the handle — and with it the
        // directory lock — before the rename.
        drop(wal);
        let stale = set_aside(dir, parent)?;
        create_dir_durably(dir, parent)?;
        let (wal, _report) = Wal::options()
            .seed(seed)
            .observer(observer())
            .open(dir, cfg.wal_config)?;
        Ok((
            Receiver::from_wal(wal, cfg),
            SeedReport {
                set_aside: Some(stale),
            },
        ))
    }

    fn from_wal(wal: Wal<O>, cfg: ReceiverConfig) -> Receiver<O> {
        let watermarks = Arc::new(ReplicaWatermarks {
            durable: AtomicU64::new(wal.durable_lsn().0),
            last: AtomicU64::new(wal.last_lsn().0),
        });
        Receiver {
            wal,
            cfg,
            poisoned: false,
            watermarks,
            out: Vec::with_capacity(64),
        }
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
                Err(
                    e @ (ReplError::Wal(_)
                    | ReplError::ReseedRequired { .. }
                    | ReplError::LsnExhausted { .. }),
                ) => return Err(e),
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
                let expected = next_lsn(self.wal.last_lsn())?;
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
                    let expected = next_lsn(self.wal.last_lsn())?;
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
        // §7 step 5 (review R5): EVERY poisoning commit is reported as
        // ERR(Poisoned) — whatever `WalError` it surfaced as (`FsyncFailed`,
        // or `Io` from a failed write/roll), and even when it replaced a
        // pending in-band error at end of session.
        let code = if self.poisoned {
            Some(ErrCode::Poisoned)
        } else {
            e.err_code()
        };
        match code {
            Some(code) => {
                let msg = e.to_string();
                self.send(stream, &Frame::Err { code, msg: &msg })
            }
            None => Ok(()),
        }
    }
}

fn check_cfg(cfg: &ReceiverConfig) -> Result<()> {
    if cfg.batch_records == 0 {
        return Err(ReplError::Wal(WalError::InvalidConfig));
    }
    Ok(())
}

// ---- §10.1 directory operations -------------------------------------------
// The only filesystem operations this crate performs outside the WAL: one
// rename, directory creation, parent-directory fsyncs, and existence checks.
// Nothing here deletes or overwrites anything.

/// `dir`'s parent directory; a bare relative `dir` has parent `.`.
fn parent_of(dir: &Path) -> Result<&Path> {
    match dir.parent() {
        Some(p) if p.as_os_str().is_empty() => Ok(Path::new(".")),
        Some(p) => Ok(p),
        None => Err(ReplError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the replica WAL directory has no parent directory",
        ))),
    }
}

/// Whether anything (a dangling symlink included) exists at `path`.
fn exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(ReplError::Io(e)),
    }
}

fn fsync_dir(dir: &Path) -> Result<()> {
    // `sync_all` on a directory handle: `fsync` on Linux, `F_FULLFSYNC` on macOS.
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Create `dir` (one level) and make its directory entry durable.
fn create_dir_durably(dir: &Path, parent: &Path) -> Result<()> {
    std::fs::create_dir(dir)?;
    fsync_dir(parent)
}

/// Rename `dir` to `<dir>.stale.<k>` with the smallest unused `k ≥ 0`, and
/// make the rename durable. Returns the new path.
fn set_aside(dir: &Path, parent: &Path) -> Result<PathBuf> {
    let name = dir.file_name().ok_or_else(|| {
        ReplError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the replica WAL directory has no name to set aside",
        ))
    })?;
    let mut k: u64 = 0;
    let stale = loop {
        let mut n = name.to_os_string();
        n.push(format!(".stale.{k}"));
        let candidate = parent.join(n);
        if !exists(&candidate)? {
            break candidate;
        }
        k = k
            .checked_add(1)
            .ok_or_else(|| ReplError::Io(io::Error::other("no unused .stale.<k> name")))?;
    };
    std::fs::rename(dir, &stale)?;
    fsync_dir(parent)?;
    Ok(stale)
}

/// `last + 1` for sequencing — never saturating (WAL v7.1: `u64::MAX` is the
/// last LSN; a saturating `next()` would repeat it).
fn next_lsn(last: Lsn) -> Result<Lsn> {
    last.checked_next().ok_or(ReplError::LsnExhausted { last })
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
