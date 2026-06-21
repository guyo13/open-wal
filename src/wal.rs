//! The single-writer `Wal` handle — open, append, commit, replay.
//!
//! **Scope through M3:** a single, pre-allocated segment. `append` is pure
//! memory (§7.1); `commit` writes the staged batch and `fdatasync`s it (§7.2,
//! single-segment only); `open` cold-starts an empty directory or reopens a
//! single segment, running full intra-segment recovery on it (§8.2,
//! [`recovery`](crate::recovery) — torn-tail truncation + durable zeroing, and
//! fatal-on-mid-log-corruption). Segment roll, commit-time split, and
//! checkpoint are later milestones (M4–M5) and are deliberately absent here.

use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::error::{Result, WalError};
use crate::observer::{DurabilityObserver, NullObserver};
use crate::reader::Reader;
use crate::recovery;
use crate::segment::{self, HEADER_SIZE};
use crate::{Lsn, WalConfig};

/// Outcome of recovery, returned by [`Wal::open`] (§6).
#[derive(Debug, Clone, Copy)]
pub struct RecoveryReport {
    /// `P`: base LSN of the oldest surviving segment (1 until the first
    /// checkpoint).
    pub oldest_lsn: Lsn,
    /// `k`: highest recovered durable LSN (`oldest_lsn - 1` if the suffix is
    /// empty).
    pub durable_lsn: Lsn,
    /// Whether the tail was clean or a torn tail was truncated.
    pub tail_state: TailState,
    /// Number of segment files inspected during recovery.
    pub segments_scanned: usize,
}

/// State of the active segment's tail after recovery (§6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailState {
    /// The tail ended cleanly (sentinel / end of records); no truncation.
    Clean,
    /// A torn tail was detected, truncated, and durably zeroed at this offset of
    /// the active segment (§8.2.1).
    TruncatedAt {
        /// `base_lsn` of the active segment that was truncated.
        segment_base: Lsn,
        /// Byte offset within that segment at which the tail was truncated.
        offset: u64,
    },
}

/// Single-writer, append-only write-ahead log handle.
///
/// `Send` but **not `Sync`** (§6.2): the write methods take `&mut self`, and the
/// `PhantomData<Cell<()>>` marker makes sharing the handle across threads a
/// compile error, so concurrent writers cannot exist. Generic over a
/// [`DurabilityObserver`]; the default [`NullObserver`] compiles away.
pub struct Wal<O: DurabilityObserver = NullObserver> {
    /// Held open for the handle's lifetime so the exclusive `flock` is retained;
    /// dropping the `Wal` releases the lock.
    _lock: File,
    /// The active (highest-`base_lsn`) segment.
    active: File,
    active_base: Lsn,
    /// Offset of the next byte to write in the active segment.
    write_offset: u64,
    oldest_lsn: Lsn,
    last_lsn: Lsn,
    durable_lsn: Lsn,
    segment_size: u64,
    max_record_size: u32,
    /// Reusable encode buffer for the current uncommitted batch (§7.1).
    staging: Vec<u8>,
    observer: O,
    /// Set after a durability failure; all subsequent ops return `Poisoned`
    /// (§12).
    poisoned: bool,
    /// Makes `Wal` `!Sync` (single-writer enforcement, §6.2).
    _not_sync: PhantomData<Cell<()>>,
}

impl Wal<NullObserver> {
    /// Open or create a WAL in `dir` with the default no-op observer.
    pub fn open(dir: &Path, config: WalConfig) -> Result<(Wal<NullObserver>, RecoveryReport)> {
        Wal::open_with(dir, config, NullObserver)
    }
}

impl<O: DurabilityObserver> Wal<O> {
    /// Open or create a WAL in `dir`, running recovery, with an explicit
    /// `observer` (§6). Acquires an exclusive advisory lock on the directory;
    /// fails with [`Locked`](WalError::Locked) if already held.
    ///
    /// Recovers a single segment (§8.2): it cold-starts an empty directory
    /// (creating `…0001.wal`) or reopens an existing segment, scanning its dense
    /// record run and handling a torn tail (truncate + durably zero `[X, EOF)`)
    /// or mid-log corruption (fatal). Multi-segment recovery is M4.
    pub fn open_with(
        dir: &Path,
        config: WalConfig,
        observer: O,
    ) -> Result<(Wal<O>, RecoveryReport)> {
        // §5.3 precondition, additive form (no `segment_size - 91` underflow).
        if u64::from(config.max_record_size) + 91 > config.segment_size {
            return Err(WalError::InvalidConfig);
        }

        std::fs::create_dir_all(dir)?;

        // Exclusive writer lock for the handle's lifetime (§6.2).
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("LOCK"))?;
        match rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {}
            // EWOULDBLOCK == EAGAIN on Linux/macOS; the lock is already held.
            Err(rustix::io::Errno::WOULDBLOCK) => {
                return Err(WalError::Locked);
            }
            Err(e) => return Err(WalError::Io(io::Error::from(e))),
        }

        // Discover segments: sorted by base_lsn, never trusting dir order (§8.6).
        let mut bases: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                if let Some(base) = segment::parse_base_lsn(name) {
                    bases.push(base);
                }
            }
        }
        bases.sort_unstable();

        let (active, active_base, write_offset, last_lsn, oldest_lsn, segments_scanned, tail_state) =
            if bases.is_empty() {
                let (f, base, off, last, oldest, n) = Self::cold_start(dir, config.segment_size)?;
                (f, base, off, last, oldest, n, TailState::Clean)
            } else {
                Self::reopen(dir, &bases, config)?
            };

        let durable_lsn = last_lsn;
        let report = RecoveryReport {
            oldest_lsn,
            durable_lsn,
            tail_state,
            segments_scanned,
        };

        let wal = Wal {
            _lock: lock,
            active,
            active_base,
            write_offset,
            oldest_lsn,
            last_lsn,
            durable_lsn,
            segment_size: config.segment_size,
            max_record_size: config.max_record_size,
            staging: Vec::new(),
            observer,
            poisoned: false,
            _not_sync: PhantomData,
        };
        Ok((wal, report))
    }

    /// Cold start (§8.4): create `…0001.wal`, then fsync the directory so the
    /// new filename is durable (§7.4 step 5).
    fn cold_start(dir: &Path, segment_size: u64) -> Result<(File, Lsn, u64, Lsn, Lsn, usize)> {
        let active = segment::create(dir, Lsn::FIRST, segment_size)?;
        fsync_dir(dir)?;
        // base 1, empty: write offset just past the header, durable_lsn = 0.
        Ok((active, Lsn::FIRST, HEADER_SIZE, Lsn::NONE, Lsn::FIRST, 1))
    }

    /// Reopen the highest-base segment and run intra-segment recovery on it
    /// (§8.2, M3). The lower bases (if any) only set `oldest_lsn`.
    ///
    /// **M3 single-segment scope:** this recovers only the highest-base (active)
    /// segment — with full torn-tail/corruption classification — but does **not**
    /// yet validate the sealed segments' headers or cross-segment LSN continuity
    /// (§8.1 steps 2–3); `segments_scanned` counts the files discovered, not the
    /// ones scanned. The writer cannot produce multiple segments before M4, so
    /// those checks (and the §8.4 discard of an incomplete-header highest-base
    /// file) arrive with the roll machinery in M4. The active segment's
    /// classifier path is already exercised here.
    fn reopen(
        dir: &Path,
        bases: &[u64],
        config: WalConfig,
    ) -> Result<(File, Lsn, u64, Lsn, Lsn, usize, TailState)> {
        let oldest_lsn = Lsn(bases[0]);
        let active_base = Lsn(*bases.last().unwrap());

        let active = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(segment::filename_for(active_base)))?;

        // Validate the active header and confirm it matches its filename. A bad
        // header is fatal (§8.1 step 2); it is written and synced at creation,
        // so it is never a torn tail. A physically truncated header (file cut
        // below 64 bytes, §14.4f) is reported as `BadSegmentHeader` rather than
        // a raw `UnexpectedEof`, keeping recovery total (D11).
        let mut header = [0u8; HEADER_SIZE as usize];
        match active.read_exact_at(&mut header, 0) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(WalError::BadSegmentHeader);
            }
            Err(e) => return Err(WalError::Io(e)),
        }
        let parsed = segment::decode_header(&header)?;
        if parsed.base_lsn != active_base {
            return Err(WalError::BadSegmentHeader);
        }

        // Intra-segment recovery: tail detection, durable zero-to-EOF on a torn
        // tail, fatal on mid-log corruption (§8.2).
        let rec = recovery::recover_segment(
            &active,
            active_base,
            true,
            config.segment_size,
            config.max_record_size,
        )?;

        Ok((
            active,
            active_base,
            rec.write_offset,
            rec.max_lsn,
            oldest_lsn,
            bases.len(),
            rec.tail_state,
        ))
    }

    /// Sequence + buffer a record (§7.1). Pure memory: no syscall, no allocation
    /// once the staging buffer is warm. The record is **not** durable until a
    /// later `commit` returns covering it.
    pub fn append(&mut self, payload: &[u8]) -> Result<Lsn> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }
        if payload.len() > self.max_record_size as usize {
            return Err(WalError::RecordTooLarge);
        }
        let lsn = self.last_lsn.next();
        crate::record::encode_into(&mut self.staging, lsn, payload);
        self.last_lsn = lsn;
        Ok(lsn)
    }

    /// Make all buffered records durable (§7.2): one `write` + `fdatasync`
    /// (`F_FULLFSYNC` on macOS), advancing `durable_lsn`. On any I/O failure the
    /// handle is **poisoned** (§12) and `durable_lsn` does not advance.
    ///
    /// M2 handles the single-segment case only; the commit-time split across
    /// segments is M4. Until then a batch that would overrun the active segment
    /// is rejected with [`RecordTooLarge`](WalError::RecordTooLarge) (no silent
    /// overrun, no poison) rather than written past the pre-allocated region.
    pub fn commit(&mut self) -> Result<Lsn> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }
        if self.staging.is_empty() {
            return Ok(self.durable_lsn);
        }

        let end = self.write_offset + self.staging.len() as u64;
        if end > self.segment_size {
            // M4 replaces this with the commit-time whole-record split (§7.3);
            // until then a batch must fit the active segment. Reject rather than
            // let `write_all_at` overrun the pre-allocated region (which would
            // silently produce a record straddling the segment boundary,
            // violating §5.3). A precondition reject, not a durability failure,
            // so the handle is not poisoned and `staging`/`last_lsn` are intact.
            return Err(WalError::RecordTooLarge);
        }

        if let Err(e) = self.active.write_all_at(&self.staging, self.write_offset) {
            self.poisoned = true;
            return Err(WalError::Io(e));
        }
        if segment::sync_data_fully(&self.active).is_err() {
            self.poisoned = true;
            return Err(WalError::FsyncFailed);
        }

        self.write_offset = end;
        self.durable_lsn = self.last_lsn;
        self.staging.clear();
        self.observer.on_durable(self.durable_lsn);
        Ok(self.durable_lsn)
    }

    /// Highest durable LSN (§6).
    #[must_use]
    pub fn durable_lsn(&self) -> Lsn {
        self.durable_lsn
    }

    /// Highest assigned LSN, durable or still buffered (§6).
    #[must_use]
    pub fn last_lsn(&self) -> Lsn {
        self.last_lsn
    }

    /// A streaming replay [`Reader`] starting at `from` (§6).
    ///
    /// `from == Lsn(0)` means "from the beginning". A `from` below the oldest
    /// available LSN is a fatal gap (§15.4) — the needed records were
    /// checkpointed away; never a silent skip. (Dormant in M2, where
    /// `oldest_lsn == 1`.)
    pub fn reader_from(&self, from: Lsn) -> Result<Reader<'_>> {
        if from.0 != 0 && from < self.oldest_lsn {
            return Err(WalError::ContiguityViolation);
        }
        let effective_from = if from.0 == 0 { Lsn::FIRST } else { from };
        Ok(Reader::new(
            &self.active,
            self.active_base,
            effective_from,
            self.segment_size,
            self.max_record_size,
        ))
    }
}

/// `fsync` a directory so a newly-created filename within it is durable (§7.4).
fn fsync_dir(dir: &Path) -> Result<()> {
    let dir_file = File::open(dir)?;
    rustix::fs::fsync(&dir_file).map_err(io::Error::from)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> WalConfig {
        // Small but single-segment: holds the modest batches these tests use.
        WalConfig {
            segment_size: 64 * 1024,
            max_record_size: 4096,
        }
    }

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn open_rejects_config_violating_section_5_3() {
        let dir = tmp();
        let bad = WalConfig {
            segment_size: 100,
            max_record_size: 100, // 100 + 91 > 100
        };
        assert!(matches!(
            Wal::open(dir.path(), bad),
            Err(WalError::InvalidConfig)
        ));

        // Degenerate sub-91 segments: the subtractive `segment_size - 91` form
        // would underflow/wrap and bypass the check entirely; the additive form
        // (`max_record_size + 91 > segment_size`) must still reject them.
        for tiny in [
            WalConfig {
                segment_size: 90,
                max_record_size: 0,
            },
            WalConfig {
                segment_size: 0,
                max_record_size: 0,
            },
        ] {
            assert!(matches!(
                Wal::open(dir.path(), tiny),
                Err(WalError::InvalidConfig)
            ));
        }
    }

    #[test]
    fn cold_start_creates_first_segment() {
        let dir = tmp();
        let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(report.oldest_lsn, Lsn(1));
        assert_eq!(report.durable_lsn, Lsn(0));
        assert_eq!(report.tail_state, TailState::Clean);
        assert_eq!(wal.last_lsn(), Lsn(0));
        assert!(dir.path().join("00000000000000000001.wal").exists());
    }

    #[test]
    fn lsn_assignment_is_monotone_dense_from_one() {
        let dir = tmp();
        let (mut wal, _) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(wal.append(b"a").unwrap(), Lsn(1));
        assert_eq!(wal.append(b"b").unwrap(), Lsn(2));
        assert_eq!(wal.append(b"c").unwrap(), Lsn(3));
        assert_eq!(wal.last_lsn(), Lsn(3));
        assert_eq!(wal.durable_lsn(), Lsn(0)); // not yet committed
    }

    #[test]
    fn append_rejects_oversized_payload() {
        let dir = tmp();
        let (mut wal, _) = Wal::open(dir.path(), cfg()).unwrap();
        let too_big = vec![0u8; cfg().max_record_size as usize + 1];
        assert!(matches!(
            wal.append(&too_big),
            Err(WalError::RecordTooLarge)
        ));
        // A max-sized payload is accepted.
        let max = vec![0u8; cfg().max_record_size as usize];
        assert!(wal.append(&max).is_ok());
    }

    #[test]
    fn max_sized_record_fits_segment_and_round_trips() {
        // §14.1: a max-sized record (max = segment - 91) fits and round-trips.
        let dir = tmp();
        let c = WalConfig {
            segment_size: 8 * 1024,
            max_record_size: 8 * 1024 - 91,
        };
        let (mut wal, _) = Wal::open(dir.path(), c).unwrap();
        let payload = vec![0xABu8; c.max_record_size as usize];
        wal.append(&payload).unwrap();
        assert_eq!(wal.commit().unwrap(), Lsn(1));

        let mut reader = wal.reader_from(Lsn(1)).unwrap();
        let (lsn, got) = reader.next().unwrap().unwrap();
        assert_eq!(lsn, Lsn(1));
        assert_eq!(got, &payload[..]);
        assert!(reader.next().is_none());
    }

    #[test]
    fn commit_then_read_back() {
        let dir = tmp();
        let (mut wal, _) = Wal::open(dir.path(), cfg()).unwrap();
        wal.append(b"hello").unwrap();
        wal.append(b"world").unwrap();
        assert_eq!(wal.commit().unwrap(), Lsn(2));
        assert_eq!(wal.durable_lsn(), Lsn(2));

        let mut reader = wal.reader_from(Lsn(1)).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), (Lsn(1), &b"hello"[..]));
        assert_eq!(reader.next().unwrap().unwrap(), (Lsn(2), &b"world"[..]));
        assert!(reader.next().is_none());
    }

    #[test]
    fn empty_commit_is_a_noop() {
        let dir = tmp();
        let (mut wal, _) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(wal.commit().unwrap(), Lsn(0));
    }

    #[test]
    fn reopen_recovers_committed_records() {
        let dir = tmp();
        {
            let (mut wal, _) = Wal::open(dir.path(), cfg()).unwrap();
            wal.append(b"one").unwrap();
            wal.append(b"two").unwrap();
            wal.commit().unwrap();
        }
        let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(2));
        assert_eq!(report.oldest_lsn, Lsn(1));
        assert_eq!(wal.last_lsn(), Lsn(2));

        let mut reader = wal.reader_from(Lsn(1)).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), (Lsn(1), &b"one"[..]));
        assert_eq!(reader.next().unwrap().unwrap(), (Lsn(2), &b"two"[..]));
        assert!(reader.next().is_none());
    }

    #[test]
    fn append_after_reopen_resumes_write_offset() {
        // Exercises the resume `write_offset` that reopen_single accumulates:
        // writing after a reopen must neither overwrite the last record nor
        // leave a hole. Replay across the boundary must be dense.
        let dir = tmp();
        {
            let (mut wal, _) = Wal::open(dir.path(), cfg()).unwrap();
            wal.append(b"a").unwrap();
            wal.append(b"b").unwrap();
            wal.append(b"c").unwrap();
            wal.commit().unwrap();
        }
        {
            let (mut wal, report) = Wal::open(dir.path(), cfg()).unwrap();
            assert_eq!(report.durable_lsn, Lsn(3));
            assert_eq!(wal.append(b"d").unwrap(), Lsn(4));
            assert_eq!(wal.append(b"e").unwrap(), Lsn(5));
            assert_eq!(wal.commit().unwrap(), Lsn(5));
        }
        let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(5));
        let mut reader = wal.reader_from(Lsn(0)).unwrap();
        let expected: [&[u8]; 5] = [b"a", b"b", b"c", b"d", b"e"];
        for (i, want) in expected.iter().enumerate() {
            let (lsn, got) = reader.next().unwrap().unwrap();
            assert_eq!(lsn, Lsn(i as u64 + 1));
            assert_eq!(got, *want);
        }
        assert!(reader.next().is_none());
    }

    #[test]
    fn commit_batch_overrunning_segment_is_rejected() {
        // A multi-record batch larger than the segment must hard-fail (not
        // silently overrun the pre-allocated region) until the M4 split lands.
        let dir = tmp();
        let c = WalConfig {
            segment_size: 512,
            max_record_size: 256,
        };
        let (mut wal, _) = Wal::open(dir.path(), c).unwrap();
        // Each framed record is 20 + 200 + pad(4) = 224 bytes; the header takes
        // 64, so two fit (64 + 448 ≤ 512) but three (64 + 672) do not.
        let payload = vec![0u8; 200];
        for _ in 0..3 {
            wal.append(&payload).unwrap();
        }
        assert!(matches!(wal.commit(), Err(WalError::RecordTooLarge)));
        // The reject is a precondition failure, not a durability one: it must
        // NOT poison. A poisoned handle would return `Poisoned` here.
        assert!(matches!(wal.commit(), Err(WalError::RecordTooLarge)));
        assert!(wal.append(&payload).is_ok());
    }

    #[test]
    fn second_open_is_locked() {
        let dir = tmp();
        let (_held, _) = Wal::open(dir.path(), cfg()).unwrap();
        assert!(matches!(
            Wal::open(dir.path(), cfg()),
            Err(WalError::Locked)
        ));
    }

    #[test]
    fn reader_from_skips_earlier_records() {
        let dir = tmp();
        let (mut wal, _) = Wal::open(dir.path(), cfg()).unwrap();
        for i in 1..=5u8 {
            wal.append(&[i]).unwrap();
        }
        wal.commit().unwrap();
        let mut reader = wal.reader_from(Lsn(3)).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), (Lsn(3), &[3u8][..]));
        assert_eq!(reader.next().unwrap().unwrap(), (Lsn(4), &[4u8][..]));
        assert_eq!(reader.next().unwrap().unwrap(), (Lsn(5), &[5u8][..]));
        assert!(reader.next().is_none());
    }
}
