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
use std::path::{Path, PathBuf};

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
    /// The WAL directory, retained so a [`Reader`] can open sealed segments by
    /// path and `commit` can create new ones on roll.
    dir: PathBuf,
    /// Sorted (ascending) `base_lsn`s of all live segments, oldest first; the
    /// last is the active segment. Updated on every roll. Lets a [`Reader`]
    /// replay across segments (§8.1).
    segments: Vec<Lsn>,
    /// The active (highest-`base_lsn`) segment. Its base is `*segments.last()`.
    active: File,
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

/// The recovered (or cold-started) writer state, produced by
/// [`Wal::cold_start`]/[`Wal::recover_all`] and consumed by [`Wal::open_with`].
struct Recovered {
    /// The active (highest-`base_lsn`) segment, open read/write for appends.
    active: File,
    /// Offset of the next byte to write in the active segment.
    write_offset: u64,
    /// Highest durable LSN (active segment's `base_lsn - 1` for an empty active
    /// segment).
    last_lsn: Lsn,
    /// `P`: base LSN of the oldest surviving segment.
    oldest_lsn: Lsn,
    /// All live segments' bases, sorted ascending (last is the active segment).
    segments: Vec<Lsn>,
    /// Tail state of the active segment after recovery.
    tail_state: TailState,
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
    /// Runs full recovery (§8): it cold-starts an empty directory (creating
    /// `…0001.wal`) or discovers every `*.wal` segment, sorts them by `base_lsn`,
    /// validates each header, discards an incomplete-header highest-base file left
    /// by a crash mid-create (§8.4), recovers each segment's record run (§8.2 —
    /// torn-tail truncation + durable zeroing on the active segment, fatal mid-log
    /// corruption), and verifies cross-segment LSN continuity (§8.1).
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

        // §8.4: a highest-base file with an incomplete/absent header (crash mid
        // segment-create, before any record) is discarded; the prior segment
        // becomes active. May empty `bases` (a crashed cold start) ⇒ cold start.
        Self::discard_incomplete_highest(dir, &mut bases, config)?;

        let rec = if bases.is_empty() {
            Self::cold_start(dir, config.segment_size)?
        } else {
            Self::recover_all(dir, &bases, config)?
        };

        let durable_lsn = rec.last_lsn;
        let report = RecoveryReport {
            oldest_lsn: rec.oldest_lsn,
            durable_lsn,
            tail_state: rec.tail_state,
            segments_scanned: rec.segments.len(),
        };

        let wal = Wal {
            _lock: lock,
            dir: dir.to_path_buf(),
            segments: rec.segments,
            active: rec.active,
            write_offset: rec.write_offset,
            oldest_lsn: rec.oldest_lsn,
            last_lsn: rec.last_lsn,
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
    fn cold_start(dir: &Path, segment_size: u64) -> Result<Recovered> {
        let active = segment::create(dir, Lsn::FIRST, segment_size)?;
        fsync_dir(dir)?;
        // base 1, empty: write offset just past the header, durable_lsn = 0.
        Ok(Recovered {
            active,
            write_offset: HEADER_SIZE,
            last_lsn: Lsn::NONE,
            oldest_lsn: Lsn::FIRST,
            segments: vec![Lsn::FIRST],
            tail_state: TailState::Clean,
        })
    }

    /// §8.4 discard of the incomplete-header highest-base file. If the
    /// highest-base segment's header does **not** validate, it is either a crash
    /// mid `segment::create` (header never fully written/synced — and since the
    /// header is synced *before* any record, such a file holds **no durable
    /// records**, so discarding it loses nothing) or media corruption of a
    /// *populated* active segment (fatal, §14.4e). The two are distinguished by
    /// whether a valid record exists at the first record slot:
    /// - **no record** ⇒ incomplete create ⇒ unlink it + dir-fsync, drop it from
    ///   `bases` (the prior segment becomes active; an emptied `bases` ⇒ cold
    ///   start);
    /// - **a record present** ⇒ a real segment with a corrupt header ⇒ fatal
    ///   [`BadSegmentHeader`].
    ///
    /// A bad header on a *non-highest* (sealed) segment is always fatal (§8.1
    /// step 2) and is handled later in [`recover_all`], not here.
    fn discard_incomplete_highest(
        dir: &Path,
        bases: &mut Vec<u64>,
        config: WalConfig,
    ) -> Result<()> {
        let Some(&highest) = bases.last() else {
            return Ok(());
        };
        let base = Lsn(highest);
        let path = dir.join(segment::filename_for(base));
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let mut header = [0u8; HEADER_SIZE as usize];
        match file.read_exact_at(&mut header, 0) {
            Ok(()) => {
                // Header valid + matching its filename ⇒ a legitimate active
                // segment (possibly empty); leave it for `recover_all`. (Written
                // as `matches!` with a guard rather than a let-chain to stay on
                // the 1.85 MSRV.)
                if matches!(segment::decode_header(&header), Ok(parsed) if parsed.base_lsn == base)
                {
                    return Ok(());
                }
                // A full 64-byte header that does not validate. Discard iff the
                // file holds no record (a `fallocate`d but not-yet-header-written
                // create); a populated segment with a corrupt header is fatal
                // (§14.4e segment-header corruption ⇒ FATAL), not a discard.
                let mut buf = Vec::new();
                let first = segment::read_record_at(
                    &file,
                    HEADER_SIZE,
                    config.segment_size,
                    config.max_record_size,
                    &mut buf,
                )?;
                if matches!(first, segment::ScanOutcome::Record { .. }) {
                    return Err(WalError::BadSegmentHeader);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Fewer than 64 bytes on disk. `segment::create` leaves the file
                // at size 0 (after `create_new`, before `fallocate`) and only ever
                // jumps to `segment_size` thereafter — so size 0 is a clean
                // incomplete create (discard), while 1..63 bytes can only be a
                // real segment physically truncated into its header (§14.4f),
                // which is fatal `BadSegmentHeader` (the records below 64 are
                // gone, so this is not a recoverable torn tail).
                if file.metadata()?.len() != 0 {
                    return Err(WalError::BadSegmentHeader);
                }
            }
            Err(e) => return Err(WalError::Io(e)),
        }

        // Incomplete create: unlink and make the unlink durable (dir-fsync), then
        // drop it so the prior segment (if any) is treated as active.
        drop(file);
        std::fs::remove_file(&path)?;
        fsync_dir(dir)?;
        bases.pop();
        Ok(())
    }

    /// Multi-segment recovery (§8.1): validate each header, recover each segment's
    /// record run (§8.2 — only the highest-base segment is `is_active`, so only it
    /// may carry a torn tail), and verify cross-segment LSN continuity. `bases` is
    /// non-empty and sorted ascending; its last element is the active segment.
    fn recover_all(dir: &Path, bases: &[u64], config: WalConfig) -> Result<Recovered> {
        let oldest_lsn = Lsn(bases[0]);
        let last_idx = bases.len() - 1;

        let mut active: Option<File> = None;
        let mut write_offset = HEADER_SIZE;
        let mut last_lsn = Lsn::NONE;
        let mut tail_state = TailState::Clean;
        // Max LSN of the previous (lower-base) segment, for the continuity check.
        // `None` until the first non-empty segment is seen.
        let mut prev_max: Option<Lsn> = None;

        for (i, &base_u64) in bases.iter().enumerate() {
            let base = Lsn(base_u64);
            let is_active = i == last_idx;

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(dir.join(segment::filename_for(base)))?;

            // Validate the header and confirm it matches its filename. A bad
            // header is fatal (§8.1 step 2): the header is written and synced at
            // creation, before any record, so it is never a torn tail. A header
            // physically truncated below 64 bytes (§14.4f) maps to
            // `BadSegmentHeader`, not a raw `UnexpectedEof`, keeping recovery
            // total (D11). (The highest-base incomplete-header case was already
            // discarded in `discard_incomplete_highest`.)
            let mut header = [0u8; HEADER_SIZE as usize];
            match file.read_exact_at(&mut header, 0) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Err(WalError::BadSegmentHeader);
                }
                Err(e) => return Err(WalError::Io(e)),
            }
            let parsed = segment::decode_header(&header)?;
            if parsed.base_lsn != base {
                return Err(WalError::BadSegmentHeader);
            }

            // Cross-segment continuity (§8.1 step 3): this segment's base must
            // immediately follow the previous non-empty segment's max LSN.
            // (`is_some_and` rather than a let-chain to stay on the 1.85 MSRV.)
            if prev_max.is_some_and(|pm| pm.next() != base) {
                return Err(WalError::ContiguityViolation);
            }

            let seg = recovery::recover_segment(
                &file,
                base,
                is_active,
                config.segment_size,
                config.max_record_size,
            )?;

            if is_active {
                active = Some(file);
                write_offset = seg.write_offset;
                last_lsn = seg.max_lsn;
                tail_state = seg.tail_state;
            } else {
                // A sealed segment must contain ≥1 record: a roll only ever
                // occurs to write records that did not fit, so an empty sealed
                // segment is an internal gap (§8.1 step 3, fatal).
                if seg.max_lsn < base {
                    return Err(WalError::ContiguityViolation);
                }
                prev_max = Some(seg.max_lsn);
            }
        }

        Ok(Recovered {
            active: active.expect("bases is non-empty ⇒ an active segment was set"),
            write_offset,
            last_lsn,
            oldest_lsn,
            segments: bases.iter().map(|&b| Lsn(b)).collect(),
            tail_state,
        })
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

    /// Make all buffered records durable (§7.2), splitting across segments on
    /// whole-record boundaries when the batch does not fit the active segment
    /// (§7.3). Each segment touched gets its own `write` + `fdatasync`
    /// (`F_FULLFSYNC` on macOS) and advances `durable_lsn` to that segment's last
    /// LSN; the observer fires after each advance (§15.3).
    ///
    /// **`commit` is not atomic** (§4.1): on a multi-segment split a crash or an
    /// I/O failure between two segments' syncs leaves the first segment durable
    /// and the rest lost — a valid dense suffix (D9), never an internal gap. On
    /// any `write`/`fdatasync`/roll failure the handle is **poisoned** (§12);
    /// `durable_lsn` keeps whatever earlier segments achieved (monotonic, never
    /// regresses).
    pub fn commit(&mut self) -> Result<Lsn> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }
        if self.staging.is_empty() {
            return Ok(self.durable_lsn);
        }

        let total = self.staging.len();
        let mut pos = 0usize;

        // Commit-time whole-record split (§7.3). Loop until the staging buffer is
        // fully written; "seal + roll" counts as progress, so an empty prefix
        // cannot spin (termination guaranteed by the §5.3 `max_record_size` bound,
        // which makes any single record fit a fresh segment).
        while pos < total {
            // Step 1: the prefix of whole records that fits the active segment's
            // remaining space. MAY be empty (the next record does not fit).
            let remaining = self.segment_size - self.write_offset;
            let mut scan = pos;
            let mut prefix_last_lsn = Lsn::NONE;
            while scan < total {
                let (lsn, framed) = crate::record::peek(&self.staging[scan..]);
                if (scan - pos) as u64 + framed as u64 > remaining {
                    break;
                }
                prefix_last_lsn = lsn;
                scan += framed;
            }

            // Step 2: write + sync the non-empty prefix, advancing durable_lsn for
            // this segment. A single `write(2)` per segment at the tracked offset.
            if scan > pos {
                if let Err(e) = self
                    .active
                    .write_all_at(&self.staging[pos..scan], self.write_offset)
                {
                    self.poisoned = true;
                    return Err(WalError::Io(e));
                }
                if segment::sync_data_fully(&self.active).is_err() {
                    self.poisoned = true;
                    return Err(WalError::FsyncFailed);
                }
                self.write_offset += (scan - pos) as u64;
                self.durable_lsn = prefix_last_lsn;
                self.observer.on_durable(self.durable_lsn);
                pos = scan;
            }

            // Step 3: records remain ⇒ seal the active segment (its remaining
            // bytes are the pre-allocated zero sentinel — no write needed, and it
            // is never touched again, D12) and roll to a new segment based at the
            // first not-yet-written record's LSN.
            if pos < total {
                let (new_base, _) = crate::record::peek(&self.staging[pos..]);
                self.roll(new_base)?;
            }
        }

        self.staging.clear();
        Ok(self.durable_lsn)
    }

    /// Seal the active segment and roll to a fresh one based at `new_base` (§7.4).
    /// Creates + pre-allocates + header-writes + `fdatasync`s the new file, then
    /// `fsync`s the directory so the new filename is durable (the §14.4d gotcha).
    /// The just-sealed segment is immutable from here on (D12) — only checkpoint
    /// (M5) deletes it. Poisons the handle on any failure (§12).
    fn roll(&mut self, new_base: Lsn) -> Result<()> {
        let new = match segment::create(&self.dir, new_base, self.segment_size) {
            Ok(f) => f,
            Err(e) => {
                self.poisoned = true;
                return Err(e);
            }
        };
        // Make the new filename durable. The `inject_no_dir_fsync` feature is the
        // §14.4d negative control: a deliberately-buggy build that omits this
        // dir-fsync MUST fail recovery under a LazyFS `clear-cache` after a roll
        // (the rolled segment's filename was never made durable, so the
        // post-roll records vanish). It is a test-only feature — never enable it
        // in a real build.
        #[cfg(not(feature = "inject_no_dir_fsync"))]
        if let Err(e) = fsync_dir(&self.dir) {
            self.poisoned = true;
            return Err(e);
        }
        self.active = new;
        self.write_offset = HEADER_SIZE;
        self.segments.push(new_base);
        Ok(())
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
        // Open the oldest segment for the reader. (Opening it here, before any
        // measured `Reader::next`, keeps the single-segment read hot path
        // zero-alloc; crossing a boundary later opens the next file lazily.)
        let first = File::open(self.dir.join(segment::filename_for(self.segments[0])))?;
        Ok(Reader::new(
            &self.dir,
            &self.segments,
            first,
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
    use crate::record;

    fn cfg() -> WalConfig {
        // Small but single-segment: holds the modest batches these tests use.
        WalConfig {
            segment_size: 64 * 1024,
            max_record_size: 4096,
        }
    }

    /// A tiny segment that forces rolls and commit-time splits: 512-byte
    /// segments (448 usable after the 64-byte header), 256-byte max record.
    fn tiny_cfg() -> WalConfig {
        WalConfig {
            segment_size: 512,
            max_record_size: 256,
        }
    }

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    /// Fabricate a segment at `base` holding `payloads` as dense records starting
    /// at `base` (empty ⇒ a header-only segment), bypassing `Wal` so multi-segment
    /// layouts can be built for recovery tests. Uses `cfg()`'s `segment_size`.
    fn fab_segment(dir: &Path, base: Lsn, payloads: &[&[u8]]) {
        let f = segment::create(dir, base, cfg().segment_size).unwrap();
        let mut offset = HEADER_SIZE;
        let mut lsn = base;
        let mut buf = Vec::new();
        for p in payloads {
            buf.clear();
            let framed = record::encode_into(&mut buf, lsn, p);
            f.write_all_at(&buf, offset).unwrap();
            offset += framed as u64;
            lsn = lsn.next();
        }
        f.sync_data().unwrap();
    }

    /// Clobber the first 8 header bytes (the magic) of the segment at `base`,
    /// durably — simulating a torn/incomplete create or header corruption.
    fn clobber_header(dir: &Path, base: Lsn) {
        let f = OpenOptions::new()
            .write(true)
            .open(dir.join(segment::filename_for(base)))
            .unwrap();
        f.write_all_at(&[0xFFu8; 8], 0).unwrap();
        f.sync_data().unwrap();
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
    fn commit_splits_batch_across_segments_on_whole_records() {
        // §7.3 / P4: a batch larger than the active segment splits on whole-record
        // boundaries; `durable_lsn` advances per segment; replay reconstructs the
        // dense sequence with no record spanning a boundary (D2/D6). Each framed
        // record is 20 + 200 + pad(4) = 224 bytes, so two fit per 512-byte segment
        // (64 + 448) and the third forces a roll.
        let dir = tmp();
        let (mut wal, _) = Wal::open(dir.path(), tiny_cfg()).unwrap();
        let payload = vec![0xCDu8; 200];
        for _ in 0..5 {
            wal.append(&payload).unwrap();
        }
        assert_eq!(wal.commit().unwrap(), Lsn(5));
        assert_eq!(wal.durable_lsn(), Lsn(5));
        // 5 records, 2 per segment ⇒ segments based at 1, 3, 5.
        assert_eq!(wal.segments, vec![Lsn(1), Lsn(3), Lsn(5)]);
        for b in [1u64, 3, 5] {
            assert!(
                dir.path().join(segment::filename_for(Lsn(b))).exists(),
                "segment {b} should exist"
            );
        }
        let mut r = wal.reader_from(Lsn(0)).unwrap();
        for i in 1..=5 {
            let (lsn, got) = r.next().unwrap().unwrap();
            assert_eq!(lsn, Lsn(i));
            assert_eq!(got, &payload[..]);
        }
        assert!(r.next().is_none());
    }

    #[test]
    fn commit_split_advances_durable_lsn_per_segment() {
        // §7.2/§15.3: the observer fires once per synced segment, with a strictly
        // monotone watermark — the achieved durable LSN of each segment in turn.
        use crate::observer::DurabilityObserver;
        struct Rec(std::rc::Rc<std::cell::RefCell<Vec<u64>>>);
        impl DurabilityObserver for Rec {
            fn on_durable(&mut self, lsn: Lsn) {
                self.0.borrow_mut().push(lsn.0);
            }
        }
        let dir = tmp();
        let seen = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let (mut wal, _) = Wal::open_with(dir.path(), tiny_cfg(), Rec(seen.clone())).unwrap();
        for _ in 0..5 {
            wal.append(&[0u8; 200]).unwrap();
        }
        assert_eq!(wal.commit().unwrap(), Lsn(5));
        // Three segments synced in order ⇒ watermarks 2, 4, 5 (monotone).
        assert_eq!(*seen.borrow(), vec![2, 4, 5]);
    }

    #[test]
    fn commit_handles_empty_prefix_seal_and_roll() {
        // §7.3 empty-prefix: when the active segment's remaining space cannot hold
        // even the next whole record, seal it as-is (its pre-allocated zeros are
        // the §5.4 sentinel) and roll — counts as progress, no spin, no split.
        let dir = tmp();
        let (mut wal, _) = Wal::open(dir.path(), tiny_cfg()).unwrap();
        // First commit: one 200-byte record (framed 224) ⇒ write_offset = 288.
        wal.append(&[0u8; 200]).unwrap();
        assert_eq!(wal.commit().unwrap(), Lsn(1));
        // Remaining = 512 - 288 = 224. A 256-byte record frames to 280 > 224, so
        // the prefix is empty ⇒ seal + roll; it then fits the fresh segment.
        wal.append(&[1u8; 256]).unwrap();
        assert_eq!(wal.commit().unwrap(), Lsn(2));
        assert_eq!(wal.segments, vec![Lsn(1), Lsn(2)]);
        // r2 decodes whole from the new segment.
        let mut r = wal.reader_from(Lsn(2)).unwrap();
        let (lsn, got) = r.next().unwrap().unwrap();
        assert_eq!(lsn, Lsn(2));
        assert_eq!(got.len(), 256);
        assert!(r.next().is_none());
    }

    #[test]
    fn append_after_reopen_resumes_across_a_roll() {
        // A reopened multi-segment log keeps appending into the active segment and
        // rolls again as needed; replay stays dense across close/reopen (D2/D6).
        let dir = tmp();
        let payload = vec![7u8; 200];
        {
            let (mut wal, _) = Wal::open(dir.path(), tiny_cfg()).unwrap();
            for _ in 0..3 {
                wal.append(&payload).unwrap();
            }
            assert_eq!(wal.commit().unwrap(), Lsn(3)); // segs 1, 3
        }
        {
            let (mut wal, report) = Wal::open(dir.path(), tiny_cfg()).unwrap();
            assert_eq!(report.durable_lsn, Lsn(3));
            for _ in 0..2 {
                wal.append(&payload).unwrap();
            }
            assert_eq!(wal.commit().unwrap(), Lsn(5)); // rolls to seg 5
        }
        let (wal, report) = Wal::open(dir.path(), tiny_cfg()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(5));
        assert_eq!(wal.segments, vec![Lsn(1), Lsn(3), Lsn(5)]);
        let mut r = wal.reader_from(Lsn(0)).unwrap();
        for i in 1..=5 {
            assert_eq!(r.next().unwrap().unwrap().0, Lsn(i));
        }
        assert!(r.next().is_none());
    }

    #[test]
    fn recover_multi_segment_clean_roundtrip() {
        // §8.1: a fabricated 2-segment log reopens, validates continuity, and
        // replays the dense sequence across the boundary (D2/D6).
        let dir = tmp();
        fab_segment(dir.path(), Lsn(1), &[b"a", b"b"]); // lsn 1,2
        fab_segment(dir.path(), Lsn(3), &[b"c"]); // lsn 3
        let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(report.oldest_lsn, Lsn(1));
        assert_eq!(report.durable_lsn, Lsn(3));
        assert_eq!(report.segments_scanned, 2);
        let mut r = wal.reader_from(Lsn(0)).unwrap();
        assert_eq!(r.next().unwrap().unwrap(), (Lsn(1), &b"a"[..]));
        assert_eq!(r.next().unwrap().unwrap(), (Lsn(2), &b"b"[..]));
        assert_eq!(r.next().unwrap().unwrap(), (Lsn(3), &b"c"[..]));
        assert!(r.next().is_none());
    }

    #[test]
    fn recover_is_idempotent_across_repeated_open() {
        // D7: reopening a multi-segment log repeatedly is stable.
        let dir = tmp();
        fab_segment(dir.path(), Lsn(1), &[b"a", b"b"]);
        fab_segment(dir.path(), Lsn(3), &[b"c", b"d"]);
        for _ in 0..3 {
            let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
            assert_eq!(report.oldest_lsn, Lsn(1));
            assert_eq!(report.durable_lsn, Lsn(4));
            assert_eq!(wal.segments, vec![Lsn(1), Lsn(3)]);
        }
    }

    #[test]
    fn recover_cross_segment_gap_is_contiguity_violation() {
        // §8.1 step 3 (D2): a gap between a sealed segment's max LSN and the next
        // segment's base is fatal, never a silent internal gap.
        let dir = tmp();
        fab_segment(dir.path(), Lsn(1), &[b"a", b"b"]); // max 2
        fab_segment(dir.path(), Lsn(5), &[b"c"]); // base 5 ≠ 3
        assert!(matches!(
            Wal::open(dir.path(), cfg()),
            Err(WalError::ContiguityViolation)
        ));
    }

    #[test]
    fn recover_empty_sealed_segment_is_contiguity_violation() {
        // §8.1 step 3: a sealed (non-highest) segment must hold ≥1 record; an
        // empty one is a fatal internal gap.
        let dir = tmp();
        fab_segment(dir.path(), Lsn(1), &[b"a"]); // max 1
        fab_segment(dir.path(), Lsn(2), &[]); // empty SEALED
        fab_segment(dir.path(), Lsn(3), &[b"c"]); // highest ⇒ seg 2 is sealed
        assert!(matches!(
            Wal::open(dir.path(), cfg()),
            Err(WalError::ContiguityViolation)
        ));
    }

    #[test]
    fn recover_empty_active_segment_yields_base_minus_one() {
        // §8.4: an empty active segment (crash right after a roll) is valid;
        // durable_lsn = base − 1 (the prior segment's max).
        let dir = tmp();
        fab_segment(dir.path(), Lsn(1), &[b"a", b"b"]); // max 2
        fab_segment(dir.path(), Lsn(3), &[]); // empty ACTIVE
        let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(report.oldest_lsn, Lsn(1));
        assert_eq!(report.durable_lsn, Lsn(2)); // base 3 − 1
        assert_eq!(report.tail_state, TailState::Clean);
        assert_eq!(wal.segments, vec![Lsn(1), Lsn(3)]);
        // Replay returns only the prior segment's records.
        let mut r = wal.reader_from(Lsn(0)).unwrap();
        assert_eq!(r.next().unwrap().unwrap().0, Lsn(1));
        assert_eq!(r.next().unwrap().unwrap().0, Lsn(2));
        assert!(r.next().is_none());
    }

    #[test]
    fn recover_discards_incomplete_highest_base_file() {
        // §8.4: a highest-base file with a corrupt/incomplete header and NO
        // records (crash mid segment-create) is discarded; the prior segment
        // becomes active.
        let dir = tmp();
        fab_segment(dir.path(), Lsn(1), &[b"a", b"b"]);
        // A freshly-created seg 3 (pre-allocated zeros, no record) with a trashed
        // header — the torn-create signature.
        segment::create(dir.path(), Lsn(3), cfg().segment_size).unwrap();
        clobber_header(dir.path(), Lsn(3));
        let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(2));
        assert_eq!(wal.segments, vec![Lsn(1)]);
        assert!(
            !dir.path().join(segment::filename_for(Lsn(3))).exists(),
            "the incomplete highest-base file must be unlinked"
        );
    }

    #[test]
    fn recover_incomplete_sole_segment_cold_starts() {
        // §8.4: a crashed cold start (sole base-1 file, incomplete header, no
        // records) is discarded and recovery cold-starts a fresh empty log.
        let dir = tmp();
        segment::create(dir.path(), Lsn(1), cfg().segment_size).unwrap();
        clobber_header(dir.path(), Lsn(1));
        let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(report.oldest_lsn, Lsn(1));
        assert_eq!(report.durable_lsn, Lsn(0));
        assert_eq!(wal.segments, vec![Lsn(1)]);
    }

    #[test]
    fn recover_corrupt_header_on_populated_highest_is_fatal() {
        // §14.4e: a populated highest segment with a corrupt header is NOT a torn
        // create — it is fatal corruption, not a discard.
        let dir = tmp();
        fab_segment(dir.path(), Lsn(1), &[b"a", b"b"]);
        fab_segment(dir.path(), Lsn(3), &[b"c"]); // has a real record
        clobber_header(dir.path(), Lsn(3));
        assert!(matches!(
            Wal::open(dir.path(), cfg()),
            Err(WalError::BadSegmentHeader)
        ));
    }

    #[test]
    fn recover_corrupt_header_on_sealed_segment_is_fatal() {
        // §8.1 step 2: a bad header on a sealed (non-highest) segment is always
        // fatal — no discard path.
        let dir = tmp();
        fab_segment(dir.path(), Lsn(1), &[b"a", b"b"]);
        fab_segment(dir.path(), Lsn(3), &[b"c"]);
        clobber_header(dir.path(), Lsn(1));
        assert!(matches!(
            Wal::open(dir.path(), cfg()),
            Err(WalError::BadSegmentHeader)
        ));
    }

    #[test]
    fn recover_missing_prefix_is_accepted_silently() {
        // §4 D2 / §8.1: a checkpointed-away prefix (P > 1) opens fine; oldest_lsn
        // is the lowest surviving base, and a reader from below it is a fatal gap.
        let dir = tmp();
        fab_segment(dir.path(), Lsn(5), &[b"e", b"f"]);
        let (wal, report) = Wal::open(dir.path(), cfg()).unwrap();
        assert_eq!(report.oldest_lsn, Lsn(5));
        assert_eq!(report.durable_lsn, Lsn(6));
        assert!(matches!(
            wal.reader_from(Lsn(4)),
            Err(WalError::ContiguityViolation)
        ));
        let mut r = wal.reader_from(Lsn(5)).unwrap();
        assert_eq!(r.next().unwrap().unwrap(), (Lsn(5), &b"e"[..]));
        assert_eq!(r.next().unwrap().unwrap(), (Lsn(6), &b"f"[..]));
        assert!(r.next().is_none());
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
