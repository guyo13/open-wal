//! M3 LazyFS gate (§14.4b + §14.4g durability-of-zeroing) — the power-loss
//! tests that intra-segment recovery must pass before M3 is done.
//!
//! These require a running [LazyFS](https://github.com/dsrhaslab/lazyfs) mount
//! (FUSE): the WAL writes into the mount, data lives in LazyFS's page cache, and
//! a `lazyfs::clear-cache` FIFO command drops everything that was not
//! `fdatasync`'d — a faithful power-loss. They are therefore `#[ignore]` by
//! default and run only in an environment where FUSE/LazyFS is set up, driven
//! by three env vars:
//!
//! - `LAZYFS_MNT`   — the FUSE mount directory the WAL writes into;
//! - `LAZYFS_FIFO`  — the faults FIFO to issue `clear-cache`;
//! - `LAZYFS_LOG`   — LazyFS's `logfile`, used as a completion barrier (we wait
//!   for a new `cache is cleared` line). This is more robust than the
//!   `fifo_path_completed` FIFO, which LazyFS opens `O_WRONLY` once at startup
//!   and which gates all command processing on a persistent reader.
//!
//! The easiest way to run these is the harness, which builds + mounts LazyFS and
//! runs this suite for you (see `scripts/README.md`):
//! ```text
//! scripts/lazyfs-gate.sh deps   # once: install fuse3 + build deps (apt)
//! scripts/lazyfs-gate.sh all    # build + mount + run + always unmount
//! ```
//! Or, against an already-running mount, set the three env vars and run directly
//! (single-threaded — `clear-cache` is global to the mount, so parallel tests
//! would corrupt each other):
//! ```text
//! LAZYFS_MNT=… LAZYFS_FIFO=… LAZYFS_LOG=… \
//!   cargo test --test lazyfs_gate -- --ignored --test-threads=1
//! ```

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use open_wal::{Lsn, TailState, Wal, WalConfig};

const SEGMENT_SIZE: u64 = 1 << 20; // 1 MiB
const MAX_RECORD_SIZE: u32 = 4096;
const HEADER_SIZE: u64 = 64;

fn config() -> WalConfig {
    WalConfig {
        segment_size: SEGMENT_SIZE,
        max_record_size: MAX_RECORD_SIZE,
    }
}

fn env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set to run the LazyFS gate (see module docs)"))
}

/// A fresh, empty WAL directory inside the LazyFS mount.
fn fresh_wal_dir(tag: &str) -> PathBuf {
    let mnt = env("LAZYFS_MNT");
    let dir = Path::new(&mnt).join(format!("wal-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Count completed `clear-cache` operations LazyFS has logged so far.
fn cleared_count(log: &str) -> usize {
    std::fs::read_to_string(log)
        .map(|s| s.matches("cache is cleared").count())
        .unwrap_or(0)
}

/// Issue a `lazyfs::clear-cache` (power-loss — drop all un-`fdatasync`'d data)
/// and block until LazyFS logs its completion, so the assertions below observe
/// the post-fault state. The write to the faults FIFO self-synchronizes with
/// LazyFS's reader (an `O_WRONLY` FIFO open blocks until the read end is open).
fn clear_cache() {
    let fifo = env("LAZYFS_FIFO");
    let log = env("LAZYFS_LOG");
    let before = cleared_count(&log);

    // Open the faults FIFO **non-blocking**: a blocking `O_WRONLY` open of a FIFO
    // hangs forever if no reader is attached, so a dead LazyFS daemon would stall
    // the test (and the CI runner) indefinitely. `O_NONBLOCK` returns `ENXIO`
    // immediately instead; we retry briefly to absorb a startup race where the
    // daemon's FIFO reader is not yet attached, then fail loudly.
    let start = Instant::now();
    let mut f = loop {
        match OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&fifo)
        {
            Ok(f) => break f,
            Err(e)
                if e.raw_os_error() == Some(libc::ENXIO)
                    && start.elapsed() < Duration::from_secs(5) =>
            {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("cannot open LazyFS faults FIFO {fifo} (daemon down?): {e}"),
        }
    };
    writeln!(f, "lazyfs::clear-cache").unwrap();
    f.flush().unwrap();
    drop(f);

    let barrier = Instant::now();
    while cleared_count(&log) <= before {
        if barrier.elapsed() > Duration::from_secs(30) {
            panic!("clear-cache did not complete within 30s (LAZYFS_LOG barrier)");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn seg_path(dir: &Path) -> PathBuf {
    dir.join("00000000000000000001.wal")
}

fn framed(len: usize) -> u64 {
    let pad = (8 - ((20 + len) % 8)) % 8;
    (20 + len + pad) as u64
}

fn replay(wal: &Wal) -> Vec<Vec<u8>> {
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let mut out = Vec::new();
    let mut expected = 1u64;
    while let Some(item) = r.next() {
        let (lsn, payload) = item.unwrap();
        assert_eq!(lsn, Lsn(expected), "recovered run must be dense from 1");
        out.push(payload.to_vec());
        expected += 1;
    }
    out
}

/// §14.4b (D1/D2/D3): every committed record survives a power-loss `clear-cache`
/// and recovers as a dense, byte-identical suffix.
#[test]
#[ignore = "requires a running LazyFS mount (M3 gate)"]
fn committed_records_survive_power_loss() {
    let dir = fresh_wal_dir("commit");
    let payloads: Vec<Vec<u8>> = (1..=200u32)
        .map(|i| format!("record-number-{i:05}").into_bytes())
        .collect();

    {
        let (mut wal, _) = Wal::open(&dir, config()).unwrap();
        for p in &payloads {
            wal.append(p).unwrap();
        }
        // commit's fdatasync makes all 200 durable in LazyFS's backing store.
        assert_eq!(wal.commit().unwrap(), Lsn(200));
    }

    // Power loss: drop everything not fdatasync'd. The committed records were
    // synced, so none may vanish.
    clear_cache();

    let (wal, report) = Wal::open(&dir, config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(200), "D1: committed records lost");
    let got = replay(&wal);
    assert_eq!(got.len(), payloads.len());
    assert_eq!(got, payloads, "D6: recovered bytes must be identical");
}

/// §14.4b cold-start durability (D9 + the dir-fsync on create): a freshly
/// created, empty segment survives power loss and reopens cleanly.
#[test]
#[ignore = "requires a running LazyFS mount (M3 gate)"]
fn cold_start_segment_survives_power_loss() {
    let dir = fresh_wal_dir("cold");
    {
        let (_wal, report) = Wal::open(&dir, config()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(0));
    }
    clear_cache();
    // The segment file + directory entry were fsync'd at creation, so reopen
    // must succeed (not lose the filename).
    let (_wal, report) = Wal::open(&dir, config()).unwrap();
    assert_eq!(report.oldest_lsn, Lsn(1));
    assert_eq!(report.durable_lsn, Lsn(0));
    assert!(
        seg_path(&dir).exists(),
        "cold-start segment lost on power loss"
    );
}

/// §14.4g durability-of-zeroing (D10): after a torn-tail recovery zeros
/// `[X, EOF)` and fdatasyncs it, a power-loss `clear-cache` issued **before any
/// new write** must leave `[X, EOF)` still reading as zero. A non-durable
/// invalidation (e.g. `PUNCH_HOLE` without `fsync`) would let the stale bytes
/// reappear and fail this assertion.
#[test]
#[ignore = "requires a running LazyFS mount (M3 gate)"]
fn zeroed_tail_survives_power_loss() {
    let dir = fresh_wal_dir("zero");

    // Two records; the second is long so its stale bytes are easy to detect if
    // the zeroing is not durable.
    let r1: &[u8] = b"keep";
    let r2: &[u8] = b"OLD-LONG-RECORD-THAT-MUST-NOT-COME-BACK";
    {
        let (mut wal, _) = Wal::open(&dir, config()).unwrap();
        wal.append(r1).unwrap();
        wal.append(r2).unwrap();
        wal.commit().unwrap();
    }

    // Durably corrupt the last record (fsync the flip) so recovery sees a torn
    // tail it must truncate + zero.
    let x = HEADER_SIZE + framed(r1.len());
    {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(seg_path(&dir))
            .unwrap();
        let mut b = [0u8; 1];
        f.read_at(&mut b, x + 20).unwrap();
        b[0] ^= 0xFF;
        f.write_all_at(&b, x + 20).unwrap();
        f.sync_all().unwrap();
    }

    // Recovery #1: truncate + durably zero [x, EOF) (§8.2.1).
    {
        let (_wal, report) = Wal::open(&dir, config()).unwrap();
        assert_eq!(report.durable_lsn, Lsn(1));
        assert_eq!(
            report.tail_state,
            TailState::TruncatedAt {
                segment_base: Lsn(1),
                offset: x
            }
        );
    }

    // Power loss BEFORE any new write: the zeroing must have been durable.
    clear_cache();

    // Recovery #2: [x, EOF) must still be all zeros — nothing resurrected.
    let (wal, report) = Wal::open(&dir, config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(1));
    let f = std::fs::File::open(seg_path(&dir)).unwrap();
    let mut tail = vec![0xAAu8; (SEGMENT_SIZE - x) as usize];
    f.read_at(&mut tail, x).unwrap();
    assert!(
        tail.iter().all(|&b| b == 0),
        "D10: [X, EOF) must remain zero after power loss (zeroing was not durable)"
    );
    assert_eq!(replay(&wal), vec![r1.to_vec()]);
}
