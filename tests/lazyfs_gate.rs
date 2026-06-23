//! LazyFS power-loss gate — the fault-injection tests that recovery must pass.
//! M3: §14.4b (lost writes) + §14.4g (durability-of-zeroing). M4 adds §14.4c
//! (split-batch survives power loss) and the positive half of the dir-fsync
//! scaffold. The §14.4d *negative control* (a dir-fsync-omitting build must FAIL)
//! is **deferred to M8**: LazyFS models data-write faults only, not the loss of an
//! unsynced directory entry, so it cannot exercise it — that needs dm-flakey /
//! power-pull (§14.8). The `inject_no_dir_fsync` feature is kept as scaffolding
//! for M8. See `roll_records_survive_power_loss`.
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

/// Tiny segments (448 usable bytes after the 64-byte header) so a handful of
/// 200-byte records roll across several segments and a single commit batch spans
/// ≥2 segments — exercising the M4 roll/split power-loss paths (§14.4c/d).
fn tiny_config() -> WalConfig {
    WalConfig {
        segment_size: 512,
        max_record_size: 256,
    }
}

/// `n` distinct 200-byte payloads (two per tiny segment ⇒ bases 1, 3, 5, …).
fn split_payloads(n: u8) -> Vec<Vec<u8>> {
    (1..=n)
        .map(|i| {
            let mut p = vec![0u8; 200];
            p[0] = i;
            p
        })
        .collect()
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
    replay_from(wal, 1)
}

/// Replay, asserting the recovered run is dense starting at `start` (the oldest
/// surviving LSN — `1` unless a checkpoint reclaimed a prefix). Returns the
/// payloads in order.
fn replay_from(wal: &Wal, start: u64) -> Vec<Vec<u8>> {
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let mut out = Vec::new();
    let mut expected = start;
    while let Some(item) = r.next() {
        let (lsn, payload) = item.unwrap();
        assert_eq!(
            lsn,
            Lsn(expected),
            "recovered run must be dense from {start}"
        );
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

/// §14.4c (D9): a single commit batch that splits across several segments is
/// fully durable after a power-loss `clear-cache` — the per-segment `fdatasync`s
/// (and the dir-fsync on each roll) made every rolled segment's records and
/// filename durable, so the whole dense suffix recovers.
#[test]
#[ignore = "requires a running LazyFS mount"]
fn split_batch_survives_power_loss() {
    let dir = fresh_wal_dir("split");
    let cfg = tiny_config();
    let payloads = split_payloads(10); // 10 records, 2/segment ⇒ 5 segments

    {
        let (mut wal, _) = Wal::open(&dir, cfg).unwrap();
        for p in &payloads {
            wal.append(p).unwrap();
        }
        // One commit: the split + rolls all happen here, each segment fdatasync'd.
        assert_eq!(wal.commit().unwrap(), Lsn(10));
    }

    // Power loss after the full split commit: nothing committed may vanish.
    clear_cache();

    let (wal, report) = Wal::open(&dir, cfg).unwrap();
    assert_eq!(
        report.durable_lsn,
        Lsn(10),
        "D9: split-batch records lost on power loss"
    );
    assert_eq!(
        replay(&wal),
        payloads,
        "D6: recovered bytes must be identical"
    );
}

/// Positive half of the directory-fsync scaffold (D9): on the **correct** build,
/// records written across a roll survive a power-loss `clear-cache` as a dense
/// suffix. This is *not* §14.4d's negative control.
///
/// **The §14.4d negative control is deferred to M8.** Its premise — a build that
/// omits the roll's directory fsync must FAIL recovery here — is **not realizable
/// under LazyFS**: LazyFS's faults are data-only (`clear-cache`/`torn-op`/
/// `torn-seq`) and it is a passthrough, so a `create`'s directory entry is
/// persisted to the backing fs and never dropped by `clear-cache`. The new
/// segment's *data* is independently `fdatasync`'d, so omitting the parent-dir
/// fsync produces no observable loss. Truly losing an unsynced directory entry
/// needs block-layer metadata-fault injection (dm-flakey) or a real power-pull —
/// §14.8 / M8. The `inject_no_dir_fsync` feature + this test body are the
/// scaffolding that injector will drive: under it, this same assertion MUST fail.
#[test]
#[ignore = "requires a running LazyFS mount"]
fn roll_records_survive_power_loss() {
    let dir = fresh_wal_dir("dirfsync");
    let cfg = tiny_config();
    let payloads = split_payloads(6); // 6 records, 2/segment ⇒ ≥1 roll (segs 1,3,5)

    {
        let (mut wal, _) = Wal::open(&dir, cfg).unwrap();
        for p in &payloads {
            wal.append(p).unwrap();
        }
        assert_eq!(wal.commit().unwrap(), Lsn(6));
    }

    // Power loss after a committed roll. On the correct build all 6 survive. Under
    // the `inject_no_dir_fsync` build *with a metadata-fault injector* (M8), the
    // rolled segments' dir entries would be lost and this assertion would fail —
    // but LazyFS alone cannot drop them (see the doc comment).
    clear_cache();

    let (wal, report) = Wal::open(&dir, cfg).unwrap();
    assert_eq!(
        report.durable_lsn,
        Lsn(6),
        "post-roll records lost on power loss"
    );
    assert_eq!(replay(&wal), payloads);
}

/// §14.4c (D8/D9): a completed `checkpoint` is durable across a power loss. The
/// oldest-first unlinks + dir-fsync make the reclamation permanent, and the
/// retained records recover as a dense, byte-identical suffix from the new
/// `oldest_lsn` — no holes, no resurrection of a reclaimed prefix.
#[test]
#[ignore = "requires a running LazyFS mount"]
fn checkpoint_survives_power_loss() {
    let dir = fresh_wal_dir("ckpt");
    let cfg = tiny_config();
    let payloads = split_payloads(10); // bases 1,3,5,7,9 (2 records/segment)

    {
        let (mut wal, _) = Wal::open(&dir, cfg).unwrap();
        for p in &payloads {
            wal.append(p).unwrap();
        }
        assert_eq!(wal.commit().unwrap(), Lsn(10));
        // Reclaim everything ≤ 4: drops segs [1,3) and [3,5) ⇒ oldest_lsn = 5.
        wal.checkpoint(Lsn(4)).unwrap();
    }

    // Power loss after a committed checkpoint: the deletions (and the dir-fsync)
    // are durable, and the retained suffix [5,10] survives intact.
    clear_cache();

    let (wal, report) = Wal::open(&dir, cfg).unwrap();
    assert_eq!(
        report.oldest_lsn,
        Lsn(5),
        "D8: reclaimed prefix must stay gone"
    );
    assert_eq!(report.durable_lsn, Lsn(10), "D8: retained records lost");
    assert!(
        !dir.join("00000000000000000001.wal").exists(),
        "checkpointed segment reappeared after power loss"
    );
    assert_eq!(
        replay_from(&wal, 5),
        payloads[4..],
        "D6: retained suffix must be byte-identical"
    );
}

/// §14.4c (D8/D9): an **interrupted** checkpoint — crash after unlinking the
/// oldest segment but *before* the directory fsync — recovers to a contiguous
/// suffix with no holes. We model the crash point by unlinking the oldest segment
/// directly (no dir-fsync) and then issuing a power-loss `clear-cache`; recovery
/// must accept the missing prefix silently (§4 D2 / §8.4 interrupted-checkpoint)
/// and reconstruct a dense suffix.
#[test]
#[ignore = "requires a running LazyFS mount"]
fn interrupted_checkpoint_recovers_contiguous_suffix() {
    let dir = fresh_wal_dir("ckpt-torn");
    let cfg = tiny_config();
    let payloads = split_payloads(10); // bases 1,3,5,7,9

    {
        let (mut wal, _) = Wal::open(&dir, cfg).unwrap();
        for p in &payloads {
            wal.append(p).unwrap();
        }
        assert_eq!(wal.commit().unwrap(), Lsn(10));
    }

    // Simulate a checkpoint killed mid-deletion: the oldest segment was unlinked,
    // but the run crashed before the dir-fsync (and before unlinking the next).
    std::fs::remove_file(dir.join("00000000000000000001.wal")).unwrap();

    // Power loss at that point.
    clear_cache();

    // Recovery accepts the missing prefix and yields a dense contiguous suffix.
    let (wal, report) = Wal::open(&dir, cfg).unwrap();
    assert_eq!(
        report.oldest_lsn,
        Lsn(3),
        "survivors must be a contiguous suffix"
    );
    assert_eq!(
        report.durable_lsn,
        Lsn(10),
        "no record above the cut may be lost"
    );
    assert_eq!(
        replay_from(&wal, 3),
        payloads[2..],
        "D8/D9: contiguous suffix, no holes, byte-identical"
    );
}
