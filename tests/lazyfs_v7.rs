//! LazyFS power-loss gate — **v7 additions** (§14.4c, cold start at a seed `N`).
//!
//! The v7 `OpenOptions::seed` cold-starts a fresh log at base `N` instead of 1
//! (§8.4). This suite re-runs the cold-start / roll / checkpoint power-loss cases
//! of `tests/lazyfs_gate.rs` with `base = N`, plus the cold-start-at-`N` crash
//! points that a real power loss can produce (header written but not
//! `fdatasync`'d), and checks the containing-segment `reader_from` (§8.5) over a
//! recovered seeded log. Kept in its own file so the existing gate file is
//! untouched; `scripts/lazyfs-gate.sh run` runs both.
//!
//! Same environment contract as `tests/lazyfs_gate.rs` (`#[ignore]` by default;
//! driven by `LAZYFS_MNT` / `LAZYFS_FIFO` / `LAZYFS_LOG`; single-threaded because
//! `clear-cache` is global to the mount):
//! ```text
//! scripts/lazyfs-gate.sh all
//! # or, against a running mount:
//! LAZYFS_MNT=… LAZYFS_FIFO=… LAZYFS_LOG=… \
//!   cargo test --test lazyfs_v7 -- --ignored --test-threads=1
//! ```

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use open_wal::{Lsn, TailState, Wal, WalConfig};

/// The seeded origin used throughout: large, so a stray base-1 assumption shows.
const N: u64 = 1_000_001;

fn config() -> WalConfig {
    WalConfig {
        segment_size: 1 << 20,
        max_record_size: 4096,
    }
}

/// Tiny segments (2 × 200-byte records each) so a batch rolls and splits.
fn tiny_config() -> WalConfig {
    WalConfig {
        segment_size: 512,
        max_record_size: 256,
    }
}

fn payloads(n: u8) -> Vec<Vec<u8>> {
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

fn fresh_wal_dir(tag: &str) -> PathBuf {
    let mnt = env("LAZYFS_MNT");
    let dir = Path::new(&mnt).join(format!("wal-v7-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn seg(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.wal"))
}

fn cleared_count(log: &str) -> usize {
    std::fs::read_to_string(log)
        .map(|s| s.matches("cache is cleared").count())
        .unwrap_or(0)
}

/// Power loss: `lazyfs::clear-cache`, then wait for LazyFS to log completion.
/// (Same protocol as `tests/lazyfs_gate.rs::clear_cache` — non-blocking FIFO
/// open so a dead daemon fails loudly instead of hanging.)
fn clear_cache() {
    let fifo = env("LAZYFS_FIFO");
    let log = env("LAZYFS_LOG");
    let before = cleared_count(&log);

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

/// The genuine 64-byte segment header for base `base`, taken from a real cold
/// start in a scratch (non-LazyFS) directory — so the torn-create test writes
/// exactly what `segment::create` would, without re-encoding the format here.
fn seeded_header(base: u64) -> Vec<u8> {
    let tmp = tempfile::tempdir().unwrap();
    drop(
        Wal::options()
            .seed(Lsn(base))
            .open(tmp.path(), config())
            .unwrap(),
    );
    let f = std::fs::File::open(seg(tmp.path(), base)).unwrap();
    let mut h = vec![0u8; 64];
    f.read_exact_at(&mut h, 0).unwrap();
    h
}

/// Replay from `from`, asserting the run is dense starting at `start`.
fn replay_dense(wal: &Wal, from: u64, start: u64) -> Vec<Vec<u8>> {
    let mut r = wal.reader_from(Lsn(from)).unwrap();
    let mut out = Vec::new();
    let mut expected = start;
    while let Some(item) = r.next() {
        let (lsn, payload) = item.unwrap();
        assert_eq!(lsn, Lsn(expected), "run must be dense from {start}");
        out.push(payload.to_vec());
        expected += 1;
    }
    out
}

/// §14.4c cold start at `N` (D9 + dir-fsync on create): the seeded, empty
/// segment survives power loss, and recovery — not the reopen's seed — decides
/// the origin afterwards (D7).
#[test]
#[ignore = "requires a running LazyFS mount"]
fn seeded_cold_start_survives_power_loss() {
    let dir = fresh_wal_dir("cold");
    {
        let (wal, report) = Wal::options().seed(Lsn(N)).open(&dir, config()).unwrap();
        assert_eq!(
            (report.oldest_lsn, report.durable_lsn),
            (Lsn(N), Lsn(N - 1))
        );
        assert_eq!(wal.oldest_lsn(), Lsn(N));
    }
    clear_cache();
    // Reopen with the default seed (1): the durable seeded segment wins.
    let (wal, report) = Wal::open(&dir, config()).unwrap();
    assert_eq!(report.oldest_lsn, Lsn(N), "seeded cold-start segment lost");
    assert_eq!(report.durable_lsn, Lsn(N - 1));
    assert_eq!(report.tail_state, TailState::Clean);
    assert_eq!(wal.oldest_lsn(), Lsn(N));
    assert!(seg(&dir, N).exists());
    assert!(!seg(&dir, 1).exists(), "a base-1 segment must not appear");
}

/// §14.4b/§14.4c on a seeded log (D1/D2/D6/D9): committed records — including a
/// batch split across rolled segments — survive power loss, dense from `N`.
#[test]
#[ignore = "requires a running LazyFS mount"]
fn seeded_split_batch_survives_power_loss() {
    let dir = fresh_wal_dir("split");
    let ps = payloads(7); // bases N, N+2, N+4, N+6
    {
        let (mut wal, _) = Wal::options()
            .seed(Lsn(N))
            .open(&dir, tiny_config())
            .unwrap();
        for p in &ps {
            wal.append(p).unwrap();
        }
        assert_eq!(wal.commit().unwrap(), Lsn(N + 6));
    }
    clear_cache();
    let (wal, report) = Wal::options()
        .seed(Lsn(42)) // ignored: the directory holds a log
        .open(&dir, tiny_config())
        .unwrap();
    assert_eq!(report.oldest_lsn, Lsn(N));
    assert_eq!(report.durable_lsn, Lsn(N + 6), "D1: committed records lost");
    assert_eq!(report.segments_scanned, 4);
    assert_eq!(replay_dense(&wal, 0, N), ps, "D6: bytes must be identical");
    // §8.5: a mid-log seek on the recovered seeded log yields the exact suffix.
    assert_eq!(replay_dense(&wal, N + 3, N + 3), ps[3..]);
}

/// §14.4c cold-start-at-`N` crash point under a real power loss: the segment
/// file was created and its header written but **not** `fdatasync`'d (and the
/// directory not fsync'd). Whatever survives — nothing, a zero-length or
/// header-less file, or a complete header — holds no durable record, so recovery
/// must yield the empty log at `N` (§8.4 discard ⇒ cold start, or adopt the
/// empty segment), never an error (D9).
#[test]
#[ignore = "requires a running LazyFS mount"]
fn seeded_cold_start_unsynced_header_recovers() {
    let dir = fresh_wal_dir("torn-create");
    // Hand-roll the first half of `segment::create` without its fdatasync.
    {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(seg(&dir, N))
            .unwrap();
        f.write_all_at(&seeded_header(N), 0).unwrap();
    }
    clear_cache();
    let (mut wal, report) = Wal::options().seed(Lsn(N)).open(&dir, config()).unwrap();
    assert_eq!(
        (report.oldest_lsn, report.durable_lsn),
        (Lsn(N), Lsn(N - 1))
    );
    assert_eq!(wal.append(b"after").unwrap(), Lsn(N));
    assert_eq!(wal.commit().unwrap(), Lsn(N));
    drop(wal);
    clear_cache();
    let (wal, report) = Wal::open(&dir, config()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(N));
    assert_eq!(replay_dense(&wal, 0, N), vec![b"after".to_vec()]);
}

/// §14.4c checkpoint on a seeded log (D8): reclamation is durable across power
/// loss, `oldest_lsn` advances past the seed, the retained suffix is intact, and
/// a below-floor read is a fatal gap.
#[test]
#[ignore = "requires a running LazyFS mount"]
fn seeded_checkpoint_survives_power_loss() {
    let dir = fresh_wal_dir("ckpt");
    let ps = payloads(10); // bases N, N+2, …, N+8
    {
        let (mut wal, _) = Wal::options()
            .seed(Lsn(N))
            .open(&dir, tiny_config())
            .unwrap();
        for p in &ps {
            wal.append(p).unwrap();
        }
        assert_eq!(wal.commit().unwrap(), Lsn(N + 9));
        wal.checkpoint(Lsn(N + 3)).unwrap(); // drops [N, N+2) and [N+2, N+4)
        assert_eq!(wal.oldest_lsn(), Lsn(N + 4));
    }
    clear_cache();
    let (wal, report) = Wal::open(&dir, tiny_config()).unwrap();
    assert_eq!(
        report.oldest_lsn,
        Lsn(N + 4),
        "D8: reclaimed prefix must stay gone"
    );
    assert_eq!(wal.oldest_lsn(), Lsn(N + 4));
    assert_eq!(report.durable_lsn, Lsn(N + 9), "D8: retained records lost");
    assert!(!seg(&dir, N).exists());
    assert!(
        wal.reader_from(Lsn(N + 3)).is_err(),
        "§15.4 gap must be fatal"
    );
    assert_eq!(replay_dense(&wal, 0, N + 4), ps[4..]);
}
