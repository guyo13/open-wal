//! v7 §14.4a — process-crash (SIGKILL) matrix for a log cold-started at a seed.
//!
//! Same model as `tests/process_crash.rs` (page cache survives a process death),
//! but the writer opens with `Wal::options().seed(N)`, so the cold start, every
//! roll and every split happen in the seeded LSN space. The child is this very
//! test binary re-executed with an env var (no extra `src/bin` target): it
//! appends self-describing `rec-{lsn}` payloads, committing every few records
//! and announcing each durable watermark on stdout.
//!
//! The parent kills it at a range of moments — including **immediately after
//! spawn**, so some kills land before or inside the seeded cold start itself
//! (create / pre-allocate / header write / dir-fsync — timing-dependent; the
//! deterministic versions of those crash points are the `wal::tests::v7_crash_*`
//! unit tests and `tests/lazyfs_v7.rs`) — then reopens and asserts (D9):
//! - the log is based at `N` (`oldest_lsn == N`), even when the reopen passes a
//!   different seed (D7 — recovery is authoritative) once a segment exists;
//! - **D3:** nothing at or below the last announced durable LSN is lost;
//! - **D2/D6:** the recovered run is dense `N..=k` and byte-identical.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use open_wal::{Lsn, RecoveryReport, Wal, WalConfig, WalError};

const CHILD_ENV: &str = "OPEN_WAL_SEEDED_CRASH_CHILD_DIR";
const SEED: u64 = 7_000_000_001;
const TOTAL: u64 = 50_000;
const BATCH: u64 = 8;

/// 64-KiB segments: the ~40-byte records roll many times and an 8-record batch
/// periodically straddles a boundary (split).
fn cfg() -> WalConfig {
    WalConfig {
        segment_size: 64 * 1024,
        max_record_size: 256,
    }
}

fn payload(lsn: u64) -> String {
    format!("rec-{lsn:020}")
}

/// The child workload. A no-op unless re-executed by the parent with
/// `CHILD_ENV` set (so a normal `cargo test` run passes it trivially).
#[test]
fn seeded_crash_child() {
    let Ok(dir) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (mut wal, report) = Wal::options()
        .seed(Lsn(SEED))
        .open(Path::new(&dir), cfg())
        .expect("open");
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut next = report.durable_lsn.0 + 1;
    let mut since = 0u64;
    while next < SEED + TOTAL {
        let got = wal.append(payload(next).as_bytes()).expect("append");
        assert_eq!(got.0, next);
        next += 1;
        since += 1;
        if next == SEED + 1 || since == BATCH {
            let durable = wal.commit().expect("commit");
            since = 0;
            writeln!(out, "{}", durable.0).unwrap();
            out.flush().unwrap();
        }
    }
}

/// Spawn the child on `dir`; if `wait_ready`, block for its first announcement
/// (steady state) before sleeping `delay`; SIGKILL it; return the highest
/// durable LSN it announced (0 if none).
fn run_and_kill(dir: &Path, delay: Duration, wait_ready: bool) -> u64 {
    let exe = std::env::current_exe().expect("current test binary");
    let mut child = Command::new(exe)
        .args([
            "--exact",
            "seeded_crash_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child");
    let mut reader = BufReader::new(child.stdout.take().expect("piped stdout"));
    let mut seen = String::new();
    if wait_ready {
        // Skip libtest's banner lines until the first numeric announcement.
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            let numeric = line.trim().parse::<u64>().is_ok();
            seen.push_str(&line);
            if numeric {
                break;
            }
        }
    }
    std::thread::sleep(delay);
    let _ = child.kill();
    let _ = reader.read_to_string(&mut seen);
    let _ = child.wait();
    seen.lines()
        .filter_map(|l| l.trim().parse::<u64>().ok())
        .next_back()
        .unwrap_or(0)
}

/// Reopen after a SIGKILL with `seed`, tolerating the brief window in which the
/// dead child's `flock` is still being released (bounded; any other error fails).
fn reopen(dir: &Path, seed: u64) -> (Wal, RecoveryReport) {
    for _ in 0..50 {
        match Wal::options().seed(Lsn(seed)).open(dir, cfg()) {
            Ok(opened) => return opened,
            Err(WalError::Locked) => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("reopen after crash failed with a non-lock error: {e:?}"),
        }
    }
    panic!("WAL dir stayed Locked >1s after the crashed child was reaped");
}

/// Assert the recovered log is a dense, byte-identical run from `SEED` no
/// shorter than `announced` (D2/D3/D6), based at `SEED`.
fn assert_recovers(wal: &Wal, report: &RecoveryReport, announced: u64) {
    assert_eq!(
        report.oldest_lsn,
        Lsn(SEED),
        "log must stay based at the seed"
    );
    assert_eq!(wal.oldest_lsn(), Lsn(SEED));
    assert!(
        report.durable_lsn.0 >= announced.max(SEED - 1),
        "D3: recovered durable {} < announced {announced}",
        report.durable_lsn.0
    );
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let mut expected = SEED;
    while let Some(item) = r.next() {
        let (lsn, p) = item.unwrap();
        assert_eq!(
            lsn,
            Lsn(expected),
            "D2: recovered run must be dense from the seed"
        );
        assert_eq!(
            p,
            payload(expected).as_bytes(),
            "D6: bytes differ at {expected}"
        );
        expected += 1;
    }
    assert_eq!(
        expected - 1,
        report.durable_lsn.0,
        "replay must reach durable_lsn"
    );
}

/// Kills landing at/near spawn — possibly inside the seeded cold start. Reopen
/// with the same seed: either the cold start never left a durable segment
/// (⇒ §8.4 discard / empty dir ⇒ cold start at the seed) or it did; both must
/// recover to a valid run based at `SEED` (D9).
#[test]
fn sigkill_during_seeded_cold_start_recovers() {
    // Before ~1 ms the child is typically still starting (empty dir); by ~3–5 ms
    // the cold start is done and records are committing. The dense 100 µs sweep
    // across that window is what lands kills inside `open`'s cold start on a
    // typical machine (best-effort: process start-up time varies by host).
    let delays_us = [0u64, 500]
        .into_iter()
        .chain((1_000..=4_000).step_by(100))
        .chain([5_000, 8_000, 12_000, 20_000]);
    for (i, us) in delays_us.enumerate() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let announced = run_and_kill(dir, Duration::from_micros(us), false);
        let (wal, report) = reopen(dir, SEED);
        assert_recovers(&wal, &report, announced);
        // Idempotent (D7): a second reopen — now with a different seed — sees the
        // same log, since a segment at SEED exists after the first reopen.
        drop(wal);
        let (wal, report2) = reopen(dir, 1 + i as u64);
        assert_eq!(report2.durable_lsn, report.durable_lsn);
        assert_recovers(&wal, &report2, announced);
    }
}

/// Steady-state kills (mid-write, mid-fdatasync, during a roll or split) of a
/// seeded log; reopened with a *different* seed, which must be ignored (D7/D9).
#[test]
fn sigkill_seeded_steady_state_recovers() {
    for (i, ms) in [0u64, 1, 3, 7, 15, 30].into_iter().enumerate() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let announced = run_and_kill(dir, Duration::from_millis(ms), true);
        assert!(announced >= SEED, "child never reached steady state");
        let (wal, report) = reopen(dir, 2 + i as u64);
        assert_recovers(&wal, &report, announced);
        // Resume the workload on the recovered log, crash again, recover again.
        drop(wal);
        let announced2 = run_and_kill(dir, Duration::from_millis(ms), true);
        assert!(announced2 >= announced);
        let (wal, report) = reopen(dir, 1);
        assert_recovers(&wal, &report, announced2);
    }
}
