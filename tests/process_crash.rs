//! §14.4a — process-crash matrix (SIGKILL) for the page-cache-survives model.
//!
//! A child process (`crash_child`) appends and commits a deterministic,
//! self-describing record stream, announcing each durable watermark on stdout.
//! The parent kills it (`Child::kill` = SIGKILL on Unix) at a range of moments,
//! then reopens the log and asserts:
//! - **D3:** no record at or below the last announced durable LSN is lost.
//! - **D2:** the recovered run is dense `1..=k` (no internal gap).
//! - **D6:** each recovered payload is byte-identical to what was written.
//! - **D9:** a crash *anywhere* recovers to a valid dense suffix (a SIGKILL
//!   mid-`write` leaves a torn tail that recovery truncates — never a fatal
//!   mid-log error, since nothing valid follows a partial sequential write).
//!
//! This is the process-crash subset: dirty page cache survives a process death,
//! so committed (and even merely-written) records persist. The power-loss
//! subset — where un-`fdatasync`'d writes vanish — needs LazyFS and is the M3
//! gate (§14.4b), run separately.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read};
use std::process::{Command, Stdio};
use std::time::Duration;

use open_wal::{Lsn, Wal, WalConfig};

fn cfg() -> WalConfig {
    WalConfig {
        segment_size: 16 * 1024 * 1024,
        max_record_size: 256,
    }
}

/// Spawn the crash child against `dir`, wait for its readiness signal (the first
/// announced durable LSN — by which point the segment is created and ≥1 record
/// is durable), let it run a further `delay` in steady state, SIGKILL it, and
/// return the highest durable LSN it announced before dying.
fn run_and_kill(dir: &std::path::Path, delay: Duration) -> u64 {
    let exe = env!("CARGO_BIN_EXE_crash_child");
    let mut child = Command::new(exe)
        .arg(dir)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn crash_child");

    let mut reader = BufReader::new(child.stdout.take().expect("piped stdout"));

    // Block until the child reports its first durable commit. This guarantees
    // the kill window below is strictly in steady-state operation, not during
    // initial segment creation (whose torn-header recovery is the M4 §8.4 path).
    let mut first = String::new();
    let _ = reader.read_line(&mut first);

    std::thread::sleep(delay);
    let _ = child.kill(); // SIGKILL on Unix

    // Drain the rest (blocks until the pipe closes, i.e. the child is dead).
    let mut rest = String::new();
    let _ = reader.read_to_string(&mut rest);
    let _ = child.wait();

    first
        .lines()
        .chain(rest.lines())
        .filter_map(|l| l.trim().parse::<u64>().ok())
        .next_back()
        .unwrap_or(0)
}

/// Reopen `dir` and assert the recovered log is a dense, byte-identical suffix
/// no shorter than `announced` (D2/D3/D6).
fn assert_recovers(dir: &std::path::Path, announced: u64) {
    let (wal, report) = Wal::open(dir, cfg()).unwrap();
    assert!(
        report.durable_lsn.0 >= announced,
        "D3: lost a committed record — recovered durable {} < announced {}",
        report.durable_lsn.0,
        announced
    );

    let mut reader = wal.reader_from(Lsn(0)).unwrap();
    let mut expected = 1u64;
    while let Some(item) = reader.next() {
        let (lsn, payload) = item.unwrap();
        assert_eq!(lsn, Lsn(expected), "D2: recovered run must be dense from 1");
        let want = format!("rec-{expected:08}");
        assert_eq!(
            payload,
            want.as_bytes(),
            "D6: payload must be byte-identical"
        );
        expected += 1;
    }
    assert_eq!(
        expected - 1,
        report.durable_lsn.0,
        "replay count must equal durable_lsn"
    );
}

#[test]
fn sigkill_at_varied_points_recovers_dense_suffix() {
    // A spread of kill delays exercises crashes between operations, mid-`write`
    // (torn tail), and mid-`fdatasync` (full batch already in page cache).
    for delay_ms in [1u64, 2, 4, 7, 11, 17, 25, 40] {
        let dir = tempfile::tempdir().unwrap();
        let announced = run_and_kill(dir.path(), Duration::from_millis(delay_ms));
        assert_recovers(dir.path(), announced);
    }
}

#[test]
fn sigkill_then_resume_then_kill_again_stays_dense() {
    // D9 across repeated crash/recover cycles: the survivors from one crashed
    // run are extended by the next, never duplicated or holed.
    let dir = tempfile::tempdir().unwrap();
    let a1 = run_and_kill(dir.path(), Duration::from_millis(6));
    assert_recovers(dir.path(), a1);
    let (_, r1) = Wal::open(dir.path(), cfg()).unwrap();
    let after_first = r1.durable_lsn.0;

    // Resume in a second process and crash again; the suffix only grows.
    let a2 = run_and_kill(dir.path(), Duration::from_millis(12));
    assert_recovers(dir.path(), a2.max(after_first));
    let (_, r2) = Wal::open(dir.path(), cfg()).unwrap();
    assert!(
        r2.durable_lsn.0 >= after_first,
        "a resumed run must not lose earlier survivors: {} < {}",
        r2.durable_lsn.0,
        after_first
    );
}
