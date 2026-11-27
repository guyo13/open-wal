//! v7.2 §8.7 — proof that `Wal::reader_at` does **not** scan the segment prefix.
//!
//! One multi-MiB segment of small records. A position near the end is resumed
//! with `reader_at`, and the bytes the reading thread pulled through `read(2)`/
//! `pread(2)` — the kernel's per-thread `rchar` counter in
//! `/proc/thread-self/io` — must be at most two framed records plus slack. The
//! same measurement around `reader_from(next_lsn)` must read at least the whole
//! prefix: that is the positive control showing the counter sees the scan.
//!
//! Linux only. Where `/proc/thread-self/io` or its `rchar` field is absent
//! (no `CONFIG_TASK_IO_ACCOUNTING`) the test skips with a loud message written
//! straight to stderr (past libtest's capture) — it neither fails nor passes
//! silently.
#![cfg(target_os = "linux")]

use std::io::Write;

use open_wal::{Lsn, Wal, WalConfig};

const PAYLOAD: usize = 100;
/// `20 + 100`, already 8-aligned (the test's own §5.3 arithmetic).
const FRAMED: u64 = 120;
const RECORDS: u64 = 40_000;
/// Slack for the test's own two reads of `/proc/thread-self/io` (each a few
/// hundred bytes at most, and each counted in `rchar`).
const SLACK: u64 = 1024;

/// The calling thread's cumulative `rchar`, or `None` if unavailable.
fn rchar() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    text.lines()
        .find_map(|l| l.strip_prefix("rchar:"))
        .and_then(|v| v.trim().parse().ok())
}

fn payload(lsn: u64) -> Vec<u8> {
    (0..PAYLOAD)
        .map(|j| (lsn as usize * 7 + j * 13) as u8)
        .collect()
}

#[test]
fn reader_at_does_not_scan_the_segment_prefix() {
    if rchar().is_none() {
        let _ = std::io::stderr().write_all(
            b"\n*** SKIPPED: reader_at_does_not_scan_the_segment_prefix -- \
              /proc/thread-self/io has no `rchar` (kernel built without \
              CONFIG_TASK_IO_ACCOUNTING). The v7.2 no-prefix-scan proof was \
              NOT EXERCISED on this machine. ***\n\n",
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        segment_size: 8 << 20,
        max_record_size: 256,
    };
    let (mut wal, _) = Wal::open(dir.path(), cfg).unwrap();
    for lsn in 1..=RECORDS {
        wal.append(&payload(lsn)).unwrap();
        if lsn % 4096 == 0 {
            wal.commit().unwrap();
        }
    }
    assert_eq!(wal.commit().unwrap(), Lsn(RECORDS));
    let wal_files = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".wal")
        })
        .count();
    assert_eq!(wal_files, 1, "everything must sit in ONE segment");

    // A position near the end: the one after record `target - 1`.
    let target = RECORDS - 50;
    let pos = {
        let mut r = wal.reader_from(Lsn(target - 1)).unwrap();
        let (lsn, _) = r.next().unwrap().unwrap();
        assert_eq!(lsn, Lsn(target - 1));
        r.position().unwrap()
    };
    assert_eq!(pos.next_lsn(), Lsn(target));
    let prefix_bytes = (target - 1) * FRAMED;
    assert!(prefix_bytes > 4 << 20, "the prefix must be multi-MiB");

    // Measured: reader_at + one next.
    let before = rchar().unwrap();
    let mut r = wal.reader_at(pos).unwrap();
    let (lsn, bytes) = r.next().unwrap().unwrap();
    let after = rchar().unwrap();
    assert_eq!(lsn, Lsn(target));
    assert_eq!(bytes, &payload(target)[..]);
    drop(r);
    let at_read = after - before;

    // Positive control: reader_from(next_lsn) + one next reads the prefix.
    let before = rchar().unwrap();
    let mut r = wal.reader_from(Lsn(target)).unwrap();
    let (lsn, bytes) = r.next().unwrap().unwrap();
    let after = rchar().unwrap();
    assert_eq!(lsn, Lsn(target));
    assert_eq!(bytes, &payload(target)[..]);
    let from_read = after - before;

    assert!(
        from_read >= prefix_bytes,
        "positive control: reader_from read {from_read} bytes, expected at least the \
         {prefix_bytes}-byte prefix — the rchar counter is not seeing the scan"
    );
    let budget = 2 * FRAMED + SLACK;
    assert!(
        at_read <= budget,
        "reader_at + one next read {at_read} bytes (budget {budget} = two framed records + \
         slack): it scanned the segment prefix (reader_from read {from_read})"
    );
}
