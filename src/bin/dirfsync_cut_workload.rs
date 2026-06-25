//! Synchronized mid-run cut workload — M8 / §14.4d Tier-2 behavioral negative
//! control (OWNER-RUN / CI under dm-flakey on **ext2**).
//!
//! WHY THIS EXISTS. The run-to-completion `power_pull_workload` cannot demonstrate
//! the dir-fsync omission even on journal-less ext2: by the time the workload
//! finishes its whole record stream and the harness cuts, every rolled segment's
//! directory entry has already been written back, so the `inject_no_dir_fsync`
//! build recovers fully (observed: PR #21 run 28193051238 — positive control LIVE,
//! asymmetry still absent). The fix (per the designer) is to cut **inside the
//! un-synced window**: this workload rolls ONCE, makes sure an *acked* record lives
//! in the brand-new (still-dirty-dirent) segment, signals the harness, and then
//! BLOCKS — holding the directory entry un-synced (default `dirty_expire_centisecs`
//! gives ~30 s of slack) so the harness can activate `drop_writes` and cut at
//! leisure. On ext2 the new segment's `fdatasync` flushed its data+inode but never
//! the parent directory block, so dropping that writeback orphans the segment ⇒
//! recovery's `readdir` misses it ⇒ the acked post-roll records vanish (D1) in the
//! inject build, while the correct build's `fsync_dir` made the dirent durable.
//!
//! It writes the SAME `seq,watermark` side channel and `rec-{lsn:016}` payloads as
//! `power_pull_workload`, so `power_pull_verify` checks it unchanged: every acked
//! LSN ≤ the contiguous watermark must survive the cut.
//!
//! Test-only (excluded from the published crate).
//!
//! Usage:
//!   dirfsync_cut_workload <wal_dir> <capture_file> <ready_file>
//!     <capture_file>, <ready_file> MUST live OFF the at-risk device (the harness
//!     puts them on /tmp) so they survive the cut and the harness can poll them.
//!   Honors WAL_SEGMENT_SIZE / WAL_MAX_RECORD_SIZE (the harness sets tiny values
//!   so the roll happens after a handful of records).

use std::io::Write;
use std::path::Path;

use open_wal::{Wal, WalConfig};

// Defaults are deliberately tiny (the harness overrides via env anyway) so a roll
// happens after a few records even without the env set.
const SEGMENT_SIZE: u64 = 4096;
const MAX_RECORD_SIZE: u32 = 256;
// Safety bound: if no roll has happened by here the config is wrong — bail loudly
// rather than spin forever (the harness treats a non-ready exit as INCONCLUSIVE).
const MAX_RECORDS: u64 = 1_000_000;

fn wal_config() -> WalConfig {
    let segment_size = std::env::var("WAL_SEGMENT_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(SEGMENT_SIZE);
    let max_record_size = std::env::var("WAL_MAX_RECORD_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(MAX_RECORD_SIZE);
    WalConfig {
        segment_size,
        max_record_size,
    }
}

/// Count the `*.wal` segment files currently in the WAL directory. A growth in this
/// count across a `commit()` means that commit rolled to a new segment.
fn count_segments(dir: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".wal")))
        .count()
}

/// Append one deterministic record and commit it, returning the durable watermark.
fn append_commit(wal: &mut Wal, lsn: u64) -> open_wal::Lsn {
    let payload = format!("rec-{lsn:016}").into_bytes();
    wal.append(&payload).expect("append");
    wal.commit().expect("commit")
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 4 {
        eprintln!("usage: {} <wal_dir> <capture_file> <ready_file>", a[0]);
        std::process::exit(2);
    }
    let dir = a[1].clone();
    let capture_path = a[2].clone();
    let ready_path = a[3].clone();
    let dir_path = Path::new(&dir);

    // Side channel: seq,watermark, fsync'd, OFF the at-risk device.
    let mut capture = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&capture_path)
        .expect("open capture");
    let mut seq = 0u64;
    let mut record_ack = |w: open_wal::Lsn, seq: &mut u64| {
        *seq += 1;
        writeln!(capture, "{},{}", *seq, w.0).expect("write capture");
        capture.sync_all().expect("fsync capture");
    };

    let cfg = wal_config();
    let (mut wal, report) = Wal::open(dir_path, cfg).expect("open WAL");
    let mut next_lsn = report.durable_lsn.0 + 1;

    let segs_before = count_segments(dir_path);
    eprintln!(
        "[dirfsync-cut] dir={dir} resume_from_lsn={next_lsn} segments={segs_before} — appending until a roll…"
    );

    // 1. Append+commit one record at a time until a commit ROLLS (segment count
    //    grows). Every committed record is acked to the side channel first.
    loop {
        if next_lsn > MAX_RECORDS {
            eprintln!(
                "[dirfsync-cut] no roll within {MAX_RECORDS} records — config wrong; exiting (harness: INCONCLUSIVE)."
            );
            std::process::exit(3);
        }
        let w = append_commit(&mut wal, next_lsn);
        record_ack(w, &mut seq);
        next_lsn += 1;
        if count_segments(dir_path) > segs_before {
            break; // a roll just happened on this commit
        }
    }

    // 2. The new segment is now active but its directory entry is, in the inject
    //    build, still un-synced. Put one MORE acked record squarely inside it (its
    //    data fdatasync does NOT persist the parent dirent on ext2), so the orphaned
    //    segment definitely holds an acked record — the LSN whose loss is the D1
    //    violation the verifier flags.
    let w = append_commit(&mut wal, next_lsn);
    record_ack(w, &mut seq);
    let acked_in_new_segment = w.0;

    // 3. Signal the harness (off-device, fsync'd) and BLOCK with the dirent still
    //    un-synced. The harness activates drop_writes, then SIGKILLs us, then cuts.
    {
        let mut ready = std::fs::File::create(&ready_path).expect("create ready file");
        writeln!(ready, "READY {acked_in_new_segment}").expect("write ready");
        ready.sync_all().expect("fsync ready");
    }
    eprintln!(
        "[dirfsync-cut] ROLLED — acked LSN {acked_in_new_segment} is in the new segment; \
         signalled ready, blocking (dirent left un-synced). Awaiting the harness cut (SIGKILL)."
    );

    // Block forever; the harness reclaims us via SIGKILL after the cut.
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
