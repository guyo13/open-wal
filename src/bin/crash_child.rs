//! Test-only crash workload for §14.4a (process-crash matrix). Driven by
//! `tests/process_crash.rs`: it appends a deterministic, self-describing record
//! stream (`rec-{lsn:08}`), committing every few records and announcing each
//! durable watermark on stdout (flushed) so the parent knows the floor that
//! MUST survive a SIGKILL. The parent kills it at varied moments and asserts the
//! reopened log is a dense, byte-identical suffix no shorter than the last
//! announced watermark (D2/D3/D6/D9).
//!
//! Not part of the library; it only exists so an integration test can crash a
//! real writer process. **Multi-segment (M4):** the segment is sized so the
//! ~32-byte records roll across many segments and an 8-record commit batch
//! periodically straddles a segment boundary (a commit-time split) — so a SIGKILL
//! can land during a roll or split as well as mid-`write`/mid-`fdatasync`, and
//! recovery must still yield a dense suffix (§14.4a, D9).
//!
//! **Checkpoint mode (M5, `crash_child <dir> checkpoint`):** uses tiny segments
//! and `checkpoint`s the fully-superseded prefix after every batch, so a SIGKILL
//! can land inside the oldest-first unlink loop (before its dir-fsync). Recovery
//! must then yield a contiguous suffix from whatever oldest segment survived —
//! no holes, no resurrection (§14.4a checkpoint-unlink points, D8/D9). It
//! announces `oldest durable` per line so the parent learns the floor.

use std::io::Write;
use std::path::Path;

use open_wal::{Wal, WalConfig};

// Append-only mode: small enough that the workload rolls many times (~2k records
// per 64-KiB segment) and an 8-record (~256-byte) batch periodically spans two
// segments.
const SEGMENT_SIZE: u64 = 64 * 1024;
const MAX_RECORD_SIZE: u32 = 256;
// Checkpoint mode: tiny segments so rolls (and thus reclaimable sealed segments)
// pile up fast and a SIGKILL frequently lands during a checkpoint unlink.
const CKPT_SEGMENT_SIZE: u64 = 512;
const CKPT_MAX_RECORD_SIZE: u32 = 256;
const TOTAL: u64 = 50_000;
const BATCH: u64 = 8;

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: crash_child <dir> [checkpoint]");
    let checkpoint_mode = std::env::args().nth(2).as_deref() == Some("checkpoint");
    let cfg = if checkpoint_mode {
        WalConfig {
            segment_size: CKPT_SEGMENT_SIZE,
            max_record_size: CKPT_MAX_RECORD_SIZE,
        }
    } else {
        WalConfig {
            segment_size: SEGMENT_SIZE,
            max_record_size: MAX_RECORD_SIZE,
        }
    };
    let (mut wal, report) = Wal::open(Path::new(&dir), cfg).expect("open");

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    // Resume from whatever survived a prior run (fresh dir ⇒ 0).
    let mut next_lsn = report.durable_lsn.0 + 1;
    let mut since_commit = 0u64;

    while next_lsn <= TOTAL {
        let payload = format!("rec-{next_lsn:08}");
        let got = wal.append(payload.as_bytes()).expect("append");
        debug_assert_eq!(got.0, next_lsn);
        next_lsn += 1;
        since_commit += 1;

        // Commit (and announce) after the very FIRST record, then every BATCH.
        // The first announcement is the parent's readiness signal: by the time
        // it arrives, the initial cold-start segment is fully created and ≥1
        // record is durable, so the kill window starts in steady-state operation
        // (not crash-during-cold-start). Mid-run *roll* creates can still be
        // interrupted — that is the §8.4 path recovery handles.
        if next_lsn == 2 || since_commit == BATCH {
            let durable = wal.commit().expect("commit");
            since_commit = 0;
            // In checkpoint mode, reclaim every fully-superseded sealed segment
            // (the active segment is never deleted). The SIGKILL may interrupt the
            // oldest-first unlink loop before its dir-fsync — recovery must still
            // yield a contiguous suffix (§14.4a, D8/D9).
            if checkpoint_mode {
                wal.checkpoint(durable).expect("checkpoint");
            }
            // Announce the durable floor, flushed, so the parent sees it before
            // any kill that follows.
            let _ = writeln!(out, "{}", durable.0);
            let _ = out.flush();
        }
    }

    let durable = wal.commit().expect("commit");
    if checkpoint_mode {
        wal.checkpoint(durable).expect("checkpoint");
    }
    let _ = writeln!(out, "{}", durable.0);
    let _ = out.flush();
}
