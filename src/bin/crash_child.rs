//! Test-only crash workload for §14.4a (process-crash matrix). Driven by
//! `tests/process_crash.rs`: it appends a deterministic, self-describing record
//! stream (`rec-{lsn:08}`), committing every few records and announcing each
//! durable watermark on stdout (flushed) so the parent knows the floor that
//! MUST survive a SIGKILL. The parent kills it at varied moments and asserts the
//! reopened log is a dense, byte-identical suffix no shorter than the last
//! announced watermark (D2/D3/D6/D9).
//!
//! Not part of the library; it only exists so an integration test can crash a
//! real writer process. Single segment (M3): the segment is sized to hold the
//! whole workload, so no roll occurs.

use std::io::Write;
use std::path::Path;

use open_wal::{Wal, WalConfig};

const SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
const MAX_RECORD_SIZE: u32 = 256;
const TOTAL: u64 = 50_000;
const BATCH: u64 = 8;

fn main() {
    let dir = std::env::args().nth(1).expect("usage: crash_child <dir>");
    let cfg = WalConfig {
        segment_size: SEGMENT_SIZE,
        max_record_size: MAX_RECORD_SIZE,
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
        // it arrives, segment creation has fully completed and ≥1 record is
        // durable, so the parent's kill window lands in steady-state operation
        // (not crash-during-create, which is the M4 §8.4 path).
        if next_lsn == 2 || since_commit == BATCH {
            let durable = wal.commit().expect("commit");
            since_commit = 0;
            // Announce the durable floor, flushed, so the parent sees it before
            // any kill that follows.
            let _ = writeln!(out, "{}", durable.0);
            let _ = out.flush();
        }
    }

    let durable = wal.commit().expect("commit");
    let _ = writeln!(out, "{}", durable.0);
    let _ = out.flush();
}
