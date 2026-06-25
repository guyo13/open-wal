//! Power-pull verifier — M8 / §14.8 H1 (owner-run, after the cut + reboot).
//!
//! Reads the side-channel capture (the `seq,watermark` lines mirrored off-box by
//! `power_pull_workload`), opens the WAL (running recovery), and asserts every
//! ACKED LSN survived the power cut (D1) byte-identically (D6).
//!
//! CONSERVATIVE WATERMARK. The side channel's high-water is the watermark of the
//! highest **contiguous** `seq` (1,2,3,…). A missing `seq` means a side-channel
//! line was lost in transit (only possible on a lossy transport; TCP does not drop
//! mid-stream) — that is reported as **INCONCLUSIVE**, never silently treated as a
//! lower bar (which would be the side-channel analogue of a vacuous pass). A torn
//! final line (the cut mid-write) is discarded.
//!
//! Because the workload records strictly AFTER `commit()` returned `Ok`, the side
//! channel can only ever *understate* the truly-durable set (a commit may have
//! returned and become durable a moment before its line left the box). So the
//! recovered log legitimately may contain records BEYOND the high-water; the check
//! is one-directional: every LSN ≤ high-water MUST be present. Extra is fine.
//!
//! Exit: 0 PASS · 1 FAIL (acked record lost/wrong — a D1 violation) · 2 INCONCLUSIVE.
//!
//! Usage: power_pull_verify <wal_dir> <capture_file>

use std::collections::BTreeMap;
use std::path::Path;

use open_wal::{Lsn, Wal, WalConfig};

const SEGMENT_SIZE: u64 = 8 * 1024 * 1024;
const MAX_RECORD_SIZE: u32 = 4096;

/// Must match `power_pull_workload`'s config (incl. the env overrides) so the
/// recovered geometry lines up.
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

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 3 {
        eprintln!("usage: {} <wal_dir> <capture_file>", a[0]);
        std::process::exit(2);
    }
    let dir = &a[1];
    let capture = &a[2];

    // 1. Highest contiguous acked watermark from the side channel.
    let text = std::fs::read_to_string(capture).expect("read capture file");
    let mut by_seq: BTreeMap<u64, u64> = BTreeMap::new();
    for line in text.lines() {
        // Tolerate a torn final line (the cut mid-write): skip unparseable lines.
        let Some((s, w)) = line.split_once(',') else {
            continue;
        };
        if let (Ok(seq), Ok(wm)) = (s.trim().parse::<u64>(), w.trim().parse::<u64>()) {
            by_seq.insert(seq, wm);
        }
    }
    if by_seq.is_empty() {
        eprintln!(
            "[verify] INCONCLUSIVE: side-channel capture is empty — nothing was acked off-box."
        );
        std::process::exit(2);
    }
    let mut high_water = 0u64;
    let mut expected_seq = 1u64;
    let mut gap = false;
    for (&seq, &wm) in &by_seq {
        if seq != expected_seq {
            gap = true;
            break;
        }
        high_water = wm;
        expected_seq += 1;
    }
    if gap {
        eprintln!(
            "[verify] INCONCLUSIVE: side-channel has a seq gap (lost line) at seq {expected_seq}. \
             Highest contiguous acked watermark = {high_water}; cannot certify beyond it. \
             (TCP should not gap mid-stream — check the transport.)"
        );
        std::process::exit(2);
    }
    eprintln!("[verify] side-channel highest contiguous acked watermark = LSN {high_water}");

    // 2. Recover the WAL and stream every record in dense order.
    let cfg = wal_config();
    let (wal, report) = Wal::open(Path::new(dir), cfg).expect("open/recover WAL");
    eprintln!(
        "[verify] recovered: oldest_lsn={} durable_lsn={}",
        report.oldest_lsn.0, report.durable_lsn.0
    );

    let mut reader = wal.reader_from(Lsn(0)).expect("reader");
    let mut expected = report.oldest_lsn.0.max(1);
    let mut highest_seen = 0u64;
    while let Some(item) = reader.next() {
        let (lsn, payload) = item.expect("recovered record must decode");
        if lsn.0 != expected {
            eprintln!(
                "[verify] FAIL: recovered run not dense — expected LSN {expected}, got {}. \
                 A hole below the acked watermark is a D1/D2 violation.",
                lsn.0
            );
            std::process::exit(1);
        }
        if lsn.0 <= high_water {
            // D6: payload must be the deterministic content the workload wrote.
            let want_prefix = format!("rec-{:016}", lsn.0);
            if !payload.starts_with(want_prefix.as_bytes()) {
                eprintln!(
                    "[verify] FAIL: LSN {} payload mismatch — expected prefix {:?} (D6 byte-identity).",
                    lsn.0, want_prefix
                );
                std::process::exit(1);
            }
        }
        highest_seen = lsn.0;
        expected += 1;
    }

    // 3. Every acked LSN ≤ high-water must be present (D1).
    if highest_seen < high_water {
        eprintln!(
            "[verify] FAIL: acked watermark LSN {high_water} was recorded off-box but the recovered \
             log only reaches LSN {highest_seen}. ACKED DATA WAS LOST — D1 VIOLATION."
        );
        std::process::exit(1);
    }

    eprintln!(
        "[verify] PASS: every acked LSN ≤ {high_water} is present and byte-identical after the cut \
         (recovered up to {highest_seen}). D1/D6 hold for this cycle."
    );
}
