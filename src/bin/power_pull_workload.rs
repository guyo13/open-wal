//! Power-pull workload — M8 / §14.8 H1 (owner-run on real hardware).
//!
//! Sustained committed writes whose highest ACKED LSN is mirrored to a side
//! channel that is **durable independently of the at-risk device**, so that after
//! a hard power cut the verifier (`power_pull_verify`) can assert every acked LSN
//! survived (D1). This binary is test-only (excluded from the published crate).
//!
//! WHY A SIDE CHANNEL, AND WHY OFF-BOX. A record of "what was acked" written to a
//! file on the SAME device the cut destroys is useless — it shares the failure
//! mode under test. The side channel must survive the cut by construction. The
//! default is a **network sink** (TCP to another host): once a line is on the
//! other host, cutting power to this box cannot un-record it. Serial and
//! separate-block-device sinks are provided as alternatives (the owner must then
//! guarantee that channel is itself durable across the same cut — see the runbook).
//!
//! THE ACK-ORDERING RULE (the one way H1 can lie). The side channel may record an
//! LSN ONLY once it is locally durable. We therefore send a watermark **strictly
//! after `commit()` returns `Ok(w)`** — never `last_lsn`, never an appended-but-
//! unconfirmed LSN. A split-batch commit that partially fails returns `Err`, so we
//! send nothing for it; the first segment's records may still be durable, but not
//! recording them only makes the side channel *understate* the true watermark —
//! the SAFE direction (the verifier then checks a slightly lower bar, never a
//! false-loss). This is §15.1 durability-visibility discipline applied to the test.
//!
//! Each line is `seq,watermark` with a contiguous `seq` so the receiver can detect
//! a dropped line (a gap ⇒ INCONCLUSIVE, never a vacuous pass — important if a
//! lossy transport is ever substituted for TCP).
//!
//! Usage:
//!   power_pull_workload <wal_dir> <sink> [total] [batch] [payload_bytes]
//!     sink := tcp:HOST:PORT | file:PATH | serial:PATH | stdout
//!     total = 0 (default) ⇒ run until the external cut kills us.
//!
//! Payloads are the deterministic `rec-{lsn:016}` so the verifier can check
//! byte-identity (D6), not just presence (D1).

use std::io::Write;
use std::net::TcpStream;
use std::path::Path;

use open_wal::{Lsn, Wal, WalConfig};

const SEGMENT_SIZE: u64 = 8 * 1024 * 1024;
const MAX_RECORD_SIZE: u32 = 4096;

/// WAL config, with `WAL_SEGMENT_SIZE` / `WAL_MAX_RECORD_SIZE` env overrides so the
/// §14.4d dm-flakey control can force frequent rolls with tiny segments. The
/// verifier reads the same overrides so its config matches.
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

/// The independently-durable record of acked watermarks.
enum Sink {
    Tcp(TcpStream),
    /// A file (separate device, or serial char device); `fsync` if it is a
    /// regular file so the ack is durable on the *other* device before we proceed.
    File {
        file: std::fs::File,
        fsync: bool,
    },
    Stdout,
}

impl Sink {
    fn open(spec: &str) -> std::io::Result<Sink> {
        if let Some(addr) = spec.strip_prefix("tcp:") {
            let stream = TcpStream::connect(addr)?;
            stream.set_nodelay(true).ok();
            Ok(Sink::Tcp(stream))
        } else if let Some(path) = spec.strip_prefix("file:") {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            Ok(Sink::File { file, fsync: true })
        } else if let Some(path) = spec.strip_prefix("serial:") {
            let file = std::fs::OpenOptions::new().write(true).open(path)?;
            // Char devices cannot be fsync'd meaningfully; the external capture is
            // the durable record.
            Ok(Sink::File { file, fsync: false })
        } else if spec == "stdout" {
            Ok(Sink::Stdout)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "unknown sink spec '{spec}' (tcp:HOST:PORT | file:PATH | serial:PATH | stdout)"
                ),
            ))
        }
    }

    /// Record one acked watermark. Returns once the line is durable on the side
    /// channel (flushed to the socket / fsync'd to the file).
    fn record(&mut self, seq: u64, watermark: Lsn) -> std::io::Result<()> {
        let line = format!("{seq},{}\n", watermark.0);
        match self {
            Sink::Tcp(s) => {
                s.write_all(line.as_bytes())?;
                s.flush()
            }
            Sink::File { file, fsync } => {
                file.write_all(line.as_bytes())?;
                if *fsync {
                    file.sync_all()?;
                }
                Ok(())
            }
            Sink::Stdout => {
                print!("{line}");
                std::io::stdout().flush()
            }
        }
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 3 {
        eprintln!(
            "usage: {} <wal_dir> <sink> [total] [batch] [payload_bytes]\n  sink := tcp:HOST:PORT | file:PATH | serial:PATH | stdout",
            a.first()
                .map(String::as_str)
                .unwrap_or("power_pull_workload")
        );
        std::process::exit(2);
    }
    let dir = a[1].clone();
    let sink_spec = a[2].clone();
    let total: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(0); // 0 = unbounded
    let batch: u64 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(64);
    let payload_bytes: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(64);

    let mut sink = Sink::open(&sink_spec).expect("open side channel");

    let cfg = wal_config();
    let (mut wal, report) = Wal::open(Path::new(&dir), cfg).expect("open WAL");

    let mut next_lsn = report.durable_lsn.0 + 1;
    let mut seq = 0u64;
    let mut since_commit = 0u64;

    eprintln!(
        "[power-pull-workload] dir={dir} sink={sink_spec} resume_from_lsn={next_lsn} batch={batch} payload={payload_bytes}B"
    );

    loop {
        if total != 0 && next_lsn > total {
            break;
        }
        // Deterministic, reconstructable payload (rec-{lsn:016} padded to width).
        let mut payload = format!("rec-{next_lsn:016}").into_bytes();
        payload.resize(payload_bytes.max(20), b'.');
        let assigned = wal.append(&payload).expect("append");
        debug_assert_eq!(assigned.0, next_lsn);
        next_lsn += 1;
        since_commit += 1;

        if since_commit >= batch {
            match wal.commit() {
                Ok(w) => {
                    // ACK-ORDERING RULE: record ONLY after commit returned Ok(w).
                    seq += 1;
                    sink.record(seq, w).expect("record to side channel");
                    since_commit = 0;
                }
                Err(e) => {
                    // A genuine durability failure. For a plain power-pull the cut
                    // kills us before any Err; this path is what the dm-flakey H3
                    // run exercises (error_writes window ⇒ fdatasync EIO ⇒ poison,
                    // §12). The handle is poisoned; nothing was acked for this batch,
                    // so we record nothing (safe understatement). Exit 7 is the
                    // distinct "poisoned by a durability failure" signal the
                    // dm-flakey harness asserts on.
                    eprintln!("[power-pull-workload] commit error (handle poisoned): {e}");
                    std::process::exit(7);
                }
            }
        }
    }

    // Final flush of any remainder.
    if let Ok(w) = wal.commit() {
        if w.0 >= 1 {
            seq += 1;
            let _ = sink.record(seq, w);
        }
    }
    eprintln!("[power-pull-workload] done: last acked watermark recorded at seq={seq}");
}
