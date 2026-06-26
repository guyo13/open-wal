//! Storage probe — M8 / §14.8 H1, §3.4 cut-mechanism calibration (owner-run).
//!
//! The §3.4 vacuous-pass GATE: before any H1 cycle is counted, prove the DUT
//! medium **genuinely loses un-synced data** across a real mains cut. If it does
//! not (mounted `sync`, a lying-cache that's actually battery-backed, the WAL dir
//! landing on an overlay instead of the DUT partition, …), every H1 result on it
//! would be VACUOUS — the data was never at risk — which is the single worst
//! outcome this milestone can produce. So we measure loss, hard.
//!
//! WHY A BINARY, NOT THE SHELL `storage-check.sh probe-*`. The calibration must
//! exercise the **same kernel write path the WAL uses**, so that "un-synced data
//! is lost here" actually predicts "an un-acked WAL record is lost here." This
//! binary writes the marker with a plain `write(2)` and **deliberately omits the
//! `fdatasync`** — exactly the WAL's data path (`File` positioned write +
//! `segment::sync_data_fully`) minus the durability step. A shell `echo` could
//! differ subtly (an implicit flush, an `O_SYNC`/mount interaction) and mis-measure
//! the very thing the gate exists to catch. It is **test-only** (excluded from the
//! published crate by `exclude = ["src/bin"]`, like `crash_child`/`power_pull_*`).
//!
//! It is NOT modelled with the WAL `append`: `append` is pure in-memory until
//! `commit`, so the bytes would never reach the page/device cache — the wrong model
//! for "data that hit the device but wasn't fsync'd."
//!
//! Usage (run on the DUT, over ssh from the controller's `h1-cycle.sh calibrate`):
//!   storage_probe write-unsynced-marker <dir>   # write marker, NO fdatasync; exit 0
//!   #   --- real mains power cut + reboot ---
//!   storage_probe verify-marker-gone   <dir>   # exit 0 = GONE (honest), 1 = SURVIVED (vacuous)
//!
//! The verify check is one-directional and fail-safe: a *surviving* marker (exit 1)
//! aborts H1, and a stale already-synced marker can only ever cause a (safe) vacuous
//! abort, never a false PASS.

use std::io::Write;
use std::path::Path;
use std::process::ExitCode;

/// Marker filename, written into the DUT WAL dir. Distinct from any `*.wal` segment
/// so it cannot collide with a WAL; the calibrate step uses a dir with no live WAL.
const MARKER_NAME: &str = ".storage_probe_marker";

fn usage(arg0: &str) -> ExitCode {
    eprintln!(
        "usage: {arg0} <write-unsynced-marker|verify-marker-gone> <dir>\n\
         \n\
         §3.4 calibration loss-probe (owner-run across a REAL power cut):\n  \
         write-unsynced-marker <dir>   write a marker via write(2) with NO fdatasync\n  \
         verify-marker-gone    <dir>   exit 0 = marker GONE (storage lost it; honest cut)\n  \
         {extra:>32}exit 1 = marker SURVIVED (vacuous; abort H1)",
        extra = ""
    );
    ExitCode::from(2)
}

/// Write the marker via the WAL's data write path, then exit WITHOUT syncing — so
/// the bytes live only in the page/device cache, exactly like an un-acked WAL write.
fn write_unsynced_marker(dir: &Path) -> ExitCode {
    let marker = dir.join(MARKER_NAME);
    // Same primitive as a WAL segment data write: a plain `File` write(2). We
    // create+truncate+write and then DROP without `sync_data`/`sync_all` — Rust does
    // not fsync on drop, so nothing here forces the bytes to stable storage.
    let mut f = match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&marker)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("storage_probe: cannot open {}: {e}", marker.display());
            return ExitCode::from(2);
        }
    };
    // A non-trivial payload so the write is a real data write, not a zero-length
    // metadata-only op. Content is irrelevant to the verify (existence is the test);
    // a pid nonce just aids human debugging of a captured marker.
    let body = format!(
        "m8-storage-probe unsynced marker; pid={}; do NOT fsync; a REAL cut must erase this\n",
        std::process::id()
    );
    if let Err(e) = f.write_all(body.as_bytes()) {
        eprintln!("storage_probe: write failed on {}: {e}", marker.display());
        return ExitCode::from(2);
    }
    // Flush the std layer to the OS (the write(2) syscall) — but emphatically NOT
    // `sync_data`/`sync_all`. The marker is now in the cache, at risk of the cut.
    if let Err(e) = f.flush() {
        eprintln!("storage_probe: flush failed on {}: {e}", marker.display());
        return ExitCode::from(2);
    }
    drop(f); // no implicit fsync on drop in Rust
    eprintln!(
        "storage_probe: wrote UN-SYNCED marker {} — now cut power HARD (mains), then verify-marker-gone.",
        marker.display()
    );
    ExitCode::SUCCESS
}

/// After the cut+reboot: the marker MUST be gone. Present ⇒ vacuous (exit 1).
fn verify_marker_gone(dir: &Path) -> ExitCode {
    let marker = dir.join(MARKER_NAME);
    // `symlink_metadata` (lstat) — existence only, no follow, no read that could be
    // confused by an empty file. Either it's there or it isn't.
    match std::fs::symlink_metadata(&marker) {
        Ok(_) => {
            eprintln!(
                "storage_probe: the un-synced marker SURVIVED the cut ({}). Storage did NOT lose \
                 un-synced data ⇒ a power-pull/H1 result here would be VACUOUS. Do NOT certify H1.",
                marker.display()
            );
            ExitCode::from(1)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!(
                "storage_probe: marker is GONE after the cut — storage genuinely loses un-synced \
                 data. H1 on this DUT is meaningful (proceed to the acked-LSN cycle loop)."
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            // Cannot determine ⇒ do NOT claim the cut was honest. Treat as a usage/
            // environment error (exit 2), never a silent pass.
            eprintln!(
                "storage_probe: cannot stat {} ({e}) — cannot confirm loss; NOT a pass.",
                marker.display()
            );
            ExitCode::from(2)
        }
    }
}

fn main() -> ExitCode {
    let a: Vec<String> = std::env::args().collect();
    let arg0 = a.first().map(String::as_str).unwrap_or("storage_probe");
    if a.len() < 3 {
        return usage(arg0);
    }
    let dir = Path::new(&a[2]);
    match a[1].as_str() {
        "write-unsynced-marker" => write_unsynced_marker(dir),
        "verify-marker-gone" => verify_marker_gone(dir),
        _ => usage(arg0),
    }
}
