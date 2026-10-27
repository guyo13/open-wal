//! Storage probe — M8 / §14.8 H1, §3.4 cut-mechanism calibration (owner-run).
//!
//! The §3.4 vacuous-pass GATE: before any H1 cycle is counted, prove the DUT
//! medium **genuinely loses un-synced data** across a real mains cut. If it does
//! not (mounted `sync`, a battery-backed cache, the WAL dir on an overlay instead
//! of the DUT partition, …), every H1 result on it would be VACUOUS — the data was
//! never at risk — the single worst outcome this milestone can produce.
//!
//! WHY A BINARY, AND WHAT IT MIRRORS. The calibration must exercise the **same
//! kernel write path the WAL uses**, so that "un-synced data is lost here" actually
//! predicts "an un-acked WAL record is lost here." So the marker is written exactly
//! like the WAL writes a record into a **pre-allocated** segment:
//!   1. `write-unsynced-marker` first builds the DURABLE part — create the probe
//!      file, zero-fill it to a fixed size, `fdatasync` it, and `fsync` the parent
//!      directory. This mirrors `segment::create` (a pre-allocated, durably-created
//!      segment). This part MUST survive the cut.
//!   2. It then does a **positioned write** of a non-zero marker pattern into that
//!      pre-allocated file **without any sync** — mirroring an un-acked record
//!      written into the segment's reserved space. THIS is the at-risk data.
//!
//! A shell `echo` cannot express "durable file, un-synced bytes inside it" and
//! could mis-measure. This binary is **test-only** (excluded from the published
//! crate by `exclude = ["src/bin"]`, like `crash_child`/`power_pull_*`).
//!
//! It does NOT model this with the WAL `append` (pure in-memory until `commit`, so
//! the bytes never reach the page/device cache — the wrong model).
//!
//! `verify-marker-gone` is **two-sided** (a positive control): a missing or
//! wrong-sized durable file is NOT "gone". The durable part was `fdatasync`'d and the
//! directory `fsync`'d, so if it is absent/truncated after the cut the device did not
//! honor the flush — a **lying-device finding** (exit 3 = DURABLE-LOST, §3.6), which
//! the caller treats as a device-honesty FAIL (it has already boot_id-proven the cut,
//! re-classified the FS and matched the block device). Only a genuine stat/open/read
//! I/O error is INCONCLUSIVE (exit 2). Only the durable file present at its exact size
//! with the marker region **zeroed** proves honest loss (exit 0).
//!
//! Usage (run on the DUT, over ssh from the controller's `h1-cycle.sh calibrate`):
//!   storage_probe write-unsynced-marker <dir>   # durable pre-alloc + un-synced marker; exit 0
//!   #   --- real mains power cut + reboot ---
//!   storage_probe verify-marker-gone   <dir>
//!     exit 0 = marker region GONE/zeroed (honest cut; un-synced data was lost)
//!     exit 1 = marker SURVIVED (vacuous; storage did not lose un-synced data)
//!     exit 3 = DURABLE-LOST (the durably-synced pre-alloc is MISSING or wrong-sized ⇒ the
//!              device did not honor fdatasync/dir-fsync: a lying-device finding, §3.6)
//!     exit 2 = INCONCLUSIVE (a real stat/open/read I/O error) — also the usage-error code.

use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::process::ExitCode;

/// Probe filename, written into the DUT WAL dir. Distinct from any `*.wal` segment;
/// the calibrate step uses a dir with no live WAL.
const PROBE_NAME: &str = ".storage_probe_marker";
/// Durable pre-allocated size (mirrors a small pre-allocated segment). The whole
/// file is zero-filled + synced; this part must survive the cut.
const PROBE_SIZE: u64 = 64 * 1024;
/// The un-synced marker: a positioned write inside the pre-allocated file.
const MARKER_OFFSET: u64 = 4096;
const MARKER_LEN: usize = 256;
/// Non-zero fill so "all zero after the cut" means the un-synced write was lost.
const MARKER_BYTE: u8 = 0xA5;

fn usage(arg0: &str) -> ExitCode {
    eprintln!(
        "usage: {arg0} <write-unsynced-marker|verify-marker-gone> <dir>\n\
         \n\
         §3.4 calibration loss-probe (owner-run across a REAL power cut):\n  \
         write-unsynced-marker <dir>   durable pre-alloc (zero-fill+fdatasync+dir-fsync),\n  \
         {extra:>32}then a positioned un-synced marker write\n  \
         verify-marker-gone    <dir>   0 = marker GONE/zeroed (honest cut)\n  \
         {extra:>32}1 = marker SURVIVED (vacuous)\n  \
         {extra:>32}2 = INCONCLUSIVE (file missing/wrong size) or usage error",
        extra = ""
    );
    ExitCode::from(2)
}

/// Build the durable pre-allocated file, then write an un-synced marker into it.
fn write_unsynced_marker(dir: &Path) -> ExitCode {
    let marker = dir.join(PROBE_NAME);
    // 1. DURABLE pre-allocation (mirrors segment::create): create, zero-fill to a
    //    fixed size, fdatasync the file, fsync the directory. This MUST survive.
    let f = match std::fs::OpenOptions::new()
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
    {
        // Explicit zero-fill (not set_len) so the bytes are really written, like the
        // WAL's fallocate-less pre-allocation fallback.
        let zeros = vec![0u8; PROBE_SIZE as usize];
        let mut w = &f;
        if let Err(e) = w.write_all(&zeros).and_then(|()| w.flush()) {
            eprintln!(
                "storage_probe: zero-fill failed on {}: {e}",
                marker.display()
            );
            return ExitCode::from(2);
        }
    }
    if let Err(e) = f.sync_data() {
        eprintln!(
            "storage_probe: fdatasync failed on {}: {e}",
            marker.display()
        );
        return ExitCode::from(2);
    }
    // fsync the parent directory so the file's creation is itself durable (the
    // classic dir-fsync; mirrors the roll-time dir fsync).
    match std::fs::File::open(dir) {
        Ok(d) => {
            if let Err(e) = d.sync_all() {
                eprintln!("storage_probe: dir fsync failed on {}: {e}", dir.display());
                return ExitCode::from(2);
            }
        }
        Err(e) => {
            eprintln!(
                "storage_probe: cannot open dir {} to fsync: {e}",
                dir.display()
            );
            return ExitCode::from(2);
        }
    }

    // 2. The AT-RISK data: a positioned write of a non-zero marker into the
    //    pre-allocated file, with NO sync (mirrors an un-acked record in reserved
    //    segment space). Rust does not fsync on drop, so it stays in the cache.
    let pattern = [MARKER_BYTE; MARKER_LEN];
    if let Err(e) = f.write_all_at(&pattern, MARKER_OFFSET) {
        eprintln!(
            "storage_probe: marker write failed on {}: {e}",
            marker.display()
        );
        return ExitCode::from(2);
    }
    drop(f); // no implicit fsync on drop
    eprintln!(
        "storage_probe: durable pre-alloc synced; UN-SYNCED marker written at offset {} of {} \
         — now cut power HARD (mains), then verify-marker-gone.",
        MARKER_OFFSET,
        marker.display()
    );
    ExitCode::SUCCESS
}

/// After the cut+reboot: the durable file must still be there at full size (else
/// DURABLE-LOST = the device didn't honor the flush), and the marker region must be
/// zero (gone). Only a genuine I/O error is INCONCLUSIVE.
fn verify_marker_gone(dir: &Path) -> ExitCode {
    let marker = dir.join(PROBE_NAME);
    let meta = match std::fs::symlink_metadata(&marker) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // The pre-alloc was fdatasync'd + dir-fsync'd before the cut. If it is gone,
            // the device did not honor the flush ⇒ DURABLE-LOST (lying device, §3.6),
            // NOT infra. (The caller has already proven the cut + re-classified + matched
            // the block device, so this is not a remount artifact.)
            eprintln!(
                "storage_probe: DURABLE-LOST — the durably-synced probe file {} is MISSING after the \
                 cut. The device acknowledged fdatasync/dir-fsync and then lost it: a lying-device \
                 finding (§3.6). This is a device-honesty FAIL, not infra.",
                marker.display()
            );
            return ExitCode::from(3);
        }
        Err(e) => {
            eprintln!(
                "storage_probe: INCONCLUSIVE — a real I/O error stat-ing {} ({e}).",
                marker.display()
            );
            return ExitCode::from(2);
        }
    };
    if meta.len() != PROBE_SIZE {
        eprintln!(
            "storage_probe: DURABLE-LOST — probe file {} is {} bytes, expected {} (the durably-synced \
             pre-alloc did not survive the cut intact). The device did not honor fdatasync/dir-fsync: \
             a lying-device finding (§3.6). Device-honesty FAIL, not infra.",
            marker.display(),
            meta.len(),
            PROBE_SIZE
        );
        return ExitCode::from(3);
    }

    let f = match std::fs::File::open(&marker) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "storage_probe: INCONCLUSIVE — cannot open {} ({e}).",
                marker.display()
            );
            return ExitCode::from(2);
        }
    };
    let mut buf = [0u8; MARKER_LEN];
    if let Err(e) = f.read_exact_at(&mut buf, MARKER_OFFSET) {
        eprintln!(
            "storage_probe: INCONCLUSIVE — cannot read the marker region of {} ({e}).",
            marker.display()
        );
        return ExitCode::from(2);
    }
    if buf.iter().all(|&b| b == 0) {
        eprintln!(
            "storage_probe: marker region is ZEROED after the cut — the un-synced write was lost \
             while the durable pre-alloc survived. Storage genuinely loses un-synced data; H1 is \
             meaningful (proceed to the cycle loop)."
        );
        ExitCode::SUCCESS
    } else {
        eprintln!(
            "storage_probe: the un-synced marker SURVIVED the cut ({}). Storage did NOT lose \
             un-synced data ⇒ a power-pull/H1 result here would be VACUOUS. Do NOT certify H1.",
            marker.display()
        );
        ExitCode::from(1)
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
