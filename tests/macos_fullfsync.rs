//! macOS `F_FULLFSYNC` check — M8 / §14.8 H4 (macOS-tier execution).
//!
//! On macOS/APFS a plain `fsync` does NOT flush the drive's write cache; durability
//! REQUIRES `fcntl(fd, F_FULLFSYNC)` (§8.3). The WAL routes every durable sync
//! through `segment::sync_data_fully`, which is `fcntl_fullfsync` on macOS and
//! `File::sync_data` elsewhere. These tests assert the macOS durable path actually
//! issues `F_FULLFSYNC`.
//!
//! This whole file is `#[cfg(target_os = "macos")]`, so it is a no-op on Linux
//! (the §14.8 production gate is Linux-only; macOS is the dev/correctness tier).
//! It therefore does NOT compile-check on a Linux CI host — it is exercised on
//! macOS dev/CI. The authoritative manual check is the `dtruss` trace in
//! `docs/m8-runbook.md` (H4); the `#[ignore]`d test below automates it where
//! `dtruss` is permitted (root + a workload it can attach to).
//!
//! Tier: OPEN-pending-macOS (cannot run on the Linux sandbox this was built in).
#![cfg(target_os = "macos")]

use std::process::Command;

use open_wal::{Lsn, Wal, WalConfig};

fn cfg() -> WalConfig {
    WalConfig {
        segment_size: 1 << 20,
        max_record_size: 4096,
    }
}

/// Smoke test (non-ignored, runs in plain macOS CI): a commit on macOS goes
/// through the durable path and recovers. This does not by itself prove
/// `F_FULLFSYNC` was issued (return values are indistinguishable from `fsync`);
/// the `dtruss` test below is the syscall-level proof.
#[test]
fn commit_is_durable_on_macos() {
    let dir = std::env::temp_dir().join(format!("wal-h4-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    {
        let (mut wal, _) = Wal::open(&dir, cfg()).unwrap();
        wal.append(b"durable-on-macos").unwrap();
        assert_eq!(wal.commit().unwrap(), Lsn(1));
    }
    // Reopen ⇒ recovery must surface the committed record (the sync persisted it).
    let (wal, report) = Wal::open(&dir, cfg()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(1));
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let (lsn, payload) = r.next().unwrap().unwrap();
    assert_eq!(lsn, Lsn(1));
    assert_eq!(payload, b"durable-on-macos");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Syscall-trace proof (`#[ignore]`): run a WAL commit under `dtruss -t fcntl` and
/// assert the trace shows an `F_FULLFSYNC` fcntl — i.e. the durable path really
/// issues it, not a plain `fsync`. Requires `dtruss` (root; on SIP-enabled macOS
/// you may need `csrutil` adjustments). Run via:
/// ```text
/// sudo cargo test --test macos_fullfsync -- --ignored --nocapture
/// ```
#[test]
#[ignore = "macOS-only; needs dtruss (root). See docs/m8-runbook.md H4."]
fn durable_path_issues_full_fsync_under_dtruss() {
    // The workload binary that performs a commit (writes 8 records, stdout sink).
    let bin = option_env!("CARGO_BIN_EXE_power_pull_workload")
        .unwrap_or("target/debug/power_pull_workload");
    let dir = std::env::temp_dir().join(format!("wal-h4-dtruss-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // `dtruss -t fcntl <cmd>` traces only fcntl(2). NOTE: on a SIP-enabled host
    // dtruss does NOT symbolize the fcntl command — it prints the raw numeric
    // command (e.g. `fcntl(0x4, 0x33, 0x0) = 0`), where `0x33` == 51 == F_FULLFSYNC
    // (`<sys/fcntl.h>`). So we accept EITHER the symbolic name (non-SIP / future
    // dtruss) OR a numeric fcntl whose command is F_FULLFSYNC.
    let out = Command::new("dtruss")
        .args(["-t", "fcntl", bin])
        .arg(&dir)
        .args(["stdout", "8", "8", "64"])
        .output()
        .expect("spawn dtruss (is it installed? are you root?)");

    // dtruss writes the trace to stderr.
    let trace = String::from_utf8_lossy(&out.stderr);
    let saw_full_fsync = trace.contains("F_FULLFSYNC") || trace.lines().any(line_is_full_fsync);
    assert!(
        saw_full_fsync,
        "expected an F_FULLFSYNC fcntl in the dtruss trace — the macOS durable path \
         must issue F_FULLFSYNC (cmd 0x33), not a plain fsync (§8.3). Trace:\n{trace}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// macOS `fcntl` command number for `F_FULLFSYNC` (`<sys/fcntl.h>`).
const F_FULLFSYNC_CMD: u64 = 51; // 0x33

/// True if a dtruss line is an `fcntl(fd, F_FULLFSYNC, …)` call, matching the raw
/// numeric command dtruss prints under SIP. Lines look like
/// `fcntl(0x4, 0x33, 0x0)\t\t = 0 0`; we read the **second** argument (the command).
fn line_is_full_fsync(line: &str) -> bool {
    let Some(rest) = line.trim_start().strip_prefix("fcntl(") else {
        return false;
    };
    let Some(args) = rest.split(')').next() else {
        return false;
    };
    // args == "0x4, 0x33, 0x0" ⇒ the command is the 2nd comma-separated field.
    let mut fields = args.split(',').map(str::trim);
    let _fd = fields.next();
    fields.next().and_then(parse_c_int) == Some(F_FULLFSYNC_CMD)
}

/// Parse a hex (`0x..`) or decimal integer as dtruss prints it.
fn parse_c_int(s: &str) -> Option<u64> {
    let s = s.trim();
    match s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => s.parse::<u64>().ok(),
    }
}
