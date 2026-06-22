//! §14.4h / §15.8 — sealed-segment immutability (D12).
//!
//! Once a higher-`base_lsn` segment exists, the segments below it are **sealed**
//! and the writer must never modify their bytes again. We checksum every sealed
//! segment right after it seals, then subject the log to arbitrary further writer
//! activity (more appends, more rolls) and a full close/reopen recovery cycle,
//! and assert each sealed segment is byte-identical — only checkpoint (M5) may
//! remove one, and never mutate it. This is what makes backup and tailing safe
//! (§15.5/§15.6).

use std::path::{Path, PathBuf};

use open_wal::{Wal, WalConfig, crc32c};

/// Tiny segments so a handful of ~200-byte records roll across several segments.
fn tiny() -> WalConfig {
    WalConfig {
        segment_size: 512,
        max_record_size: 256,
    }
}

fn seg(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.wal"))
}

/// Sorted `base_lsn`s of every `*.wal` file in `dir`.
fn discover(dir: &Path) -> Vec<u64> {
    let mut bases: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().into_string().ok()?;
            let digits = name.strip_suffix(".wal")?;
            if digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()) {
                digits.parse::<u64>().ok()
            } else {
                None
            }
        })
        .collect();
    bases.sort_unstable();
    bases
}

/// CRC-32C of a segment file's full contents — a cheap byte-identity fingerprint.
fn fingerprint(dir: &Path, base: u64) -> u32 {
    crc32c(&std::fs::read(seg(dir, base)).unwrap())
}

#[test]
fn sealed_segments_are_immutable_across_activity_and_recovery() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: write enough to create several sealed segments (bases 1, 3, 5;
    // 5 is active, 1 and 3 are sealed).
    {
        let (mut wal, _) = Wal::open(dir.path(), tiny()).unwrap();
        for i in 0..6u8 {
            wal.append(&[i; 200]).unwrap();
        }
        assert_eq!(wal.commit().unwrap().0, 6);
    }

    // Fingerprint every segment that is sealed *now* (all but the highest base).
    let bases = discover(dir.path());
    assert!(
        bases.len() >= 2,
        "expected multiple segments, got {bases:?}"
    );
    let sealed: Vec<(u64, u32)> = bases[..bases.len() - 1]
        .iter()
        .map(|&b| (b, fingerprint(dir.path(), b)))
        .collect();

    // Phase 2: arbitrary further writer activity — more appends + rolls.
    {
        let (mut wal, report) = Wal::open(dir.path(), tiny()).unwrap();
        assert_eq!(report.durable_lsn.0, 6);
        for i in 0..5u8 {
            wal.append(&[100 + i; 200]).unwrap();
        }
        assert_eq!(wal.commit().unwrap().0, 11);
    }

    // Phase 3: a full recovery cycle (open + drop) — tail handling must touch
    // only the active segment, never a sealed one.
    {
        let _ = Wal::open(dir.path(), tiny()).unwrap();
    }

    // D12: every originally-sealed segment is byte-identical.
    for (base, before) in &sealed {
        assert!(
            seg(dir.path(), *base).exists(),
            "sealed segment {base} must still exist (no checkpoint here)"
        );
        assert_eq!(
            fingerprint(dir.path(), *base),
            *before,
            "sealed segment {base} was modified — D12 violated"
        );
    }
}
