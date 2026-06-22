//! Shared helpers for the recovery integration suites (corruption / truncation /
//! resurrection / multi-segment). Included via `mod common;` in each test crate.
//!
//! `#![allow(dead_code)]` because each test binary uses only a subset of these,
//! and an unused helper would otherwise trip `-D warnings`.
#![allow(dead_code)]

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use open_wal::{Lsn, Wal, WalConfig};

/// One segment, comfortably larger than any batch these tests write.
pub const SEGMENT_SIZE: u64 = 64 * 1024;
pub const MAX_RECORD_SIZE: u32 = 4096;
pub const HEADER_SIZE: u64 = 64;

pub fn config() -> WalConfig {
    WalConfig {
        segment_size: SEGMENT_SIZE,
        max_record_size: MAX_RECORD_SIZE,
    }
}

/// On-disk framed size of a record with a `len`-byte payload (header + payload +
/// 8-byte-alignment padding) — mirrors the internal `record::framed_size`.
pub fn framed(len: usize) -> u64 {
    let pad = (8 - ((20 + len) % 8)) % 8;
    (20 + len + pad) as u64
}

/// Byte offset of the `i`-th record (0-based) in a segment holding `payloads`.
pub fn offset_of(payloads: &[&[u8]], i: usize) -> u64 {
    HEADER_SIZE + payloads[..i].iter().map(|p| framed(p.len())).sum::<u64>()
}

/// Path of the base-1 segment file in `dir`.
pub fn seg_path(dir: &Path) -> PathBuf {
    dir.join("00000000000000000001.wal")
}

/// Write + commit `payloads` as a clean single-segment log, then close it.
pub fn write_clean(dir: &Path, payloads: &[&[u8]]) {
    let (mut wal, _) = Wal::open(dir, config()).unwrap();
    for p in payloads {
        wal.append(p).unwrap();
    }
    wal.commit().unwrap();
}

/// Flip the bits of one byte at `offset` in the base-1 segment file (durably).
pub fn flip_byte(dir: &Path, offset: u64) {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(seg_path(dir))
        .unwrap();
    let mut b = [0u8; 1];
    f.read_at(&mut b, offset).unwrap();
    b[0] ^= 0xFF;
    f.write_all_at(&b, offset).unwrap();
    f.sync_all().unwrap();
}

/// Read `[from, to)` of the base-1 segment file.
pub fn read_range(dir: &Path, from: u64, to: u64) -> Vec<u8> {
    let f = std::fs::File::open(seg_path(dir)).unwrap();
    let mut buf = vec![0u8; (to - from) as usize];
    f.read_exact_at(&mut buf, from).unwrap();
    buf
}

/// Replay the whole log from the beginning into owned payloads, asserting the
/// recovered LSNs are dense from 1 (D2/D6).
pub fn replay(wal: &Wal) -> Vec<Vec<u8>> {
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let mut out = Vec::new();
    let mut expected = 1u64;
    while let Some(item) = r.next() {
        let (lsn, payload) = item.unwrap();
        assert_eq!(lsn, Lsn(expected), "recovered LSNs must be dense from 1");
        out.push(payload.to_vec());
        expected += 1;
    }
    out
}
