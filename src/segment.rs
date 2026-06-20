//! On-disk segment: header (§5.2), filenames (§5.1), creation/pre-allocation
//! (§5.4), and the sequential record scanner shared by recovery and `Reader`.
//!
//! A segment is a fixed-size, pre-allocated file named `{base_lsn:020}.wal`.
//! Its first 64 bytes are the header; records (§5.3) follow contiguously, and
//! the pre-allocated remainder is zero — the end-of-records sentinel (§5.4).
//!
//! **M2 scope:** clean creation, header validation, and a forward scan that
//! stops at the first non-record (sentinel / short read / invalid). Torn-tail
//! detection, zeroing, the bounded forward scan, and mid-log corruption
//! classification are M3 — they are deliberately not here.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::Lsn;
use crate::crc::crc32c;
use crate::error::{Result, WalError};
use crate::record::{self, Decoded, RECORD_HEADER_SIZE};

/// Fixed segment-header size in bytes (§5.2).
pub(crate) const HEADER_SIZE: u64 = 64;

/// `magic` — ASCII `WAL\0SEG1` (§5.2).
const MAGIC: [u8; 8] = *b"WAL\0SEG1";

/// `format_version` (§5.2).
const FORMAT_VERSION: u16 = 1;

// Field offsets within the 64-byte header.
const VERSION_OFF: usize = 8;
const FLAGS_OFF: usize = 10;
const BASE_LSN_OFF: usize = 12;
const CREATED_OFF: usize = 20;
const HEADER_CRC_OFF: usize = 60;

/// The validated contents of a segment header. `created_unix_nanos` is
/// informational and MUST NOT influence any recovery decision (§8.6); it is
/// retained only for diagnostics.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentHeader {
    pub(crate) base_lsn: Lsn,
}

/// Encode a 64-byte segment header for `base_lsn` (with `created_unix_nanos`),
/// including the trailing CRC over `[0, 60)`.
pub(crate) fn encode_header(base_lsn: Lsn, created_unix_nanos: u64) -> [u8; HEADER_SIZE as usize] {
    let mut h = [0u8; HEADER_SIZE as usize];
    h[0..8].copy_from_slice(&MAGIC);
    h[VERSION_OFF..VERSION_OFF + 2].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    // flags (reserved, 0) and reserved bytes are already zero from the array.
    let _ = FLAGS_OFF;
    h[BASE_LSN_OFF..BASE_LSN_OFF + 8].copy_from_slice(&base_lsn.0.to_le_bytes());
    h[CREATED_OFF..CREATED_OFF + 8].copy_from_slice(&created_unix_nanos.to_le_bytes());
    let crc = crc32c(&h[0..HEADER_CRC_OFF]);
    h[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
    h
}

/// Validate and decode a 64-byte segment header. A bad `magic`, version, or
/// `header_crc` is fatal (`BadSegmentHeader`) — the header is written and synced
/// at creation, before any record, so it is never a torn tail (§5.2).
pub(crate) fn decode_header(bytes: &[u8]) -> Result<SegmentHeader> {
    if bytes.len() < HEADER_SIZE as usize {
        return Err(WalError::BadSegmentHeader);
    }
    if bytes[0..8] != MAGIC {
        return Err(WalError::BadSegmentHeader);
    }
    let stored_crc = u32::from_le_bytes(
        bytes[HEADER_CRC_OFF..HEADER_CRC_OFF + 4]
            .try_into()
            .unwrap(),
    );
    if crc32c(&bytes[0..HEADER_CRC_OFF]) != stored_crc {
        return Err(WalError::BadSegmentHeader);
    }
    let version = u16::from_le_bytes(bytes[VERSION_OFF..VERSION_OFF + 2].try_into().unwrap());
    if version != FORMAT_VERSION {
        return Err(WalError::BadSegmentHeader);
    }
    let base_lsn = Lsn(u64::from_le_bytes(
        bytes[BASE_LSN_OFF..BASE_LSN_OFF + 8].try_into().unwrap(),
    ));
    // `Lsn(0)` is the reserved "none" sentinel and is never a legal segment
    // base (records are dense from 1). Rejecting it keeps recovery total over an
    // adversarial directory (D11) and removes the `base_lsn - 1` underflow on an
    // empty active segment.
    if base_lsn.is_none() {
        return Err(WalError::BadSegmentHeader);
    }
    Ok(SegmentHeader { base_lsn })
}

/// Segment filename for `base_lsn`: 20 decimal digits (`u64::MAX` width) + `.wal`.
pub(crate) fn filename_for(base_lsn: Lsn) -> String {
    format!("{:020}.wal", base_lsn.0)
}

/// Parse `base_lsn` from a segment filename, or `None` if it is not a
/// `{20 digits}.wal` name. Used for directory discovery (§8.1).
pub(crate) fn parse_base_lsn(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".wal")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u64>().ok()
}

/// `fdatasync` that is durable on every supported platform: `F_FULLFSYNC` on
/// macOS (plain fsync does not flush the drive cache there — §8.3), `sync_data`
/// elsewhere. This is the single chokepoint for the durability syscall.
#[cfg(target_os = "macos")]
pub(crate) fn sync_data_fully(file: &File) -> io::Result<()> {
    rustix::fs::fcntl_fullfsync(file).map_err(io::Error::from)
}

/// See the macOS variant above.
#[cfg(not(target_os = "macos"))]
pub(crate) fn sync_data_fully(file: &File) -> io::Result<()> {
    file.sync_data()
}

/// Create, pre-allocate, header-write, and sync a fresh segment at `base_lsn`.
///
/// `O_CREAT|O_EXCL` (never clobber an existing segment), `fallocate` to
/// `segment_size` (the unwritten remainder is zero-filled — the §5.4 sentinel
/// region), `pwrite` the 64-byte header at offset 0, then `sync_data_fully` so
/// the header is durable. The caller is responsible for the **directory** fsync
/// that makes the new filename durable (§7.4 step 5).
pub(crate) fn create(dir: &Path, base_lsn: Lsn, segment_size: u64) -> Result<File> {
    let path = dir.join(filename_for(base_lsn));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;

    rustix::fs::fallocate(&file, rustix::fs::FallocateFlags::empty(), 0, segment_size)
        .map_err(io::Error::from)?;

    let header = encode_header(base_lsn, created_unix_nanos());
    file.write_all_at(&header, 0)?;
    sync_data_fully(&file)?;
    Ok(file)
}

/// Best-effort wall-clock stamp for the informational `created_unix_nanos`
/// header field. Never read back for any decision (§8.6), so a clock that is
/// unavailable degrades harmlessly to 0.
fn created_unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Outcome of reading the record at one scan offset (§8.2 record-level checks).
///
/// `Record` carries the payload's location **within `buf`** (the caller slices
/// `buf[RECORD_HEADER_SIZE..][..payload_len]`); `End` collapses sentinel, short
/// read, and invalid into "no more clean records here" — sufficient for M2's
/// clean scan. M3 will split `End` into the tail-vs-corruption classification.
#[derive(Debug)]
pub(crate) enum ScanOutcome {
    /// A structurally valid record (CRC verified, bounds checked).
    Record {
        /// The record's LSN (continuity is the caller's check, not the codec's).
        lsn: Lsn,
        /// Payload length; payload bytes live at `buf[RECORD_HEADER_SIZE..]`.
        payload_len: usize,
        /// Total framed bytes consumed; advance the scan offset by this.
        framed_len: usize,
    },
    /// No further clean record at this offset (sentinel / short read / invalid).
    End,
}

/// Read and classify the record at `offset` in `file`, into the reusable `buf`.
///
/// All reads are bounded by `segment_size`, so a corrupt `length` can never
/// drive an out-of-bounds or unbounded read (D11 at the I/O level). `buf` is
/// grown with `resize`, which does not reallocate once it has reached the
/// largest record seen — the basis of the §14.7 zero-alloc `Reader::next`.
pub(crate) fn read_record_at(
    file: &File,
    offset: u64,
    segment_size: u64,
    max_record_size: u32,
    buf: &mut Vec<u8>,
) -> Result<ScanOutcome> {
    // `framed` below is computed with the `usize` `framed_size`; that is
    // overflow-safe only because `open()` validated this relation (§5.3), so the
    // framed size of any in-bounds record is ≤ `segment_size - 64`. Document the
    // coupling rather than let an unvalidated caller reintroduce the overflow.
    debug_assert!(
        u64::from(max_record_size) + 91 <= segment_size,
        "scanner assumes an open()-validated config (§5.3)"
    );
    let remaining = segment_size.saturating_sub(offset);
    if remaining < RECORD_HEADER_SIZE as u64 {
        return Ok(ScanOutcome::End);
    }

    // Read the fixed header first to learn `length`, then size the full read.
    buf.resize(RECORD_HEADER_SIZE, 0);
    file.read_exact_at(&mut buf[..RECORD_HEADER_SIZE], offset)?;

    // A short-circuit on the sentinel keeps a cleanly-rolled / partially-filled
    // tail cheap (no length parse, no second read).
    if let Decoded::Sentinel = record::decode(&buf[..RECORD_HEADER_SIZE], max_record_size) {
        return Ok(ScanOutcome::End);
    }

    let length = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if length > max_record_size {
        return Ok(ScanOutcome::End);
    }
    // u64 math: a near-u32::MAX length cannot overflow the framed size here.
    let framed = record::framed_size(length as usize);
    if framed as u64 > remaining {
        // The framed record overruns the segment — a short/torn tail.
        return Ok(ScanOutcome::End);
    }

    // Read the payload + padding tail, then validate the whole framed record.
    buf.resize(framed, 0);
    file.read_exact_at(
        &mut buf[RECORD_HEADER_SIZE..framed],
        offset + RECORD_HEADER_SIZE as u64,
    )?;

    match record::decode(&buf[..framed], max_record_size) {
        Decoded::Record {
            lsn,
            payload,
            framed_len,
        } => {
            let payload_len = payload.len();
            debug_assert_eq!(framed_len, framed);
            Ok(ScanOutcome::Record {
                lsn,
                payload_len,
                framed_len,
            })
        }
        _ => Ok(ScanOutcome::End),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips() {
        let h = encode_header(Lsn(100_001), 1234);
        let decoded = decode_header(&h).expect("valid header");
        assert_eq!(decoded.base_lsn, Lsn(100_001));
    }

    #[test]
    fn corrupted_magic_rejected() {
        let mut h = encode_header(Lsn(1), 0);
        h[0] ^= 0xFF;
        assert!(matches!(decode_header(&h), Err(WalError::BadSegmentHeader)));
    }

    #[test]
    fn corrupted_crc_rejected() {
        // Flip a covered byte without fixing the CRC ⇒ BadSegmentHeader.
        let mut h = encode_header(Lsn(7), 0);
        h[BASE_LSN_OFF] ^= 0x01;
        assert!(matches!(decode_header(&h), Err(WalError::BadSegmentHeader)));
    }

    #[test]
    fn base_lsn_zero_rejected() {
        // base 0 is the reserved Lsn sentinel, never a legal segment base.
        let h = encode_header(Lsn(0), 0);
        assert!(matches!(decode_header(&h), Err(WalError::BadSegmentHeader)));
    }

    #[test]
    fn wrong_version_rejected() {
        let mut h = encode_header(Lsn(1), 0);
        h[VERSION_OFF] = 2;
        // Recompute the CRC so it is the version, not the CRC, that fails.
        let crc = crc32c(&h[0..HEADER_CRC_OFF]);
        h[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(decode_header(&h), Err(WalError::BadSegmentHeader)));
    }

    #[test]
    fn short_header_rejected() {
        assert!(matches!(
            decode_header(&[0u8; 10]),
            Err(WalError::BadSegmentHeader)
        ));
    }

    #[test]
    fn filename_round_trips_including_edges() {
        for base in [1u64, 42, 100_001, u64::MAX] {
            let name = filename_for(Lsn(base));
            assert_eq!(name.len(), 24); // 20 digits + ".wal"
            assert_eq!(parse_base_lsn(&name), Some(base));
        }
    }

    #[test]
    fn parse_rejects_non_segment_names() {
        assert_eq!(parse_base_lsn("LOCK"), None);
        assert_eq!(parse_base_lsn("0000000000000000001.wal"), None); // 19 digits
        assert_eq!(parse_base_lsn("000000000000000000001.wal"), None); // 21 digits
        assert_eq!(parse_base_lsn("0000000000000000000a.wal"), None); // non-digit
        assert_eq!(parse_base_lsn("00000000000000000001.dat"), None); // wrong ext
    }
}
