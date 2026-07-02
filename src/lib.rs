//! `open-wal` — a focused, embeddable, single-writer append-only write-ahead
//! log for an LMAX-style, event-sourced system.
//!
//! **Durability-first:** a committed record survives process crash and power
//! loss on honest hardware. The WAL stores **opaque byte payloads** —
//! serialization is entirely the caller's concern. It is not a database, not
//! multi-writer, and runs no background threads.
//!
//! The normative design lives in `docs/wal_design_v6.md`; the durability
//! invariants D1–D12 there are binding on every change.
//!
//! This crate is built in milestones (§13). **M0 (foundations)** provides the
//! core value types — [`Lsn`], [`WalConfig`], [`WalError`] — and the CRC-32C
//! checksum ([`crc32c`]). **M1** adds the internal record codec (`record` —
//! encode/decode of the §5.3 framing). **M2** adds the single-segment write
//! path and replay: [`Wal::open`]/[`append`](Wal::append)/[`commit`](Wal::commit),
//! a streaming [`Reader`], the [`DurabilityObserver`] hook, segment
//! pre-allocation and `fdatasync`, and a zero-allocation hot path. **M3** adds
//! intra-segment crash recovery (torn-tail detection + durable zeroing, fatal
//! mid-log corruption). **M4** adds the multi-segment write path — segment roll,
//! commit-time whole-record split, sealed-segment immutability — and
//! multi-segment recovery (discovery, cross-segment continuity, crash-during-roll
//! handling). Checkpoint/retention (M5) arrives later.

// This is an embeddable library; every public item must be documented. With
// CI's `clippy -D warnings`, an undocumented public item fails the build.
#![warn(missing_docs)]

mod config;
mod crc;
mod error;
mod lsn;
mod observer;
mod reader;
mod record;
mod recovery;
mod segment;
mod wal;

pub use config::WalConfig;
pub use crc::crc32c;
pub use error::{Result, WalError};
pub use lsn::Lsn;
pub use observer::{DurabilityObserver, NullObserver};
pub use reader::Reader;
pub use wal::{RecoveryReport, TailState, Wal};

/// Internal hooks exposed **only** under the `fuzzing` feature for the M9
/// cargo-fuzz targets (§14.5). Not part of the public API or the §6 surface:
/// `#[doc(hidden)]` and feature-gated, so a normal build neither sees nor pays
/// for any of it.
///
/// **The public path stays the source of truth.** The F1 recovery fuzzer's
/// *primary* surface is the real public [`Wal::open`] driven over an adversarial
/// directory of segment files — that is the production entry point and the thing
/// actually under test (filename parse → discovery → sort → header validate →
/// cross-segment continuity → `recover_segment`). The helpers here are for the
/// *secondary direct-probe mode only* (the bounded-scan counter and the isolated
/// single-record decoder), plus input *generators* the fuzzer uses to craft
/// valid bytes to feed that public path. They must never become the thing being
/// tested in place of production.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    use std::fs::File;

    use crate::Lsn;

    /// F2 / secondary: decode one framed record from arbitrary bytes, bounded by
    /// `max_record_size`. Returns `Some((payload_len, framed_len))` for a decoded
    /// record (so the harness can assert `payload_len <= max_record_size` and
    /// `framed_len <= buf.len()` — bounds-soundness), `None` for any non-record
    /// outcome. Never panics or reads OOB for any input (D11, record level).
    #[must_use]
    pub fn decode_record(buf: &[u8], max_record_size: u32) -> Option<(usize, usize)> {
        match crate::record::decode(buf, max_record_size) {
            crate::record::Decoded::Record {
                payload,
                framed_len,
                ..
            } => Some((payload.len(), framed_len)),
            _ => None,
        }
    }

    /// F1 secondary direct-probe mode: run intra-segment recovery (§8.2) over a
    /// single open segment `file`, then assert the bounded forward-scan probe
    /// stayed within [`scan_bound`]. Returns whether recovery succeeded; both
    /// `Ok` and a clean `Err` are acceptable (D11), only a panic / OOB / unbounded
    /// scan is a bug. The caller is responsible for the header-validated `base_lsn`
    /// contract that the real `open` enforces upstream (here we pass arbitrary
    /// bases deliberately, but `base_lsn == 0` would underflow `base_lsn - 1`, so
    /// it is clamped to 1 — the lowest legal base).
    #[must_use]
    pub fn recover_segment_probe(
        file: &File,
        base_lsn: u64,
        is_active: bool,
        segment_size: u64,
        max_record_size: u32,
    ) -> bool {
        scan_probe_reset();
        let base = Lsn(base_lsn.max(1));
        let r =
            crate::recovery::recover_segment(file, base, is_active, segment_size, max_record_size);
        let peak = scan_probe_peak();
        assert!(
            peak <= scan_bound(max_record_size),
            "bounded-scan probe peak {peak} exceeded scan_bound {} (max_record_size {max_record_size})",
            scan_bound(max_record_size),
        );
        r.is_ok()
    }

    /// The production bounded forward-scan distance limit, exposed so a fuzz
    /// harness asserts against the **same symbol** the scan loop uses (never a
    /// re-typed number that could silently drift from production).
    #[must_use]
    pub fn scan_bound(max_record_size: u32) -> u64 {
        crate::recovery::scan_bound(max_record_size)
    }

    /// Reset the bounded-scan peak before a recovery run the harness will
    /// bound-check (e.g. around the real [`Wal::open`](crate::Wal::open)).
    pub fn scan_probe_reset() {
        crate::recovery::scan_probe::reset();
    }

    /// The largest forward-scan distance observed since [`scan_probe_reset`].
    #[must_use]
    pub fn scan_probe_peak() -> u64 {
        crate::recovery::scan_probe::peak()
    }

    /// Generator helper: the 64-byte segment header bytes for `base_lsn` (the
    /// `created_unix_nanos` field is informational and fixed to 0 — it never
    /// influences recovery, §8.6). Lets the fuzzer craft *valid* segment files so
    /// the public `Wal::open` reaches discovery / continuity / `recover_segment`
    /// with fuzzer-chosen bases, rather than always tripping on a bad header.
    #[must_use]
    pub fn segment_header_bytes(base_lsn: u64) -> Vec<u8> {
        crate::segment::encode_header(Lsn(base_lsn), 0).to_vec()
    }

    /// Generator helper: append one valid framed record for `lsn`/`payload` to
    /// `buf`, returning the framed byte count. Used to build dense, valid segment
    /// bodies for the public-path fuzzer.
    pub fn encode_record_into(buf: &mut Vec<u8>, lsn: u64, payload: &[u8]) -> usize {
        crate::record::encode_into(buf, Lsn(lsn), payload)
    }

    /// The production per-segment recovery **classification** (§8.2), flattened to
    /// a plain value for the §14.9 differential tester (`tests/differential.rs`).
    /// This is the exact classification surface an independent reference parser
    /// must reproduce byte-for-byte; a divergence is a recovery-classifier bug.
    ///
    /// `Truncated`/`Clean` carry `max_lsn` and the truncation `offset`; the two
    /// fatal arms carry the failing `offset`. `OtherErr` catches any error the
    /// single-segment `recover_segment` is not expected to produce here (e.g. an
    /// I/O error) so the differential can flag it rather than silently coerce it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SegClass {
        /// Clean end of records; `max_lsn` is the highest valid LSN (`base-1` if
        /// the segment is empty).
        Clean { max_lsn: u64 },
        /// Active-segment torn tail truncated at `offset`; `max_lsn` is the last
        /// valid record before it.
        Truncated { offset: u64, max_lsn: u64 },
        /// Mid-log corruption: an invalid record with a valid record still ahead
        /// within the bounded forward scan (active segment) — fatal (D5).
        TornMidLog { offset: u64 },
        /// Any invalid record in a sealed segment — fatal, no forward scan (D5).
        Corruption { offset: u64 },
        /// An error outside the classification surface (e.g. I/O). The differential
        /// treats this as its own class so it is never silently equated.
        OtherErr,
    }

    /// Run the **real** production `recover_segment` (§8.2) over one open segment
    /// `file` and return its classification. Used only by the §14.9 differential
    /// tester to compare production against an independent reference parser.
    ///
    /// NOTE: on a torn tail this performs the production durable zeroing of
    /// `[offset, EOF)` (§8.2.1) as a side effect — so the differential must pass
    /// production its **own** copy of the segment file and read the classification
    /// from this return value, never re-derive it from the mutated file.
    #[must_use]
    pub fn recover_segment_classify(
        file: &File,
        base_lsn: u64,
        is_active: bool,
        segment_size: u64,
        max_record_size: u32,
    ) -> SegClass {
        use crate::error::WalError;
        use crate::wal::TailState;
        let base = Lsn(base_lsn.max(1));
        match crate::recovery::recover_segment(file, base, is_active, segment_size, max_record_size)
        {
            Ok(rec) => match rec.tail_state {
                TailState::Clean => SegClass::Clean {
                    max_lsn: rec.max_lsn.0,
                },
                TailState::TruncatedAt { offset, .. } => SegClass::Truncated {
                    offset,
                    max_lsn: rec.max_lsn.0,
                },
            },
            Err(WalError::TornMidLog { offset, .. }) => SegClass::TornMidLog { offset },
            Err(WalError::Corruption { offset, .. }) => SegClass::Corruption { offset },
            Err(_) => SegClass::OtherErr,
        }
    }
}
