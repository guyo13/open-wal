//! F3 — structure-aware tail-vs-corruption fuzz (§14.5).
//!
//! Builds a **mostly-valid** dense single segment (proper header + CRC-correct
//! frames) from a fuzzer-chosen shape, applies ONE **localized mutation** at a
//! fuzzer-chosen record, then drives the real public `Wal::open` and checks the
//! recovery classifier's verdict. This is the target with teeth on the
//! tail-vs-corruption state machine:
//!
//! - **D4** torn tail: a corrupt **last** record truncates at its offset (and the
//!   region is durably zeroed — verified by an idempotent reopen).
//! - **D5** mid-log corruption is **fatal**, never silently truncated: a corrupt
//!   **interior** record (a valid record still follows) makes `open` error.
//! - **D6/D10** no resurrection / no garbage: the surviving suffix is a dense,
//!   byte-identical prefix of the records we built — nothing past the cut, no
//!   mutated bytes surfaced.
//! - **D11** bounded/total: never panics; the forward scan stays within
//!   `scan_bound` (asserted around the production `open`).
//!
//! Because *this code* builds the valid frames (correct CRCs via the `fuzzing`
//! generators), the fuzzer only supplies the scenario shape — so the classifier
//! states a blind byte fuzzer can't reach are hit on every run.

#![no_main]

use std::fs;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use open_wal::{Lsn, TailState, Wal, WalConfig, WalError};

const HEADER_SIZE: usize = 64;
const RECORD_HEADER_SIZE: usize = 20;
const REC_TYPE_OFF: usize = 16;

/// The localized mutation applied to one record.
#[derive(Arbitrary, Debug, Clone, Copy)]
enum Mutation {
    /// Flip a byte of the 4-byte CRC field ⇒ invalid record.
    FlipCrc,
    /// Flip a CRC-covered body byte ⇒ invalid record.
    FlipBody,
    /// Zero the `rec_type` byte ⇒ a corruption the CRC catches (no longer a
    /// sentinel; a genuine sentinel is an all-zero header — issue #26). Invalid.
    ZeroRecType,
    /// Enlarge the `length` field ⇒ CRC mismatch / overrun ⇒ invalid record.
    ExtendLength,
    /// Flip a padding byte (padding is inside CRC coverage) ⇒ invalid record.
    TamperPadding,
    /// Set `rec_type` to a reserved value AND fix the CRC ⇒ a CRC-valid record
    /// the decoder rejects as `UnknownRecType` (invalid).
    ReservedRecType,
}

impl Mutation {
    /// Whether this mutation makes the record *invalid* (a corruption boundary).
    /// After the issue #26 fix the sentinel is recognized only by an all-zero
    /// header, so `ZeroRecType` (zeroing only the `rec_type` byte) leaves a CRC
    /// mismatch the classifier catches — i.e. **every** mutation here now
    /// invalidates the record. Kept as a method for the oracle's structure and any
    /// future non-invalidating mutation.
    fn invalidates(self) -> bool {
        true
    }
}

#[derive(Arbitrary, Debug)]
struct Scenario {
    base: u64,
    seg_big: bool,
    max_sel: u32,
    payloads: Vec<Vec<u8>>,
    mutate: bool,
    mutation: Mutation,
    target_sel: u32,
    byte_sel: u32,
    trailing_zeros: u8,
}

/// Padding to the next 8-byte boundary for a `payload_len`-byte payload.
fn pad_for(payload_len: usize) -> usize {
    (8 - ((RECORD_HEADER_SIZE + payload_len) % 8)) % 8
}
fn framed_size(payload_len: usize) -> usize {
    RECORD_HEADER_SIZE + payload_len + pad_for(payload_len)
}

fuzz_target!(|s: Scenario| {
    // ---- config ----
    let seg: u64 = if s.seg_big { 65536 } else { 4096 };
    let max_hdr = (seg - 91) as u32; // §5.3: max_record_size + 91 <= segment_size
    let max_record_size = s.max_sel % (max_hdr + 1);
    let cfg = WalConfig {
        segment_size: seg,
        max_record_size,
    };
    // base in [1, 1<<40] so base + n never overflows and the header accepts it.
    let base = (s.base % (1u64 << 40)) + 1;

    // ---- build a valid dense segment: header + up to 6 records ----
    let payload_cap = (max_record_size as usize).min(64);
    let mut bytes = open_wal::fuzzing::segment_header_bytes(base);
    // (offset, framed, payload_len) per record, and the original (lsn, payload).
    let mut recs: Vec<(usize, usize, usize)> = Vec::new();
    let mut origs: Vec<Vec<u8>> = Vec::new();
    for raw in s.payloads.iter().take(6) {
        let plen = raw.len().min(payload_cap);
        let off = bytes.len();
        if off + framed_size(plen) > seg as usize {
            break; // keep the whole segment within segment_size (single segment)
        }
        let lsn = base + recs.len() as u64;
        let framed = open_wal::fuzzing::encode_record_into(&mut bytes, lsn, &raw[..plen]);
        recs.push((off, framed, plen));
        origs.push(raw[..plen].to_vec());
    }
    let n = recs.len();

    // ---- apply one localized mutation in the record region ----
    let mut mutated_index: Option<usize> = None;
    if s.mutate && n > 0 {
        let m = (s.target_sel as usize) % n;
        let (off, framed, plen) = recs[m];
        apply_mutation(&mut bytes, s.mutation, off, framed, plen, s.byte_sel);
        mutated_index = Some(m);
    }

    // Optional trailing zero bytes (a partial sentinel region after the records).
    bytes.extend(std::iter::repeat(0u8).take(s.trailing_zeros as usize));

    // ---- materialize and run the REAL public recovery path ----
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(_) => return,
    };
    let path = dir.path().join(format!("{base:020}.wal"));
    if fs::write(&path, &bytes).is_err() {
        return;
    }

    open_wal::fuzzing::scan_probe_reset();
    let res = Wal::open(dir.path(), cfg);
    let peak = open_wal::fuzzing::scan_probe_peak();
    assert!(
        peak <= open_wal::fuzzing::scan_bound(cfg.max_record_size),
        "forward scan exceeded bound: peak {peak} > {}",
        open_wal::fuzzing::scan_bound(cfg.max_record_size)
    );

    match res {
        Ok((wal, report)) => {
            assert_eq!(report.oldest_lsn, Lsn(base), "oldest must be the segment base");
            // durable is within the records we wrote (never invented).
            assert!(
                report.durable_lsn.0 + 1 >= base && report.durable_lsn.0 <= base + n as u64,
                "durable_lsn {} out of range for base {base}, n {n}",
                report.durable_lsn.0
            );

            // Replay: dense run oldest..=durable, byte-identical to the prefix we
            // built (D2/D6/D10 — nothing past the cut, no mutated/garbage bytes).
            let replay = replay(&wal);
            check_dense_prefix(&replay, base, report.durable_lsn.0, &origs);

            // Idempotence + durable zeroing (D7/D10): reopen must succeed, agree on
            // the watermarks, and present a clean tail.
            drop(wal);
            let (_wal2, report2) = Wal::open(dir.path(), cfg).expect("reopen must succeed");
            assert_eq!(report2.durable_lsn, report.durable_lsn, "D7: durable changed on reopen");
            assert_eq!(report2.oldest_lsn, report.oldest_lsn, "D7: oldest changed on reopen");
            assert_eq!(report2.tail_state, TailState::Clean, "D7/D10: reopen tail not clean");

            // ---- sharp classifier oracle ----
            // Every mutation in the menu invalidates the target record (post
            // issue #26, `ZeroRecType` is a CRC-caught corruption too — see
            // `Mutation::invalidates`). An invalid record that nonetheless returned
            // Ok can ONLY be the LAST record (interior corruption is fatal and is
            // handled in the Err arm); it must be a torn-tail truncation at its
            // offset (D4/D5).
            if let Some(m) = mutated_index {
                let (off_m, _, _) = recs[m];
                assert!(s.mutation.invalidates());
                assert_eq!(m, n - 1, "D5: interior corruption returned Ok (silent truncation!)");
                assert_eq!(
                    report.durable_lsn.0,
                    base + m as u64 - 1,
                    "D4: torn-tail durable_lsn wrong"
                );
                match report.tail_state {
                    TailState::TruncatedAt { segment_base, offset } => {
                        assert_eq!(segment_base, Lsn(base));
                        assert_eq!(offset, off_m as u64, "D4: truncation offset wrong");
                    }
                    TailState::Clean => panic!("D4: corrupt last record not reported as truncated"),
                }
            }
        }
        Err(e) => {
            // The ONLY legitimate failure for a header-valid single segment is
            // mid-log corruption: an invalid INTERIOR record with a valid record
            // still after it (D5 — fatal, never silent).
            assert!(
                matches!(
                    e,
                    WalError::TornMidLog { .. } | WalError::Corruption { .. }
                ),
                "unexpected error kind: {e:?}"
            );
            match mutated_index {
                Some(m) if s.mutation.invalidates() && m < n - 1 => { /* expected D5 */ }
                _ => panic!("D5: open errored without an interior corruption (m={mutated_index:?}, mutation={:?}, n={n})", s.mutation),
            }
        }
    }
});

/// Apply `mutation` to record `m` (at absolute `off`, `framed` bytes, `plen`
/// payload) within the segment `bytes`.
fn apply_mutation(bytes: &mut [u8], mutation: Mutation, off: usize, framed: usize, plen: usize, sel: u32) {
    let sel = sel as usize;
    match mutation {
        Mutation::FlipCrc => {
            bytes[off + (sel % 4)] ^= 0xFF;
        }
        Mutation::FlipBody => {
            // Any CRC-covered byte [4, framed).
            bytes[off + 4 + (sel % (framed - 4))] ^= 0xFF;
        }
        Mutation::ZeroRecType => {
            bytes[off + REC_TYPE_OFF] = 0;
        }
        Mutation::ExtendLength => {
            let new_len = (plen as u32).wrapping_add(8);
            bytes[off + 4..off + 8].copy_from_slice(&new_len.to_le_bytes());
        }
        Mutation::TamperPadding => {
            let pad = framed - RECORD_HEADER_SIZE - plen;
            if pad > 0 {
                bytes[off + RECORD_HEADER_SIZE + plen + (sel % pad)] ^= 0xFF;
            } else {
                bytes[off + 4 + (sel % (framed - 4))] ^= 0xFF;
            }
        }
        Mutation::ReservedRecType => {
            bytes[off + REC_TYPE_OFF] = 2; // reserved type
            let crc = open_wal::crc32c(&bytes[off + 4..off + framed]);
            bytes[off..off + 4].copy_from_slice(&crc.to_le_bytes());
        }
    }
}

/// Replay the whole surviving log into `(lsn, payload)` pairs.
fn replay(wal: &Wal) -> Vec<(u64, Vec<u8>)> {
    let mut r = match wal.reader_from(Lsn(0)) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    while let Some(item) = r.next() {
        match item {
            Ok((lsn, payload)) => out.push((lsn.0, payload.to_vec())),
            Err(_) => break,
        }
    }
    out
}

/// Assert the replay is a dense run `base..=durable` and byte-identical to the
/// records we built (`origs[lsn - base]`).
fn check_dense_prefix(replay: &[(u64, Vec<u8>)], base: u64, durable: u64, origs: &[Vec<u8>]) {
    if durable + 1 == base {
        assert!(replay.is_empty(), "empty suffix expected but replay non-empty");
        return;
    }
    let expected_len = (durable - base + 1) as usize;
    assert_eq!(replay.len(), expected_len, "replay length != dense suffix length");
    for (i, (lsn, payload)) in replay.iter().enumerate() {
        assert_eq!(*lsn, base + i as u64, "D2: replay not dense at index {i}");
        assert_eq!(
            payload,
            &origs[i],
            "D6/D10: replayed record {lsn} not byte-identical to the built record"
        );
    }
}
