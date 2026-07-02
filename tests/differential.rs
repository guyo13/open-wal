//! §14.9 — differential / reference-parser tester.
//!
//! A deliberately slow, obviously-correct **reference** segment parser, written
//! from the on-disk-format spec (§5.2/§5.3/§8.2) in a **separate code path**, run
//! alongside the production `recover_segment` classifier. **Any divergence in
//! classification is a bug** — this is the one technique that catches a
//! recovery-classifier error *by construction* (two independent implementations
//! disagreeing) rather than probabilistically (a fuzzer happening to hit the
//! trigger). It exists because the issue-#26 sentinel hole was exactly that class
//! of bug: a classifier that mis-mapped one byte pattern.
//!
//! ## Independence (the whole point — see the module's hard rules)
//!
//! The reference below calls **no** production parse code: not `record::decode`,
//! not `recover_segment`, not the `segment.rs` read helpers. It re-derives the
//! constants and re-implements the length-bound check, the all-zero-header
//! sentinel rule, the CRC-validation ordering, the bounded tail-vs-corruption
//! forward scan, and the sealed-vs-active distinction from scratch, reading raw
//! bytes. It **may** use the `crc32c` crate (via `open_wal::crc32c`) — that is a
//! shared *dependency*, not shared *parse logic*; re-implementing CRC-32C would
//! test the crate, not the parser.
//!
//! It implements the **post-issue-#26 contract**: the sentinel is an all-zero
//! 20-byte header, and a `rec_type == 0` record with a non-zero CRC is `Invalid`
//! (→ `TornMidLog` interior / torn-tail at the end), never a clean sentinel. A
//! naive `rec_type == 0 ⇒ sentinel` reference would make the differential fire on
//! the corpus — which would be catching the *reference's* bug (see the
//! falsifiability note in the PR).
//!
//! ## Inputs
//!
//! 1. A deterministic **scenario matrix** (`scenario_cases`) that enumerates every
//!    classification arm — clean runs, torn tails, interior corruption (incl. the
//!    `rec_type→0` case), reserved types, LSN gaps, physical truncation, buried
//!    stale records, garbage — for both active and sealed segments. This is the
//!    exact-match oracle: production and reference must return the identical
//!    `SegClass` variant *and* offsets/`max_lsn`.
//! 2. The committed **fuzz corpora** (`fuzz/corpus/{recovery,structure}`, the
//!    Task-1 regrown set) fed as raw segment *bodies* after a valid header — real
//!    fuzzer-discovered byte patterns over which the two parsers must still agree.
//!    (Scope: we consume the raw corpus bytes as bodies rather than re-decoding
//!    each target's `arbitrary` envelope — duplicating those generators would be
//!    fragile; the differential property "both parsers agree on these bytes" holds
//!    regardless of how the bytes were originally produced. The envelope-specific
//!    deep states are covered exhaustively by the scenario matrix instead.)
//!
//! Requires the `fuzzing` feature (for the `recover_segment_classify` accessor):
//! `cargo test --features fuzzing --test differential`.
#![cfg(feature = "fuzzing")]

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

use open_wal::crc32c;
use open_wal::fuzzing::{self, SegClass};

// ---- constants, re-derived independently from §5.2/§5.3 (NOT imported) ----
const HEADER_SIZE: usize = 64; // §5.2 segment header
const REC_HEADER: usize = 20; // §5.3 record header
const CRC_OFF: usize = 0;
const LEN_OFF: usize = 4;
const LSN_OFF: usize = 8;
const REC_TYPE_OFF: usize = 16;
const REC_TYPE_FULL: u8 = 1;

/// Padding after a `payload_len`-byte payload to the next 8-byte boundary (§5.3):
/// `pad = (8 − ((20 + payload_len) mod 8)) mod 8`. Re-derived here.
fn ref_padding(payload_len: u64) -> u64 {
    (8 - ((REC_HEADER as u64 + payload_len) % 8)) % 8
}

/// The bounded forward-scan distance (§8.2 step 5): the largest frame a single
/// record can occupy — `max_record_size` payload + 20-byte header + up to 7
/// padding + 1. Re-derived independently (production hoists the same value into
/// `recovery::scan_bound`; we do NOT import it — a differential that shared the
/// constant would not be independent).
fn ref_scan_bound(max_record_size: u32) -> u64 {
    u64::from(max_record_size) + 28
}

/// Outcome of reading one candidate record at an offset (mirrors, independently,
/// `segment::read_record_at`'s three-way split).
enum RScan {
    Record { lsn: u64, framed: u64 },
    CleanEnd,
    Invalid,
}

/// Independent, read-only reimplementation of `segment::read_record_at` (§8.2
/// record-level checks). `bytes` is the physical file content; `segment_size` is
/// the logical bound. A read that runs past `bytes.len()` models a short physical
/// read (a file truncated below `segment_size`, §14.4f) ⇒ `Invalid`.
fn ref_read_record_at(bytes: &[u8], offset: u64, segment_size: u64, max_record_size: u32) -> RScan {
    let remaining = segment_size.saturating_sub(offset);
    // §8.2 step 1: fewer than a header's worth of logical space left ⇒ clean end.
    if remaining < REC_HEADER as u64 {
        return RScan::CleanEnd;
    }
    let off = offset as usize;
    // Physical header read: a short read (truncated file) is a candidate boundary.
    let header = match bytes.get(off..off + REC_HEADER) {
        Some(h) => h,
        None => return RScan::Invalid,
    };
    // §8.2 step 1: the end-of-records sentinel is an ALL-ZERO 20-byte header — NOT
    // `rec_type == 0` alone (issue #26). A `rec_type == 0` record with any non-zero
    // header byte falls through to the CRC check below and is `Invalid`.
    if header.iter().all(|&b| b == 0) {
        return RScan::CleanEnd;
    }
    // Length bound BEFORE touching payload (§5.3 / D11): caps `length` at
    // `max_record_size`, so the framed size below cannot be adversarially huge.
    let length = u32::from_le_bytes(header[LEN_OFF..LEN_OFF + 4].try_into().unwrap());
    if length > max_record_size {
        return RScan::Invalid;
    }
    let framed = REC_HEADER as u64 + u64::from(length) + ref_padding(u64::from(length));
    // Framed record must fit the logical remaining space, else short/torn tail.
    if framed > remaining {
        return RScan::Invalid;
    }
    // Physical payload+padding read: a short read is again a truncated file.
    let full = match bytes.get(off..off + framed as usize) {
        Some(f) => f,
        None => return RScan::Invalid,
    };
    // CRC-32C over [4, framed): header tail + payload + padding (§5.3). Using the
    // shared crc32c crate is sanctioned — it is the checksum primitive, not parse
    // logic.
    let stored = u32::from_le_bytes(full[CRC_OFF..CRC_OFF + 4].try_into().unwrap());
    if crc32c(&full[LEN_OFF..framed as usize]) != stored {
        return RScan::Invalid;
    }
    // CRC is intact ⇒ the bytes are genuine; a non-Full type is then a real
    // reserved/unknown record (UnknownRecType), still `Invalid` to recovery.
    if full[REC_TYPE_OFF] != REC_TYPE_FULL {
        return RScan::Invalid;
    }
    let lsn = u64::from_le_bytes(full[LSN_OFF..LSN_OFF + 8].try_into().unwrap());
    RScan::Record { lsn, framed }
}

/// Independent reimplementation of the §8.2 bounded forward scan: from `x + 8`,
/// step 8 bytes at a time up to `x + 8 + bound` (inclusive), looking for a
/// structurally valid record that *continues the log* (`lsn >= expected`, the
/// v6.1 corrected condition). Read-only — it does not zero anything (the
/// classification of a single pass does not depend on the durable zeroing, which
/// only affects a *later* recovery's idempotence).
fn ref_forward_scan_finds_valid(
    bytes: &[u8],
    x: u64,
    expected: u64,
    segment_size: u64,
    max_record_size: u32,
) -> bool {
    let bound = ref_scan_bound(max_record_size);
    let start = x.saturating_add(8);
    let end = start.saturating_add(bound);
    let mut p = start;
    while p <= end {
        if let RScan::Record { lsn, .. } =
            ref_read_record_at(bytes, p, segment_size, max_record_size)
        {
            if lsn >= expected {
                return true;
            }
        }
        p += 8;
    }
    false
}

/// The independent reference classifier (mirrors `recovery::recover_segment` +
/// `classify`, from scratch). Returns the same `SegClass` the production accessor
/// returns, so a divergence is a plain `assert_eq!` failure.
fn reference_classify(
    bytes: &[u8],
    base_lsn: u64,
    is_active: bool,
    segment_size: u64,
    max_record_size: u32,
) -> SegClass {
    // Production clamps an out-of-range base to the lowest legal value (§5.2:
    // Lsn(0) is the reserved sentinel); mirror it so `base - 1` cannot underflow.
    let base = base_lsn.max(1);
    let mut offset = HEADER_SIZE as u64;
    let mut expected = base;
    let mut last_valid = base - 1; // base-1: empty segment ⇒ Clean{base-1} (§8.1)

    loop {
        match ref_read_record_at(bytes, offset, segment_size, max_record_size) {
            RScan::Record { lsn, framed } => {
                if lsn != expected {
                    // A structurally valid record with the wrong LSN is invalid at
                    // this offset (§8.2 step 4) — classify tail vs corruption.
                    return ref_classify_boundary(
                        bytes,
                        base,
                        is_active,
                        segment_size,
                        max_record_size,
                        offset,
                        expected,
                        last_valid,
                    );
                }
                last_valid = lsn;
                offset += framed;
                expected = lsn + 1;
            }
            RScan::CleanEnd => {
                return SegClass::Clean {
                    max_lsn: last_valid,
                };
            }
            RScan::Invalid => {
                return ref_classify_boundary(
                    bytes,
                    base,
                    is_active,
                    segment_size,
                    max_record_size,
                    offset,
                    expected,
                    last_valid,
                );
            }
        }
    }
}

/// Classify an invalid record at offset `x` (§8.2 step 5), independently.
#[allow(clippy::too_many_arguments)]
fn ref_classify_boundary(
    bytes: &[u8],
    _base: u64,
    is_active: bool,
    segment_size: u64,
    max_record_size: u32,
    x: u64,
    expected: u64,
    last_valid: u64,
) -> SegClass {
    if !is_active {
        // A sealed segment is fully synced before the next segment exists (§7.3):
        // no torn tail, any invalid record is fatal corruption. No forward scan.
        return SegClass::Corruption { offset: x };
    }
    if ref_forward_scan_finds_valid(bytes, x, expected, segment_size, max_record_size) {
        // A genuine acked record after the gap ⇒ truncating would drop it (D5).
        SegClass::TornMidLog { offset: x }
    } else {
        // Torn tail: truncate at x (production also durably zeroes [x, EOF)).
        SegClass::Truncated {
            offset: x,
            max_lsn: last_valid,
        }
    }
}

// -------------------------------------------------------------------------
// Harness: write bytes to a real file, run BOTH parsers, assert identical.
// -------------------------------------------------------------------------

struct Harness {
    dir: tempfile::TempDir,
    counter: usize,
    checked: usize,
}

impl Harness {
    fn new() -> Self {
        Harness {
            dir: tempfile::tempdir().expect("tempdir"),
            counter: 0,
            checked: 0,
        }
    }

    /// Run production and reference on the same `bytes` and assert they classify
    /// identically. Production gets its OWN file (it may durably zero a torn tail,
    /// §8.2.1); the reference reads the pristine in-memory bytes.
    fn check(&mut self, label: &str, bytes: &[u8], base: u64, is_active: bool, seg: u64, max: u32) {
        self.counter += 1;
        let path = self.dir.path().join(format!("case-{}.bin", self.counter));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("open case file");
        file.write_all_at(bytes, 0).expect("write case bytes");
        file.sync_data().ok();

        let prod = fuzzing::recover_segment_classify(&file, base, is_active, seg, max);
        let reference = reference_classify(bytes, base, is_active, seg, max);
        assert_eq!(
            prod, reference,
            "DIVERGENCE [{label}] active={is_active} base={base} seg={seg} max={max}: \
             production={prod:?} reference={reference:?}"
        );
        self.checked += 1;
    }
}

// -------------------------------------------------------------------------
// Segment builders (use the production encoders to lay valid bytes; corruption
// is applied afterward, so the reference and production see identical inputs).
// -------------------------------------------------------------------------

fn framed_size(payload_len: usize) -> usize {
    REC_HEADER + payload_len + ref_padding(payload_len as u64) as usize
}

/// Build `header + dense records` for `base`, returning the bytes and the
/// absolute offset of each record.
fn build_segment(base: u64, payloads: &[&[u8]]) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = fuzzing::segment_header_bytes(base);
    let mut offsets = Vec::new();
    for (i, p) in payloads.iter().enumerate() {
        offsets.push(bytes.len());
        fuzzing::encode_record_into(&mut bytes, base + i as u64, p);
    }
    (bytes, offsets)
}

/// Recompute a record's CRC over [4, framed) in place (for reserved-type cases).
fn refix_crc(bytes: &mut [u8], off: usize, framed: usize) {
    let crc = crc32c(&bytes[off + 4..off + framed]);
    bytes[off..off + 4].copy_from_slice(&crc.to_le_bytes());
}

/// The deterministic scenario matrix — one closure per case that returns
/// `(label, bytes, base, seg, max)`; the harness runs each for active AND sealed.
fn scenario_cases(h: &mut Harness) {
    let configs: &[(u64, u32)] = &[(4096, 256), (65536, 4096)];

    for &(seg, max) in configs {
        let base = 1u64;
        let pcap = (max as usize).min(48);
        let p = |n: usize| vec![0xABu8; n.min(pcap)];
        let sizes = [0usize, 1, 7, 8, 20, pcap];

        // 1. Empty segment (header only, padded with sentinel zeros to seg).
        {
            let mut bytes = fuzzing::segment_header_bytes(base);
            bytes.resize(seg as usize, 0);
            for &active in &[true, false] {
                h.check("empty", &bytes, base, active, seg, max);
            }
        }

        // 2. Clean dense runs of k records, various payload sizes, padded to seg.
        for k in 1..=5usize {
            for &sz in &sizes {
                let pl: Vec<Vec<u8>> = (0..k).map(|_| p(sz)).collect();
                let refs: Vec<&[u8]> = pl.iter().map(|v| v.as_slice()).collect();
                let (mut bytes, _offs) = build_segment(base, &refs);
                if bytes.len() > seg as usize {
                    continue;
                }
                bytes.resize(seg as usize, 0);
                for &active in &[true, false] {
                    h.check("clean-run", &bytes, base, active, seg, max);
                }
            }
        }

        // 3. Torn/invalid LAST record, several corruption kinds.
        //    active ⇒ Truncated (or TornMidLog if a continuation is planted);
        //    sealed ⇒ Corruption.
        for kind in [
            "flip_crc",
            "zero_rectype",
            "extend_len",
            "reserved_type",
            "flip_pad",
        ] {
            let pl = [p(8), p(8), p(8)];
            let refs: Vec<&[u8]> = pl.iter().map(|v| v.as_slice()).collect();
            let (mut bytes, offs) = build_segment(base, &refs);
            let last = *offs.last().unwrap();
            let framed = framed_size(8);
            match kind {
                "flip_crc" => bytes[last] ^= 0xFF,
                "zero_rectype" => bytes[last + REC_TYPE_OFF] = 0, // issue #26 vector
                "extend_len" => {
                    let nl = 8u32.wrapping_add(8);
                    bytes[last + 4..last + 8].copy_from_slice(&nl.to_le_bytes());
                }
                "reserved_type" => {
                    bytes[last + REC_TYPE_OFF] = 2;
                    refix_crc(&mut bytes, last, framed);
                }
                "flip_pad" => {
                    // padding byte (payload 8 ⇒ framed 32 ⇒ 4 pad bytes at 28..32)
                    bytes[last + REC_HEADER + 8] ^= 0xFF;
                }
                _ => unreachable!(),
            }
            bytes.resize(seg as usize, 0);
            for &active in &[true, false] {
                h.check(&format!("torn-last-{kind}"), &bytes, base, active, seg, max);
            }
        }

        // 4. Interior corruption (record 1 of 3 corrupt; record 2 valid after it).
        //    active ⇒ TornMidLog; sealed ⇒ Corruption. Covers the issue-#26
        //    interior rec_type→0 vector explicitly.
        for kind in ["flip_crc", "zero_rectype"] {
            let pl = [p(8), p(8), p(8)];
            let refs: Vec<&[u8]> = pl.iter().map(|v| v.as_slice()).collect();
            let (mut bytes, offs) = build_segment(base, &refs);
            let mid = offs[1];
            match kind {
                "flip_crc" => bytes[mid] ^= 0xFF,
                "zero_rectype" => bytes[mid + REC_TYPE_OFF] = 0,
                _ => unreachable!(),
            }
            bytes.resize(seg as usize, 0);
            for &active in &[true, false] {
                h.check(&format!("interior-{kind}"), &bytes, base, active, seg, max);
            }
        }

        // 5. LSN gap: a structurally valid record with a skipped LSN in the middle.
        {
            let (mut bytes, offs) = build_segment(base, &[&p(8), &p(8)]);
            // Overwrite record 2 with a valid record whose LSN is base+5 (a gap).
            let mut rec = fuzzing::segment_header_bytes(base); // scratch, unused header
            rec.clear();
            fuzzing::encode_record_into(&mut rec, base + 5, &p(8));
            let at = offs[1];
            bytes[at..at + rec.len()].copy_from_slice(&rec);
            bytes.resize(seg as usize, 0);
            for &active in &[true, false] {
                h.check("lsn-gap", &bytes, base, active, seg, max);
            }
        }

        // 6. Sentinel (all-zero header) mid-run ⇒ Clean at that offset.
        {
            let (mut bytes, offs) = build_segment(base, &[&p(8), &p(8)]);
            let at = offs[1];
            for b in &mut bytes[at..at + REC_HEADER] {
                *b = 0;
            }
            bytes.resize(seg as usize, 0);
            for &active in &[true, false] {
                h.check("mid-sentinel", &bytes, base, active, seg, max);
            }
        }

        // 7. Physically truncated file mid-last-record (short read ⇒ Invalid).
        {
            let (bytes, offs) = build_segment(base, &[&p(8), &p(8), &p(8)]);
            let last = *offs.last().unwrap();
            let cut = last + REC_HEADER + 2; // mid-way through the last record
            let short = bytes[..cut.min(bytes.len())].to_vec();
            for &active in &[true, false] {
                h.check("phys-truncated", &short, base, active, seg, max);
            }
        }

        // 8. Interior torn tail with a genuine continuation just WITHIN the bound
        //    (active ⇒ TornMidLog) and one just BEYOND it (active ⇒ Truncated).
        for within in [true, false] {
            let (mut bytes, offs) = build_segment(base, &[&p(8)]);
            let x = offs[0] + framed_size(8); // offset just past record 1 (expected base+1)
            // A torn record at x (bad CRC).
            let mut torn = Vec::new();
            fuzzing::encode_record_into(&mut torn, base + 1, &p(4));
            torn[0] ^= 0xFF;
            if x + torn.len() <= seg as usize {
                bytes.resize(x, 0);
                bytes.extend_from_slice(&torn);
            }
            // Plant a valid continuation (lsn base+1) within/beyond the scan bound.
            let bound = ref_scan_bound(max);
            let end = (x as u64) + 8 + bound;
            let cont_off = if within {
                ((end / 8) * 8) as usize // largest 8-aligned start <= end
            } else {
                (((end / 8) * 8) + 8) as usize // first strictly beyond
            };
            let mut cont = Vec::new();
            fuzzing::encode_record_into(&mut cont, base + 1, &p(8));
            let needed = cont_off + cont.len();
            if needed <= seg as usize {
                if bytes.len() < needed {
                    bytes.resize(needed, 0);
                }
                bytes[cont_off..cont_off + cont.len()].copy_from_slice(&cont);
                bytes.resize(seg as usize, 0);
                for &active in &[true, false] {
                    let label = if within {
                        "cont-within-bound"
                    } else {
                        "cont-beyond-bound"
                    };
                    h.check(label, &bytes, base, active, seg, max);
                }
            }
        }

        // 9. Reserved rec_type on record 1 of 2 (CRC fixed) ⇒ UnknownRecType.
        //    active ⇒ TornMidLog (valid record 2 follows); sealed ⇒ Corruption.
        {
            let (mut bytes, offs) = build_segment(base, &[&p(8), &p(8)]);
            let at = offs[0];
            bytes[at + REC_TYPE_OFF] = 3;
            refix_crc(&mut bytes, at, framed_size(8));
            bytes.resize(seg as usize, 0);
            for &active in &[true, false] {
                h.check("reserved-interior", &bytes, base, active, seg, max);
            }
        }

        // 10. length > max_record_size at the first record ⇒ Invalid boundary.
        {
            let (mut bytes, offs) = build_segment(base, &[&p(8), &p(8)]);
            let at = offs[0];
            let huge = max.wrapping_add(1);
            bytes[at + 4..at + 8].copy_from_slice(&huge.to_le_bytes());
            bytes.resize(seg as usize, 0);
            for &active in &[true, false] {
                h.check("len-over-max", &bytes, base, active, seg, max);
            }
        }

        // 11. A non-1 base (offsets/max_lsn must track it) with a torn tail.
        {
            let b2 = 1000u64;
            let (mut bytes, offs) = build_segment(b2, &[&p(8), &p(8)]);
            let last = *offs.last().unwrap();
            bytes[last] ^= 0xFF;
            bytes.resize(seg as usize, 0);
            for &active in &[true, false] {
                h.check("nonone-base-torn", &bytes, b2, active, seg, max);
            }
        }
    }
}

#[test]
fn differential_scenario_matrix() {
    let mut h = Harness::new();
    scenario_cases(&mut h);
    assert!(
        h.checked > 100,
        "expected a broad scenario matrix, ran {}",
        h.checked
    );
    eprintln!("differential scenario matrix: {} cases agreed", h.checked);
}

#[test]
fn differential_over_fuzz_corpora() {
    // The committed (Task-1 regrown) corpora, consumed as raw segment bodies.
    // Config chosen so a body up to ~64 KiB fits after the header.
    const SEG: u64 = 65536;
    const MAX: u32 = 4096;
    let mut h = Harness::new();

    for sub in ["recovery", "structure", "decode", "model"] {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fuzz/corpus")
            .join(sub);
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue, // corpus dir absent in some checkouts — skip, not fail
        };
        for entry in entries.flatten() {
            let raw = match std::fs::read(entry.path()) {
                Ok(b) => b,
                Err(_) => continue,
            };
            // Build a valid header + the corpus bytes as the record body, capped
            // to the segment body capacity. Both parsers see identical bytes.
            let cap = (SEG as usize) - HEADER_SIZE;
            let body = &raw[..raw.len().min(cap)];
            let mut bytes = fuzzing::segment_header_bytes(1);
            bytes.extend_from_slice(body);
            for &active in &[true, false] {
                h.check(&format!("corpus/{sub}"), &bytes, 1, active, SEG, MAX);
            }
        }
    }
    eprintln!(
        "differential over fuzz corpora: {} inputs agreed",
        h.checked
    );
    // Not asserting a minimum count: a fresh checkout may have a thin corpus. The
    // scenario matrix carries the exact-match coverage; this pass adds breadth.
}
