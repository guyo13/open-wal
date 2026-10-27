//! F2 — single-record decoder fuzz (§14.5).
//!
//! Drives the record codec's `decode` in **isolation** over arbitrary bytes,
//! asserting it never panics / reads OOB and that any returned record is
//! **bounds-sound** (the record-level slice of **D11**). This complements F1
//! (whole-segment / directory recovery) by hammering the framing edge cases —
//! length field, padding, 8-alignment, the 32-bit-safe `u64` sizing — with no
//! segment, file, or scan logic in the way.
//!
//! The raw fuzzer bytes ARE the decode buffer (no `arbitrary` envelope, so the
//! corpus is just record bytes). Each buffer is decoded against a representative
//! set of `max_record_size` thresholds, so the length bound is exercised at every
//! boundary — crucially including `max < payload`, which is what makes the
//! payload-bound assertion non-vacuous (and falsifiable).
//!
//! The structure-aware "mostly-valid record + localized mutation" generation
//! (flip CRC, extend length, tamper padding, reserved `rec_type`) that exercises
//! the **classifier** is F3's job; F2 is the raw bounds-soundness surface.

#![no_main]

use libfuzzer_sys::fuzz_target;

/// The fixed record header size (§5.3); a decoded record's framed length is
/// always header + payload + padding-to-8.
const RECORD_HEADER_SIZE: usize = 20;

/// Boundary-biased `max_record_size` thresholds: zero, sub-/at-/super-alignment,
/// and the extremes. Decoding each buffer against all of them exercises the
/// length bound from both sides for any record the buffer encodes.
const MAXES: [u32; 8] = [0, 1, 7, 8, 64, 4096, 1 << 20, u32::MAX];

fuzz_target!(|data: &[u8]| {
    for &max in &MAXES {
        // `decode_record` returns `Some((payload_len, framed_len))` for a
        // structurally valid record, `None` for any non-record outcome
        // (sentinel / incomplete / invalid). It must never panic or read OOB for
        // ANY input (D11, record level).
        if let Some((payload_len, framed_len)) = open_wal::fuzzing::decode_record(data, max) {
            // 1. the payload honors the configured bound (length checked before
            //    any payload access);
            assert!(
                payload_len as u64 <= u64::from(max),
                "payload_len {payload_len} exceeds max_record_size {max}"
            );
            // 2. the framed record fits within the input slice (no over-read);
            assert!(
                framed_len <= data.len(),
                "framed_len {framed_len} exceeds input len {}",
                data.len()
            );
            // 3. it is a well-formed frame: at least a header, 8-aligned, and the
            //    header + payload fit inside the frame (padding is the remainder).
            assert!(
                framed_len >= RECORD_HEADER_SIZE,
                "framed_len {framed_len} below header size"
            );
            assert!(framed_len % 8 == 0, "framed_len {framed_len} not 8-aligned");
            assert!(
                RECORD_HEADER_SIZE + payload_len <= framed_len,
                "header + payload {} exceeds framed_len {framed_len}",
                RECORD_HEADER_SIZE + payload_len
            );
        }
    }
});