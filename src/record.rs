//! Record codec — encode/decode of a single framed record (§5.3).
//!
//! This is the **M1** layer: a pure, in-memory codec for the on-disk record
//! framing. It performs **no I/O, no allocation in steady state, and no segment
//! logic** — segments, rolls, and fsync discipline live in the write/recovery
//! paths (M2+). A record is laid out as a fixed 20-byte header followed by the
//! opaque payload and zero padding to the next 8-byte boundary:
//!
//! | Offset | Size | Field | Notes |
//! |---|---|---|---|
//! | 0 | 4 | `crc` | CRC-32C over `[4, 4 + 16 + length + pad)` — header tail, payload, **and** padding |
//! | 4 | 4 | `length` | `u32` payload length |
//! | 8 | 8 | `lsn` | `u64` |
//! | 16 | 1 | `rec_type` | `1` = Full; `0` = sentinel (never a real record); `2..` reserved |
//! | 17 | 1 | `rflags` | reserved, 0 |
//! | 18 | 2 | `reserved` | 0 |
//! | 20 | `length` | `payload` | opaque caller bytes |
//! | 20+`length` | `pad` | `padding` | zeros to the next 8-byte boundary |
//!
//! The CRC sits at the front so a reader can validate the whole record once
//! `length` is known; `length` and `padding` are **inside** CRC coverage, which
//! closes the "hide bytes in the padding" gap (§5.3). The decoder is bounded and
//! never panics or reads out of bounds for any input — the record-level slice of
//! D11. LSN-continuity is *not* a codec concern: recovery (M3) layers
//! `lsn == expected_next` on top of [`decode`].

use crate::Lsn;
use crate::crc::crc32c;

/// Size of the fixed record header in bytes (§5.3).
pub(crate) const RECORD_HEADER_SIZE: usize = 20;

/// `rec_type` for a full (non-fragmented) record — the only real record type in
/// v1. `2..` are reserved for future fragmentation.
const REC_TYPE_FULL: u8 = 1;

/// `rec_type` sentinel / zero. Never a real record; marks the end-of-records in
/// a partially-filled or cleanly-rolled segment (the pre-allocated zero region,
/// §5.4).
const REC_TYPE_SENTINEL: u8 = 0;

// Field offsets within a framed record.
const LEN_OFF: usize = 4;
const LSN_OFF: usize = 8;
const REC_TYPE_OFF: usize = 16;
const PAYLOAD_OFF: usize = RECORD_HEADER_SIZE;

/// Reusable source of zero padding bytes (`pad` is always `0..=7`).
const ZERO_PAD: [u8; 7] = [0u8; 7];

/// Number of zero padding bytes after a `payload_len`-byte payload, to reach the
/// next 8-byte boundary: `pad = (8 − ((20 + payload_len) mod 8)) mod 8` (§5.3).
#[inline]
#[must_use]
pub(crate) const fn padding_for(payload_len: usize) -> usize {
    (8 - ((RECORD_HEADER_SIZE + payload_len) % 8)) % 8
}

/// `padding_for` over a `u64` length, so the decoder can size a record without
/// risking a `usize` overflow on a 32-bit target (D11: never panic for *any*
/// input). `+ 4` is `RECORD_HEADER_SIZE mod 8`; reducing mod 8 keeps the
/// intermediate small, so this cannot overflow even for `length == u64::MAX`.
#[inline]
#[must_use]
const fn padding_for_u64(payload_len: u64) -> u64 {
    (8 - ((RECORD_HEADER_SIZE as u64 % 8 + payload_len % 8) % 8)) % 8
}

/// Total on-disk framed size of a record with a `payload_len`-byte payload
/// (header + payload + padding). Always a multiple of 8.
#[inline]
#[must_use]
pub(crate) const fn framed_size(payload_len: usize) -> usize {
    RECORD_HEADER_SIZE + payload_len + padding_for(payload_len)
}

/// `framed_size` computed entirely in `u64`, so a near-`u32::MAX` `payload_len`
/// cannot wrap on a 32-bit target (where `usize` is 32-bit and `20 + len` would
/// overflow). Used by the recovery scanner to size a read **before** narrowing
/// to `usize`, keeping recovery panic-free for any on-disk `length` (D11).
#[inline]
#[must_use]
pub(crate) const fn framed_size_u64(payload_len: u64) -> u64 {
    RECORD_HEADER_SIZE as u64 + payload_len + padding_for_u64(payload_len)
}

/// Outcome of decoding one record from the front of a byte slice.
///
/// The borrow of [`Decoded::Record::payload`] is tied to the input slice.
#[derive(Debug)]
pub(crate) enum Decoded<'a> {
    /// A structurally valid `Full` record: CRC verified, bounds checked.
    Record {
        /// The record's log sequence number (not validated for continuity here).
        lsn: Lsn,
        /// The opaque payload, borrowed from the input slice.
        payload: &'a [u8],
        /// Total framed size consumed (header + payload + padding); the caller
        /// advances its scan offset by this amount.
        framed_len: usize,
    },
    /// A `rec_type == 0` header: the end-of-records sentinel / pre-allocated zero
    /// region (§5.4). Not a record.
    Sentinel,
    /// Fewer bytes than a full header, or the framed record would overrun the
    /// slice. At a physical tail this is a short/torn write; the codec does not
    /// classify tail-vs-corruption (that is recovery's job, §8.2).
    Incomplete,
    /// A header is present but the record is not valid. The inner reason is
    /// consumed by recovery's tail-vs-corruption classification (M3); M2's clean
    /// scan collapses it to end-of-records, so it is not read in the library
    /// build yet.
    Invalid(#[allow(dead_code)] DecodeError),
}

/// Why a present record header failed validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeError {
    /// `length` exceeds the configured `max_record_size`. Checked **before** any
    /// payload access, so a corrupt or adversarial length can never drive an
    /// out-of-bounds read or unbounded work (D11).
    LengthTooLarge,
    /// CRC-32C over `[4, framed)` did not match the stored checksum — corruption
    /// somewhere in the header tail, payload, or padding.
    BadCrc,
    /// CRC matched but `rec_type` is neither `0` (sentinel) nor `1` (Full): a
    /// reserved/unknown type that v1 never writes.
    UnknownRecType,
}

/// Encode one framed record for `lsn`/`payload`, appending it to `buf`, and
/// return the framed byte count.
///
/// Appending to a reused `Vec` keeps the steady-state hot path allocation-free
/// (M2's `append` reuses a staging buffer). This is pure memory: no I/O, no
/// segment logic.
///
/// The caller is responsible for the `max_record_size` precondition; since
/// `max_record_size` is a `u32`, a validated payload always fits the `length`
/// field. The `debug_assert!` documents that contract.
pub(crate) fn encode_into(buf: &mut Vec<u8>, lsn: Lsn, payload: &[u8]) -> usize {
    debug_assert!(
        payload.len() <= u32::MAX as usize,
        "payload length must fit u32 (caller enforces max_record_size)"
    );
    let length = payload.len() as u32;
    let pad = padding_for(payload.len());
    let start = buf.len();

    // One up-front reservation so a cold/under-capacity staging buffer grows
    // exactly once for this record instead of reallocating across the writes
    // below — keeps the M2 `append` hot path's growth deterministic.
    buf.reserve(framed_size(payload.len()));

    // 4-byte CRC placeholder; backfilled once the body is laid down.
    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(&length.to_le_bytes());
    buf.extend_from_slice(&lsn.0.to_le_bytes());
    buf.push(REC_TYPE_FULL);
    buf.push(0); // rflags (reserved, MUST be 0)
    buf.extend_from_slice(&[0u8; 2]); // reserved (MUST be 0)
    debug_assert_eq!(buf.len() - start, RECORD_HEADER_SIZE);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(&ZERO_PAD[..pad]);

    let framed = buf.len() - start;
    debug_assert_eq!(framed, framed_size(payload.len()));

    // CRC covers everything after the checksum field: header tail + payload +
    // padding (§5.3).
    let crc = crc32c(&buf[start + LEN_OFF..start + framed]);
    buf[start..start + LEN_OFF].copy_from_slice(&crc.to_le_bytes());

    framed
}

/// Decode the record at the front of `buf`, bounded by `max_record_size`.
///
/// Never panics and never reads out of bounds for any input (D11, record level).
/// Ordering mirrors the §8.2 record-level checks: sentinel, length bound, short
/// read, CRC, then unknown type.
pub(crate) fn decode(buf: &[u8], max_record_size: u32) -> Decoded<'_> {
    if buf.len() < RECORD_HEADER_SIZE {
        return Decoded::Incomplete;
    }

    // rec_type == 0 is the end-of-records sentinel regardless of the other
    // (zeroed) header bytes (§8.2 step 1).
    let rec_type = buf[REC_TYPE_OFF];
    if rec_type == REC_TYPE_SENTINEL {
        return Decoded::Sentinel;
    }

    let length = u32::from_le_bytes(buf[LEN_OFF..LEN_OFF + 4].try_into().unwrap());

    // Bound the length BEFORE touching any payload bytes — this is what makes the
    // decoder immune to a corrupt/huge length (no OOB, no unbounded work).
    if length > max_record_size {
        return Decoded::Invalid(DecodeError::LengthTooLarge);
    }

    // Size the framed record in u64 to avoid a `usize` overflow on 32-bit targets
    // (a near-`u32::MAX` `length` would overflow `20 + length + pad` there) — D11
    // requires no panic for *any* input. If it overruns the slice it is a short
    // read; otherwise `framed <= buf.len() <= usize::MAX`, so the cast is safe.
    let framed_u64 =
        RECORD_HEADER_SIZE as u64 + u64::from(length) + padding_for_u64(u64::from(length));
    if framed_u64 > buf.len() as u64 {
        // Short read: a torn tail or a record split by the slice boundary.
        return Decoded::Incomplete;
    }
    let length = length as usize;
    let framed = framed_u64 as usize;

    let stored_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let computed_crc = crc32c(&buf[LEN_OFF..framed]);
    if computed_crc != stored_crc {
        return Decoded::Invalid(DecodeError::BadCrc);
    }

    // CRC is intact, so the bytes are genuine; a non-Full type is then a real
    // reserved/unknown record, not corruption.
    if rec_type != REC_TYPE_FULL {
        return Decoded::Invalid(DecodeError::UnknownRecType);
    }

    let lsn = Lsn(u64::from_le_bytes(
        buf[LSN_OFF..LSN_OFF + 8].try_into().unwrap(),
    ));
    Decoded::Record {
        lsn,
        payload: &buf[PAYLOAD_OFF..PAYLOAD_OFF + length],
        framed_len: framed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A generous `max_record_size` for tests that are not exercising the bound.
    const MAX: u32 = 1 << 20;

    /// Decode helper that asserts a clean round-trip of `(lsn, payload)`.
    fn assert_roundtrip(lsn: Lsn, payload: &[u8]) {
        let mut buf = Vec::new();
        let framed = encode_into(&mut buf, lsn, payload);

        // Encoded size is always 8-aligned and matches the size helper.
        assert_eq!(framed, framed_size(payload.len()));
        assert_eq!(framed % 8, 0, "framed size must be 8-aligned");
        assert_eq!(buf.len(), framed);

        match decode(&buf, MAX) {
            Decoded::Record {
                lsn: got_lsn,
                payload: got_payload,
                framed_len,
            } => {
                assert_eq!(got_lsn, lsn);
                assert_eq!(got_payload, payload);
                assert_eq!(framed_len, framed);
            }
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_alignment_edge_sizes() {
        // §14.1: sizes 0, 1, 7, 8, 9 straddle the 8-byte alignment boundary.
        for len in [0usize, 1, 7, 8, 9] {
            let payload: Vec<u8> = (0..len).map(|i| i as u8).collect();
            assert_roundtrip(Lsn(1), &payload);
        }
    }

    #[test]
    fn roundtrip_spread_and_max() {
        for len in [16usize, 100, 1000, MAX as usize] {
            let payload: Vec<u8> = (0..len).map(|i| (i * 31 + 7) as u8).collect();
            assert_roundtrip(Lsn(42), &payload);
        }
    }

    #[test]
    fn padding_formula_and_zero_bytes() {
        // §14.1: pad formula correct; the padding bytes in the encoding are zero.
        for len in 0usize..=64 {
            let expected_pad = (8 - ((RECORD_HEADER_SIZE + len) % 8)) % 8;
            assert_eq!(padding_for(len), expected_pad);
            assert_eq!(framed_size(len), RECORD_HEADER_SIZE + len + expected_pad);

            let payload = vec![0xABu8; len];
            let mut buf = Vec::new();
            let framed = encode_into(&mut buf, Lsn(1), &payload);
            // Trailing `pad` bytes must be zero.
            for &b in &buf[PAYLOAD_OFF + len..framed] {
                assert_eq!(b, 0);
            }
        }
    }

    #[test]
    fn padding_is_inside_crc_coverage() {
        // §14.1 / D11: flipping a padding byte MUST fail the CRC. len 1 ⇒ pad 3.
        let mut buf = Vec::new();
        let framed = encode_into(&mut buf, Lsn(1), &[0x55]);
        let pad = padding_for(1);
        assert!(pad > 0, "need a padded payload to test this");

        // Flip the first padding byte (just past the 1-byte payload).
        let pad_idx = PAYLOAD_OFF + 1;
        assert!(pad_idx < framed);
        buf[pad_idx] ^= 0xFF;

        assert!(matches!(
            decode(&buf, MAX),
            Decoded::Invalid(DecodeError::BadCrc)
        ));
    }

    #[test]
    fn payload_and_crc_field_corruption_detected() {
        let mut buf = Vec::new();
        encode_into(&mut buf, Lsn(7), &[1, 2, 3, 4, 5]);

        // Corrupt a payload byte.
        let mut a = buf.clone();
        a[PAYLOAD_OFF] ^= 0x01;
        assert!(matches!(
            decode(&a, MAX),
            Decoded::Invalid(DecodeError::BadCrc)
        ));

        // Corrupt the front CRC field itself (outside [4, framed) coverage, but
        // the stored value now disagrees with the body).
        let mut b = buf.clone();
        b[0] ^= 0x01;
        assert!(matches!(
            decode(&b, MAX),
            Decoded::Invalid(DecodeError::BadCrc)
        ));
    }

    #[test]
    fn length_over_max_rejected_without_oob() {
        // §14.1 / D11: a length beyond max_record_size is rejected before any
        // payload access. Build a header claiming a huge length over a tiny buf.
        let mut buf = vec![0u8; RECORD_HEADER_SIZE];
        buf[REC_TYPE_OFF] = REC_TYPE_FULL;
        buf[LEN_OFF..LEN_OFF + 4].copy_from_slice(&u32::MAX.to_le_bytes());

        assert!(matches!(
            decode(&buf, 64),
            Decoded::Invalid(DecodeError::LengthTooLarge)
        ));
    }

    #[test]
    fn framed_overrun_is_incomplete_without_oob() {
        // A plausible length (within max) whose framed record overruns the slice
        // ⇒ Incomplete (short read), never an OOB panic.
        let mut buf = vec![0u8; RECORD_HEADER_SIZE];
        buf[REC_TYPE_OFF] = REC_TYPE_FULL;
        buf[LEN_OFF..LEN_OFF + 4].copy_from_slice(&100u32.to_le_bytes());
        // Only the header is present; payload bytes are missing.
        assert!(matches!(decode(&buf, MAX), Decoded::Incomplete));
    }

    #[test]
    fn short_buffer_is_incomplete() {
        // Fewer than a header's worth of bytes (incl. empty) ⇒ Incomplete, no
        // panic (D11).
        for n in 0..RECORD_HEADER_SIZE {
            assert!(matches!(decode(&vec![0xFFu8; n], MAX), Decoded::Incomplete));
        }
    }

    #[test]
    fn sentinel_header_detected() {
        // rec_type == 0 ⇒ Sentinel, even with otherwise garbage bytes.
        let mut buf = vec![0xFFu8; RECORD_HEADER_SIZE];
        buf[REC_TYPE_OFF] = REC_TYPE_SENTINEL;
        assert!(matches!(decode(&buf, MAX), Decoded::Sentinel));

        // The all-zero pre-allocated region is also a sentinel.
        assert!(matches!(
            decode(&[0u8; RECORD_HEADER_SIZE], MAX),
            Decoded::Sentinel
        ));
    }

    #[test]
    fn unknown_rec_type_with_valid_crc_is_invalid() {
        // Encode a valid record, retype it to a reserved value, and recompute the
        // CRC so the bytes are intact ⇒ UnknownRecType (not BadCrc).
        let mut buf = Vec::new();
        let framed = encode_into(&mut buf, Lsn(3), &[9, 8, 7]);
        buf[REC_TYPE_OFF] = 2; // reserved type
        let crc = crc32c(&buf[LEN_OFF..framed]);
        buf[0..LEN_OFF].copy_from_slice(&crc.to_le_bytes());

        assert!(matches!(
            decode(&buf, MAX),
            Decoded::Invalid(DecodeError::UnknownRecType)
        ));
    }

    #[test]
    fn lsn_roundtrips_small_and_large() {
        assert_roundtrip(Lsn(1), b"first");
        assert_roundtrip(Lsn(u64::MAX), b"last");
    }

    #[test]
    fn padding_for_u64_matches_usize_helper() {
        // The 32-bit-safe decoder path must agree with the const helper used by
        // the encoder, including past the u32 range it guards against.
        for len in 0u64..=64 {
            assert_eq!(padding_for_u64(len), padding_for(len as usize) as u64);
        }
        // Equivalent reduction at and beyond u32::MAX (where the usize form would
        // overflow on a 32-bit target).
        for len in [u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX] {
            let expected = (8 - ((20u64 + len % 8) % 8)) % 8;
            assert_eq!(padding_for_u64(len), expected);
        }
    }

    #[test]
    fn framed_size_u64_matches_and_does_not_wrap() {
        // Agrees with the usize helper across normal sizes.
        for len in [0usize, 1, 7, 8, 9, 20, 4096] {
            assert_eq!(framed_size_u64(len as u64), framed_size(len) as u64);
        }
        // The scanner sizes reads with this: a near-`u32::MAX` `length` (the
        // largest a record header can encode) must yield its true, un-wrapped
        // framed size — on a 32-bit target the usize `framed_size` would overflow
        // `20 + len` and could collapse below the 20-byte header, panicking the
        // payload slice in `read_record_at` (D11).
        let big = u32::MAX as u64;
        let f = framed_size_u64(big);
        assert!(
            f >= 20 + big,
            "framed_size_u64 must not wrap for a huge length"
        );
        assert_eq!(f % 8, 0, "framed size is always 8-aligned");
    }

    proptest! {
        /// Round-trip fidelity for arbitrary payloads (D6 at the codec level).
        #[test]
        fn prop_roundtrip(lsn in any::<u64>(), payload in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let mut buf = Vec::new();
            let framed = encode_into(&mut buf, Lsn(lsn), &payload);
            prop_assert_eq!(framed, framed_size(payload.len()));
            prop_assert_eq!(framed % 8, 0);
            match decode(&buf, MAX) {
                Decoded::Record { lsn: got, payload: got_p, framed_len } => {
                    prop_assert_eq!(got, Lsn(lsn));
                    prop_assert_eq!(got_p, &payload[..]);
                    prop_assert_eq!(framed_len, framed);
                }
                other => prop_assert!(false, "expected Record, got {:?}", other),
            }
        }

        /// Padding-in-CRC: flipping ANY single byte of the framed record (CRC
        /// field, length, lsn, type, payload, AND padding) is detected — the
        /// decode never returns a Record byte-identical to the original. This is
        /// the assertion that the whole `[0, framed)` region, padding included,
        /// is CRC-protected.
        #[test]
        fn prop_single_bit_flip_is_detected(
            payload in proptest::collection::vec(any::<u8>(), 0..256),
            bit in any::<u8>(),
        ) {
            let mut buf = Vec::new();
            let framed = encode_into(&mut buf, Lsn(1), &payload);

            let idx = (bit as usize) % framed;
            let bitpos = bit % 8;
            buf[idx] ^= 1 << bitpos;

            // After any single bit flip, we must NOT see the original record.
            let resurfaced = matches!(
                decode(&buf, MAX),
                Decoded::Record { lsn, payload: p, .. }
                    if lsn == Lsn(1) && p == &payload[..]
            );
            prop_assert!(!resurfaced, "flip at byte {} bit {} was not detected", idx, bitpos);
        }

        /// Bounded, never-panicking decode for arbitrary bytes and arbitrary
        /// max_record_size — the M1-level slice of D11 (in lieu of the M9
        /// libFuzzer F2 target). proptest fails the case if `decode` panics or
        /// reads OOB; we additionally assert the length-bound is honored.
        #[test]
        fn prop_decode_arbitrary_is_bounded(
            bytes in proptest::collection::vec(any::<u8>(), 0..2048),
            max in any::<u32>(),
        ) {
            match decode(&bytes, max) {
                Decoded::Record { payload, framed_len, .. } => {
                    // A returned record fits within the input and respects max.
                    prop_assert!(payload.len() as u64 <= max as u64);
                    prop_assert!(framed_len <= bytes.len());
                }
                Decoded::Invalid(DecodeError::LengthTooLarge)
                | Decoded::Invalid(DecodeError::BadCrc)
                | Decoded::Invalid(DecodeError::UnknownRecType)
                | Decoded::Sentinel
                | Decoded::Incomplete => {}
            }
        }
    }
}
