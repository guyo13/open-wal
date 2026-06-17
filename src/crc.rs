//! CRC-32C (Castagnoli) checksum.
//!
//! All CRCs in the on-disk format are **CRC-32C (Castagnoli)**, the same choice
//! as ext4/RocksDB/iSCSI (§5 of `docs/wal_design_v6.md`). This module is the
//! single chokepoint so every caller (record codec, segment header) uses one
//! function and the polynomial is asserted in exactly one place's tests.
//!
//! Castagnoli polynomial (`0x1EDC6F41` reflected), **not** the ISO-HDLC/zlib
//! polynomial used by `crc32fast` — getting that wrong is silent and
//! catastrophic.

/// Compute the CRC-32C (Castagnoli) checksum of `data`.
#[inline]
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    /// Canonical CRC-32C check value of the string "123456789".
    const CHECK_123456789: u32 = 0xE306_9283;

    /// Check value the ISO-HDLC/zlib polynomial (`crc32fast`) would produce for
    /// "123456789". Used as a negative control.
    const ISO_HDLC_123456789: u32 = 0xCBF4_3926;

    #[test]
    fn empty_input_is_zero() {
        assert_eq!(crc32c(b""), 0x0000_0000);
    }

    #[test]
    fn canonical_check_vector() {
        // §14.1: CRC-32C vs published Castagnoli vector.
        assert_eq!(crc32c(b"123456789"), CHECK_123456789);
    }

    #[test]
    fn is_castagnoli_not_iso_hdlc() {
        // §14.1: assert it is Castagnoli, NOT ISO-HDLC (the crc32fast/zlib
        // polynomial). If this ever passes by equalling the ISO value, the
        // wrong crate is wired in and the on-disk format is silently corrupt.
        assert_ne!(crc32c(b"123456789"), ISO_HDLC_123456789);
    }

    #[test]
    fn known_answer_all_zeros() {
        // 32 bytes of 0x00. Guards the table/HW path against the empty case.
        assert_eq!(crc32c(&[0x00u8; 32]), 0x8A91_36AA);
    }

    #[test]
    fn known_answer_all_ones() {
        // 32 bytes of 0xFF.
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
    }

    #[test]
    fn detects_single_bit_flip() {
        let a = crc32c(b"the quick brown fox");
        let b = crc32c(b"the quick brown gox");
        assert_ne!(a, b);
    }
}
