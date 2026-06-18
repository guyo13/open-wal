//! WAL configuration.

/// Configuration for a WAL instance.
///
/// Both fields are caller-supplied (§11 of `docs/wal_design_v6.md`). Tests use
/// tiny sizes to force segment rolls and commit-time splits.
#[derive(Clone, Copy, Debug)]
pub struct WalConfig {
    /// Bytes pre-allocated per segment file. Default in production: 128 MiB.
    pub segment_size: u64,

    /// Hard upper bound on a single record's payload length.
    ///
    /// A record must not span segments (§5.3), so this is constrained by
    /// `max_record_size + 91 <= segment_size` (64-byte segment header +
    /// 20-byte record header + up to 7 padding bytes). The bound is written in
    /// additive form on purpose: the equivalent `segment_size - 91` underflows
    /// for `segment_size < 91`, which would *bypass* the check. That precondition
    /// is **not** enforced here; `open()` validates it and returns
    /// [`InvalidConfig`](crate::WalError::InvalidConfig) (a later milestone).
    pub max_record_size: u32,
}

impl Default for WalConfig {
    /// Production-oriented defaults: a 128 MiB segment with a 1 MiB max payload.
    ///
    /// These match the sizing guidance in §5.3/§11 (payloads ≤ ~1 MiB with a
    /// 64–128 MiB segment) and satisfy the `max_record_size ≤ segment_size − 91`
    /// precondition with large headroom, so a default-configured WAL always
    /// passes `open()`'s validation.
    fn default() -> Self {
        WalConfig {
            segment_size: 128 * 1024 * 1024,
            max_record_size: 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_satisfies_section_5_3_bound() {
        // max_record_size + 91 ≤ segment_size (§5.3) must hold for the default,
        // so a default-configured WAL never trips InvalidConfig at open().
        // Additive form avoids the segment_size − 91 underflow trap.
        let c = WalConfig::default();
        assert!(u64::from(c.max_record_size) + 91 <= c.segment_size);
    }
}
