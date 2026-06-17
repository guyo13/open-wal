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
    /// `max_record_size <= segment_size - 91` (64-byte segment header +
    /// 20-byte record header + up to 7 padding bytes). That precondition is
    /// **not** enforced here; `open()` validates it and returns
    /// [`InvalidConfig`](crate::WalError::InvalidConfig) (a later milestone).
    pub max_record_size: u32,
}
