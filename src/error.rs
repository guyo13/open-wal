//! Error type for the WAL.
//!
//! `WalError` is a non-panicking enum (§10 of `docs/wal_design_v6.md`). The
//! recovery parser MUST return these errors, never panic, for all inputs (D11).

use std::fmt;

use crate::Lsn;

/// Convenience alias for results returned by this crate.
pub type Result<T> = std::result::Result<T, WalError>;

/// All error conditions surfaced by the WAL.
///
/// This is the normative `WalError` shape from §10. Variants beyond `Io` carry
/// enough context (segment base LSN, byte offset) to locate the fault.
#[derive(Debug)]
#[non_exhaustive]
pub enum WalError {
    /// An underlying I/O error that is not itself a durability failure.
    Io(std::io::Error),

    /// A record or header failed CRC / structural validation mid-segment.
    Corruption {
        /// `base_lsn` of the segment containing the fault.
        segment: Lsn,
        /// Byte offset within the segment where the fault was detected.
        offset: u64,
        /// Short, static description of what was wrong.
        detail: &'static str,
    },

    /// A valid record exists *after* a bad/torn one — a non-truncatable
    /// internal gap. Fatal and loud, never silently truncated (D5).
    TornMidLog {
        /// `base_lsn` of the segment containing the fault.
        segment: Lsn,
        /// Byte offset within the segment of the torn record.
        offset: u64,
    },

    /// An `append` payload exceeds `max_record_size` (no silent truncation,
    /// no fragmentation in v1).
    RecordTooLarge,

    /// The supplied [`WalConfig`](crate::WalConfig) is invalid (e.g.
    /// `max_record_size > segment_size - 91`, §5.3).
    InvalidConfig,

    /// A segment header has bad `magic` or `header_crc` (§5.2). The header is
    /// written and synced at creation, so it is never a torn tail — always
    /// fatal.
    BadSegmentHeader,

    /// The directory's exclusive writer lock is already held.
    Locked,

    /// A `write`/`fdatasync`/`fsync` failed. This **poisons** the handle
    /// (§12); there is no safe resume for a dense-LSN log.
    FsyncFailed,

    /// Retained segments do not form a contiguous LSN suffix (§8.1).
    ContiguityViolation,

    /// The handle was poisoned by a prior durability failure (§12). All
    /// subsequent `append`/`commit` calls return this.
    Poisoned,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "I/O error: {e}"),
            WalError::Corruption {
                segment,
                offset,
                detail,
            } => write!(
                f,
                "corruption in segment {} at offset {offset}: {detail}",
                segment.0
            ),
            WalError::TornMidLog { segment, offset } => write!(
                f,
                "torn record mid-log in segment {} at offset {offset}",
                segment.0
            ),
            WalError::RecordTooLarge => write!(f, "record exceeds max_record_size"),
            WalError::InvalidConfig => write!(f, "invalid WAL configuration"),
            WalError::BadSegmentHeader => write!(f, "bad segment header"),
            WalError::Locked => write!(f, "WAL directory is already locked by another writer"),
            WalError::FsyncFailed => write!(f, "fsync failed; handle is poisoned"),
            WalError::ContiguityViolation => write!(f, "retained segments are not contiguous"),
            WalError::Poisoned => write!(f, "WAL handle is poisoned by a prior durability failure"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_source_is_preserved() {
        let io = std::io::Error::other("boom");
        let err = WalError::from(io);
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn display_includes_location_context() {
        let err = WalError::Corruption {
            segment: Lsn(100001),
            offset: 84,
            detail: "crc mismatch",
        };
        let s = err.to_string();
        assert!(s.contains("100001"));
        assert!(s.contains("84"));
        assert!(s.contains("crc mismatch"));
    }
}
