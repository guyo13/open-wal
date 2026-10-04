//! Error type for the replication layer (§14 of `docs/replica_design_v1.md`).
//!
//! `ReplError` is **non-panicking**: every fault a peer, the network, or the
//! WAL can produce surfaces as a value. In particular the wire decoder is
//! attacker-facing (§6) and returns these errors — never panics — for any input.

use std::fmt;

use open_wal::{Lsn, WalError};

use crate::wire::ErrCode;

/// Convenience alias for results returned by this crate.
pub type Result<T> = std::result::Result<T, ReplError>;

/// All error conditions surfaced by the replication layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReplError {
    /// The local `open-wal` returned an error. A durability failure
    /// (`FsyncFailed`/`Poisoned`) poisons the WAL handle (WAL §12): the owner
    /// must drop it and reopen (recovery) before resuming.
    Wal(WalError),

    /// Stream contiguity violation (R3): a record or `SERVING` did not start at
    /// `expected` (`= last_lsn + 1` on the replica). The record is never
    /// appended; the connection is closed and the replica re-HELLOs.
    Contiguity {
        /// The only LSN the receiver could accept.
        expected: Lsn,
        /// The LSN the peer actually sent.
        got: Lsn,
    },

    /// A `RECORD` frame's CRC-32C over `(lsn ‖ payload)` did not match (R9).
    /// Nothing is appended; the connection is closed and the replica re-HELLOs.
    WireCrc,

    /// A malformed, oversize, unknown, or out-of-sequence frame (§6), or a
    /// protocol-version mismatch. The detail is a short static description.
    Protocol(&'static str),

    /// The primary no longer retains the records this replica needs (R6); the
    /// replica must be re-seeded from a snapshot (§10).
    ReseedRequired {
        /// The primary's oldest retained LSN.
        oldest: Lsn,
    },

    /// The primary's WAL is poisoned (WAL §12) and cannot serve.
    PrimaryPoisoned,

    /// A transport (socket) error. Never a durability failure on its own.
    Io(std::io::Error),

    /// Producing or applying a re-seed snapshot failed (§10, RM4).
    SnapshotFailed,

    /// The peer sent an `ERR` frame and is closing the connection.
    Remote {
        /// The peer's error class.
        code: ErrCode,
        /// The peer's (bounded, UTF-8) message.
        msg: String,
    },
}

impl fmt::Display for ReplError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplError::Wal(e) => write!(f, "WAL error: {e}"),
            ReplError::Contiguity { expected, got } => {
                write!(
                    f,
                    "stream contiguity violation: expected lsn {expected}, got {got}"
                )
            }
            ReplError::WireCrc => write!(f, "wire RECORD CRC-32C mismatch"),
            ReplError::Protocol(d) => write!(f, "protocol error: {d}"),
            ReplError::ReseedRequired { oldest } => {
                write!(
                    f,
                    "re-seed required: primary's oldest retained lsn is {oldest}"
                )
            }
            ReplError::PrimaryPoisoned => write!(f, "primary WAL is poisoned"),
            ReplError::Io(e) => write!(f, "transport I/O error: {e}"),
            ReplError::SnapshotFailed => write!(f, "snapshot produce/apply failed"),
            ReplError::Remote { code, msg } => write!(f, "peer sent ERR({code:?}): {msg}"),
        }
    }
}

impl std::error::Error for ReplError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReplError::Wal(e) => Some(e),
            ReplError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<WalError> for ReplError {
    fn from(e: WalError) -> Self {
        ReplError::Wal(e)
    }
}

impl From<std::io::Error> for ReplError {
    fn from(e: std::io::Error) -> Self {
        ReplError::Io(e)
    }
}

impl ReplError {
    /// The `ERR` code a receiver/shipper sends to its peer for this error, if
    /// the error is one the protocol reports in-band (§6).
    #[must_use]
    pub fn err_code(&self) -> Option<ErrCode> {
        match self {
            ReplError::Contiguity { .. } => Some(ErrCode::Contiguity),
            ReplError::WireCrc => Some(ErrCode::WireCrc),
            ReplError::Protocol(_) => Some(ErrCode::Protocol),
            ReplError::Wal(WalError::FsyncFailed | WalError::Poisoned) => Some(ErrCode::Poisoned),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_carries_context() {
        let e = ReplError::Contiguity {
            expected: Lsn(7),
            got: Lsn(9),
        };
        let s = e.to_string();
        assert!(s.contains('7') && s.contains('9'));
    }

    #[test]
    fn err_codes_map_in_band_errors_only() {
        assert_eq!(ReplError::WireCrc.err_code(), Some(ErrCode::WireCrc));
        assert_eq!(
            ReplError::Wal(WalError::FsyncFailed).err_code(),
            Some(ErrCode::Poisoned)
        );
        assert_eq!(ReplError::Io(std::io::Error::other("x")).err_code(), None);
    }
}
