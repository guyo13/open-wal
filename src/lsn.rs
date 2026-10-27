//! Log Sequence Numbers.

use std::fmt;

/// Log Sequence Number. Newtype to prevent mixing with byte offsets/counts.
///
/// `Lsn(0)` is reserved as "none"; real records are dense starting at 1
/// (§6 of `docs/wal_design_v6.md`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    /// The reserved "none" sentinel. Never assigned to a real record.
    pub const NONE: Lsn = Lsn(0);

    /// The first valid record LSN. Records are dense starting here.
    pub const FIRST: Lsn = Lsn(1);

    /// The next LSN, **saturating**: `Lsn(u64::MAX).next() == Lsn(u64::MAX)`
    /// (never wraps, never panics).
    ///
    /// Because it saturates, it is **not** used for sequencing: a saturated
    /// "next" would silently reuse `u64::MAX`. The WAL's own sequencing (append,
    /// the recovery scan, cross-segment continuity) uses
    /// [`checked_next`](Lsn::checked_next) and treats `u64::MAX` as the end of
    /// the LSN space (v7.1, `WalError::LsnExhausted`).
    #[inline]
    #[must_use]
    pub const fn next(self) -> Lsn {
        Lsn(self.0.saturating_add(1))
    }

    /// The next LSN in sequence, or `None` if `self` is `Lsn(u64::MAX)` — the
    /// last LSN of the finite LSN space `[1, u64::MAX]` (v7.1, §4 D2). Used for
    /// all sequencing, so exhaustion is detected instead of an LSN being reused.
    #[inline]
    #[must_use]
    pub const fn checked_next(self) -> Option<Lsn> {
        match self.0.checked_add(1) {
            Some(n) => Some(Lsn(n)),
            None => None,
        }
    }

    /// True for the reserved `Lsn(0)` sentinel.
    #[inline]
    #[must_use]
    pub const fn is_none(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Lsn {
    /// Formats the LSN as its underlying integer, so error and log messages can
    /// use `{lsn}` instead of poking at the `.0` field.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_and_first() {
        assert_eq!(Lsn::NONE, Lsn(0));
        assert_eq!(Lsn::FIRST, Lsn(1));
        assert!(Lsn::NONE.is_none());
        assert!(!Lsn::FIRST.is_none());
    }

    #[test]
    fn next_is_monotone_and_dense() {
        assert_eq!(Lsn::NONE.next(), Lsn::FIRST);
        assert_eq!(Lsn(41).next(), Lsn(42));
    }

    #[test]
    fn checked_next_detects_exhaustion() {
        // v7.1: sequencing uses checked_next; u64::MAX is the last LSN.
        assert_eq!(Lsn(1).checked_next(), Some(Lsn(2)));
        assert_eq!(Lsn::NONE.checked_next(), Some(Lsn::FIRST));
        assert_eq!(Lsn(u64::MAX - 1).checked_next(), Some(Lsn(u64::MAX)));
        assert_eq!(Lsn(u64::MAX).checked_next(), None);
    }

    #[test]
    fn next_saturates_without_panicking() {
        assert_eq!(Lsn(u64::MAX).next(), Lsn(u64::MAX));
    }

    #[test]
    fn ordering() {
        assert!(Lsn(1) < Lsn(2));
        assert!(Lsn::NONE < Lsn::FIRST);
    }

    #[test]
    fn display_is_the_underlying_integer() {
        assert_eq!(Lsn(0).to_string(), "0");
        assert_eq!(Lsn(100001).to_string(), "100001");
    }
}
