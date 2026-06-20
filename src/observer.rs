//! Durability observation — the `DurabilityObserver` hook (§15.3).
//!
//! This lives in **core**, not in a separate "replication" module, because the
//! §6 public API is generic over it: `Wal<O: DurabilityObserver = NullObserver>`.
//! The observer publishes only the durable *watermark* (an [`Lsn`]); shipping
//! actual record bytes is a downstream consumer's job (via a `Reader`), never
//! the observer's.
//!
//! It fires on the writer thread at the end of [`commit`](crate::Wal::commit),
//! **after** `durable_lsn` has advanced — strictly downstream of durability, so
//! it can never affect the D1–D12 invariants. Its contract is "cheap,
//! non-blocking, no I/O, must not panic"; it has no path to fail durability.

use crate::Lsn;

/// Notified after each successful durability advance, on the writer thread.
///
/// `on_durable` runs synchronously inside [`commit`](crate::Wal::commit) once
/// the `fdatasync` has completed and `durable_lsn` has moved forward. It MUST be
/// cheap and non-blocking (an atomic release-store or a queue push), and MUST
/// NOT perform I/O, block, or panic. `durable_lsn` is monotonic across calls.
pub trait DurabilityObserver {
    /// `durable_lsn` is the new (monotonic) durable watermark.
    fn on_durable(&mut self, durable_lsn: Lsn);
}

/// The default observer: a verified no-op that inlines to nothing.
///
/// Because `Wal` defaults its observer type parameter to `NullObserver`, the
/// don't-ship case compiles away with no vtable call on the commit path
/// (static dispatch). Use a real observer only when a downstream consumer needs
/// the watermark.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullObserver;

impl DurabilityObserver for NullObserver {
    #[inline]
    fn on_durable(&mut self, _durable_lsn: Lsn) {}
}
