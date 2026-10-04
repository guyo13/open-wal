//! Loom-swappable concurrency primitives (§15.7.1 of `docs/replica_design_v1.md`).
//!
//! The shipper ring ([`crate::ring`]) is written **only** against these names,
//! so `RUSTFLAGS="--cfg loom"` model-checks the exact code that ships: under
//! `cfg(loom)` every atomic, `UnsafeCell`, `Arc`, and park/unpark is loom's
//! instrumented version; otherwise it is the `std` one with the same API. The
//! swap is the only difference between the two builds — the non-loom code path
//! is not altered by it.

#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(loom)]
pub(crate) use loom::thread::Thread;

#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(loom))]
pub(crate) use std::thread::Thread;

#[cfg(not(loom))]
pub(crate) use self::cell::UnsafeCell;
#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;

/// Park the current thread until unparked or `timeout` elapses (spurious
/// wakeups allowed — callers re-check their condition).
///
/// Under loom there is no timed park; a plain `park` is used, which is the
/// stricter model (a wakeup can only come from a real `unpark`, so a lost
/// wakeup cannot be hidden by a timeout).
#[cfg(not(loom))]
pub(crate) fn park_timeout(timeout: std::time::Duration) {
    std::thread::park_timeout(timeout);
}

#[cfg(loom)]
pub(crate) fn park_timeout(_timeout: std::time::Duration) {
    loom::thread::park();
}

#[cfg(not(loom))]
mod cell {
    /// `std` stand-in for `loom::cell::UnsafeCell`: the same closure-based
    /// access API (`with` / `with_mut`), compiling to a plain `UnsafeCell`.
    #[derive(Debug)]
    pub(crate) struct UnsafeCell<T>(core::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        pub(crate) fn new(v: T) -> UnsafeCell<T> {
            UnsafeCell(core::cell::UnsafeCell::new(v))
        }

        #[inline]
        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        #[inline]
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}
