//! `OpenOptions` — the open-time builder (v7 §6).
//!
//! Open-time choices that are not steady-state configuration live here rather
//! than on [`WalConfig`]: today the cold-start **seed** and the
//! [`DurabilityObserver`]. They are independent optional dimensions (a replica
//! seeded from a snapshot may also host downstream consumers off its observer),
//! so they compose as a two-method chain instead of a cross-product of `open_*`
//! functions. Future open-time options go here, never as new positional openers.
//!
//! [`Wal::open`] and [`Wal::open_with`] remain as conveniences:
//! `open(d, c)` ≡ `Wal::options().open(d, c)` and
//! `open_with(d, c, o)` ≡ `Wal::options().observer(o).open(d, c)`.

use std::path::Path;

use crate::error::Result;
use crate::observer::{DurabilityObserver, NullObserver};
use crate::wal::{RecoveryReport, Wal};
use crate::{Lsn, WalConfig};

/// Builder for opening a [`Wal`] with non-default open-time options (v7 §6).
///
/// Start from [`Wal::options`], chain [`seed`](OpenOptions::seed) and/or
/// [`observer`](OpenOptions::observer), and finish with
/// [`open`](OpenOptions::open):
///
/// ```no_run
/// # use open_wal::{Lsn, Wal, WalConfig};
/// # fn main() -> Result<(), open_wal::WalError> {
/// // A replica seeded from a snapshot at LSN 1000 is born at base 1001.
/// let (wal, report) = Wal::options()
///     .seed(Lsn(1001))
///     .open(std::path::Path::new("/var/lib/app/replica-wal"), WalConfig::default())?;
/// assert_eq!(report.oldest_lsn, wal.oldest_lsn());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
#[must_use = "an OpenOptions does nothing until `.open(dir, config)` is called"]
pub struct OpenOptions<O: DurabilityObserver = NullObserver> {
    seed: Lsn,
    observer: O,
}

impl Wal<NullObserver> {
    /// Start an [`OpenOptions`] builder from the defaults: seed `Lsn(1)` and the
    /// no-op [`NullObserver`] — i.e. exactly what [`Wal::open`] does.
    pub fn options() -> OpenOptions<NullObserver> {
        OpenOptions {
            seed: Lsn::FIRST,
            observer: NullObserver,
        }
    }
}

impl<O: DurabilityObserver> OpenOptions<O> {
    /// Cold-start seed: the first LSN of a **fresh** log (§8.4, §11).
    ///
    /// Honored only when [`open`](OpenOptions::open) finds no log in the
    /// directory: the first segment is created with `base_lsn = initial_lsn`, so
    /// `oldest_lsn == initial_lsn`, `durable_lsn == initial_lsn − 1`, and the
    /// first `append` yields `initial_lsn`. If the directory already holds a log
    /// the seed is **ignored** and normal recovery runs — the on-disk log is
    /// authoritative. Do not use it as an assertion: a seeded log that later
    /// checkpoints legitimately has `oldest_lsn > initial_lsn`.
    ///
    /// Used by replicas seeded from a snapshot at LSN `N` (pass `N + 1`). Must be
    /// `≥ Lsn(1)`; `open` rejects `Lsn(0)` with
    /// [`InvalidConfig`](crate::WalError::InvalidConfig).
    pub fn seed(mut self, initial_lsn: Lsn) -> Self {
        self.seed = initial_lsn;
        self
    }

    /// Attach a [`DurabilityObserver`], replacing any previously set one (this
    /// changes the builder's — and the resulting `Wal`'s — observer type).
    pub fn observer<P: DurabilityObserver>(self, observer: P) -> OpenOptions<P> {
        OpenOptions {
            seed: self.seed,
            observer,
        }
    }

    /// Open or create the WAL in `dir`, running full recovery (§8), exactly as
    /// [`Wal::open_with`] does — plus the [`seed`](OpenOptions::seed) for a cold
    /// start. Validates `config` (§5.3) and the seed (`≥ Lsn(1)`), returning
    /// [`InvalidConfig`](crate::WalError::InvalidConfig) otherwise.
    pub fn open(self, dir: &Path, config: WalConfig) -> Result<(Wal<O>, RecoveryReport)> {
        Wal::open_seeded(dir, config, self.observer, self.seed)
    }
}
