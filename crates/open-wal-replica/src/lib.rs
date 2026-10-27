//! `open-wal-replica` — async primary/replica log shipping for [`open_wal`].
//!
//! One static primary, N replicas, no election, no sharding. A replica only
//! ever appends records `≤` the primary's `durable_lsn`, in order, so it is
//! always a **dense prefix** of the primary and never needs to truncate. The
//! normative contract (invariants R1–R9) is `docs/replica_design_v1.md`.
//!
//! Built in milestones (§14 of the design). **RM0** provides the crate
//! skeleton, the non-panicking [`ReplError`], and the §6 wire codec
//! ([`wire`]): length-prefixed, CRC-protected frames with an attacker-safe,
//! allocation-free decoder. **RM1** adds the replica side, [`Receiver`] (§7):
//! its own WAL, the R3 hard contiguity check before every append, group commit,
//! and R4-honest acks of its `durable_lsn` only. **RM2** adds the primary side,
//! [`Shipper`] (§8): capture-at-append into a preallocated SPMC [`ring`],
//! release-at-commit as a Release/Acquire watermark (the R1 gate), one network
//! thread per replica, and overflow ⇒ `NeedsCatchUp` without ever blocking the
//! writer. The ring is model-checked with loom (§15.7, `tests/loom_ring.rs`).

#![warn(missing_docs)]

mod error;
mod receiver;
pub mod ring;
// The shipper spawns real `std` threads around the ring; under `cfg(loom)` only
// the ring itself is built against loom's primitives and model-checked.
#[cfg(not(loom))]
mod shipper;
mod sync;
pub mod wire;

pub use error::{ReplError, Result};
pub use receiver::{Receiver, ReceiverConfig, ReplicaWatermarks};
#[cfg(not(loom))]
pub use shipper::{ReplicaId, ReplicaStatus, Shipper, ShipperConfig};
