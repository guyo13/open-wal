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
//! and R4-honest acks of its `durable_lsn` only.

#![warn(missing_docs)]

mod error;
mod receiver;
pub mod wire;

pub use error::{ReplError, Result};
pub use receiver::{Receiver, ReceiverConfig, ReplicaWatermarks};
