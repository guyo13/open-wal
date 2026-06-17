//! `open-wal` — a focused, embeddable, single-writer append-only write-ahead
//! log for an LMAX-style, event-sourced system.
//!
//! **Durability-first:** a committed record survives process crash and power
//! loss on honest hardware. The WAL stores **opaque byte payloads** —
//! serialization is entirely the caller's concern. It is not a database, not
//! multi-writer, and runs no background threads.
//!
//! The normative design lives in `docs/wal_design_v6.md`; the durability
//! invariants D1–D12 there are binding on every change.
//!
//! This crate is built in milestones (§13). **M0 (foundations)** provides the
//! core value types — [`Lsn`], [`WalConfig`], [`WalError`] — and the CRC-32C
//! checksum ([`crc32c`]). The write path, recovery, and checkpoint arrive in
//! later milestones.

mod config;
mod crc;
mod error;
mod lsn;

pub use config::WalConfig;
pub use crc::crc32c;
pub use error::{Result, WalError};
pub use lsn::Lsn;
