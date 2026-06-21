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
//! checksum ([`crc32c`]). **M1** adds the internal record codec (`record` —
//! encode/decode of the §5.3 framing). **M2** adds the single-segment write
//! path and replay: [`Wal::open`]/[`append`](Wal::append)/[`commit`](Wal::commit),
//! a streaming [`Reader`], the [`DurabilityObserver`] hook, segment
//! pre-allocation and `fdatasync`, and a zero-allocation hot path. Multi-segment
//! roll/split (M4), torn-tail recovery (M3), and checkpoint (M5) arrive later.

// This is an embeddable library; every public item must be documented. With
// CI's `clippy -D warnings`, an undocumented public item fails the build.
#![warn(missing_docs)]

mod config;
mod crc;
mod error;
mod lsn;
mod observer;
mod reader;
mod record;
mod recovery;
mod segment;
mod wal;

pub use config::WalConfig;
pub use crc::crc32c;
pub use error::{Result, WalError};
pub use lsn::Lsn;
pub use observer::{DurabilityObserver, NullObserver};
pub use reader::Reader;
pub use wal::{RecoveryReport, TailState, Wal};
