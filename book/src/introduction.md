# Introduction

`open-wal` is an embeddable, single-writer, append-only **write-ahead log** for
Rust. Its one job: when `commit()` returns, the records it covers are durable —
they survive a process crash and a power loss on honest hardware. Everything
else about the crate follows from taking that one promise seriously.

It was built as the journal for an LMAX-style, event-sourced system: a single
writer thread appends opaque event payloads, syncs them in batches, and only
then acknowledges or publishes downstream. After a restart, replaying the log
rebuilds state exactly. In such a system the log is not a cache or a
convenience — it *is* the source of truth, and the durability boundary is where
correctness lives.

## Design philosophy

**Durability first, honestly.** Many logging libraries treat fsync as a
configuration knob and crash behavior as a footnote. Here it is inverted: the
crash and power-loss behavior is the specified, tested core, and the API is
shaped so you cannot easily mistake "written" for "durable". `append` is pure
memory; only `commit` — a `write` plus `fdatasync` (`F_FULLFSYNC` on macOS) —
makes records durable, and it tells you exactly how far durability reached.

**Loud failure over silent loss.** A torn write at the tail of the log (the
normal result of a crash mid-write) is detected, truncated, and durably
invalidated. Corruption *in the middle* of the log — which would mean losing
acknowledged data — is a fatal, distinct error, never a silent truncation. A
failed fsync poisons the writer handle rather than pretending a retry can make
the data safe. These choices borrow deliberately from PostgreSQL (the
*fsyncgate* lesson), RocksDB (torn-tail tolerance vs. absolute consistency),
and Kafka (segmented retention).

**Small and predictable.** No background threads, no compaction, no manifest
files, no hidden I/O. Two runtime dependencies (`crc32c`, `rustix`). Payloads
are opaque byte slices — serialization is entirely the caller's concern. The
steady-state append/commit path performs zero heap allocations.

**Single-writer, enforced.** Exactly one writer per log, and the type system
plus an OS lock make that a compile-time and open-time guarantee rather than a
documentation footnote. This is a feature: it removes locks from the hot path
and removes multi-writer interleaving from the correctness argument. See
[Single-writer by construction](single-writer.md).

## What it is not

- **Not a database.** Records are located by log sequence number (LSN) only —
  no keys, indexes, or queries.
- **Not multi-writer.** One exclusive writer per WAL directory.
- **Not a replication system.** The crate provides the durable substrate —
  ordered records, immutable sealed segments, a durable-watermark hook — but
  shipping, acknowledgements, and failover belong to the integrator
  ([details](external-access.md)).
- **No multi-record transactions.** The atomicity primitive is a single
  record; see [the durability model](durability-model.md) for what that means
  in practice.

## How this book relates to the design spec

This book is the human on-ramp. The normative contract — the twelve durability
invariants (D1–D12), the exact on-disk byte layout, the recovery algorithm, and
the fault-injection test plan that backs the guarantees — lives in the
[design specification](https://github.com/guyo13/open-wal/blob/main/docs/wal_design_v6.md).
Where this book says "the log guarantees X", the spec is where X is stated
precisely and mapped to tests. When in doubt, the spec wins.

## Status

`open-wal` is pre-1.0 and under active development. The write, recovery, and
checkpoint paths are implemented and tested well beyond unit level — SIGKILL
crash matrices, LazyFS power-loss simulation, dm-flakey fsync-failure
injection, property tests, a model-based oracle, and continuous fuzzing — but
the public API may still see breaking changes before 1.0. Linux is the
production target; macOS is supported for development and correctness work
only; Windows is out of scope for v1.
