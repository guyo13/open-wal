# open-wal

[![CI](https://github.com/guyo13/open-wal/actions/workflows/ci.yml/badge.svg)](https://github.com/guyo13/open-wal/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/open-wal.svg)](https://crates.io/crates/open-wal)
[![docs.rs](https://img.shields.io/docsrs/open-wal)](https://docs.rs/open-wal)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

An embeddable, **single-writer**, **durability-first** write-ahead log for Rust —
built for LMAX-style event-sourced systems, where the journal *is* the database
and an acknowledged record must survive process crash and power loss.

- **Acknowledge only after durable.** `append` is pure memory; `commit` is
  `write` + `fdatasync` (`F_FULLFSYNC` on macOS) and returns the durable
  watermark. Group commit amortizes one fsync over many appends.
- **Correct under crashes, verified by fault injection.** Torn tails are
  detected, truncated, and durably invalidated; mid-log corruption is a loud
  fatal error, never a silent truncation. Recovery is deterministic, idempotent,
  and bounded for arbitrary input bytes. Tested with SIGKILL matrices, LazyFS
  power-loss simulation, dm-flakey fsync faults, property tests, a model-based
  oracle, and continuous fuzzing.
- **Single-writer by construction.** The handle is `Send` but not `Sync`, write
  methods take `&mut self`, and an OS lock excludes a second process. Concurrent
  writers are a compile error, not a runtime surprise.
- **Opaque payloads, minimal machinery.** Records are byte slices —
  serialization is yours. No background threads, no manifest files, two runtime
  dependencies (`crc32c`, `rustix`).
- **Tailable, immutable format.** Sealed segments never change, which makes
  backup a plain file copy and gives external readers a stable substrate.

## Quickstart

```toml
[dependencies]
open-wal = "0.2"
```

```rust
use open_wal::{Lsn, Wal, WalConfig};

fn main() -> Result<(), open_wal::WalError> {
    // Open (or create) a WAL directory. Recovery runs here: on a reopen after
    // a crash, `report` tells you what survived.
    let dir = std::path::Path::new("./journal");
    let (mut wal, report) = Wal::open(dir, WalConfig::default())?;
    println!("recovered up to LSN {}", report.durable_lsn);

    // `append` is pure memory: it assigns an LSN and buffers the record.
    // Nothing is durable yet.
    wal.append(b"order-created:42")?;
    wal.append(b"order-paid:42")?;

    // `commit` writes the buffered records and fdatasyncs them. When it
    // returns Ok(w), every record with lsn <= w is durable — this is the
    // point where you may acknowledge upstream.
    let durable = wal.commit()?;
    println!("durable up to {durable}");

    // Replay everything, in order, byte-identical. The reader is streaming
    // and zero-copy: each payload borrow is valid until the next call.
    let mut reader = wal.reader_from(Lsn(0))?;
    while let Some(record) = reader.next() {
        let (lsn, payload) = record?;
        println!("{lsn}: {} bytes", payload.len());
    }
    Ok(())
}
```

Reopening the same directory re-runs recovery and hands back a log with exactly
the committed records — a crash loses at most the appended-but-not-committed
tail, never anything at or below a `durable_lsn` that `commit` returned.

## The durability model in 30 seconds

The write path has two halves. `append` sequences and buffers a record in
memory — no syscall — and returns its LSN. `commit` writes everything buffered
since the last commit and syncs it in one `fdatasync`, then returns the new
durable watermark. Batch as many appends per commit as your latency budget
allows: that single shared fsync is the throughput lever.

One caveat matters more than any other: **`commit` is not atomic.** Durability
is per-record, and the durable log is always a dense LSN prefix — but a commit
batch that spans a segment boundary can be split by a crash, keeping the first
part and losing the rest. If several events must be all-or-nothing, encode them
into **one record**; never rely on commit batching for atomicity. See
[the durability model](book/src/durability-model.md) for the full story.

## When to use it — and when not to

**A good fit:** an event-sourced or state-machine-replication system with one
writer thread that needs an honest durability boundary — append events, fsync,
acknowledge, replay after restart. Also as the journal substrate under your own
replication or backup scheme (sealed segments are immutable; a durable-watermark
hook is built in).

**Not a fit:** anything multi-writer (one exclusive writer per WAL directory,
enforced); key-value lookups or queries (records are located by LSN only); a
turnkey replication system (open-wal supplies ordered durable records and the
watermark — transport, acks, and failover are yours); or transactions spanning
multiple records.

## Platform support & status

| Platform | Tier |
|---|---|
| Linux | Production target; hardware durability validated by fault injection |
| macOS | Development/correctness only (`F_FULLFSYNC` is honored, but no production durability claim) |
| Windows | Out of scope for v1 |

The crate is **under active development** (pre-1.0). The core write, recovery,
and checkpoint paths are implemented and heavily tested; the public API may
still see breaking changes before 1.0. MSRV is 1.85.

## Learn more

- **[The book](https://guyo13.github.io/open-wal/)** — a readable guide:
  getting started, the durability model, recovery, checkpointing, backup and
  external readers. (Source in [`book/`](book/); build locally with
  `mdbook serve book`.)
- **[API docs on docs.rs](https://docs.rs/open-wal)** — every public item is
  documented.
- **[Design specification](docs/wal_design_v7.md)** — the normative contract
  (durability invariants D1–D12, on-disk format, recovery algorithm) and the
  test plan behind the guarantees.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
