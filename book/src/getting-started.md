# Getting started

> Every example in this book is compiled and run against the real API as part
> of the crate's test suite (`tests/book_examples.rs`), so the code you see
> here is the code that works.

## Install

```toml
[dependencies]
open-wal = "0.2"
```

MSRV is Rust 1.85. Linux is the production platform; macOS works for
development (see [Introduction](introduction.md#status)).

## The mental model

Three moments matter in the life of a record:

1. **`append`** — the record gets the next LSN and is buffered *in memory*.
   No syscall happens. It is not durable.
2. **`commit`** — everything appended since the last commit is written to the
   log file and `fdatasync`'d. `commit` returns the new **durable watermark**:
   every record with an LSN at or below it is now safe against crash and power
   loss.
3. **Replay** — after a restart, `Wal::open` runs recovery and a `Reader`
   yields the committed records back, in LSN order, byte-identical.

LSNs are dense (`1, 2, 3, …`) and the durable log is always a gap-free prefix.
That is the shape everything else in this book builds on.

## Hello, durability

```rust,ignore
use open_wal::{Lsn, Wal, WalConfig};

fn main() -> Result<(), open_wal::WalError> {
    let dir = std::path::Path::new("./journal");
    let (mut wal, report) = Wal::open(dir, WalConfig::default())?;
    println!("recovered up to LSN {}", report.durable_lsn);

    // Pure memory: assigns LSNs 1 and 2, buffers the payloads.
    wal.append(b"order-created:42")?;
    wal.append(b"order-paid:42")?;

    // One write + one fdatasync for the whole batch. After Ok(w),
    // every record with lsn <= w survives crash and power loss.
    let durable = wal.commit()?;
    println!("durable up to {durable}");

    // Replay from the beginning (Lsn(0) means "from the start").
    let mut reader = wal.reader_from(Lsn(0))?;
    while let Some(record) = reader.next() {
        let (lsn, payload) = record?;
        println!("{lsn}: {} bytes", payload.len());
    }
    Ok(())
}
```

Run it twice. The second run's `report.durable_lsn` is `2`: recovery found the
committed records and the log continues from there. Kill the process between
`append` and `commit` and the buffered records are gone — which is exactly the
contract: a crash loses at most the un-committed tail, never anything a
returned `commit` covered.

What's on disk afterwards is deliberately boring — a `LOCK` file and one
pre-allocated segment file named after its base LSN:

```text
journal/
├── 00000000000000000001.wal
└── LOCK
```

See [the on-disk format](on-disk-format.md) if you want to know what's inside.

## Configuration

`WalConfig` has exactly two knobs:

```rust,ignore
let config = WalConfig {
    segment_size: 128 * 1024 * 1024, // bytes pre-allocated per segment file
    max_record_size: 1024 * 1024,    // hard cap on a single payload
};
// …which happen to be the defaults: WalConfig::default()
```

- **`segment_size`** — the log is a sequence of fixed-size, pre-allocated
  segment files. Larger segments mean fewer rolls; smaller segments mean
  finer-grained space reclamation (checkpointing deletes whole segments).
- **`max_record_size`** — payloads above this are rejected at `append` with
  `RecordTooLarge`. No silent truncation, no splitting.

One constraint ties them together: a record never spans segments, so a maximal
record (plus headers and padding) must fit inside one segment —
`max_record_size + 91 <= segment_size`. `open` validates this up front:

```rust,ignore
let bad = WalConfig { segment_size: 1024, max_record_size: 1024 };
assert!(matches!(
    Wal::open(dir, bad),
    Err(open_wal::WalError::InvalidConfig)
));
```

Pass the **same config** every time you open a given WAL directory; the config
is not persisted in the log.

## Open-time options

`Wal::open` covers the common case. For anything that is chosen once, at open
time, rather than being steady-state configuration, use the `Wal::options()`
builder — `Wal::open(dir, config)` is exactly `Wal::options().open(dir, config)`:

```rust,ignore
// A replica seeded from a snapshot at LSN 1000 starts its own log at 1001,
// mirroring the primary's LSN space.
let (mut wal, report) = Wal::options()
    .seed(Lsn(1001))          // cold-start origin; default Lsn(1)
    .observer(MyObserver)     // optional; default NullObserver
    .open(dir, WalConfig::default())?;
assert_eq!(report.oldest_lsn, Lsn(1001));
assert_eq!(report.durable_lsn, Lsn(1000));
assert_eq!(wal.append(b"first")?, Lsn(1001));
```

- **`seed(initial_lsn)`** only matters when `open` finds an **empty**
  directory: the first segment is created at that base, so the first `append`
  gets `initial_lsn`. If the directory already holds a log the seed is
  **ignored** and normal recovery runs — the on-disk log is authoritative, so
  don't treat the seed as an assertion (a seeded log that later checkpoints
  will have `oldest_lsn() > initial_lsn`). `Lsn(0)` is reserved and rejected
  with `InvalidConfig`.
- **`observer(o)`** is the same durability hook as `Wal::open_with`; see
  [external access](external-access.md#publishing-the-watermark-durabilityobserver).

The current retention floor — the oldest LSN still in the log — is available
at any time as `wal.oldest_lsn()` (it equals `RecoveryReport::oldest_lsn` at
open and moves forward on [checkpoint](checkpointing.md)). See §6/§8.4 of the
[design spec](https://github.com/guyo13/open-wal/blob/main/docs/wal_design_v7.md)
for the normative definition.

## Where to go next

The single most important thing to understand before shipping anything on top
of this crate is what `commit` does and does not promise — that's
[the durability model](durability-model.md).
