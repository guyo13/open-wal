# The durability model

This is the centerpiece chapter. If you read only one, read this one — the
misconceptions it corrects are the ones that cause silent data loss in systems
built on logs.

## Append is memory; commit is durability

`append` assigns the next LSN and encodes the record into an in-memory staging
buffer. It performs no I/O and, in steady state, no allocation. The record is
**not durable** — it isn't even in the page cache yet.

`commit` takes everything staged since the last commit, writes it to the
active segment file, and calls `fdatasync` (on macOS, `F_FULLFSYNC`, because
plain `fsync` there does not flush the drive cache). Only when `commit`
returns `Ok(w)` may you treat records with `lsn <= w` as durable — and `w`,
the **durable watermark**, is exactly the value to gate acknowledgements on:

```rust,ignore
let (mut wal, _) = Wal::open(dir, WalConfig::default())?;
wal.append(b"durable")?;
wal.commit()?;
wal.append(b"buffered only")?; // never committed

assert_eq!(wal.last_lsn(), Lsn(2));    // assigned...
assert_eq!(wal.durable_lsn(), Lsn(1)); // ...but not durable

// Dropping the handle without committing loses the buffered tail —
// exactly what a crash would do.
drop(wal);
let (_, report) = Wal::open(dir, WalConfig::default())?;
assert_eq!(report.durable_lsn, Lsn(1));
```

`last_lsn()` is the highest LSN *assigned*; `durable_lsn()` is the highest LSN
*safe*. The gap between them is what a crash may cost you. Never acknowledge,
publish, or replicate past `durable_lsn`.

## Group commit: the throughput lever

An `fdatasync` costs the same whether it covers one record or ten thousand.
`commit`'s batch is simply "everything appended since the last commit", so the
way to trade latency for throughput is to batch:

```rust,ignore
for event in ["a", "b", "c", "d"] {
    wal.append(event.as_bytes())?; // pure memory, no syscall
}
let durable = wal.commit()?; // one write + one fdatasync for the batch
```

This is the classic LMAX journaling pattern: drain whatever events are
available, append them all, commit once, then acknowledge the batch. Per-event
fsync latency disappears; the fsync cost amortizes across the batch.

## The non-guarantees — read these twice

### `commit` is not atomic

A commit batch is a *performance grouping*, not a transaction. The log's
guarantees are **per-record durability** and a **dense LSN prefix** — never
all-or-nothing durability of a batch.

Concretely: when a batch doesn't fit in the active segment, `commit` splits it
at whole-record boundaries and syncs each segment separately. A crash (or an
I/O failure) between those syncs keeps the first part of the batch — durable,
acknowledged by the watermark — and loses the rest. That outcome is
contract-compliant: what survives is still a dense prefix, and `durable_lsn`
reported exactly how far durability got.

When a batch happens to fit in one segment it is covered by a single shared
`fdatasync`, so it *looks* atomic. That is incidental, not guaranteed. Do not
build logic that assumes a commit batch is indivisible.

### The atomicity primitive is a single record

A single record is all-or-nothing: it never spans segments, it's covered by
one CRC, and recovery either yields it byte-identical or (if it was the torn
tail) drops it entirely. So if several events must live or die together,
encode them into **one record** with a compound payload:

```rust,ignore
// WRONG for atomicity: two appends in one commit batch. A crash (or a
// split across segments) can keep the first and lose the second.
//
// RIGHT: if two events must be all-or-nothing, encode them into ONE
// payload. A single record is the only atomicity primitive.
let mut compound = Vec::new();
compound.extend_from_slice(b"debit:alice:100;");
compound.extend_from_slice(b"credit:bob:100");
let lsn = wal.append(&compound)?;
wal.commit()?;
```

The encoding of the compound payload is your concern — the WAL stores opaque
bytes.

## When fsync fails: the handle poisons

A failed `fdatasync` is not a transient hiccup you can retry. On Linux, a
failed fsync may mean the dirty pages were already dropped — the data is
*gone*, and a retried fsync can "succeed" while durable state is missing (the
PostgreSQL *fsyncgate* lesson). For a dense-LSN log there is no safe resume:
the API has no way to rewrite a lost LSN slot, and continuing would create a
permanent gap.

So there is exactly one policy. On any `write`/`fdatasync` failure:

- The failing `commit` returns an error (`FsyncFailed` or `Io`).
- `durable_lsn` keeps whatever earlier segments in the batch achieved — it is
  monotonic and never lies.
- The handle is **poisoned**: every subsequent `append`/`commit`/`checkpoint`
  returns `WalError::Poisoned`.

The only way forward is to drop the handle and `Wal::open` afresh — recovery
truncates any torn tail and hands you an honest durable state to rebuild from.
Treat a poisoned handle as "this process's writer is done", not as an error to
swallow.

## What this buys you

Summing up the contract in user terms (the spec states these precisely as
invariants D1–D12 in
[§4 of the design spec](https://github.com/guyo13/open-wal/blob/main/docs/wal_design_v7.md)):

- After `commit()` returns `Ok(w)`, records `<= w` survive process crash and
  power loss.
- The durable log is always a dense, gap-free run of LSNs.
- A crash loses at most the un-committed tail.
- Replay returns exactly what was committed, in order, byte-identical.

How those promises survive torn writes and corruption is the subject of
[Recovery](recovery.md).
