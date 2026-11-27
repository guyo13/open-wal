# Writing and reading

## The writer surface

The whole write API is four methods on `Wal`:

| Method | What it does | Cost |
|---|---|---|
| `append(&mut self, payload: &[u8]) -> Result<Lsn>` | Assign the next LSN, buffer the record | Memory only |
| `commit(&mut self) -> Result<Lsn>` | Write + fsync everything staged; return the durable watermark | 1 write + 1 fsync per segment touched |
| `durable_lsn(&self) -> Lsn` | Highest durable LSN | Free |
| `last_lsn(&self) -> Lsn` | Highest assigned LSN (durable or buffered) | Free |

`Lsn` is a transparent newtype over `u64` (`Lsn(pub u64)`). `Lsn(0)` is the
reserved "none" value — real records start at `Lsn(1)` and are dense from
there. The newtype exists so an LSN can't be silently mixed up with a byte
offset or a count.

`append` can fail two ways: `RecordTooLarge` if the payload exceeds
`max_record_size` (nothing is truncated or split — size your records or raise
the limit), and `Poisoned` if a previous durability failure killed the handle:

```rust,ignore
let big = vec![0u8; 2048]; // exceeds max_record_size
assert!(matches!(
    wal.append(&big),
    Err(open_wal::WalError::RecordTooLarge)
));
```

A `commit` with nothing staged is a cheap no-op that returns the current
watermark. Segment management — rolling to a new segment when the active one
fills, splitting a large batch across segments — happens entirely inside
`commit`; `append` neither knows nor cares about segments.

## Reading back: the streaming `Reader`

`reader_from(&self, from: Lsn)` returns a `Reader` that yields records with
`lsn >= from`, in order, across segment boundaries. `Lsn(0)` means "from the
beginning". Reading is how you replay state at startup, and how a shipping
consumer pulls newly durable records.

```rust,ignore
let mut reader = wal.reader_from(Lsn(3))?; // start mid-log
let mut retained: Vec<(Lsn, Vec<u8>)> = Vec::new();
while let Some(record) = reader.next() {
    let (lsn, payload) = record?;
    // `payload` borrows the reader's internal buffer and is only valid
    // until the next `reader.next()` call. Copy it to keep it.
    retained.push((lsn, payload.to_vec()));
}
```

Two things to notice:

**It is a lending iterator, not a `std::iter::Iterator`.** Each call to
`next()` returns `Option<Result<(Lsn, &[u8])>>` where the payload slice
borrows the reader's reused internal buffer — valid only until the next
`next()` call. The standard `Iterator` trait cannot express an item that
borrows from the iterator per call, which is exactly the shape that makes
replay zero-copy and (after warm-up) allocation-free. The practical
consequences: you drive it with `while let Some(..) = reader.next()` rather
than a `for` loop, and if you need to keep a payload past the next call you
copy it (`.to_vec()`). Consumers that ship records over a network pay that
copy at the serialization boundary anyway.

**Reading below the retention floor is a loud error.** After a
[checkpoint](checkpointing.md) has deleted old segments, asking for records
that no longer exist fails with an error rather than silently starting at the
oldest available record. A reader silently jumping from LSN 100 to LSN 100000
is the footgun this rule forbids.

The reader holds a shared borrow of the `Wal`, so the borrow checker prevents
appending while a `Reader` is alive — read, drop the reader, then write. (For
readers that run *concurrently* with the writer — tailing from another thread
or process — see [External readers](external-access.md).)

## Resuming a reader

`reader_from(from)` finds the segment containing `from` and scans it from its
start, reading and checksumming every record before `from`. That is fine for a
one-off replay, but a consumer that reads in **bounded steps** — read a chunk,
drop the reader so the writer can append and commit, come back — would pay that
prefix scan again on every step.

Instead, ask the reader where it is before dropping it, and start the next one
there:

```rust,ignore
// Step 1: read a bounded chunk, then remember where we stopped.
let mut reader = wal.reader_from(Lsn(1))?;
for _ in 0..2 {
    let (lsn, payload) = reader.next().expect("a record")?;
    ship(lsn, payload);
}
let pos = reader.position().expect("resumable");
drop(reader); // release the borrow so the writer can run

// The writer keeps going: appends, commits, rolls and checkpoints
// do not invalidate the position.
wal.append(b"more")?;
wal.commit()?;

// Step 2: pick up exactly where we left off — no prefix rescan.
let mut reader = wal.reader_at(pos)?;
while let Some(record) = reader.next() {
    let (lsn, payload) = record?;
    ship(lsn, payload);
}
let pos = reader.position(); // `Some` at a clean end of log: resume later
```

A `ReaderPos` is a small opaque `Copy` value that borrows nothing, so it
outlives the reader and any `append`/`commit`/`checkpoint` in between.
`pos.next_lsn()` tells you which record a resumed reader yields first.

- **`position()` is `Some` at a clean end of log.** Resuming there later yields
  whatever was committed since. It is `None` only when the reader cannot be
  resumed: after an error, after the stream ended on an invalid record, or after
  the very last possible LSN (`u64::MAX`) was yielded.
- **Right after `reader_from(from)`, before the first `next()`, the position is
  the start of the containing segment** — the skip to `from` happens inside the
  first `next()`. A position does not remember `from`.
- **The retention floor still applies.** If a checkpoint deleted the record the
  position points at, `reader_at` fails with `ContiguityViolation`, exactly as
  `reader_from` would.
- **A position belongs to the log it came from.** Never hand `reader_at` a
  position taken from another log. Where the WAL can tell — the position does
  not land on a record carrying the expected LSN — it returns
  `WalError::InvalidPosition` (which does not poison the handle), and every
  record a reader yields is still checksummed and LSN-checked. But payloads are
  opaque bytes, so it cannot always tell: treat a foreign position as a bug in
  the caller, not as something the WAL will catch for you.
- **After closing and reopening the directory, discard any position whose
  `next_lsn()` is above the recovered `durable_lsn + 1`.** It pointed into an
  uncommitted tail that recovery removed; `reader_at` cannot detect that later.

## LSNs are yours to correlate

The WAL hands out LSNs; it does not interpret payloads. Typical integrations
keep a mapping from domain state to "applied up to LSN x", persist that with
their snapshots, and use it to know where replay must start. If you run two
WALs (say, an input journal and an output journal), their LSN spaces are
completely independent — correlate them inside your payloads if you need to.
