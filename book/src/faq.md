# FAQ and gotchas

The recurring misconceptions, in roughly the order they bite.

### Is a `commit` batch atomic?

**No.** Durability is per-record; the batch is a performance grouping. A crash
during a commit that spans a segment boundary can keep the first part of the
batch and lose the rest (still a dense prefix — never a gap). If several
events must be all-or-nothing, encode them as **one record**. This is the top
misconception; [the durability model](durability-model.md) covers it in full.

### I can see the record in the file — is it durable?

**Not necessarily.** `write(2)` makes bytes visible before `fdatasync` makes
them durable, so an external reader can observe complete, CRC-valid records
that would vanish on power loss. CRC proves integrity; only the durable
watermark (`durable_lsn`, or the `DurabilityObserver` publication) proves
durability. See [the durability-visibility gap](external-access.md#the-durability-visibility-gap--the-central-hazard).

### Can I `checkpoint(wal.durable_lsn())` to reclaim everything durable?

**No — this is the classic silent-data-loss mistake.** Recovery is *snapshot +
replay after it*. Checkpoint only up to the LSN your latest **durable
snapshot** covers; deleting the log between snapshot and `durable_lsn` caps
every future recovery at the stale snapshot, and nothing warns you until a
restart. See [the caller rule](checkpointing.md#the-caller-rule--the-one-way-to-lose-data-with-a-correct-wal).

### How do I write from two threads?

You don't — the handle is `Send` but not `Sync`, so sharing it doesn't
compile. Move it into one writer thread and put a queue in front. A second
*process* is excluded by the directory lock (`WalError::Locked`). See
[Single-writer by construction](single-writer.md).

### Why isn't `Reader` a `std::iter::Iterator`?

Because it lends: each yielded `&[u8]` borrows the reader's reused buffer and
is valid only until the next call — a shape `Iterator` cannot express. That is
what keeps replay zero-copy. Use `while let Some(record) = reader.next()`, and
`.to_vec()` anything you need to keep. See
[Writing and reading](writing-and-reading.md#reading-back-the-streaming-reader).

### `commit` failed and now everything returns `Poisoned`. Can I recover the handle?

No — by design. After a failed fsync the data's fate is unknowable (the OS may
already have dropped the dirty pages), and a dense-LSN log has no safe way to
resume past a hole. `durable_lsn` still tells the truth about what made it.
Drop the handle, `Wal::open` again (recovery truncates any torn tail), and
rebuild from your snapshot + the recovered log. See
[the durability model](durability-model.md#when-fsync-fails-the-handle-poisons).

### Recovery says `TruncatedAt { .. }` — did I lose data?

You lost only records that were never covered by a successful `commit` — the
un-acknowledged tail a crash is allowed to take. Log it and carry on. The
error cases are different: `TornMidLog` / `Corruption` mean acknowledged data
is damaged mid-log, and recovery deliberately refuses to continue. See
[Recovery](recovery.md#torn-tail-vs-mid-log-corruption--the-crucial-distinction).

### Can I put JSON / protobuf / bincode in records? What about compression?

Yes — payloads are opaque bytes and the WAL never interprets them. The only
constraint is `max_record_size`. Serialization, versioning, and compression
are all yours.

### Does it work on macOS? Windows?

macOS: yes for development and correctness — the crate correctly uses
`F_FULLFSYNC` (plain `fsync` on macOS does not flush the drive cache) — but it
carries **no production durability claim**; the hardware fault-injection gate
runs on Linux only. Windows: out of scope for v1.

### My payload is bigger than `max_record_size`.

`append` rejects it with `RecordTooLarge` — no silent truncation, no
splitting. Raise `max_record_size` (it must satisfy
`max_record_size + 91 <= segment_size`), or chunk at the application level if
the pieces don't need single-record atomicity.

### What is the `fuzzing` Cargo feature I see in the source?

Internal-only: it exposes hooks for the crate's fuzz targets and differential
tests. It is not part of the public API and must never be enabled in a real
build. (Same for `inject_no_dir_fsync`, a deliberately-broken build used as a
fault-injection negative control.)

### Where are the exact guarantees written down?

[§4 (invariants D1–D12) and §5 (on-disk format) of the design
spec](https://github.com/guyo13/open-wal/blob/main/docs/wal_design_v6.md) are
normative, and §14 maps every invariant to the tests that enforce it.
