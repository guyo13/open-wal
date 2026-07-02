# Checkpointing and retention

A WAL grows forever unless something reclaims it. In an event-sourced system
the thing that makes old log entries redundant is a **snapshot**: once you
have durably persisted your application state as of LSN `s`, recovery becomes
"load the snapshot, replay the log after `s`" — and log entries at or below
`s` are no longer needed.

`checkpoint(up_to)` is the reclamation half of that bargain.

## What `checkpoint` does

```rust,ignore
// Only ever pass the LSN covered by your latest DURABLE SNAPSHOT —
// never `wal.durable_lsn()`. Recovery is snapshot + replay of the log
// after it; deleting the log past your snapshot caps recovery at the
// stale snapshot. The WAL trusts the caller here.
wal.checkpoint(snapshot_lsn)?;
```

It deletes **whole sealed segments** that are fully at or below `up_to` —
oldest first, followed by a directory fsync so the deletions are durable. It
never rewrites, compacts, or partially truncates anything, and it **never
deletes the active segment**. Afterwards the oldest available LSN advances;
records above `up_to` are exactly as readable as before. If no segment is
fully superseded, the call is a no-op — space is reclaimed at segment
granularity, so `segment_size` is also your retention granularity.

Because deletion is oldest-first and only ever removes a prefix, a crash at
any point mid-checkpoint leaves the survivors a contiguous suffix; re-running
the checkpoint after recovery is safe and idempotent.

## The caller rule — the one way to lose data with a correct WAL

The WAL guarantees it will never delete a record above `up_to`. What it
*cannot* check is whether **you** can afford to lose the records below it.

The safe bound is your latest **durable snapshot LSN — not `durable_lsn()`**.
It is tempting to "clean up everything that's durable" by calling
`checkpoint(wal.durable_lsn())`, and it is exactly wrong: recovery needs the
log *between your snapshot and the durable watermark* to replay. Delete it and
nothing fails today — but the next restart silently comes up with state frozen
at the stale snapshot. That is the inverse of the WAL's own promise: the WAL
won't delete what you kept; you must not ask it to delete what you cannot
rebuild.

The discipline that follows:

1. Persist a snapshot of application state, durably, recording the LSN `s` it
   covers.
2. Only after the snapshot is safely down: `wal.checkpoint(s)`.
3. On restart: load the snapshot, then replay with `reader_from(Lsn(s + 1))`.

## Retention margin for readers and backups

Checkpointing interacts with anything else reading the log. A backup job or a
tailing reader that still needs a deleted segment hits the
[fatal-gap rule](external-access.md#the-retention-floor-gap-is-fatal): it
fails loudly rather than silently skipping ahead. The writer does not track
reader positions in v1 — retention policy is yours. Practical options: keep a
margin of N segments behind your snapshot, checkpoint on a schedule that
outlasts your slowest consumer, or accept that a lagging consumer re-seeds
from a fresh snapshot.

One convenience on the reading side: an already-open file descriptor survives
`unlink` on Linux, so a reader that is *mid-segment* when a checkpoint deletes
that segment finishes it normally — the gap only bites a reader that hadn't
opened the file yet.
