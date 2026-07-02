# External readers, backup, and replication

The log is single-writer, but its on-disk format is deliberately *tailable*:
records are self-describing, LSN-stamped, and CRC-protected, and **sealed
segments are immutable** — once a newer segment exists, a sealed segment's
bytes never change until a checkpoint deletes the whole file. That immutability
is what makes everything in this chapter safe.

This chapter covers what v1 supports directly, the one hazard every external
consumer must understand, and where the crate's responsibility ends.

## The durability-visibility gap — the central hazard

The writer's `write(2)` makes bytes visible in the OS page cache *before* the
`fdatasync` that makes them durable. A concurrent reader (another process
tailing the files, or even a thread reading between the writer's `write` and
sync) can therefore see records that are complete and **CRC-valid but not yet
durable**. On power loss, those records vanish.

**A valid CRC proves integrity, not durability.** Any consumer whose actions
must never outrun the primary's durable state — a replica, a downstream system
receiving acknowledgements — must consume only up to the **published durable
watermark**, no matter how many valid-looking records are visible past it.
(A pure backup consumer is the exception; see below.)

## Publishing the watermark: `DurabilityObserver`

The writer publishes its watermark through a pluggable observer. `Wal` is
generic over it — `Wal<O: DurabilityObserver = NullObserver>` — and the
default `NullObserver` compiles to nothing, so you pay for the hook only if
you use it. The observer fires on the writer thread at the end of each
`commit`, strictly *after* durability has advanced, so it can never affect the
durability guarantees; it receives a monotonic `durable_lsn`.

```rust,ignore
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use open_wal::{DurabilityObserver, Lsn, Wal, WalConfig};

/// Publishes the durable watermark to an atomic another thread reads.
struct WatermarkPublisher(Arc<AtomicU64>);

impl DurabilityObserver for WatermarkPublisher {
    fn on_durable(&mut self, durable_lsn: Lsn) {
        // Contract: cheap, non-blocking, no I/O, must not panic.
        // A release-store (or a queue push) is the intended shape.
        self.0.store(durable_lsn.0, Ordering::Release);
    }
}

let watermark = Arc::new(AtomicU64::new(0));
let (mut wal, _report) = Wal::open_with(
    dir,
    WalConfig::default(),
    WatermarkPublisher(Arc::clone(&watermark)),
)?;
```

Keep `on_durable` to exactly that shape — an atomic store or a queue push. It
runs synchronously inside `commit`, so blocking or doing I/O there puts your
observer's latency on the commit path. The observer carries only the
*watermark*; shipping actual record bytes is a separate consumer's job,
reading via a `Reader` up to the watermark it was handed.

## Backup

Backup needs no watermark and no coordination beyond retention:

1. **Sealed segments:** copy freely, any time, in any order — they are
   immutable, so a copy is coherent by construction. You can checksum one once
   and trust it until it is deleted.
2. **Active segment:** copy it as-is, even mid-write. The copy may end in a
   torn record; that is fine, because restoring is just running normal
   recovery — `Wal::open` on the copied directory truncates the torn tail and
   yields a consistent dense prefix.

A backup taken this way may include a few records that were never durable on
the primary (the visibility gap above). For backup that is harmless: the
restore is still an internally consistent log. Only if a backup must match an
exact durability point do you need to trim to the watermark.

Schedule copies against your [checkpoint cadence](checkpointing.md) so a slow
copy isn't undercut by segment deletion.

## The retention floor: gap is fatal

A checkpoint can delete segments a lagging reader still needs. When a reader's
next-expected LSN is older than the oldest available LSN, it must **fail
loudly** — silently skipping to the oldest available record would be silent
data loss on the consumer's side. The in-process `Reader` enforces this
(`reader_from` an already-reclaimed LSN errors), and any external reader you
write must do the same. Recovery from a gap is operational: re-seed the
consumer from a fresh snapshot or backup. The v1 writer does not track or wait
for readers; retention margin is integrator policy.

## Replication: what v1 gives you and what it doesn't

v1 supports **in-process log shipping**: a consumer in the writer's process
learns the watermark from the observer (or your own cursor), reads newly
durable records with a `Reader`, copies them out (`.to_vec()` — the network
serialization boundary pays that copy anyway), and ships them. The one
inviolable rule: **never ship a record above `durable_lsn`** — otherwise a
power loss on the primary leaves the replica *ahead* of the primary, which is
divergence.

What v1 does **not** include: a cross-process watermark channel (so
*replication* readers in another process are deferred — cross-process
*backup* is fine, as above), the transport, the replica-ack protocol, and
failover. The crate's responsibility ends at providing committed records in
order plus a truthful watermark; see
[§15 of the design spec](https://github.com/guyo13/open-wal/blob/main/docs/wal_design_v6.md)
for the full normative treatment of external access.
