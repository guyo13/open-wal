# Single-writer by construction

`open-wal` allows exactly one writer per log directory. This is not a
limitation to be worked around — it is a load-bearing design decision, and the
crate spends type-system and OS machinery making it impossible to violate by
accident.

## Why single-writer

A multi-writer log needs interior locking, write coordination, and a much
larger correctness argument — precisely the machinery an LMAX-style system
exists to avoid. With one writer:

- The hot path has no locks, no atomics, no contention — `append` is a plain
  memory write into a buffer owned by one thread.
- LSN assignment is trivially dense and ordered.
- The crash-recovery story stays tractable: every on-disk state is the result
  of one sequential writer stopping at some point, which is what makes the
  [recovery](recovery.md) classification (torn tail vs. real corruption)
  sound.

If you have multiple producers, put a queue in front of the single writer
thread — that is the intended integration shape, not a workaround.

## Two layers of enforcement

**Within a process: the type system.** `Wal` is `Send` but deliberately
**not `Sync`**, and every write method takes `&mut self`. You can move the
handle to another thread, but you cannot share it between two: `Arc<Wal>`
gives you no way to call `append`, and `&Wal` cannot cross threads. Concurrent
writers are a *compile error*. (This is verified in CI with a compile-fail
test.)

**Between processes: an OS lock.** `open` takes an exclusive advisory `flock`
on a `LOCK` file in the WAL directory and holds it for the handle's lifetime.
A second process (or a second handle in the same process) gets a clean error:

```rust,ignore
let (wal, _) = Wal::open(dir, WalConfig::default())?;
match Wal::open(dir, WalConfig::default()) {
    Err(open_wal::WalError::Locked) => { /* expected: one writer at a time */ }
    Err(other) => panic!("expected Locked, got {other:?}"),
    Ok(_) => panic!("expected Locked, got a second writer"),
}
drop(wal); // releases the lock; now a new writer may open
let (_wal, _) = Wal::open(dir, WalConfig::default())?;
```

One operational wrinkle: after a writer process *crashes*, the OS may release
its lock a beat after the process disappears, so an immediate restart can see
a transient `Locked`. Retry briefly (up to ~1 s) before concluding another
writer is alive — see [Recovery](recovery.md#one-operational-note-reopening-after-a-crash).

Note that read-only access is not writer-locked: `Reader`s borrow the writer
in-process, and external readers attach without touching the exclusive lock
(see [External readers](external-access.md)).

## What the integrator owns

Single-writer scoping means the crate deliberately stops at the durability
boundary. Around it, you own:

- **The publish/ack barrier.** The WAL returns `durable_lsn`; gating
  downstream consumers on it (an atomic cursor, a channel — the LMAX
  daisy-chain) is your code. The
  [`DurabilityObserver`](external-access.md#publishing-the-watermark-durabilityobserver)
  hook exists to feed it.
- **Serialization.** Payloads are opaque bytes; encode/decode however you
  like, subject only to `max_record_size`.
- **Snapshots and the checkpoint trigger.** The WAL reclaims what you tell it
  to; knowing what is safe to reclaim requires your snapshot — see
  [Checkpointing](checkpointing.md).
- **Replication transport and failover**, if any — see
  [External readers](external-access.md).

A common deployment runs **two independent instances** — an input journal and
an output journal. Each has its own directory, lock, LSN space, and recovery;
the WAL neither knows nor cares that the other exists.
