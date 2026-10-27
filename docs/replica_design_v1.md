# `open-wal-replica` — Design, Implementation & Test Specification

**Status:** Draft **v1** — async master/slave, static primary, no election, no sharding.
**Depends on:** `open-wal` at **spec v7** (`Wal::options().seed(..)` cold-start seed,
`oldest_lsn()`, containing-segment `reader_from`). Do not begin RM3+ until the v7 WAL delta has landed.
**Intended audience:** a coding agent implementing the crate, plus a human reviewer.
**Crate name:** `open-wal-replica` (owner's call; the name is not load-bearing).
**Revisions:** 2026-10-27 — §9 step 2 off-by-one fixed: the "ahead of the primary" test is
`d > durable_lsn` (as §12.2 always said), not `d + 1 > durable_lsn`, which rejected every
fully caught-up replica as a former primary. 2026-10-27 (RM3 prep) — §9 step 3 now requires
**bounded, chunked** catch-up (§9.1) with an explicit cutover rule and ring-sizing constraint
(§9.2). The old step 3 copied the whole lag range in one writer-thread call, an unbounded
writer stall and memory spike for a replica gigabytes behind. 2026-10-27 (RM3 prep, after an
adversarial critique, each finding re-verified against the RM2 code) — §9 rewritten:
- a request/answer writer↔network handshake (§9.3), so an attach can never race a session
  teardown;
- a serve ceiling of `released` (R1's own watermark), with `Ahead` ⇒ wait, never reject;
- below-oldest ⇒ close and re-HELLO, so R6 still answers at join;
- loud, sticky failure on an unreadable committed record;
- in-session fall-back on ring overflow;
- an honest writer-stall bound (the containing-segment rescan);
- a normative `poll(wal)` cadence;
- the ring LSN domain enforced at RM3.

This is a build contract in the same style as the WAL spec: invariants are normative,
tests are co-equal deliverables, and **the crate is "done" only when its fault-injection tests
pass** — a replication layer that has not been *executed* against crash and power-loss
injection is not trustworthy. Normative keywords MUST / MUST NOT / SHOULD / MAY per RFC 2119.

---

## 1. Goals and Non-Goals

### Goals
- **Data redundancy** for an `open-wal` log: every record the primary has durably committed
  eventually exists, byte-identical, in one or more replicas' own durable WALs.
- **Async**: the primary's `commit()` latency is **never** affected by replication. Client
  acknowledgement remains gated on *local* durability exactly as today.
- **Never diverge**: a replica is always a **dense prefix** of the primary's durable log.
- **Bounded loss on primary failure**: at most the un-shipped durable tail (bounded by lag).
- **Manual promotion** that is safe and mechanical (no destructive ops needed).
- Sits **on top** of the WAL: no WAL invariant weakened, no on-disk format change.

### Non-Goals (v1)
- **Not availability redundancy.** No failure detection, no leader election, no automatic
  failover. If the primary dies, an *operator* promotes a replica (§12).
- **Not synchronous replication.** The ack-barrier *surface* is designed (§13) so an
  integrator can gate on replica durability later, but v1 does not gate.
- **Not sharding / multi-primary.** One primary, N replicas.
- **Not a snapshot system.** Re-seeding a replica needs an application snapshot; the crate
  defines the trait, the application implements it (§10).
- **Not cross-process tailing.** The shipper runs in the primary's process (§15.7 of the WAL
  spec, and forced by `Wal` being `!Sync` — see §4.1).

---

## 2. What redundancy this gives — read this first

Async replication protects the **data**, not the **service**. With a primary and replicas:

- **Primary disk dies, primary process survives** → nothing is lost that was shipped; the
  operator promotes the most-advanced replica; loss = un-shipped tail (typically milliseconds).
- **Primary machine dies** → same; a human promotes; the service is down until they do.
- **A replica dies** → nothing lost; it resumes from its own `durable_lsn` on return, or is
  re-seeded if it fell behind the primary's retention floor.

The **loss window** on primary failure is the replication **lag**: records the primary
committed (and acknowledged to clients) but had not yet shipped. This is inherent to async.
The integrator MUST decide whether that window is acceptable; if not, the sync barrier
(§13) is the v2 path and it costs a network round-trip + remote fsync per commit — which
fights the LMAX latency goal, so it is a genuine product decision, not a default.

---

## 3. Grounding

| Source | What we borrow |
|---|---|
| **PostgreSQL streaming replication** | WAL shipping by LSN; the standby's own WAL; catch-up from the primary's retained WAL; `pg_basebackup` re-seed when the standby falls behind retention. |
| **open-wal §15** | The substrate: immutable sealed segments (D12), `durable_lsn` as the only ship ceiling (§15.1), gap-is-fatal (§15.4), "replica must never be ahead of the primary" (§15.7). |
| **Raft (what we deliberately don't do yet)** | Election, terms, fencing, and **log truncation on reconciliation** — the exact machinery this v1 avoids by keeping replicas always-prefixes. Named so the v2 boundary is explicit. |

Guiding insight: **with a static primary and a ship-only-≤-`durable_lsn` rule, a replica can
never hold a record the primary did not durably commit, so it is always a prefix and never
needs to truncate.** Every hard problem in replication comes from breaking that property;
v1 keeps it, which is why v1 is tractable.

---

## 4. Architecture

```
PRIMARY PROCESS (writer thread = LMAX event loop)          REPLICA PROCESS
 ┌────────────────────────────────────────────────┐        ┌───────────────────────────┐
 │  wal.append(ev)  ──► shipper.capture(lsn, ev)  │        │  Receiver                 │
 │      ...           (memcpy into ring, no I/O)   │  TCP   │   verify lsn == last+1    │
 │  wal.commit() → Ok(w) ─► shipper.on_commit(w)  │ ═════► │   wal.append(payload)     │
 │      (release ring entries ≤ w for sending)     │        │   group commit()          │
 │                                                 │ ◄═════ │   ack(durable_lsn)        │
 │  [network thread]: drain ring → send RECORD     │  ACK   │  (own open-wal, own dir)  │
 │  [catch-up, rare]: wal.reader_from(n) on writer │        └───────────────────────────┘
 │     thread → copy → ring/net                    │
 └────────────────────────────────────────────────┘
```

Three components:
- **`Shipper`** (primary, in-process): a ring of captured records + per-replica cursors; a
  network thread per replica (or one multiplexed) that sends released records and receives acks.
- **`Receiver`** (replica): a TCP acceptor that validates stream contiguity, appends to the
  replica's own `Wal`, group-commits, and acks its `durable_lsn`.
- **Wire protocol** (§6): length-prefixed, CRC-protected frames.

### 4.1 Why capture-at-append, not read-after-commit (NORMATIVE rationale)
Two facts about the WAL force this design:
1. `Wal` is `!Sync` and `reader_from(&self)` borrows it, so **no other thread can read the
   log** through the `Wal` handle.
2. `reader_from(from)` scans from a segment start (and in v6 from the *oldest* segment), so
   calling it after every commit would rescan the log per commit — fine in a 10-record test,
   fatal in production.

Therefore the shipper MUST obtain record bytes by **capturing them at append time** (a memcpy
into a preallocated ring, on the writer thread, no I/O) and MUST **release** them for sending
only when `commit()` returns `Ok(w)` covering them. `reader_from` is used **only** for the
rare catch-up path (§9). The network I/O runs on a separate thread. The writer thread's cost
per record is one bounded memcpy; per commit, one ring release + one wake.

`DurabilityObserver` is **not required** for v1: the integrator calls
`shipper.on_commit(w)` with `commit()`'s returned watermark, which is the same value the
observer would deliver (§15.3 fires after durability). Using the observer to trigger the same
call is permitted and equivalent. (The observer becomes the wake mechanism for the v2
off-thread `WalReader`-based shipper — §16.)

### 4.2 Thread model (NORMATIVE)

"In the primary's **process**" is not "on the writer **thread**." The only hard constraint is
that anything touching `&Wal` is pinned to the writer thread (`Wal` is `!Sync`). Everything
else runs on separate threads — that is what makes the design *async*: the writer never waits
on the network.

| Operation | Thread | Touches `&Wal`? | May block? | Cost on writer |
|---|---|---|---|---|
| `capture(lsn, payload)` | **writer** (after `append`) | no (just the lsn) | **never** | one bounded memcpy into a ring slot |
| `on_commit(w)` | **writer** (after `commit` Ok) | no | **never** | Release-store `released = w` + one wake; **sends nothing** |
| Catch-up read (§9) | **writer**, inside `poll(wal)` | **yes** (`reader_from`) | I/O, bounded per call (§9.1) | ≤ one containing-segment prefix + one chunk per opened reader; ≤ `catchup_replicas_per_poll` readers per `poll` |
| Drain ring → send `RECORD` | **network** (one per replica) | no | yes (socket) | none |
| Read `ACK`/`HEARTBEAT`, reconnect/backoff | **network** | no | yes | none |
| Slot free (all cursors passed) | **writer** (on `capture`, lazily) | no | never | Acquire-load of consumer cursors |
| Overflow evict (§8.3) | **writer** (on `capture`) | no | **never** | mark affected replicas `NeedsCatchUp` |

- **One network thread per replica** (the default). A slow replica fills only its own send
  path and can never head-of-line-block the others or the writer. A single multiplexed thread
  with non-blocking sockets is permitted but MUST preserve "writer never blocks."
- The ring is therefore a **single-producer (writer) / multi-consumer (one per replica)**
  structure with per-consumer cursors. Its cross-thread correctness is the subject of §15.7
  (loom).

---

## 5. Replication Contract (NORMATIVE) — R1–R9

Each maps to tests in §15.

- **R1 — Never ahead (divergence guard).** At all times, for every replica: the set of
  records in the replica's durable log ⊆ the set of records the primary has durably
  committed. Concretely, the shipper MUST NOT send a record with `lsn > w` where `w` is the
  latest `commit() → Ok(w)` watermark. A CRC-valid captured record whose commit has not
  returned is **not shippable** (the §15.1 durability-visibility gap, applied to the ring).
- **R2 — LSN mirroring.** Every record has the **same LSN** on primary and replica. The
  replica's `Wal` is cold-started at the primary LSN it begins from (`Wal::options().seed(..)`), and
  because `append` assigns `last_lsn + 1`, mirroring holds iff R3 holds.
- **R3 — Stream contiguity (hard check).** The receiver MUST verify each incoming record's
  `lsn == replica_wal.last_lsn() + 1` **before** `append`. Any other value (gap, duplicate,
  reorder) is a protocol violation ⇒ the receiver MUST NOT append; it MUST close the
  connection with `ERR(Contiguity)` and re-HELLO from its `durable_lsn` (§9). Never skip,
  never "best effort". (The WAL's dense-LSN invariant becomes the stream-integrity check.)
- **R4 — Ack honesty.** A replica acks **only** its own `durable_lsn` (after its
  `commit() → Ok`), never `last_lsn`. An ack of `n` means "records ≤ n are durable on this
  replica" — the same meaning `durable_lsn` has on the primary.
- **R5 — Prefix property / bounded loss.** Every replica's durable log is a dense prefix
  `oldest_r..=durable_r` of the primary's durable log. On primary failure, promoting the
  replica with the highest `durable_lsn` loses at most the primary's un-shipped tail, and
  every other replica is a prefix of the new primary (so it catches up without truncation).
- **R6 — Gap is fatal at catch-up.** If `replica.durable_lsn + 1 < primary.oldest_lsn()`,
  the records the replica needs were checkpointed away ⇒ the primary MUST answer
  `RESEED_REQUIRED`, and the replica MUST NOT resume from any other point. Never silent skip.
- **R7 — Stateless primary.** The shipper persists **no** shipping position. On primary
  restart, each replica's HELLO carries its `durable_lsn`, and shipping resumes from there.
  (No manifest, no side file, no new crash-consistency surface — same philosophy as the WAL.)
- **R8 — Replica is durability-first.** The replica writes a real `open-wal` with the same
  commit discipline; its ack is a durability claim (R4) with the WAL's D1 behind it.
- **R9 — Byte fidelity.** Shipped payloads are byte-identical to the primary's; each wire
  frame carries CRC-32C over `(lsn, payload)`; a mismatch is `ERR(WireCrc)` ⇒ reconnect and
  re-HELLO (defense in depth over TCP's weak checksum; the replica's WAL re-frames and
  re-CRCs on its own append regardless).

### 5.1 Non-guarantees
- **No loss-free failover.** The lag window is lost on primary failure (async).
- **No automatic promotion.** An operator promotes (§12).
- **A former primary MUST be re-seeded, never resumed as a replica** (§12.2). It may hold
  durable records beyond the new primary — the *only* divergence case v1 admits — and v1
  has no suffix-truncate, so wipe-and-reseed is the only safe path.
- **The primary detects a returning former primary only while it is ahead.** §9 step 2
  rejects a HELLO with `durable_lsn > primary.durable_lsn`. Once the new primary's log passes
  that value, an un-wiped former primary looks like an honest replica and is served on top of
  its divergent tail. The operator wipe (§12.2) is the guard; step 2 is a tripwire (§16).
- **Ring overflow drops nothing durable** — it only forces a catch-up from the log (§8.3).

---

## 6. Wire protocol (NORMATIVE)

TCP. All frames: `len:u32 | type:u8 | body`. Little-endian. Protocol version negotiated in
HELLO; unknown type ⇒ `ERR(Protocol)`.

| Type | Direction | Body | Meaning |
|---|---|---|---|
| `HELLO` | R→P | `proto_ver:u16, replica_id:u64, durable_lsn:u64` | "I am durable to `durable_lsn`; serve me from `+1`." Sent on every (re)connect. |
| `SERVING` | P→R | `from_lsn:u64, primary_oldest:u64` | Primary will stream from `from_lsn` (= `durable_lsn+1`). |
| `RESEED_REQUIRED` | P→R | `primary_oldest:u64` | `durable_lsn+1 < oldest_lsn` (R6). Replica must re-seed (§10). Only ever sent **in place of** `SERVING`; a mid-stream below-oldest ends the session instead, and the re-HELLO gets this frame (§9.1). |
| `RECORD` | P→R | `lsn:u64, crc:u32, payload:[u8]` | One record; `crc = crc32c(lsn ‖ payload)`. Strictly ascending `lsn`. |
| `ACK` | R→P | `durable_lsn:u64` | Replica's durable watermark (R4). Monotonic. |
| `HEARTBEAT` | both | `durable_lsn:u64` | Keepalive; carries the sender's watermark. |
| `ERR` | both | `code:u16, msg` | `Contiguity`, `WireCrc`, `Protocol`, `Poisoned`. Then close. |

- `RECORD` batching: the sender MAY pack several records per TCP write; framing is per-record.
- Backpressure is TCP's; the shipper never blocks the writer thread waiting on the socket (§8.3).

---

## 7. Receiver (replica) — NORMATIVE

1. **Open** its own `Wal` in its own directory (`WalConfig` with the primary's
   `segment_size`/`max_record_size`; via `Wal::options().seed(Lsn(s_lsn + 1)).open(dir, cfg)`
   on a fresh seed per §10, else plain `Wal::open`; add `.observer(o)` if the replica hosts
   downstream consumers — §7.1).
   Recovery runs as usual; `durable_lsn` is the resume point.
2. **Connect** to the primary; send `HELLO{durable_lsn}`.
3. On `SERVING{from}`: assert `from == last_lsn() + 1`, else `ERR(Contiguity)`.
4. For each `RECORD{lsn, crc, payload}`:
   - verify `crc` (R9) — mismatch ⇒ `ERR(WireCrc)`, close, reconnect;
   - verify `lsn == wal.last_lsn() + 1` (R3) — else `ERR(Contiguity)`, close, reconnect;
   - `wal.append(payload)`; the returned LSN MUST equal `lsn` (assert — mirroring, R2).
5. **Group commit:** `wal.commit()` when either `batch_records` or `batch_interval` is
   reached (config). After `commit() → Ok(w)`, send `ACK{w}` (R4). A `commit()` `Err`
   poisons the replica WAL per §12 of the WAL spec ⇒ send `ERR(Poisoned)`, close, and the
   replica process must reopen (recovery) before reconnecting.
6. **Checkpoint** its own WAL on the replica-integrator's policy (independent of the primary;
   bounded by *its* snapshot LSN per WAL §9 — a replica with no snapshots keeps everything or
   uses `oldest_lsn` of a promoted-from-snapshot seed).

The receiver MUST NOT accept records out of order, MUST NOT append on a CRC failure, and
MUST NOT ack `last_lsn`.

### 7.1 Replica-hosted downstream consumers (NORMATIVE where marked)

A replica is a full `open-wal` node, so it MAY host downstream consumers (read models,
subscribers, a further replication tier) off its **own** `DurabilityObserver` or its own
`on_commit`-style hook, instead of the primary publishing to every consumer. This is not just
fan-out offload — it has a stronger safety property than primary-hosted publishing:

- **What a replica publishes is durable on at least two nodes.** A replica's observer fires
  after the *replica's* `commit() → Ok`, and by R1 every such record is already durable on the
  primary. A consumer fed from a replica therefore never sees a record durable on only one node.
- **It survives failover.** Under the §12.1 promotion rule (highest `durable_lsn` wins),
  everything any replica has published is retained by the new primary. A consumer fed from the
  *primary's* observer, by contrast, can see the un-shipped tail — records that are **lost**
  when the primary dies — and act on data that does not survive. Replica-hosted publishing
  bounds consumers to the 2-durable prefix at zero cost to primary latency: a poor-man's
  semi-sync for the consumer path.
- **Consequence (NORMATIVE):** the §12.1 rule "promote the replica with the highest
  `durable_lsn`" is **load-bearing for consumer correctness**, not only for log density.
  Promoting a lower replica while a higher one has published would make that higher replica's
  consumers diverge from the new primary. Operators MUST NOT promote below the highest
  publishing replica.
- **Latency:** replica-fed publish latency = replication lag + the replica's group-commit
  interval (`batch_records`/`batch_interval`). Tune the replica's batch for the consumer SLA.
- **Consumer rule (NORMATIVE):** a replica-hosted consumer MUST act only on records the
  replica has **committed** (observer / post-`commit` hook), never on `append` — the replica-side
  analogue of R1.
- **Chain replication** (a replica running its own `Shipper` fed by its own commits, i.e.
  primary → A → B) is permitted by this architecture and needs no new mechanism; it is
  deferred as a documented v1.x pattern, not built in v1.

---

## 8. Shipper (primary) — NORMATIVE

### 8.1 Capture (writer thread)
`capture(lsn, payload)` — called by the integrator immediately after each `wal.append` returns
`lsn`. Copies `(lsn, payload)` into a **preallocated ring** (bounded `ring_bytes`). No I/O, no
syscall, no allocation in steady state (a ring of fixed slots; oversized payloads MAY use a
fallback allocation — document it). The record is **not shippable yet**.

### 8.2 Release (writer thread)
`on_commit(w)` — called with `commit()`'s returned `Ok(w)`. Marks every ring entry with
`lsn ≤ w` as **shippable** and wakes the network thread. This is the R1 gate: the network
thread MUST only ever send entries with `lsn ≤ released_watermark`. It MUST NOT be called on
`commit() → Err` (the handle is poisoned; captured-but-unreleased entries are discarded on
reopen — they were never durable-acknowledged, and any that *were* durable in a split batch
are recovered by the replica's catch-up from the log, §9). A `Shipper` MAY outlive an in-process
WAL reopen: `capture` discards the stale captured-but-unreleased tail on re-capture (§8.1, as
built). After the reopen, and before the next `append`, the integrator MUST call
`shipper.on_commit(wal.commit()?)`. An empty `commit()` on the reopened handle returns
`Ok(durable_lsn)`, so this releases the split-batch survivors under R1's own wording. A new
`Shipper` built after a process restart starts at `released = wal.durable_lsn()` and needs
nothing extra.

### 8.3 Network thread & overflow — never block the writer
- Drains shippable entries per replica cursor, sends `RECORD`s, reads `ACK`s, updates
  `replica_acked[id]`.
- A ring slot is **freed** once every connected replica's *send* cursor has passed it.
- **Overflow rule:** if `capture` finds the ring full, it MUST evict the oldest slot and mark
  every replica whose cursor still pointed at it as **`NeedsCatchUp`**. It MUST NOT block, MUST
  NOT drop the capture of the *new* record, and MUST NOT affect `append`/`commit`. A
  `NeedsCatchUp` replica falls back, **within its session**, to catch-up from the log (§9.1),
  then cuts back over to the ring (§9.2). There is no reconnect. Nothing durable is ever lost
  by overflow: the log is the source of truth and the ring is a fast path.
- A disconnected replica re-HELLOs and is caught up from its `durable_lsn` (R7, §9).

### 8.4 Shipper API surface (informative shape)
```rust
pub struct Shipper { /* ring, cursors, acked map, net handles */ }
impl Shipper {
    pub fn new(cfg: ShipperConfig, wal: &Wal<O>) -> Result<Self>;  // Err past ring::MAX_LSN (§9.2)
    pub fn capture(&mut self, lsn: Lsn, payload: &[u8]);          // after append
    pub fn on_commit(&mut self, durable: Lsn);                     // after commit Ok: release + wake only
    pub fn poll(&mut self, wal: &Wal<O>) -> Result<()>;            // §9: joins + catch-up; MUST be called
                                                                   // after every on_commit and ≥ once per
                                                                   // heartbeat when idle (§9.1 cadence)
    pub fn catching_up(&self) -> usize;                            // replicas in CatchingUp (poll faster)
    pub fn min_replica_acked_lsn(&self) -> Option<Lsn>;            // §11 checkpoint policy
    pub fn replica_acked_lsn(&self, id: ReplicaId) -> Option<Lsn>; // §13 sync surface
}
```

---

## 9. Catch-up / resume (NORMATIVE)

Triggered by a replica `HELLO{durable_lsn = d}` on every (re)connect.
- A HELLO with `d ≥ ring::MAX_LSN` lies outside the crate's LSN domain (§9.2). The network
  thread refuses it with `ERR(Protocol, "LSN domain exhausted")` and does not post a join.
- Otherwise the network thread posts a **join request** carrying `d`. The writer thread
  answers it inside `Shipper::poll(&mut self, wal)`, because the decision needs `&Wal`
  (§9.3).

Let `released` be the shipper's released watermark: the latest `on_commit(w)`, or
`wal.durable_lsn()` at `Shipper::new`.

1. If `d + 1 < wal.oldest_lsn()` ⇒ send `RESEED_REQUIRED{oldest}` (R6). Stop.
2. Else if `d > wal.durable_lsn()` ⇒ the replica claims more than we have durably ⇒ send
   `ERR(Contiguity)`. The replica MUST NOT be served (§12.2). Log loudly.
   - `d == wal.durable_lsn()` is a **fully caught-up** replica, not an ahead one, and step 3
     serves it.
   - Step 2 is the **only** source of this rejection. A ring `AttachError::Ahead` never
     rejects (see the serve ceiling below).
   - Step 2 is a **tripwire, not a guard**. It catches a former primary only while that
     primary is still ahead of this one. Once this primary's `durable_lsn` passes its `d`,
     nothing in the protocol can tell it from an honest replica. The §12.2 operator wipe is
     the only guard (§5.1, §16).
3. Else send `SERVING{from = d+1, primary_oldest = wal.oldest_lsn()}` (read at decision
   time) and serve `d+1..`. Serve from the ring if it holds the cursor. Otherwise serve from
   the log in bounded chunks (§9.1) until the replica can cut over to the ring (§9.2). The
   join answer for step 3 carries the first chunk answer, for `cursor = d + 1`, computed in
   the same `poll`.

`d + 1` is always computed with `Lsn::checked_next()` (WAL v7.1), never the saturating
`next()`. The same holds for every cursor advance below.

**Serve ceiling (R1).** Every record shipped, from the ring or from the log, has
`lsn ≤ released`, which is exactly R1's "latest `commit() → Ok(w)`". Catch-up reads stop at
`released`. They never stop at `wal.durable_lsn()` or at the reader's end.
- `released ≤ wal.durable_lsn()` always holds.
- The two differ between `commit()` and `on_commit()`, and after a `commit()` `Err`: records
  durable in a split batch that no `Ok` returned. §8.2 says how those are released.
- The ceiling makes every cutover cursor `≤ released + 1` by construction.
  `AttachError::Ahead` can therefore only mean `released < d ≤ wal.durable_lsn()`. That
  replica **waits**, served nothing, until `released ≥ d`. It is never rejected.

### 9.1 Bounded, chunked catch-up (NORMATIVE — RM3)

A lagging replica may be gigabytes behind. Copying the lag in one writer-thread call would
stall the writer for the whole lag and buffer it in memory. Therefore:

- **Where.** Catch-up reads run **only** inside `Shipper::poll(&mut self, wal)` on the writer
  thread, and only to answer a **chunk request** posted by the replica's network thread
  (§9.3).
  - Never in `on_commit`. It is a release plus wakes and nothing else (§4.2). RM2's
    `on_commit` also decided joins; RM3 moves that into `poll(wal)`.
  - Never on a network thread (`Wal` is `!Sync`).
- **Answering a chunk request.** A chunk request names `cursor`, the next LSN the network
  thread will put on the wire.
  1. If `attach(id, cursor)` succeeds ⇒ **Attached{cursor}**, with no records. This is also
     the fast path for a replica whose `d + 1` is still in the ring.
  2. Else if `cursor > released` ⇒ **Wait**, **without opening a reader**. A replica that is
     caught up but outside the ring, or one that is ahead of `released`, costs no log I/O.
  3. Else open `wal.reader_from(cursor)`. Copy records into the replica's chunk buffer while
     `lsn ≤ released` and the copied payload stays within `catchup_chunk_bytes` (always at
     least one record). Then `attach(id, last + 1)`:
     - Ok ⇒ **Chunk{records, attached}**;
     - `Behind` ⇒ **Chunk{records}**;
     - `Ahead` cannot occur, because of the ceiling.
  - `AlreadyAttached` cannot occur either, because the network thread always detaches before
    it posts a request. If it does occur, it is an internal bug: end the session loudly and
    `debug_assert!`.
- **Below oldest (R6).** If `reader_from` fails with the below-oldest gap error, the
  integrator checkpointed past the replica (§11) ⇒ **BelowOldest**.
  - The network thread closes the session with a FIN (no `ERR`) and reconnects immediately.
  - The replica's next HELLO is decided by step 1, which answers `RESEED_REQUIRED`.
  - This is loud and skips nothing. It needs no new frame and no new receiver state:
    `RESEED_REQUIRED` is only ever sent in place of `SERVING` (§6).
- **Unreadable committed record ⇒ loud and sticky.** If the reader returns `None`, an `Err`,
  or an LSN `≠` the expected one while the expected LSN is `≤ released`, a record the
  primary durably committed cannot be read ⇒ **ReadFailed{lsn}**.
  - `Reader::next` ends with `None`, not `Err`, on an invalid record or an LSN-continuity
    break. "`None` below the ceiling" MUST therefore be treated as this failure, never as an
    empty chunk.
  - `poll` returns `Err(ReplError::PrimaryLogUnreadable{lsn})`.
  - The replica's status becomes `Failed` and its session ends.
  - The shipper remembers `lsn`. Any later request whose cursor is `≤ lsn` is answered
    ReadFailed without reading. There is no rescan loop, and a ring copy of `lsn` is not
    used to paper over it.
  - The condition persists until the integrator recreates the `Shipper`.
- **Poisoned primary.** `poll(wal)` MAY be called on a poisoned primary handle; the crate
  cannot detect poison through `&Wal`. The ceiling still binds: nothing above `released` is
  served, even though the file may hold CRC-valid records above it.
- **Never blocks; bounded memory.**
  - The chunk buffer is owned by whichever side the §9.3 handshake says, so the writer never
    waits on a lock or a queue.
  - At most one request per replica is outstanding.
  - Memory is one reused buffer per catching-up replica: `≤ catchup_chunk_bytes +
    max_record_size` payload bytes.
- **Writer stall per `poll` (honest bound).**
  - `reader_from(cursor)` starts at the base of the segment containing `cursor`. It reads and
    CRC-checks every earlier record in that segment (WAL §8.5). A `Reader` borrows `&Wal`, so
    it cannot be kept across calls.
  - One answer that opens a reader therefore reads at most `segment_size +
    catchup_chunk_bytes + max framed record` bytes.
  - `poll` opens a reader for at most `catchup_replicas_per_poll` replicas, round-robin
    (default 1). Answers that read nothing (Attached, Wait, join decisions) are not limited.
  - The per-`poll` stall is therefore bounded by the chunk and segment sizes, not by the
    number of lagging replicas.
  - Total log reads for a lag `L` are about `L · (1 + (S/C − 1)/2)`, where
    `S = segment_size` and `C = catchup_chunk_bytes`. With `C ≥ S` this is `≤ 2L + S`.
  - Measured hot-cache cost is about 12 ms per 16 MiB rescanned. With the WAL's default
    128 MiB segments, one answer can therefore stall the writer for about 100 ms.
  - A replicated WAL SHOULD use `segment_size ≤ 32 MiB`. Then one answer reads ≤ 48 MiB and
    the amplification is ≤ 1.5× at the default `C`.
  - Removing the prefix rescan needs a resumable reader position from the WAL (§16). That is
    a WAL API change, out of RM3 scope.
  - Defaults: `catchup_chunk_bytes = 16 MiB`, `catchup_replicas_per_poll = 1`. Neither the
    shipper nor `Wal` can read `segment_size`, so the documentation states the formula and
    the recommendation.
- **Cadence (integrator obligation, NORMATIVE).**
  - The integrator MUST call `shipper.poll(&wal)` after every `on_commit`, and at least once
    per `heartbeat` when no commits occur. A `poll` with nothing pending is one atomic swap.
  - This bounds the join decision to one commit or one heartbeat. That bound MUST stay well
    under the receiver's `idle_timeout`, because nothing the receiver accepts can be sent
    before `SERVING`.
  - While `catching_up() > 0`, an idle integrator SHOULD poll more often. Catch-up proceeds
    at about one chunk per opened reader per `poll`.

### 9.2 Cutover to the live ring (NORMATIVE — RM3)

- **Attach only when answering.** The writer calls `Producer::attach` only while answering a
  pending request (§9.3), at that request's cursor (answer 1), or at the end of the chunk it
  just copied for it (answer 3). Nothing else attaches. A session teardown therefore never
  races an attach.
- **Ordering on the wire.**
  - On **Chunk{…, attached}**, the network thread sends the chunk's records, then resumes the
    ring consumer.
  - On **Attached{c}**, it resumes the consumer directly.
  - On **Chunk{records}**, it sends the records and posts the next chunk request.
  - On **Wait**, it posts the next request after the next release wake or `heartbeat`,
    whichever comes first.
  - `Consumer::resume()` MUST return the answered cursor. Otherwise the session ends with
    `ERR(Contiguity)`.
- **Sending-side contiguity (R3, primary side; defence in depth).**
  - The network thread keeps `next = d + 1` for the session. It checks every record it
    encodes against `next`, whether the record came from catch-up or from the ring.
  - A record with `lsn ≠ next` is **never sent**. The session ends with
    `ERR(Contiguity)`, so the replica sees an in-band error, not a silent EOF.
  - Otherwise it sets `next = lsn.checked_next()`.
  - The tests' oracles stay independent of this check (§15.3).
- **Overflow falls back in-session.**
  - Suppose the ring evicts the consumer's cursor (`NeedsCatchUp`), either before the first
    drain after a cutover or at any time while streaming.
  - The network thread detaches the consumer and posts a chunk request at its own `next`.
  - The session continues: no reconnect, no re-HELLO. This replaces RM2's close-and-re-HELLO
    (`SessionEnd::FellBehind`).
  - `catch_up_events` counts entries into `CatchingUp`.
- **Sizing (liveness, not safety).** A cutover sticks if the ring still holds the cursor when
  the consumer first drains, i.e. after the network thread has sent one chunk.
  - Size `ring_bytes / slot_bytes ≥` the largest commit batch plus the records the primary
    captures while one chunk is on the wire. That is about `A · (C / R + poll interval)`,
    where `A` is the append rate and `R` the replica's ingest rate.
  - Convergence also needs `A < R`.
  - An undersized ring degrades to serving from the log. It never causes loss, a gap, or a
    reconnect storm.
- **Retention.** `acked` is set to the HELLO's `d` when the session is accepted, and raised
  only by ACKs within the session (RM2 R4). The §11 `min_replica_acked_lsn` term therefore
  protects the catch-up range. An integrator that checkpoints past it gets BelowOldest, which
  leads to step 1 and R6.
- **LSN domain.** The ring tags LSNs `≤ ring::MAX_LSN` (2^56 − 2). This rule is pulled forward
  from RM4, because RM3 makes mid-stream attach the normal path.
  - `Shipper::new` MUST return `Err` when `wal.last_lsn() ≥ ring::MAX_LSN`.
  - `Producer::capture` MUST check the bound on its fast path too: `lsn != next || lsn >
    MAX_LSN` goes to the cold path. This is a producer-local compare that touches no atomic.
  - If a capture crosses the bound at runtime, the shipper becomes **terminal**:
    - every session ends with `ERR(Protocol, "LSN domain exhausted")`;
    - statuses become `Failed`;
    - `poll` returns `Err(ReplError::LsnDomainExhausted)`.
  - It never answers Ahead⇒reject and never catches up forever.

### 9.3 Writer ↔ network handshake (NORMATIVE — RM3)

The writer keeps **no per-session state** between polls. Every interaction is a request the
network thread posts and the writer answers in a later `poll`.

- **The slot.** Each replica has one request slot holding a request (`Join{d}` or
  `Chunk{cursor}`), its answer, and the chunk buffer.
  - A state word moves `IDLE → PENDING → TAKEN → DONE → IDLE`.
  - The writer touches the slot only between `TAKEN` and `DONE`.
  - The network thread touches it only in `IDLE`, or after it has observed `DONE`.
  - The slot lives in its own module, written against the crate's loom-swappable atomics, so
    §15.7 L8 drives the shipped code.
- **Network thread.**
  - It detaches its consumer, fills the slot, and stores `PENDING` (Release). It then sets
    the shipper's pending flag and waits for `DONE` (Acquire).
  - While it waits after `SERVING`, it sends `HEARTBEAT{released}` every `heartbeat`.
  - It MUST NOT end a session (socket dead, any error) with a request outstanding.
    - First it tries to take the request back with CAS `PENDING → IDLE`.
    - If that CAS fails, the writer holds the request. The network thread waits for `DONE`,
      detaches the consumer if the answer attached it, and stores `IDLE`.
    - The wait is short, because the writer is mid-answer.
    - Shutdown is the only exception: after shutdown nothing is served.
- **Writer (`poll`).**
  - For each replica, CAS `PENDING → TAKEN` (Acquire).
  - On success it answers. That is the **only** call site of `Producer::attach`.
  - It then stores `DONE` (Release) and wakes the network thread.
- **R4.** The `sent` bound is published **before** every write, catch-up records included (RM2
  R1). The ACK reader runs from `SERVING` through catch-up and streaming.
- **Status.** `Connecting → Joining → CatchingUp ⇄ Streaming`, plus three terminal-for-session
  states: `Rejected` (step 2), `NeedsReseed` (step 1), and `Failed` (ReadFailed, LSN domain).
  `CatchingUp` replaces RM2's `NeedsCatchUp`; the crate is unpublished.

---

## 10. Re-seed (snapshot) — traits, NORMATIVE

When a replica is fresh, or `RESEED_REQUIRED`, it must be initialized from an application
snapshot. The crate defines the boundary; the application implements it:

```rust
/// Primary side: produce a durable snapshot covering all records ≤ `lsn`.
pub trait SnapshotProvider {
    /// Returns (snapshot_lsn, bytes/stream). snapshot_lsn ≤ wal.durable_lsn().
    fn produce(&mut self) -> Result<(Lsn, SnapshotStream)>;
}
/// Replica side: install a snapshot into application state.
pub trait SnapshotApplier {
    fn apply(&mut self, snapshot_lsn: Lsn, s: SnapshotStream) -> Result<()>;
}
```

Re-seed procedure (replica):
1. Wipe its WAL directory (it is about to be replaced wholesale).
2. Obtain snapshot `(s_lsn, stream)` from the primary (out-of-band transfer; v1 MAY ship it
   over the same TCP as a `SNAPSHOT` frame set, or use any side channel — implementation choice).
3. `applier.apply(s_lsn, stream)`.
4. `Wal::options().seed(Lsn(s_lsn + 1))[.observer(o)].open(dir, cfg)` (WAL v7). Now
   `durable_lsn == s_lsn`.
5. `HELLO{durable_lsn = s_lsn}` ⇒ primary serves from `s_lsn + 1` (§9), which MUST be
   `≥ oldest_lsn` — the primary MUST NOT checkpoint past `s_lsn` while the re-seed is in
   flight (§11 margin), else the re-seed loops. Track in-flight re-seeds in the checkpoint policy.

---

## 11. Checkpoint coordination (integrator policy — NOT a WAL hook)

The WAL's `checkpoint(up_to)` is called by the **integrator** (WAL §17.3). The crate exposes
`min_replica_acked_lsn()`. The integrator's policy MUST be:

```
up_to = min(latest_durable_snapshot_lsn,                 // WAL §9 rule — binding
            min_replica_acked_lsn − retention_margin,     // keep lagging replicas catch-up-able
            min(in-flight re-seed snapshot LSNs))         // §10 step 5
```
- If a replica is disconnected for longer than the margin allows, the integrator MAY choose
  to checkpoint past it — that replica will then hit R6 and re-seed. This is a deliberate
  availability-vs-disk trade the integrator makes; the crate makes it **loud** (R6), never silent.
- There is **no** "checkpoint observer" and no writer-side gating; WAL §15.4 stays gap-is-fatal.

---

## 12. Promotion (manual failover) — NORMATIVE procedure

### 12.1 Promote a replica
1. **Fence the old primary**: stop its process (kill), and ensure it cannot restart as primary
   (config/orchestration). Async has no in-band fencing; this step is operational and MUST
   happen first.
2. Choose the replica with the **highest `durable_lsn`** (query each). Because of R5, every
   other replica is a prefix of it. **This choice is also load-bearing for any replica-hosted
   consumers (§7.1):** promoting a lower replica would make consumers on a higher one diverge.
3. That replica's `Wal` **is already a valid primary log** — the same directory, opened as the
   writer. Start the application as primary on it; start its `Shipper`.
4. Point the remaining replicas at the new primary; they `HELLO` with their `durable_lsn` and
   catch up (§9) — no truncation needed (R5).
5. Loss = the old primary's un-shipped tail (records > new primary's `durable_lsn` that the
   old primary had committed). The integrator MUST treat those as lost.

### 12.2 The former primary MUST be re-seeded
If the old primary machine returns, its WAL may contain durable records **beyond** the new
primary's log (the lost tail). It is the one node that is *not* a prefix. v1 has no
suffix-truncate. Therefore it MUST NOT be resumed as a replica. The operator MUST wipe it and
re-seed (§10); resuming it would create divergence. The primary's §9 step 2 (reject a HELLO
with `durable_lsn > primary.durable_lsn`) is only a **tripwire**: it fires while the former
primary is still ahead. Once the new primary's `durable_lsn` passes it, the former primary is
indistinguishable from an honest replica and would be served silently on top of its divergent
tail. The operator fence-and-wipe is the only guard (§5.1; v1.x hardening in §16). (A `truncate_after` that lets it rejoin
without re-seed is a v2 WAL item — WAL v7 §17 Decision 7.)

---

## 13. Sync-replication surface (designed, not gated in v1)

`replica_acked_lsn(id)` and a `wait_for_ack(lsn, quorum, timeout)` MAY be provided so an
integrator can gate its *own* client acknowledgement on replica durability. v1 MUST NOT put
this on the WAL commit path and MUST NOT change what `commit()` means. If adopted, it is an
additional barrier in the integrator's daisy chain (WAL §15.7 "synchronous replication"),
costing a round-trip + remote fsync per acknowledged batch — documented as such.

---

## 14. Configuration, errors, milestones

**`ShipperConfig`:** `ring_bytes` (e.g. 64 MiB), `replicas: Vec<Addr>`, `heartbeat`,
`reconnect_backoff`. **`ReceiverConfig`:** `listen`, `wal_dir`, `wal_config`
(`segment_size`/`max_record_size` MUST match the primary's), `batch_records`,
`batch_interval`.

**`ReplError`** (non-panicking): `Wal(WalError)`, `Contiguity{expected, got}`, `WireCrc`,
`Protocol`, `ReseedRequired{oldest}`, `PrimaryPoisoned`, `Io`, `SnapshotFailed`.

**Milestones** (each gated on its §15 tests):
- **RM0** — crate skeleton, wire codec (frames, CRC, HELLO/SERVING/RECORD/ACK/ERR). *§15.1.*
- **RM1** — `Receiver`: own WAL, R3 contiguity, group commit, R4 ack. *§15.1, §15.2 P1–P2.*
- **RM2** — `Shipper`: ring capture/release, network thread, overflow ⇒ `NeedsCatchUp`, R1
  gate. Ring written against loom-swappable primitives (§15.7.1). *§15.2 P3–P4, §15.3
  ring-overflow, **§15.7 loom L1–L6 + all falsifiability mutations** (gate — do not start RM3
  until the ring's cross-thread barrier is model-checked).*
- **RM3** — catch-up (§9, §9.1–§9.3) using WAL v7 `oldest_lsn()` + containing-segment
  `reader_from`: the request/answer handshake, chunked catch-up under the `released` ceiling,
  in-session overflow fall-back, the ring LSN domain (pulled forward from RM4).
  *§15.3 catch-up, §15.2 P5, §15.7 L7/L7b/L8.*
- **RM4** — re-seed traits + `options().seed(..)` cold start; re-seed loop guard. *§15.3 re-seed.*
- **RM5** — checkpoint policy helper + promotion runbook + former-primary rejection (§12.2).
  *§15.3 promotion, rejoin-rejected.*
- **RM6** — model/oracle test (§15.4).
- **RM7** — fault injection: the LazyFS divergence headline + crash matrices (§15.5).
- **RM8** — hardening: fuzz the wire decoder, soak (primary+2 replicas, hours), CI.

---

## 15. Testing suite (co-equal deliverable)

### 15.1 Unit
Wire codec round-trip + CRC detection (flip a byte ⇒ `WireCrc`); frame length bounds (no
OOB, no unbounded alloc — fuzz the decoder in RM8); `HELLO`/`SERVING` arithmetic; ring
slot accounting (capture/release/free/evict).

### 15.2 Property (proptest)
- **P1 Mirroring (R2/R9):** arbitrary primary op-script (append/commit/roll) ⇒ replica log
  byte-identical with identical LSNs.
- **P2 Contiguity (R3):** inject gaps/dups/reorders into the stream ⇒ receiver never appends,
  always `ERR(Contiguity)`, and after reconnect converges to the correct log.
- **P3 Never-ahead (R1):** with commits interleaved arbitrarily, at every instant
  `replica.durable_lsn ≤ primary.durable_lsn` and the replica is a dense prefix (R5).
- **P4 Overflow (§8.3):** tiny ring ⇒ replicas repeatedly `NeedsCatchUp` ⇒ final logs still
  identical; no record duplicated at the ring/catch-up cutover.
- **P5 Catch-up after checkpoint (R6):** primary checkpoints past a lagging replica ⇒
  `RESEED_REQUIRED` as the **frame** answering the next HELLO, never a silent skip; a replica
  *within* retention catches up exactly. Plus a deterministic boundary table:
  - set up `P = wal.oldest_lsn() > 1`;
  - check `d ∈ {P−2, P−1, P}`, both with `d+1` in the ring and not;
  - `SERVING.primary_oldest == P`.

### 15.3 Scenario / integration
- **Ring overflow → catch-up → live cutover** with no gap/dup (assert at cutover).
  - **Oracles independent of the shipper:**
    - a loopback **forwarding proxy** between `Shipper` and the real `Receiver` checks, per
      session:
      - `SERVING.from == HELLO.d + 1`;
      - dense `RECORD` LSNs;
      - no `ERR` frame;
      - every `ACK` ≤ the replica WAL's `DurableTap` at forwarding time;
    - the proxy also counts sessions;
    - the receiver's exact session-result count;
    - no in-band `Err`/`Remote{code}` on the receiver side.
  - **A self-reported shipper counter is never the oracle.** With in-session overflow
    fall-back (§9.2), a session end in this test is itself a failure.
- **Re-seed end to end:** replica behind floor ⇒ `RESEED_REQUIRED` ⇒ snapshot ⇒
  `options().seed(s+1).open(..)` ⇒ resume ⇒ converges; and the in-flight-reseed checkpoint guard prevents the loop.
- **Promotion:** kill primary; promote highest replica; others catch up without truncation;
  content = dense prefix of old primary's durable log.
- **Former primary rejoin is REJECTED:** old primary (ahead) sends HELLO ⇒ `ERR(Contiguity)`,
  never served; after wipe+reseed it joins cleanly.
- **Primary restart (R7):** primary crashes and reopens; replicas re-HELLO; shipping resumes
  from each replica's `durable_lsn` with no persisted state on the primary.
- **Replica poison:** replica `commit()` fails ⇒ `ERR(Poisoned)`; after reopen it resumes.

### 15.4 Model / oracle (centerpiece, like WAL §14.3)
Drive a randomized program — primary `Append/Commit/Checkpoint/Crash`, replica
`Crash/Reconnect/Reseed`, `RingOverflow`, `NetPartition` — against the real components and
an oracle. After every step assert: every replica ⊆ primary durable set (R1); dense prefix
(R5); byte-identical (R9); acks ≤ replica durable (R4); mirrored LSNs (R2). Reuse the WAL
crate's model harness pattern; run high-iteration in CI and as an RM8 fuzz target.

### 15.5 Fault injection (core — the gate)
- **Headline — replica-never-ahead under power loss (LazyFS on the primary).** Primary
  appends + captures a batch; `commit()` is in flight (write done, `fdatasync` not yet
  returned or lost via `clear-cache`); inject power loss. Assert the replica **never received
  and never durably stored** those records (R1) — because `on_commit` was never called with a
  covering watermark. Then assert the contrapositive: a *mis-built* shipper that releases on
  `capture` (or reads `last_lsn`) **would** have shipped them ⇒ divergence — the negative
  control proves the test can fail. This is the analogue of WAL §15.8's watermark-divergence
  test, applied to the shipper.
- **Split-batch commit failure:** first segment synced, second fails ⇒ primary poisoned,
  `on_commit` not called; after reopen the replica catches up **exactly** the durable
  prefix (R1/R5) and nothing from the lost segment.
- **Crash matrices (SIGKILL):** primary killed before/after `on_commit`, mid-send, mid
  catch-up; replica killed before/after `append`, mid-`commit`, before `ACK`. Assert R1–R5
  hold after every recovery; INCONCLUSIVE ≠ PASS (same discipline as M8).
- **Wire corruption:** flip bytes on the socket ⇒ `WireCrc`, no append, converges after reconnect.
- **Partition/backoff:** partition mid-stream; heal; converges; no duplicate at resume.

### 15.6 Definition of done
Every R1–R9 has a passing test; §15.5 headline + negative control pass; §15.4 oracle
high-iteration clean; crash matrices green; **§15.7 loom L1–L6 pass and each falsifiability
mutation is demonstrated to fail**; wire-decoder fuzz N CPU-hours zero crashes
(contingent, dedicated runner); soak clean. Nothing self-certified without execution.

### 15.7 loom — model-checking the ring barrier (RM2 gate; NORMATIVE)

The capture/release/drain ring (§8, §4.2) is this crate's one genuine shared-memory
concurrency boundary: the writer thread produces, network thread(s) consume, and the R1
divergence guard is enforced across that boundary by a Release/Acquire watermark. **This is
exactly where loom earns its place** — unlike the WAL, where `!Sync` made a loom harness
moot. What loom proves here is the *memory-model half* of R1:

> If a consumer Acquire-observes `released = w`, it sees exactly the records with `lsn ≤ w`,
> each fully written, and never a record `> w`, never torn data, never a reused slot's new
> contents mistaken for the old record.

#### 15.7.1 Model the production ring — never a copy (the lesson from the WAL loom PR)
The ring MUST be written against **loom-swappable primitives** so the loom test drives the
**same ring code that ships**:
```rust
#[cfg(loom)]      use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(not(loom))] use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(loom)]      use loom::cell::UnsafeCell;   // loom tracks every access
#[cfg(not(loom))] use crate::cell::UnsafeCell;  // thin std wrapper with the same API
// likewise loom::sync::{Arc, Mutex, Condvar} and loom::thread under cfg(loom)
```
A hand-written model of the ring inside the test proves nothing about the ring. The test
file MUST import the crate's ring type and drive it; it MUST NOT reimplement it.

#### 15.7.2 Slot storage is `UnsafeCell` so loom detects races directly
Each slot's payload bytes and its `(lsn, generation)` header live in an `UnsafeCell`. Loom
instruments every `UnsafeCell` access and **reports any unsynchronized concurrent
read/write as a data race by itself** — a torn read is found by loom, not inferred from an
assertion that might be too weak. This is the strongest form of the check and is why the
ring must not hide slot contents behind `Mutex` in the model (that would serialize away the
very interleavings we want explored).

#### 15.7.3 Model bounds (keep it tiny — loom is exhaustive *within* the bound)
- Ring: **2–3 slots.** Records: **3–4** (so overflow/eviction and slot reuse both occur).
- Threads: **1 producer + 1 consumer** for L1–L5; **1 producer + 2 consumers** for L6.
- Payloads modeled as a small fixed pattern derived from the lsn (e.g. `[lsn as u8; 4]`) so a
  torn/reused read is detectable by value, not just by loom's race report.
- Bounded by `loom::model` defaults; if a model exceeds the preemption bound, shrink the
  model — do not raise the bound silently (document any change).

#### 15.7.4 Properties — one `loom::model` test each
- **L1 Visibility (the core R1 edge).** Producer writes slots, then `released.store(w,
  Release)`. Consumer `released.load(Acquire)`, then reads every slot with `lsn ≤ w`. Assert
  each reads as fully written (value == expected pattern) and loom reports no race. This is
  the happens-before edge that makes "released" mean "safe to read."
- **L2 Never-ahead.** With the producer advancing `released` concurrently *while the consumer
  is mid-drain*, assert the consumer never emits a record whose `lsn` exceeds the `released`
  value it loaded for that drain pass. (Guards against re-reading `released` mid-pass and
  sending past the watermark that was actually checked.)
- **L3 Slot-reuse safety.** Each consumer publishes its cursor with `Release` ("I am done with
  slots `< cursor`"); the producer `Acquire`-loads the **minimum** over all consumer cursors
  before overwriting a slot. Loom must find **no race** when a consumer is mid-read of a slot
  the producer wants to reuse — i.e. the producer must be forced to wait/evict, never overwrite
  in place.
- **L4 Eviction consistency (overflow).** Ring full ⇒ producer evicts the oldest slot and
  overwrites it, bumping the slot's **generation tag** and marking the affected consumer
  `NeedsCatchUp`. A consumer whose cursor pointed at that slot MUST observe the generation
  mismatch and MUST NOT emit the overwritten contents as the old record. Assert: no emitted
  record has `(lsn, payload)` inconsistent with the producer's log of what it wrote.
- **L5 No lost wakeup (liveness via end-state).** Consumer sleeps (Condvar) only when nothing
  is shippable; producer does release-then-notify. Loom explores the interleaving
  "consumer checks empty → producer releases+notifies → consumer sleeps." End-of-model
  assertion: **every released record was emitted.** A lost wakeup leaves records unsent ⇒ the
  assertion fails. (Loom won't flag this as a deadlock because the producer completes, so the
  *end-state* assertion is what catches it — make it explicit.)
- **L6 SPMC.** Two consumers with independent cursors on one ring: a slot is freed only when
  **both** cursors have passed it. Loom must find no race and both consumers must emit the
  full released set. (Catches a producer that frees on *any* cursor instead of the min.)
- **L7 Late re-attach after fall-behind (RM3).** RM3 makes mid-stream attach the normal path.
  - **Shared prelude**, run sequentially on the main thread:
    - build a 2-slot, 1-consumer ring and attach at 1;
    - capture 1–2 and release 2;
    - capture 3 (this evicts 1) and release 3;
    - `drain` returns `NeedsCatchUp` with nothing emitted, then detach;
    - capture and release 4 while detached;
    - assert `oldest_valid == 3`.
  - **Spawn order and handoff.** Spawn the consumer **before** the attach. It polls
    `resume()` with bounded `yield_now` retries, so the cursor's Release/Acquire is the only
    edge. Use no waker and no side flag: a spawn-after-attach or a separate flag masks the
    ordering mutations below.
  - **L7** cuts over at `oldest_valid`: `attach(0, 3)`, then capture and release 5 and 6.
    - Both captures can evict a pinned record.
    - Oracle: the emitted LSNs are dense from the **constant** 3, each `payload == pat(lsn)`,
      and every LSN is ≤ its pass watermark.
    - `Streaming` ⇒ exactly 3..=6. `NeedsCatchUp{n}` ⇒ `n == 3 + emitted`.
    - Preemption bound 4.
  - **L7b** is a caught-up cutover (`d == durable`): `attach(0, 5) = released + 1`, then
    capture and release 5 and 6.
    - No eviction is possible, so assert exactly `[5, 6]` and `Streaming`. A spurious
      `NeedsCatchUp` fails it.
    - Exhaustive.
- **L8 Handshake teardown (RM3, §9.3).** It drives the crate's request-slot module, not a
  copy.
  - The writer thread answers a pending request by attaching.
  - Concurrently, the network thread abandons the session: it tries CAS `PENDING → IDLE`,
    otherwise waits for `DONE` and detaches.
  - End state: the consumer's cursor is `PARKED` (no stale attach) and the slot is `IDLE`.
    Loom reports no race on the slot's answer or buffer.

#### 15.7.5 Falsifiability — REQUIRED, each mutation demonstrated to fail, then reverted
The gate is only meaningful if it can fail. Apply each mutation, run the corresponding model,
confirm loom reports a race or an assertion trips, revert, and record it in the PR:
| Mutation | Must fail |
|---|---|
| `released.store(w, Release)` → `Relaxed` | **L1** (loom race, or a stale/torn slot read) |
| `released.load(Acquire)` → `Relaxed` | **L1** |
| Remove the min-cursor `Acquire` check before slot reuse | **L3** (loom `UnsafeCell` race) |
| Free a slot on *any* consumer cursor instead of the min | **L6** |
| Drop the generation bump on eviction | **L4** (overwritten data emitted as old record) |
| Drop the `Condvar::notify` in `on_commit` | **L5** (unsent records at end) |
| Re-read `released` mid-drain and send up to the new value | **L2** |
| *(RM3)* Drop the generation bump on eviction | **L7** |
| *(RM3)* Consumer pin CAS `Release` → `Relaxed` | **L7** at bound 4 |
| *(RM3)* `attach` does not clear the consumer's catch-up flag | **L7b** (spurious `NeedsCatchUp`) |
| *(RM3)* `attach` stores the cursor **before** clearing the catch-up flag | **L7b** |
| *(RM3)* `attach`'s cursor store `Release` → `Relaxed` | **L7b** |
| *(RM3)* `resume`'s cursor load `Acquire` → `Relaxed` | **L7b** |
| *(RM3)* Network thread abandons a request with a plain store instead of the CAS | **L8** (stale attach) |
| *(RM3)* Writer stores `DONE` before it attaches | **L8** |
| *(RM3)* Network thread does not wait for `DONE` when the CAS fails | **L8** |

If a mutation does **not** make its model fail, the model is too weak — fix the model, do
not ship it.

#### 15.7.6 What loom does NOT prove (state this in the test file header)
- It proves the **in-memory handoff** is race-free and ordering-correct within the bound. It
  does **not** prove the *durability* half of R1 — that `released` is only ever advanced to a
  `w` returned by `commit() → Ok(w)`. That is a logic invariant of the integrator's call
  order, covered by the §15.5 LazyFS headline and its negative control.
- It does not cover the network, the wire codec, the WAL, or catch-up's log reads (§9.1 runs
  on the writer thread). Its shared-memory parts are covered: the late attach (L7/L7b) and the
  writer↔network handshake (L8).
- Exhaustive only for the modeled ring size/record count; larger configurations are covered by
  the §15.4 oracle and §15.5 crash matrices, not by loom.

#### 15.7.7 Where it runs
`RUSTFLAGS="--cfg loom" cargo test --test loom_ring`. Fast at this bound ⇒ **per-PR,
blocking** (a loom failure is a real concurrency bug). Also `cargo test` (non-loom) MUST still
build and run the ring's ordinary unit tests — the `cfg(loom)` swap must not change the
non-loom code path.

---

## 16. Deferred (explicit)
- **Sync replication** (barrier gating client acks) — surface in §13; v2.
- **Leader election / auto-failover** — requires fencing, terms, and **suffix-truncate** in
  the WAL; adopt Raft with `open-wal` as the log store rather than hand-roll; v2+.
- **`truncate_after` in the WAL** — needed for election and for former-primary rejoin without
  re-seed; touches D2/D8/D12; WAL v7 §17 Decision 7.
- **Off-thread / cross-process shipper via a read-only `WalReader`** (WAL §15.2 pattern) —
  moves capture/catch-up I/O off the writer thread; drop-in shipper swap (wire + receiver
  unchanged); v2.
- **Snapshot transport standardization** — v1 leaves it to the integrator.
- **Resumable WAL reader position** (WAL API addition; owner decision). Examples: a `Reader`
  position token that `Wal::reader_resume(token)` validates against the live segment list,
  or `Wal::segment_bases()`. Either removes the containing-segment rescan that dominates the
  §9.1 writer stall and read amplification. Until then, RM3 documents the bound and
  recommends `segment_size ≤ 32 MiB` for replicated WALs.
- **Former-primary detection past the tripwire** (v1.x, heuristic). `HELLO` would carry the
  crc32c of the replica's record `d`, and the primary would compare it with its own record
  `d` before serving.
  - It needs a protocol-version bump.
  - It needs an explicit "unknown" value for `d == 0`, for `d` already checkpointed on either
    side, and for a freshly seeded replica.
  - It is probabilistic: equal payloads at `d` pass.
  - Not in RM3. The operator wipe (§12.2) stays the guard.

## 17. References
- open-wal `docs/wal_design_v7.md` §4 (D1–D12), §6 (`OpenOptions`), §8.5, §9, §15 (esp. §15.1, §15.4, §15.7).
- PostgreSQL streaming replication & `pg_basebackup`; Raft (Ongaro & Ousterhout) — for what v2 must add.
