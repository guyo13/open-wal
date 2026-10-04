# Kickoff B — `open-wal-replica` RM0–RM2 (workspace migration, codec, receiver, shipper + loom gate)

You are starting a **new companion crate**, `open-wal-replica`: async master/slave replication on
top of `open-wal` (static primary, N replicas, no election, no sharding). This kickoff covers
**RM0, RM1, RM2** — everything that needs **no** WAL changes. RM3+ waits for WAL spec v7 (a
separate, parallel effort — do not block on it and do not touch `open-wal`'s `src/`).

## Read first — in this order, before any code
1. `crates/open-wal-replica/CLAUDE.md` (already on `main`; read it). Re-read before each milestone.
2. `docs/replica_design_v1.md` — the build contract. **§4.1–4.2** (why capture-at-append, thread
   model), **§5** (R1–R9), **§6** (wire), **§7** (receiver), **§8** (shipper/ring), and
   **§15.7 (loom) — read this BEFORE designing the ring; the loom tests pin its structure.**
3. Root `CLAUDE.md` + `docs/wal_design_v6.md` §15.1 (durability-visibility gap) and §15.7.
4. The WAL's public API you will drive: `Wal::open`/`append`/`commit`/`durable_lsn`/`last_lsn`,
   `WalConfig`, `WalError` (`src/lib.rs`, `src/wal.rs`). Every call must compile against it.

## RM0 — workspace migration + crate skeleton + wire codec

**Workspace (root package + members — chosen to keep every existing gate untouched):**
`open-wal` stays the **root package** exactly where it is; the replica is a member under
`crates/`. Add to the root `Cargo.toml` (keep the existing `[package]` as-is — do not move
`src/`, `tests/`, `benches/`, and do not refactor the root package to inherit workspace fields):
```toml
[workspace]
members = ["crates/open-wal-replica"]
exclude = ["fuzz"]   # cargo-fuzz crate declares its own [workspace]; keep it standalone
```
- **Acceptance test for the migration:** every existing workflow and script
  (`ci.yml`, `fuzz.yml`, `soak.yml`, `bench.yml`, `m8-*.yml`, `publish.yml`, `scripts/**`) runs
  **byte-identical** and green. `cargo test` at root still tests only `open-wal` (default member).
  If any existing invocation needs editing to stay green, stop and report — the layout was
  chosen so none do.
- Make `publish.yml` explicit: `cargo publish -p open-wal` (safer in a workspace). Add a
  workspace-wide `cargo fmt --check` if the root one doesn't already cover members.
- Add CI for the new crate (new job(s), not edits to the open-wal ones): `cargo test -p
  open-wal-replica`, `cargo clippy -p open-wal-replica --all-targets -- -D warnings`, MSRV
  `cargo +1.85.0 check -p open-wal-replica --all-targets --locked`, and (from RM2) the loom job.

**Crate skeleton:** `crates/open-wal-replica/` — `Cargo.toml` (edition 2024, `rust-version =
"1.85"`, `open-wal = { path = "../..", version = "0.2" }`, `crc32c`; `loom` under
`[target.'cfg(loom)'.dev-dependencies]`), `src/lib.rs`, `src/error.rs` (`ReplError` per §14 —
non-panicking), `src/wire.rs`, the `CLAUDE.md`, and `docs/replica_design_v1.md` committed at the
repo's `docs/` (the issue's copy is a snapshot; the repo copy is canonical).

**Wire codec (§6):** `len:u32 | type:u8 | body`, little-endian; `HELLO`, `SERVING`,
`RESEED_REQUIRED`, `RECORD{lsn, crc, payload}` with `crc = crc32c(lsn_le ‖ payload)`, `ACK`,
`HEARTBEAT`, `ERR{code,msg}`. The decoder is attacker-facing: bounded `len`, no OOB, no
unbounded alloc, unknown type ⇒ `Protocol` error. *Tests §15.1:* round-trip every frame;
flip a byte ⇒ `WireCrc`; oversize/short frames rejected cleanly; a decoder fuzz target may be
added now or in RM8 (plan for it — keep `decode_frame(&[u8]) -> Result<..>` pure).

## RM1 — `Receiver` (the replica)

Per §7. Opens its **own** `Wal` in its own dir (config must match the primary's
`segment_size`/`max_record_size`), accepts a TCP connection, sends `HELLO{durable_lsn}`, then:
for each `RECORD`: **CRC check → contiguity check `lsn == wal.last_lsn()+1` → `wal.append`
→ assert returned LSN == shipped LSN (R2)**; group-commit by `batch_records`/`batch_interval`;
after `commit() → Ok(w)` send `ACK{w}` (R4 — never `last_lsn`). A `commit()` `Err` ⇒ WAL
poisoned ⇒ `ERR(Poisoned)`, close; the process must reopen before reconnecting.

*Tests §15.1 + §15.2 P1–P2:* drive the receiver with an in-process fake primary (just frames
over a socket or a channel): byte-identical, LSN-mirrored log (P1); injected gaps / dups /
reorders ⇒ **never appended**, `ERR(Contiguity)`, and after reconnect it converges (P2); CRC
corruption ⇒ no append; ack only ever equals the replica's `durable_lsn`; `SERVING{from}` with
`from != last_lsn+1` ⇒ `ERR(Contiguity)`.

## RM2 — `Shipper` (the primary) + the ring + the loom gate

Per §8 and §4.2. **Design the ring against §15.7 first.**

- `capture(lsn, payload)` (writer thread, after `append`): memcpy into a **preallocated** ring
  slot; no I/O, no syscall, no steady-state allocation; record is **not shippable**.
- `on_commit(w)` (writer thread, after `commit() → Ok(w)`): `released.store(w, Release)` + one
  wake. **Sends nothing.** Never called on `Err`.
- One **network thread per replica**: `released.load(Acquire)`, drain entries `≤` that value
  from its cursor, send `RECORD`s, read `ACK`s into `replica_acked[id]`, heartbeats, reconnect
  with backoff. The writer thread **never blocks** on any of this.
- **Overflow (§8.3):** `capture` on a full ring evicts the oldest slot (bumping its generation)
  and marks replicas whose cursor was on it `NeedsCatchUp`. Never blocks, never drops the new
  capture, never touches `append`/`commit`. (`NeedsCatchUp` is *served* in RM3; in RM2 just
  mark it, expose it, and test the marking.)
- Ring structure (pinned by §15.7): `released: AtomicU64` (Release/Acquire); per-consumer
  cursor `AtomicU64` published with Release, producer takes the **min** over all cursors with
  Acquire before reusing a slot; per-slot **generation tag**; slot contents in **`UnsafeCell`**
  (not `Mutex`); all primitives `#[cfg(loom)]`-swappable from day one (§15.7.1). Also expose
  `min_replica_acked_lsn()` and `replica_acked_lsn(id)` (§8.4).

*Tests §15.2 P3–P4, §15.3 overflow:* at every instant `replica.durable ≤ primary.durable` and
the replica is a dense prefix (P3); tiny ring ⇒ repeated `NeedsCatchUp` marking with no
record duplicated or lost in what *was* shipped (P4, overflow); the writer thread is never
blocked — assert `capture`/`on_commit` latency is bounded with a stalled consumer.

### The RM2 gate — loom L1–L6 + all seven mutations (§15.7) — GATE before RM3
`crates/open-wal-replica/tests/loom_ring.rs`, `#![cfg(loom)]`, run via
`RUSTFLAGS="--cfg loom" cargo test -p open-wal-replica --test loom_ring`, **per-PR blocking**.
- **Import the crate's ring type and drive it.** Do not reimplement the ring in the test.
- Model bounds: 2–3 slots, 3–4 records, 1 producer + 1 consumer (L1–L5), + 2 consumers (L6).
- **L1** visibility (Acquire sees fully-written slots `≤ w`, no `UnsafeCell` race); **L2**
  never-ahead with `released` advancing mid-drain; **L3** slot reuse only after min-cursor
  Acquire (no race mid-read); **L4** eviction: generation mismatch ⇒ consumer never emits
  overwritten data as the old record; **L5** no lost wakeup — **end-of-model assertion that
  every released record was emitted** (loom won't flag this as deadlock; the end-state check is
  what catches it); **L6** SPMC: free only when **both** cursors passed.
- **Falsifiability (required, each shown to fail then reverted, recorded in the PR):**
  store Release→Relaxed ⇒ L1; load Acquire→Relaxed ⇒ L1; drop min-cursor check ⇒ L3; free on
  any cursor ⇒ L6; drop generation bump ⇒ L4; drop `notify` ⇒ L5; re-read `released` mid-drain
  ⇒ L2. **If a mutation does not fail its model, the model is too weak — fix the model.**
- State in the test header what loom does NOT prove (§15.7.6): the durability half of R1
  (`released` only ever advances to a commit-returned `w`) is covered by the RM7 LazyFS
  headline, not here. Never conflate them.
- `cargo test` (non-loom) must still build and run the ring's ordinary unit tests — the
  `cfg(loom)` swap must not change the non-loom code path.

## Guardrails
- **Do not touch `open-wal`'s `src/`.** If RM0–RM2 seem to need a WAL change, stop and flag it
  (they shouldn't — only RM3+ needs v7).
- **Never `reader_from` on the hot path.** Capture-at-append only. Catch-up (`reader_from`) is RM3.
- **No `DurabilityObserver` plumbing, no checkpoint observer, no truncate.**
- Never weaken a test; never mark the loom gate passed without the mutation demonstrations.
- Always run the **root** `cargo test` too, to prove `open-wal` is untouched.

## Branch / PRs
New branch off `main`. **One PR per milestone** (RM0 / RM1 / RM2) so the workspace migration is
reviewable in isolation and the loom gate is a reviewable artifact. Commits reference the
R-invariants (e.g. "RM2: ring release watermark — R1 / loom L1"). Each PR description lists
the §15 tests run and, for RM2, the seven mutation results.

## Definition of done (this kickoff)
Workspace migrated with zero changes to existing gates; codec with bounds-safe decoder;
`Receiver` passing P1–P2 with the R3 hard check and R4 ack honesty; `Shipper` + ring passing
P3–P4/overflow with the writer never blocking; **loom L1–L6 green and all seven mutations
demonstrated to fail**; root `cargo test` untouched-and-green. RM3 is blocked on WAL v7 and is
**not** part of this kickoff.
