//! The shipper's capture ring: single producer (the WAL writer thread), multiple
//! consumers (one network thread per replica). §8 and §4.2 of
//! `docs/replica_design_v1.md`; its cross-thread correctness is model-checked by
//! loom (§15.7, `tests/loom_ring.rs`).
//!
//! # What it does
//! - [`Producer::capture`] copies `(lsn, payload)` into a preallocated slot on
//!   the writer thread, right after `wal.append` returned `lsn`. No I/O, no
//!   syscall, no steady-state allocation. The record is **not shippable**.
//! - [`Producer::release`] is called with `commit() → Ok(w)`'s `w`: one
//!   `Release` store of the watermark plus one unpark per consumer. It sends
//!   nothing. Only records `≤ w` ever become visible to consumers (R1).
//! - [`Consumer::drain`] (network thread) `Acquire`-loads the watermark **once**
//!   per pass and copies out records `cursor..=w`, never past `w` (R1, L2).
//! - When the ring is full, `capture` **evicts** the oldest record instead of
//!   waiting: consumers that still needed it are marked
//!   [`NeedsCatchUp`](ConsumerStatus::NeedsCatchUp) (they resume from the log,
//!   §9 — RM3). The writer never blocks and the new capture is never dropped.
//!
//! # Structure (pinned by §15.7)
//! - `released: AtomicU64` — the R1 watermark. `Release` store / `Acquire` load.
//!   This edge is what makes every record `≤ w` fully written when a consumer
//!   reads it (L1): consumer-side pinning below is deliberately `Relaxed`, so
//!   `released` is the **only** producer→consumer happens-before edge for data.
//! - Per-consumer `cursor: AtomicU64` = "I am done with every record `< cursor`",
//!   published with `Release` after each copy-out. Before reusing a slot **in
//!   place**, the producer `Acquire`-loads the **minimum** cursor over all
//!   consumers (L3, L6); only if every consumer has passed the slot's record is
//!   it overwritten in place.
//! - Slot payloads live in [`UnsafeCell`](crate::sync::UnsafeCell)s (not a
//!   `Mutex`), so loom reports any unsynchronized access as a race (§15.7.2).
//! - Per-buffer **generation tag**: each payload buffer has one atomic word
//!   `state = tag << 8 | readers`, where `tag` is the LSN it holds (or
//!   `TOMBSTONE`). A consumer reads a buffer only after **pinning** it — a CAS
//!   that bumps `readers` *iff* `tag` is the LSN it wants — and unpins with a
//!   `Release` decrement.
//!
//! # Eviction without a race (refinement of §15.7.4 L4 — see the design doc)
//! Overwriting a slot a lagging consumer might be copying *at that instant*
//! would be a data race no generation check could excuse (the consumer would
//! already be reading the bytes). So eviction never writes a buffer that is
//! pinned: the producer first **tombstones** it (a CAS on the same `state`
//! word, keeping the reader count — this is the generation bump: no new pin can
//! succeed), and then
//! - if no reader holds it, overwrites it in place (the CAS `Acquire`s the last
//!   unpin, so the old reads happen-before the new writes);
//! - otherwise retires it and moves the slot to a **spare buffer**. The ring
//!   owns `slots + consumers` buffers; each consumer pins at most one at a
//!   time, so a free spare always exists. A retired buffer is reclaimed once
//!   its reader count is observed (`Acquire`) to be zero.
//!
//! A consumer whose wanted record was evicted sees the generation mismatch
//! (`tag != lsn`) and never emits the overwritten contents as the old record
//! (L4); it marks itself `NeedsCatchUp` and detaches.
//!
//! # Wakeups (refinement of §15.7.4 L5)
//! Consumers sleep with `park` and the producer wakes them with `unpark` after
//! the `released` store. The park/unpark token makes "check empty → producer
//! releases + unparks → consumer parks" safe (the park returns at once), and,
//! unlike a `Mutex` + `Condvar`, the writer never takes a lock a consumer could
//! hold (§4.2: the writer never blocks).
//!
//! # Limits
//! LSNs must stay below `2^56 − 1` (the tag shares a word with an 8-bit reader
//! count); at most [`MAX_CONSUMERS`] consumers. A payload larger than
//! `slot_bytes` grows that buffer once (the documented §8.1 fallback
//! allocation); steady state is allocation-free.

use std::time::Duration;

use open_wal::Lsn;

use crate::sync::{Arc, AtomicBool, AtomicU64, AtomicUsize, Ordering, Thread, UnsafeCell};

/// Maximum consumers (replicas) per ring: the per-buffer reader count is 8 bits.
pub const MAX_CONSUMERS: usize = 255;

/// Bits of the state word holding the reader count.
const READER_BITS: u32 = 8;
const READERS: u64 = (1 << READER_BITS) - 1;
/// Tag meaning "no record" (empty, evicted, or discarded).
const TOMBSTONE: u64 = u64::MAX >> READER_BITS;
/// Largest LSN the ring can tag.
pub const MAX_LSN: u64 = TOMBSTONE - 1;
/// Cursor value of a detached consumer: it touches no slot.
const PARKED: u64 = u64::MAX;

#[inline]
const fn pack(tag: u64, readers: u64) -> u64 {
    (tag << READER_BITS) | readers
}

#[inline]
const fn tag_of(state: u64) -> u64 {
    state >> READER_BITS
}

/// Ring geometry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingConfig {
    /// Number of slots (≥ 2). The ring retains the last `slots` captures.
    pub slots: usize,
    /// Preallocated payload capacity per buffer, in bytes.
    pub slot_bytes: usize,
    /// Number of consumers, `1..=MAX_CONSUMERS`.
    pub consumers: usize,
}

struct BufData {
    lsn: u64,
    payload: Vec<u8>,
}

struct Buf {
    /// `tag << 8 | readers` — the generation tag and pin count.
    state: AtomicU64,
    data: UnsafeCell<BufData>,
}

// SAFETY: `data` is only accessed under the ring protocol documented in the
// module docs: the producer writes a buffer only when it is unreachable by
// consumers (every consumer cursor has passed its record, or it was tombstoned
// with zero readers, or it is a free/retired-and-drained spare), and a
// consumer reads it only while holding a pin whose CAS matched the tag of a
// record `≤ released` — whose write happens-before the read via `released`.
unsafe impl Sync for Buf {}
// SAFETY: `BufData` is plain owned data; moving a `Buf` between threads is fine.
unsafe impl Send for Buf {}

struct Shared {
    slots: Box<[AtomicUsize]>,
    bufs: Box<[Buf]>,
    released: AtomicU64,
    cursors: Box<[AtomicU64]>,
    catchup: Box<[AtomicBool]>,
}

/// Create a ring and its consumers. Records start at `next` (the WAL's
/// `last_lsn + 1`); `released` starts at the WAL's `durable_lsn` (the producer
/// never re-releases anything at or below it). All consumers start
/// **detached**; [`Producer::attach`] joins one at an LSN.
///
/// # Panics
/// If `cfg` is out of range (`slots < 2`, `consumers` not in
/// `1..=MAX_CONSUMERS`) or `released ≥ next`.
#[must_use]
pub fn ring(cfg: RingConfig, next: Lsn, released: Lsn) -> (Producer, Vec<Consumer>) {
    assert!(cfg.slots >= 2, "ring needs at least 2 slots");
    assert!(
        (1..=MAX_CONSUMERS).contains(&cfg.consumers),
        "1..={MAX_CONSUMERS} consumers"
    );
    assert!(
        released < next,
        "released watermark must be below the next LSN"
    );
    let n_bufs = cfg.slots + cfg.consumers;
    let bufs: Box<[Buf]> = (0..n_bufs)
        .map(|_| Buf {
            state: AtomicU64::new(pack(TOMBSTONE, 0)),
            data: UnsafeCell::new(BufData {
                lsn: 0,
                payload: Vec::with_capacity(cfg.slot_bytes),
            }),
        })
        .collect();
    let shared = Arc::new(Shared {
        slots: (0..cfg.slots).map(AtomicUsize::new).collect(),
        bufs,
        released: AtomicU64::new(released.0),
        cursors: (0..cfg.consumers).map(|_| AtomicU64::new(PARKED)).collect(),
        catchup: (0..cfg.consumers).map(|_| AtomicBool::new(false)).collect(),
    });
    let producer = Producer {
        shared: Arc::clone(&shared),
        n: cfg.slots as u64,
        next: next.0,
        released: released.0,
        oldest_valid: next.0,
        cached_min: PARKED,
        slot_buf: (0..cfg.slots).collect(),
        slot_tag: vec![TOMBSTONE; cfg.slots],
        free: (cfg.slots..n_bufs).rev().collect(),
        retired: Vec::with_capacity(cfg.consumers),
        wakers: vec![None; cfg.consumers],
    };
    let consumers = (0..cfg.consumers)
        .map(|id| Consumer {
            shared: Arc::clone(&shared),
            id,
            cursor: 0,
            attached: false,
        })
        .collect();
    (producer, consumers)
}

/// Why [`Producer::attach`] refused to join a consumer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachError {
    /// The consumer is still attached (it must detach first).
    AlreadyAttached,
    /// `from` is past `released + 1`: the replica claims records the primary
    /// has not durably committed (§9 step 2 — a former primary, or a bug).
    Ahead {
        /// The current released watermark.
        released: Lsn,
    },
    /// `from` is older than the ring retains; serve it from the log (§9, RM3).
    Behind {
        /// The oldest LSN the ring can still serve contiguously.
        oldest: Lsn,
    },
}

/// The writer-thread half of the ring. `!Sync` by `&mut self` discipline; it is
/// `Send` so it can live wherever the `Wal` lives.
pub struct Producer {
    shared: Arc<Shared>,
    n: u64,
    /// Next LSN `capture` expects.
    next: u64,
    /// Last value stored to `shared.released`.
    released: u64,
    /// Oldest LSN such that every record in `oldest_valid..next` is in the ring.
    oldest_valid: u64,
    /// A previously `Acquire`-loaded lower bound of the minimum cursor.
    cached_min: u64,
    /// Producer mirror of `shared.slots` (slot → buffer).
    slot_buf: Vec<usize>,
    /// Tag (LSN) of the record in each slot, or `TOMBSTONE`.
    slot_tag: Vec<u64>,
    free: Vec<usize>,
    retired: Vec<usize>,
    wakers: Vec<Option<Thread>>,
}

impl Producer {
    /// Copy `(lsn, payload)` into the ring (§8.1). Call right after
    /// `wal.append` returned `lsn`. Never blocks, never does I/O; allocation
    /// only if `payload` exceeds `slot_bytes` (once per buffer).
    ///
    /// `lsn` must be the next LSN after the previous capture. Discontinuities
    /// are handled conservatively, never unsafely:
    /// - `released < lsn < next` (re-capture after a failed commit and WAL
    ///   reopen): captured records `≥ lsn` were never durable-acknowledged and
    ///   are **discarded** (tombstoned) before `lsn` is captured again;
    /// - `lsn > next` (skipped captures): the gap is never served from the ring
    ///   — consumers reaching it see a generation mismatch ⇒ `NeedsCatchUp`;
    /// - `lsn ≤ released` or `lsn > MAX_LSN` (a caller bug — a released LSN can
    ///   never be re-appended): nothing is written and every attached consumer
    ///   is marked `NeedsCatchUp` (the log stays the source of truth).
    pub fn capture(&mut self, lsn: Lsn, payload: &[u8]) {
        let lsn = lsn.0;
        if lsn != self.next && !self.discontinuity(lsn) {
            return;
        }
        let i = (lsn % self.n) as usize;
        let prev = self.slot_tag[i];
        let target = if prev == TOMBSTONE || self.all_cursors_past(prev) {
            // In-place reuse: no consumer can be reading this slot — either it
            // holds nothing, or every cursor has passed its record (L3/L6: the
            // Acquire min-cursor load orders their reads before our writes).
            self.slot_buf[i]
        } else {
            self.evict(i, prev)
        };
        let buf = &self.shared.bufs[target];
        buf.data.with_mut(|d| {
            // SAFETY: `target` is unreachable by consumers (see above / `evict`).
            let d = unsafe { &mut *d };
            d.lsn = lsn;
            d.payload.clear();
            d.payload.extend_from_slice(payload);
        });
        buf.state.store(pack(lsn, 0), Ordering::Release);
        if target != self.slot_buf[i] {
            self.slot_buf[i] = target;
            self.shared.slots[i].store(target, Ordering::Release);
        }
        self.slot_tag[i] = lsn;
        self.next = lsn + 1;
        // The record `lsn - n` (if any) just left the ring.
        if lsn + 1 > self.n {
            self.oldest_valid = self.oldest_valid.max(lsn + 1 - self.n);
        }
    }

    /// Handle `capture(lsn)` with `lsn != next`. Returns whether to proceed.
    #[cold]
    fn discontinuity(&mut self, lsn: u64) -> bool {
        if lsn <= self.released || lsn > MAX_LSN {
            self.mark_all_catchup();
            debug_assert!(
                false,
                "capture({lsn}) at or below released {}",
                self.released
            );
            return false;
        }
        if lsn < self.next {
            // Discard captures ≥ lsn: they were never released (lsn > released)
            // so no consumer can pin them; tombstone them so a stale record
            // can never be mistaken for the re-captured one.
            for i in 0..self.slot_tag.len() {
                let t = self.slot_tag[i];
                if t != TOMBSTONE && t >= lsn {
                    self.shared.bufs[self.slot_buf[i]]
                        .state
                        .store(pack(TOMBSTONE, 0), Ordering::Release);
                    self.slot_tag[i] = TOMBSTONE;
                }
            }
        } else {
            // Skipped LSNs are not in the ring; nothing older is contiguous.
            self.oldest_valid = lsn;
        }
        self.next = lsn;
        true
    }

    /// True iff every consumer's cursor is past `lsn` (Acquire-loaded).
    fn all_cursors_past(&mut self, lsn: u64) -> bool {
        if self.cached_min > lsn {
            return true;
        }
        let min = self
            .shared
            .cursors
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .min()
            .unwrap_or(PARKED);
        self.cached_min = min;
        min > lsn
    }

    /// Overflow (§8.3): evict record `prev` from slot `i`. Marks every consumer
    /// that still needed it `NeedsCatchUp`, bumps the generation (tombstone),
    /// and returns a buffer no consumer can be reading.
    #[cold]
    fn evict(&mut self, i: usize, prev: u64) -> usize {
        for (k, c) in self.shared.cursors.iter().enumerate() {
            let cur = c.load(Ordering::Acquire);
            if cur != PARKED && cur <= prev {
                self.shared.catchup[k].store(true, Ordering::Release);
            }
        }
        let b = self.slot_buf[i];
        let state = &self.shared.bufs[b].state;
        // Generation bump: tag → TOMBSTONE, keeping the reader count, so no new
        // pin can succeed. AcqRel: the success Acquire-reads the latest unpin.
        let mut cur = state.load(Ordering::Relaxed);
        let readers = loop {
            match state.compare_exchange(
                cur,
                pack(TOMBSTONE, cur & READERS),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break cur & READERS,
                Err(actual) => cur = actual,
            }
        };
        if readers == 0 {
            return b;
        }
        // A consumer is mid-copy of `b`: never touch it. Use a spare.
        self.retired.push(b);
        self.take_free()
    }

    fn take_free(&mut self) -> usize {
        if let Some(f) = self.free.pop() {
            return f;
        }
        let bufs = &self.shared.bufs;
        let free = &mut self.free;
        self.retired.retain(|&r| {
            // Acquire: the last unpin's reads happen-before our future writes.
            let idle = bufs[r].state.load(Ordering::Acquire) & READERS == 0;
            if idle {
                free.push(r);
            }
            !idle
        });
        // `slots + consumers` buffers: `slots − 1` are mapped (the evicted one
        // was just retired), and each consumer pins at most one buffer at a
        // time, so at most `consumers` retired buffers can still look pinned ⇒
        // ≥ 1 is free. "Look": a retired buffer is retired because our
        // tombstone CAS observed a pin; pins are Release, so when we later
        // observe a consumer's *next* pin we also observe its earlier unpin —
        // per consumer, at most its latest-observed pin can still look held.
        self.free
            .pop()
            .expect("ring invariant: a spare buffer is always free")
    }

    fn mark_all_catchup(&self) {
        for (k, c) in self.shared.cursors.iter().enumerate() {
            if c.load(Ordering::Acquire) != PARKED {
                self.shared.catchup[k].store(true, Ordering::Release);
            }
        }
    }

    /// Release every captured record `≤ w` for shipping (§8.2) and wake the
    /// consumers. Call **only** with the `w` from `commit() → Ok(w)` — never
    /// `last_lsn`, never after `commit` failed (R1). Monotonic; clamped to the
    /// last captured LSN (an uncaptured record is never "in" the ring).
    pub fn release(&mut self, w: Lsn) {
        let w = w.0.min(self.next - 1);
        if w <= self.released {
            return;
        }
        self.released = w;
        self.shared.released.store(w, Ordering::Release);
        for t in self.wakers.iter().flatten() {
            t.unpark();
        }
    }

    /// Register the thread that runs consumer `id`, so `release` can wake it.
    /// Unparks it once, so a release that raced with registration is not lost.
    pub fn set_waker(&mut self, id: usize, thread: Thread) {
        thread.unpark();
        self.wakers[id] = Some(thread);
    }

    /// Wake consumer `id` (e.g. after deciding its attach request).
    pub fn wake(&self, id: usize) {
        if let Some(t) = &self.wakers[id] {
            t.unpark();
        }
    }

    /// Join a **detached** consumer at `from` (writer thread only, §9). The
    /// ring must hold every record `from..next`, and `from ≤ released + 1`.
    /// On success the consumer picks the cursor up via [`Consumer::resume`].
    pub fn attach(&mut self, id: usize, from: Lsn) -> Result<(), AttachError> {
        let from = from.0;
        if self.shared.cursors[id].load(Ordering::Acquire) != PARKED {
            return Err(AttachError::AlreadyAttached);
        }
        if from > self.released + 1 {
            return Err(AttachError::Ahead {
                released: Lsn(self.released),
            });
        }
        if from < self.oldest_valid {
            return Err(AttachError::Behind {
                oldest: Lsn(self.oldest_valid),
            });
        }
        self.shared.catchup[id].store(false, Ordering::Relaxed);
        self.shared.cursors[id].store(from, Ordering::Release);
        self.cached_min = self.cached_min.min(from);
        Ok(())
    }

    /// The oldest LSN the ring can serve contiguously (records
    /// `oldest_valid()..next` are all present).
    #[must_use]
    pub fn oldest_valid(&self) -> Lsn {
        Lsn(self.oldest_valid)
    }

    /// The released (shippable) watermark.
    #[must_use]
    pub fn released(&self) -> Lsn {
        Lsn(self.released)
    }

    /// The next LSN `capture` expects.
    #[must_use]
    pub fn next_lsn(&self) -> Lsn {
        Lsn(self.next)
    }

    /// Whether consumer `id` is marked `NeedsCatchUp` (§8.3).
    #[must_use]
    pub fn needs_catch_up(&self, id: usize) -> bool {
        self.shared.catchup[id].load(Ordering::Acquire)
    }
}

/// A consumer's state after a [`Consumer::drain`] pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerStatus {
    /// Attached and current; nothing went wrong this pass.
    Streaming,
    /// A record it still needed was evicted (§8.3). The consumer has detached;
    /// it must be served `next..` from the log (§9, RM3) and re-attached.
    NeedsCatchUp {
        /// The first LSN it did not emit.
        next: Lsn,
    },
    /// Not attached (never attached, or detached).
    Detached,
}

/// Result of one [`Consumer::drain`] pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Drain {
    /// The `released` watermark loaded (once) for this pass. Every record
    /// emitted in the pass is `≤ watermark` (L2).
    pub watermark: Lsn,
    /// Records emitted in this pass.
    pub emitted: usize,
    /// The consumer's status after the pass.
    pub status: ConsumerStatus,
}

/// A network-thread half of the ring (one per replica).
pub struct Consumer {
    shared: Arc<Shared>,
    id: usize,
    cursor: u64,
    attached: bool,
}

impl Consumer {
    /// This consumer's index.
    #[must_use]
    pub fn id(&self) -> usize {
        self.id
    }

    /// Pick up an attach performed by the producer ([`Producer::attach`]).
    /// Returns the cursor (next LSN to emit) if attached.
    pub fn resume(&mut self) -> Option<Lsn> {
        let c = self.shared.cursors[self.id].load(Ordering::Acquire);
        if c == PARKED {
            self.attached = false;
            return None;
        }
        self.attached = true;
        self.cursor = c;
        Some(Lsn(c))
    }

    /// Stop consuming: the producer no longer waits for this consumer.
    pub fn detach(&mut self) {
        self.attached = false;
        self.shared.cursors[self.id].store(PARKED, Ordering::Release);
    }

    /// The next LSN this consumer will emit (meaningful while attached).
    #[must_use]
    pub fn cursor(&self) -> Lsn {
        Lsn(self.cursor)
    }

    /// Emit up to `max` released records, in LSN order, to `emit` (which runs
    /// while the record's buffer is pinned — copy out and return; never block
    /// in it). Loads `released` **once** (Acquire) and never emits past it.
    pub fn drain<F: FnMut(Lsn, &[u8])>(&mut self, max: usize, mut emit: F) -> Drain {
        if !self.attached {
            return Drain {
                watermark: Lsn(self.shared.released.load(Ordering::Acquire)),
                emitted: 0,
                status: ConsumerStatus::Detached,
            };
        }
        // The producer's NeedsCatchUp mark is checked once per pass; within a
        // pass an eviction is detected by the generation tag (`copy_out`).
        if self.shared.catchup[self.id].load(Ordering::Acquire) {
            return self.fall_behind(self.shared.released.load(Ordering::Acquire), 0);
        }
        let w = self.shared.released.load(Ordering::Acquire);
        let mut emitted = 0;
        while self.cursor <= w && emitted < max {
            if !self.copy_out(&mut emit) {
                return self.fall_behind(w, emitted);
            }
            self.cursor += 1;
            // "Done with every record < cursor": lets the producer reuse it.
            self.shared.cursors[self.id].store(self.cursor, Ordering::Release);
            emitted += 1;
        }
        Drain {
            watermark: Lsn(w),
            emitted,
            status: ConsumerStatus::Streaming,
        }
    }

    /// Pin, copy out, unpin the record at `self.cursor`. `false` if it is no
    /// longer in the ring (generation mismatch — evicted).
    fn copy_out<F: FnMut(Lsn, &[u8])>(&self, emit: &mut F) -> bool {
        let c = self.cursor;
        let n = self.shared.slots.len() as u64;
        let b = self.shared.slots[(c % n) as usize].load(Ordering::Acquire);
        let buf = &self.shared.bufs[b];
        // Pin: +1 reader iff the generation tag is still `c`. No Acquire — the
        // data's visibility comes from the `released` Acquire (c ≤ released);
        // the pin only has to exclude the producer, which it does because the
        // producer's tombstone is an RMW on this same word. Release (found by
        // loom, L4): a producer CAS that observes this pin must also observe
        // our *previous* unpin, or it could count an already-released buffer as
        // still pinned and run out of spares.
        let mut cur = buf.state.load(Ordering::Relaxed);
        loop {
            if tag_of(cur) != c {
                return false;
            }
            match buf
                .state
                .compare_exchange(cur, cur + 1, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        buf.data.with(|d| {
            // SAFETY: pinned with tag == c, and c ≤ released: the producer will
            // not write this buffer until we unpin, and its write of record c
            // happens-before this read via `released`.
            let d = unsafe { &*d };
            debug_assert_eq!(d.lsn, c, "pinned buffer holds a different record");
            emit(Lsn(c), &d.payload);
        });
        // Unpin. Release: our reads happen-before the producer's next write.
        buf.state.fetch_sub(1, Ordering::Release);
        true
    }

    fn fall_behind(&mut self, w: u64, emitted: usize) -> Drain {
        self.shared.catchup[self.id].store(true, Ordering::Release);
        self.detach();
        Drain {
            watermark: Lsn(w),
            emitted,
            status: ConsumerStatus::NeedsCatchUp {
                next: Lsn(self.cursor),
            },
        }
    }

    /// True if a record past the cursor has been released (work is waiting).
    #[must_use]
    pub fn has_released(&self) -> bool {
        self.attached && self.shared.released.load(Ordering::Acquire) >= self.cursor
    }

    /// Sleep until the producer releases more, or `timeout` (spurious wakeups
    /// allowed). Returns at once if work is already waiting. Requires the
    /// calling thread to be registered with [`Producer::set_waker`].
    pub fn wait(&self, timeout: Duration) {
        if !self.has_released() {
            crate::sync::park_timeout(timeout);
        }
    }

    /// Whether the producer marked this consumer `NeedsCatchUp`.
    #[must_use]
    pub fn needs_catch_up(&self) -> bool {
        self.shared.catchup[self.id].load(Ordering::Acquire)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn cfg(slots: usize, consumers: usize) -> RingConfig {
        RingConfig {
            slots,
            slot_bytes: 16,
            consumers,
        }
    }

    fn drain_all(c: &mut Consumer) -> (Vec<(u64, Vec<u8>)>, Drain) {
        let mut out = Vec::new();
        let d = c.drain(usize::MAX, |l, p| out.push((l.0, p.to_vec())));
        (out, d)
    }

    fn pat(lsn: u64) -> Vec<u8> {
        vec![lsn as u8; 4]
    }

    #[test]
    fn captured_records_are_not_shippable_until_released() {
        let (mut p, mut cs) = ring(cfg(4, 1), Lsn(1), Lsn(0));
        p.attach(0, Lsn(1)).unwrap();
        cs[0].resume();
        p.capture(Lsn(1), &pat(1));
        p.capture(Lsn(2), &pat(2));
        let (out, d) = drain_all(&mut cs[0]);
        assert!(out.is_empty(), "R1: nothing before release");
        assert_eq!(d.watermark, Lsn(0));
        p.release(Lsn(1));
        let (out, d) = drain_all(&mut cs[0]);
        assert_eq!(out, vec![(1, pat(1))]);
        assert_eq!(d.watermark, Lsn(1));
        p.release(Lsn(2));
        assert_eq!(drain_all(&mut cs[0]).0, vec![(2, pat(2))]);
    }

    #[test]
    fn release_is_monotonic_and_clamped_to_captured() {
        let (mut p, _cs) = ring(cfg(4, 1), Lsn(1), Lsn(0));
        p.capture(Lsn(1), b"a");
        p.release(Lsn(9));
        assert_eq!(p.released(), Lsn(1));
        p.release(Lsn(0));
        assert_eq!(p.released(), Lsn(1));
    }

    #[test]
    fn slot_reuse_waits_for_every_cursor() {
        let (mut p, mut cs) = ring(cfg(2, 2), Lsn(1), Lsn(0));
        for (id, c) in cs.iter_mut().enumerate() {
            p.attach(id, Lsn(1)).unwrap();
            c.resume();
        }
        p.capture(Lsn(1), &pat(1));
        p.capture(Lsn(2), &pat(2));
        p.release(Lsn(2));
        // Consumer 0 drains everything; consumer 1 has read nothing.
        assert_eq!(drain_all(&mut cs[0]).0.len(), 2);
        // Capturing 3 must evict 1 (consumer 1 still needs it), not reuse it.
        p.capture(Lsn(3), &pat(3));
        p.release(Lsn(3));
        assert!(p.needs_catch_up(1));
        assert!(!p.needs_catch_up(0));
        let (out, d) = drain_all(&mut cs[1]);
        assert!(out.is_empty());
        assert_eq!(d.status, ConsumerStatus::NeedsCatchUp { next: Lsn(1) });
        assert_eq!(drain_all(&mut cs[0]).0, vec![(3, pat(3))]);
    }

    #[test]
    fn eviction_of_a_pinned_buffer_uses_a_spare() {
        // Simulate a consumer mid-copy by pinning by hand, then overflow.
        let (mut p, mut cs) = ring(cfg(2, 1), Lsn(1), Lsn(0));
        p.attach(0, Lsn(1)).unwrap();
        cs[0].resume();
        p.capture(Lsn(1), &pat(1));
        p.capture(Lsn(2), &pat(2));
        p.release(Lsn(2));
        let b1 = p.slot_buf[1];
        p.shared.bufs[b1].state.fetch_add(1, Ordering::Relaxed); // pin lsn 1
        p.capture(Lsn(3), &pat(3)); // evicts 1 while pinned
        assert_ne!(p.slot_buf[1], b1, "pinned buffer must not be overwritten");
        assert_eq!(p.retired, vec![b1]);
        assert_eq!(
            p.shared.bufs[b1].state.load(Ordering::Relaxed),
            pack(TOMBSTONE, 1)
        );
        p.shared.bufs[b1]
            .data
            .with(|d| assert_eq!(unsafe { &*d }.payload, pat(1)));
        // Still pinned: not reclaimable.
        p.free.clear();
        p.free.push(usize::MAX); // sentinel so take_free does not need b1 yet
        assert_eq!(p.take_free(), usize::MAX);
        p.shared.bufs[b1].state.fetch_sub(1, Ordering::Release); // unpin
        // Once unpinned (observed with Acquire) it is reclaimed as a spare.
        assert_eq!(p.take_free(), b1);
        assert!(p.retired.is_empty());
    }

    #[test]
    fn stalled_consumer_is_marked_and_the_producer_keeps_going() {
        let (mut p, mut cs) = ring(cfg(4, 1), Lsn(1), Lsn(0));
        p.attach(0, Lsn(1)).unwrap();
        cs[0].resume();
        for l in 1..=1000 {
            p.capture(Lsn(l), &pat(l));
            p.release(Lsn(l));
        }
        assert!(p.needs_catch_up(0));
        assert_eq!(p.oldest_valid(), Lsn(997));
        let (out, d) = drain_all(&mut cs[0]);
        assert!(out.is_empty());
        assert!(matches!(
            d.status,
            ConsumerStatus::NeedsCatchUp { next: Lsn(1) }
        ));
        // Re-attach at the ring's oldest and stream the retained tail.
        assert_eq!(
            p.attach(0, Lsn(5)),
            Err(AttachError::Behind { oldest: Lsn(997) })
        );
        p.attach(0, Lsn(997)).unwrap();
        cs[0].resume();
        let got: Vec<u64> = drain_all(&mut cs[0]).0.into_iter().map(|r| r.0).collect();
        assert_eq!(got, vec![997, 998, 999, 1000]);
    }

    #[test]
    fn attach_bounds() {
        let (mut p, mut cs) = ring(cfg(4, 1), Lsn(11), Lsn(10));
        assert_eq!(
            p.attach(0, Lsn(12)),
            Err(AttachError::Ahead { released: Lsn(10) })
        );
        assert_eq!(
            p.attach(0, Lsn(10)),
            Err(AttachError::Behind { oldest: Lsn(11) })
        );
        p.attach(0, Lsn(11)).unwrap();
        assert_eq!(p.attach(0, Lsn(11)), Err(AttachError::AlreadyAttached));
        assert_eq!(cs[0].resume(), Some(Lsn(11)));
        cs[0].detach();
        p.attach(0, Lsn(11)).unwrap();
    }

    #[test]
    fn recapture_after_failed_commit_discards_unreleased() {
        let (mut p, mut cs) = ring(cfg(8, 1), Lsn(1), Lsn(0));
        p.attach(0, Lsn(1)).unwrap();
        cs[0].resume();
        for l in 1..=6 {
            p.capture(Lsn(l), &pat(l));
        }
        p.release(Lsn(2));
        // commit failed; reopen recovered durable = 4; the WAL re-appends 5, 6.
        p.capture(Lsn(5), b"new5");
        p.capture(Lsn(6), b"new6");
        p.release(Lsn(6));
        let out = drain_all(&mut cs[0]).0;
        let want: Vec<(u64, Vec<u8>)> = vec![
            (1, pat(1)),
            (2, pat(2)),
            (3, pat(3)),
            (4, pat(4)),
            (5, b"new5".to_vec()),
            (6, b"new6".to_vec()),
        ];
        assert_eq!(out, want);
    }

    #[test]
    fn recapture_then_skip_never_resurrects_a_discarded_record() {
        let (mut p, mut cs) = ring(cfg(8, 1), Lsn(1), Lsn(0));
        p.attach(0, Lsn(1)).unwrap();
        cs[0].resume();
        for l in 1..=6 {
            p.capture(Lsn(l), &pat(l));
        }
        p.capture(Lsn(4), b"new4"); // discards 4..=6
        p.capture(Lsn(6), b"new6"); // skips 5 (a stale 5 must not reappear)
        p.release(Lsn(6));
        let (out, d) = drain_all(&mut cs[0]);
        assert_eq!(
            out,
            vec![(1, pat(1)), (2, pat(2)), (3, pat(3)), (4, b"new4".to_vec())]
        );
        assert_eq!(d.status, ConsumerStatus::NeedsCatchUp { next: Lsn(5) });
    }

    #[test]
    fn capture_at_or_below_released_writes_nothing() {
        let (mut p, mut cs) = ring(cfg(4, 1), Lsn(1), Lsn(0));
        p.attach(0, Lsn(1)).unwrap();
        cs[0].resume();
        p.capture(Lsn(1), &pat(1));
        p.release(Lsn(1));
        // Caller bug: re-capturing a released LSN. Debug builds assert; the
        // ring itself never overwrites released data.
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            p.capture(Lsn(1), b"evil");
        }));
        assert!(r.is_err() == cfg!(debug_assertions));
        assert!(p.needs_catch_up(0));
        let (out, _) = drain_all(&mut cs[0]);
        assert!(out.iter().all(|(_, b)| b != b"evil"));
    }

    #[test]
    fn oversized_payload_falls_back_to_growing_the_buffer() {
        let (mut p, mut cs) = ring(cfg(2, 1), Lsn(1), Lsn(0));
        p.attach(0, Lsn(1)).unwrap();
        cs[0].resume();
        let big = vec![9u8; 1000];
        p.capture(Lsn(1), &big);
        p.release(Lsn(1));
        assert_eq!(drain_all(&mut cs[0]).0, vec![(1, big)]);
    }

    #[test]
    fn drain_budget_and_pass_watermark() {
        let (mut p, mut cs) = ring(cfg(8, 1), Lsn(1), Lsn(0));
        p.attach(0, Lsn(1)).unwrap();
        cs[0].resume();
        for l in 1..=5 {
            p.capture(Lsn(l), &pat(l));
        }
        p.release(Lsn(5));
        let mut got = Vec::new();
        let d = cs[0].drain(2, |l, _| got.push(l.0));
        assert_eq!((d.emitted, d.watermark), (2, Lsn(5)));
        let d = cs[0].drain(10, |l, _| got.push(l.0));
        assert_eq!(d.emitted, 3);
        assert_eq!(got, vec![1, 2, 3, 4, 5]);
        assert!(!cs[0].has_released());
    }
}
