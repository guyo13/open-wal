//! The writer ↔ network **request slot** (§9.3 of `docs/replica_design_v1.md`).
//!
//! The writer thread keeps no per-session state between `Shipper::poll` calls.
//! Every interaction is a request the replica's network thread posts here and
//! the writer answers in a later `poll`: a join decision (`Join{d}`) or one
//! bounded catch-up chunk (`Chunk{cursor, pos}`), plus the chunk's records in
//! the slot's buffer.
//!
//! # State machine
//! One state word moves `IDLE → PENDING → TAKEN → DONE → IDLE`.
//! - The **network** thread touches the request, answer and buffer only in
//!   `IDLE`, or after it observed `DONE` (`Acquire`).
//! - The **writer** thread touches them only between `TAKEN` (its own
//!   `PENDING → TAKEN` CAS, `Acquire`) and its `DONE` store (`Release`).
//!
//! The request, the answer, the buffer and the waker sit in
//! [`UnsafeCell`](crate::sync::UnsafeCell)s — not behind a `Mutex` — so loom
//! reports any unsynchronized access by itself (the §15.7.2 analogue; models
//! L8 and L8b in `tests/loom_ring.rs` drive exactly this module).
//!
//! # The exported sequences
//! `shipper.rs` calls these verbatim; it never touches the state word.
//! - [`Slot::post`] (network): write the request, store `PENDING`.
//! - [`Slot::answer`] (writer): CAS `PENDING → TAKEN`, run `f` (the **only**
//!   place `Producer::attach` is ever called), write the answer, store `DONE`,
//!   then unpark the network thread.
//! - [`Slot::await_answer`] (network): park until `DONE`, re-checking after
//!   every wake, then take the answer and the buffer.
//! - [`Slot::abandon`] (network, session teardown): take the request back
//!   (CAS `PENDING → IDLE`), or find the slot `IDLE`, or wait for `DONE` and
//!   undo an attach the answer made.
//!
//! **`await_answer` and `abandon` have no exit with a request outstanding,
//! except shutdown.** There is deliberately no timeout and no retry bound: a
//! network thread that gave up while the writer holds the request (`TAKEN`)
//! would let the writer attach a consumer nobody is running (a stale attach)
//! and race the next session on the answer and the buffer. `await_answer` can
//! leave early only *through* `abandon` (its `idle` callback asks for it when
//! the session's socket died), which itself returns only once the slot is
//! `IDLE` again.

use std::time::Duration;

use open_wal::{Lsn, ReaderPos};

use crate::ring::Consumer;
use crate::sync::{AtomicBool, AtomicUsize, Ordering, Thread, UnsafeCell};

const IDLE: usize = 0;
const PENDING: usize = 1;
const TAKEN: usize = 2;
const DONE: usize = 3;

/// How long `abandon` / `await_answer` park between re-checks of the state
/// word and the shutdown flag. This is a re-check period, **not** a give-up
/// bound: both keep waiting until `DONE` (or shutdown).
const RECHECK: Duration = Duration::from_millis(50);

/// What a network thread asks the writer thread (§9, §9.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Request {
    /// A replica said `HELLO{durable_lsn = d}`; decide §9 steps 1–3.
    Join {
        /// The replica's durable watermark.
        d: Lsn,
    },
    /// Serve the replica from `cursor` — the next LSN the network thread will
    /// put on the wire: attach it to the ring there, or copy one bounded chunk
    /// of the log.
    Chunk {
        /// The next LSN to send.
        cursor: Lsn,
        /// The reader position returned with this session's latest `Chunk`
        /// answer, if any. Used only when `pos.next_lsn() == cursor`.
        pos: Option<ReaderPos>,
    },
}

/// How the replica is served once a join was accepted (§9 step 3). A join
/// answer never opens a reader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Serve {
    /// The ring holds `d + 1`: the consumer is attached there.
    Attached,
    /// `d + 1` is above `released` (the replica is caught up, or ahead of the
    /// released watermark): wait, served nothing, until `released ≥ d + 1`.
    Wait,
    /// The log must be read: after `SERVING`, post `Chunk{d + 1}`.
    NeedChunk,
}

/// How a `Chunk` answer ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkEnd {
    /// The consumer was attached right after the chunk's last record: send the
    /// records, then resume the ring.
    Attached,
    /// The ring does not hold the next record yet: send the records and ask
    /// for another chunk, carrying this reader position (if the reader had
    /// one).
    More(Option<ReaderPos>),
}

/// The writer thread's answer to a [`Request`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    // ---- join answers (§9 steps 1–3) ----
    /// Step 1: `d + 1 < oldest` ⇒ send `RESEED_REQUIRED{oldest}` (R6).
    Reseed {
        /// `wal.oldest_lsn()` at decision time.
        oldest: Lsn,
    },
    /// Step 2: `d > wal.durable_lsn()` ⇒ `ERR(Contiguity)`; never served.
    Reject,
    /// Step 3: send `SERVING{d + 1, oldest}`, then serve as `how` says.
    Serve {
        /// `wal.oldest_lsn()` at decision time.
        oldest: Lsn,
        /// What follows `SERVING`.
        how: Serve,
    },

    // ---- chunk answers (§9.1) ----
    /// Answer 1: the ring holds `cursor`; the consumer is attached there.
    Attached {
        /// The cursor the consumer was attached at.
        cursor: Lsn,
    },
    /// Answer 2: `cursor > released`; nothing was read. Re-post once
    /// `released ≥ cursor`.
    Wait,
    /// The per-`poll` reader cap was reached; nothing was read. Re-post the
    /// same request at once.
    Deferred,
    /// Answer 3: the slot's buffer holds the records `cursor..next`.
    Chunk {
        /// The LSN after the chunk's last record.
        next: Lsn,
        /// Whether the consumer was attached at `next`.
        end: ChunkEnd,
    },
    /// The records were checkpointed away (R6): close with a FIN and
    /// reconnect; the next `HELLO` is answered `RESEED_REQUIRED`.
    BelowOldest,
    /// A record the primary durably committed could not be read.
    ReadFailed {
        /// The LSN catch-up expected and did not get.
        lsn: Lsn,
    },

    // ---- either ----
    /// The shipper left the ring's LSN domain (§9.2): `ERR(Protocol)`.
    Terminal,
    /// An internal invariant broke (e.g. `AttachError::AlreadyAttached`): end
    /// the session loudly.
    Internal,
}

impl Answer {
    /// Whether the writer attached the consumer while producing this answer.
    #[must_use]
    pub fn attached(&self) -> bool {
        matches!(
            self,
            Answer::Attached { .. }
                | Answer::Serve {
                    how: Serve::Attached,
                    ..
                }
                | Answer::Chunk {
                    end: ChunkEnd::Attached,
                    ..
                }
        )
    }
}

/// What `await_answer`'s `idle` callback wants next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Idle {
    /// Keep waiting: park for up to this long, then re-check.
    Park(Duration),
    /// The session is over (its socket died): run [`Slot::abandon`].
    Abandon,
}

/// How [`Slot::await_answer`] returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Awaited {
    /// The writer answered; the caller's buffer now holds the chunk bytes.
    Answer(Answer),
    /// `idle` asked to abandon; the slot is `IDLE` and the consumer detached.
    Abandoned,
    /// The shipper is shutting down; nothing is served any more.
    Shutdown,
}

/// One replica's request slot.
pub struct Slot {
    state: AtomicUsize,
    /// Ordering hint only (`poll` answers joins first): whether the request
    /// last posted was a `Join`. Never used to interpret the request.
    join: AtomicBool,
    shutdown: AtomicBool,
    request: UnsafeCell<Request>,
    answer: UnsafeCell<Answer>,
    buf: UnsafeCell<Vec<u8>>,
    waker: UnsafeCell<Option<Thread>>,
}

// SAFETY: the cells are only accessed under the state-machine protocol in the
// module docs: the network thread in `IDLE` or after an `Acquire` load of
// `DONE`, the writer thread between its `Acquire` CAS to `TAKEN` and its
// `Release` store of `DONE`. Loom models L8 and L8b check exactly this.
unsafe impl Sync for Slot {}
// SAFETY: plain owned data.
unsafe impl Send for Slot {}

impl Default for Slot {
    fn default() -> Slot {
        Slot::new()
    }
}

impl Slot {
    /// An empty (`IDLE`) slot.
    #[must_use]
    pub fn new() -> Slot {
        Slot {
            state: AtomicUsize::new(IDLE),
            join: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            request: UnsafeCell::new(Request::Join { d: Lsn(0) }),
            answer: UnsafeCell::new(Answer::Deferred),
            buf: UnsafeCell::new(Vec::new()),
            waker: UnsafeCell::new(None),
        }
    }

    /// Register the network thread that owns this slot, so `answer` can wake
    /// it. **Network thread only, once, before its first [`post`](Slot::post)**
    /// (the slot is `IDLE` and the writer cannot be inside `answer`).
    pub fn register_waker(&self, thread: Thread) {
        debug_assert_eq!(self.state.load(Ordering::Relaxed), IDLE);
        self.waker.with_mut(|w| {
            // SAFETY: IDLE, and only the network thread leaves IDLE.
            unsafe { *w = Some(thread) };
        });
    }

    /// Post a request (network thread; the slot must be `IDLE` and the
    /// replica's consumer detached). The caller then sets the shipper's
    /// pending flag.
    pub fn post(&self, req: Request) {
        debug_assert_eq!(
            self.state.load(Ordering::Relaxed),
            IDLE,
            "post on a busy slot"
        );
        self.join
            .store(matches!(req, Request::Join { .. }), Ordering::Relaxed);
        self.request.with_mut(|r| {
            // SAFETY: IDLE — the writer does not touch the slot.
            unsafe { *r = req };
        });
        // Release: the request (and the hint) are visible to the writer's
        // Acquire CAS.
        self.state.store(PENDING, Ordering::Release);
    }

    /// Answer the pending request, if any (writer thread, inside `poll`).
    ///
    /// CAS `PENDING → TAKEN`; run `f` on the request and the (cleared) chunk
    /// buffer — `f` is the only place `Producer::attach` may be called; write
    /// the answer; store `DONE`; unpark the network thread. Returns the answer,
    /// or `None` if nothing was pending.
    pub fn answer<F>(&self, f: F) -> Option<Answer>
    where
        F: FnOnce(&Request, &mut Vec<u8>) -> Answer,
    {
        self.state
            .compare_exchange(PENDING, TAKEN, Ordering::Acquire, Ordering::Relaxed)
            .ok()?;
        let ans = self.request.with(|r| {
            self.buf.with_mut(|b| {
                // SAFETY: TAKEN — the network thread does not touch the slot
                // until it observes DONE; the request was written before the
                // PENDING store our CAS acquired.
                let (r, b) = unsafe { (&*r, &mut *b) };
                b.clear();
                f(r, b)
            })
        });
        self.answer.with_mut(|a| {
            // SAFETY: TAKEN, as above.
            unsafe { *a = ans };
        });
        // SAFETY: the waker is written once, in IDLE, before the first post.
        let waker = self.waker.with(|w| unsafe { (*w).clone() });
        // Release: the answer, the buffer and any attach `f` made are visible
        // to whoever Acquire-observes DONE.
        self.state.store(DONE, Ordering::Release);
        self.loom_preemption_point();
        if let Some(t) = waker {
            t.unpark();
        }
        Some(ans)
    }

    /// A scheduling point for the loom model checker between the `DONE` store
    /// and the unpark; compiled to nothing otherwise.
    ///
    /// loom switches threads only at its own operations, and its `unpark` is
    /// not a branch point — it also joins the unparker's causality into the
    /// target at once. Without this point no modelled execution has the
    /// network thread running between the `DONE` store and the unpark, so loom
    /// could not see a network thread that reads the answer without having
    /// `Acquire`d `DONE`, or a writer that writes the answer after `DONE` (two
    /// §15.7.5 rows passed L8 / L8b until this was added). Real hardware can
    /// preempt there. `yield_now` hands the model's schedule to the network
    /// thread if it is runnable; it has no memory-ordering effect. (A `Relaxed`
    /// load of `state` is also a branch point, but loom's partial-order
    /// reduction treats it as independent of the network thread's load and
    /// never reorders them: the `Relaxed` row still passed with it.)
    #[cfg(loom)]
    fn loom_preemption_point(&self) {
        loom::thread::yield_now();
    }

    #[cfg(not(loom))]
    #[inline(always)]
    fn loom_preemption_point(&self) {}

    /// Wait for the answer to the posted request (network thread).
    ///
    /// Parks until `DONE` (`Acquire`), re-checking the state after every wake;
    /// then reads the answer, swaps the chunk buffer with `buf`, and stores
    /// `IDLE`. Before each park it calls `idle` (with the detached consumer, for its
    /// reporting-only `released()`), which lets the session keep
    /// its heartbeat timer running and report a dead socket: on
    /// [`Idle::Abandon`] this runs [`abandon`](Slot::abandon) and returns
    /// [`Awaited::Abandoned`]. There is no other exit before `DONE` except
    /// shutdown.
    pub fn await_answer<I>(
        &self,
        consumer: &mut Consumer,
        buf: &mut Vec<u8>,
        mut idle: I,
    ) -> Awaited
    where
        I: FnMut(&Consumer) -> Idle,
    {
        loop {
            if self.state.load(Ordering::Acquire) == DONE {
                // SAFETY: DONE was Acquire-observed: the writer is finished
                // with the slot and its writes happen-before these reads.
                let ans = self.answer.with(|a| unsafe { *a });
                self.buf.with_mut(|b| {
                    // SAFETY: as above.
                    std::mem::swap(unsafe { &mut *b }, buf);
                });
                self.state.store(IDLE, Ordering::Release);
                return Awaited::Answer(ans);
            }
            if self.shutdown.load(Ordering::Acquire) {
                return Awaited::Shutdown;
            }
            match idle(consumer) {
                Idle::Park(t) => crate::sync::park_timeout(t),
                Idle::Abandon => {
                    self.abandon(consumer);
                    return Awaited::Abandoned;
                }
            }
        }
    }

    /// End a session without leaving a request outstanding (network thread).
    ///
    /// - CAS `PENDING → IDLE`: the request is taken back; the writer never
    ///   sees it.
    /// - Else, if the slot is `IDLE` (no request outstanding): detach and
    ///   return. Only the network thread leaves `IDLE`, so this cannot race.
    /// - Else the writer holds the request (`TAKEN`) or already answered it
    ///   (`DONE`): wait for `DONE` — however long the answer's read takes —
    ///   detach if the answer attached the consumer, and store `IDLE`.
    ///
    /// It never gives up before `DONE` (shutdown aside): see the module docs.
    pub fn abandon(&self, consumer: &mut Consumer) {
        if self
            .state
            .compare_exchange(PENDING, IDLE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            consumer.detach();
            return;
        }
        if self.state.load(Ordering::Acquire) == IDLE {
            consumer.detach();
            return;
        }
        while self.state.load(Ordering::Acquire) != DONE {
            if self.shutdown.load(Ordering::Acquire) {
                consumer.detach();
                return;
            }
            crate::sync::park_timeout(RECHECK);
        }
        // SAFETY: DONE was Acquire-observed (see `await_answer`).
        let ans = self.answer.with(|a| unsafe { *a });
        if ans.attached() {
            consumer.detach();
        }
        self.state.store(IDLE, Ordering::Release);
    }

    /// Mark the shipper as shutting down and wake the network thread. After
    /// this the writer answers nothing more. **Writer thread only** (so it
    /// never runs concurrently with [`answer`](Slot::answer)).
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Whether a request is posted and not yet taken. Read-only reachability
    /// aid (`Shipper::request_pending`); never an oracle.
    #[must_use]
    pub fn pending(&self) -> bool {
        self.state.load(Ordering::Acquire) == PENDING
    }

    /// Whether a **join** request is pending (ordering hint for `poll`'s
    /// joins-first pass). The `Acquire` load of `PENDING` makes the hint the
    /// one stored for this request or a later one of the same thread.
    #[must_use]
    pub fn pending_join(&self) -> bool {
        self.state.load(Ordering::Acquire) == PENDING && self.join.load(Ordering::Relaxed)
    }

    /// Whether no request is outstanding. Tests and loom models only.
    #[cfg(any(test, loom))]
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.state.load(Ordering::Acquire) == IDLE
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Instant;

    use super::*;
    use crate::ring::{self, RingConfig};

    fn one_ring() -> (ring::Producer, Consumer) {
        let (p, mut cs) = ring::ring(
            RingConfig {
                slots: 4,
                slot_bytes: 8,
                consumers: 1,
            },
            Lsn(1),
            Lsn(0),
        );
        (p, cs.pop().unwrap())
    }

    /// U1 (§15.7.5, last row). The writer is inside `answer` — its `f` signals,
    /// sleeps 300 ms, then attaches — when the network thread calls `abandon`.
    /// `abandon` must return only after `f` finished, leaving the cursor
    /// `PARKED` (no stale attach) and the slot `IDLE`.
    #[test]
    fn u1_abandon_waits_for_done_while_the_writer_holds_the_request() {
        let (mut p, mut c) = one_ring();
        let slot = Arc::new(Slot::new());
        let (sig_tx, sig_rx) = mpsc::channel::<()>();
        let (fin_tx, fin_rx) = mpsc::channel::<Instant>();
        slot.register_waker(std::thread::current());
        slot.post(Request::Chunk {
            cursor: Lsn(1),
            pos: None,
        });
        let s2 = Arc::clone(&slot);
        let w = std::thread::spawn(move || {
            let a = s2.answer(|_, _| {
                sig_tx.send(()).unwrap();
                std::thread::sleep(Duration::from_millis(300));
                p.attach(0, Lsn(1)).unwrap();
                fin_tx.send(Instant::now()).unwrap();
                Answer::Attached { cursor: Lsn(1) }
            });
            assert_eq!(a, Some(Answer::Attached { cursor: Lsn(1) }));
            p
        });
        sig_rx.recv().unwrap(); // the writer holds the request (TAKEN)
        slot.abandon(&mut c);
        let returned = Instant::now();
        let f_finished = fin_rx
            .try_recv()
            .expect("abandon returned while the writer was still inside `answer`");
        assert!(returned >= f_finished);
        assert!(slot.is_idle(), "slot must be IDLE after abandon");
        let _p = w.join().unwrap();
        assert_eq!(
            c.resume(),
            None,
            "stale attach: the consumer's cursor must be PARKED after abandon"
        );
    }

    /// U1, second half: `abandon` with no request outstanding returns at once.
    #[test]
    fn u1_abandon_on_an_idle_slot_returns_at_once() {
        let (mut p, mut c) = one_ring();
        let slot = Slot::new();
        p.attach(0, Lsn(1)).unwrap();
        assert_eq!(c.resume(), Some(Lsn(1)));
        let slot = Arc::new(slot);
        let s2 = Arc::clone(&slot);
        let (tx, rx) = mpsc::channel();
        // Not joined on failure: a hung `abandon` must fail the test, not hang it.
        std::thread::spawn(move || {
            s2.abandon(&mut c);
            tx.send(c).unwrap();
        });
        let mut c = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("abandon on an IDLE slot must return (it hung)");
        assert!(slot.is_idle());
        assert_eq!(c.resume(), None, "abandon detaches the consumer");
    }

    #[test]
    fn abandon_takes_a_pending_request_back() {
        let (_p, mut c) = one_ring();
        let slot = Slot::new();
        slot.post(Request::Join { d: Lsn(3) });
        assert!(slot.pending() && slot.pending_join());
        slot.abandon(&mut c);
        assert!(slot.is_idle());
        assert_eq!(slot.answer(|_, _| unreachable!("taken back")), None);
    }

    #[test]
    fn post_answer_await_round_trip_hands_over_request_answer_and_buffer() {
        let (_p, mut c) = one_ring();
        let slot = Arc::new(Slot::new());
        slot.register_waker(std::thread::current());
        let req = Request::Chunk {
            cursor: Lsn(7),
            pos: None,
        };
        slot.post(req);
        assert!(!slot.pending_join());
        let s2 = Arc::clone(&slot);
        let w = std::thread::spawn(move || {
            s2.answer(|r, b| {
                assert_eq!(*r, req);
                assert!(b.is_empty());
                b.extend_from_slice(b"chunk");
                Answer::Chunk {
                    next: Lsn(8),
                    end: ChunkEnd::More(None),
                }
            })
        });
        let mut buf = b"stale".to_vec();
        let got = slot.await_answer(&mut c, &mut buf, |_| Idle::Park(Duration::from_millis(5)));
        assert_eq!(
            got,
            Awaited::Answer(Answer::Chunk {
                next: Lsn(8),
                end: ChunkEnd::More(None)
            })
        );
        assert_eq!(buf, b"chunk");
        assert!(slot.is_idle());
        assert!(w.join().unwrap().is_some());
    }
}
