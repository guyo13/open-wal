//! loom model of the integrator's **publish barrier** (§14.6 / §15.3).
//!
//! The WAL itself is single-writer and uses no atomics — but an integrator that
//! ships records to a follower/replica reads the durable watermark the WAL
//! publishes (via the `DurabilityObserver` hook / a journal cursor) from a
//! *different* thread than the writer. That hand-off is a Release/Acquire
//! barrier, and getting its ordering wrong is a classic "costly to get wrong"
//! hazard: the publish side could observe the advanced watermark **before** the
//! record bytes it covers are visible, and ship torn/uninitialized data.
//!
//! This is an **integrator-facing** model — it does not touch WAL internals; it
//! pins the *correct pattern* the WAL's `on_durable(durable_lsn)` contract
//! assumes (§15.3): the watermark store is **Release**, the publish-side load is
//! **Acquire**, and every record at or below an observed watermark is therefore
//! visible with its committed bytes. loom exhaustively explores the thread
//! interleavings and memory-orderings and fails if any lets the publish side see
//! a watermark ahead of the data it covers.
//!
//! Gated to `--cfg loom` (and the `cfg(loom)`-only `loom` dev-dependency), so it
//! is invisible to a normal `cargo test` / `cargo build` / the MSRV check. Run:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --test loom_publish
//! ```

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;

/// Number of records the writer commits + publishes in the model. Kept tiny —
/// loom's state space is exponential in shared-memory operations.
const N: usize = 2;

#[test]
fn publish_never_runs_ahead_of_commit() {
    loom::model(|| {
        // `data[i]` = record (i+1)'s committed payload (0 = not yet committed).
        // `cursor` = the published durable watermark (0 = nothing durable; k = the
        // first k records are durable), exactly the `durable_lsn` the WAL hands to
        // `on_durable`.
        let data = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let cursor = Arc::new(AtomicUsize::new(0));

        // Writer (WAL / journal-consumer side): commit each record by writing its
        // payload, THEN advance the watermark with a **Release** store. The
        // Release publishes all prior writes (the payload) to any thread that
        // Acquire-observes the new watermark.
        let writer = {
            let data = data.clone();
            let cursor = cursor.clone();
            thread::spawn(move || {
                for i in 0..N {
                    data[i].store((i + 1) * 11, Ordering::Relaxed);
                    cursor.store(i + 1, Ordering::Release);
                }
            })
        };

        // Publish consumer: read the watermark with an **Acquire** load, then ship
        // every record at or below it. Each such record MUST already carry its
        // committed payload — i.e. the publish side never runs ahead of the
        // commit whose bytes it would ship.
        let reader = {
            let data = data.clone();
            let cursor = cursor.clone();
            thread::spawn(move || {
                let w = cursor.load(Ordering::Acquire);
                for i in 0..w {
                    let v = data[i].load(Ordering::Relaxed);
                    assert_eq!(
                        v,
                        (i + 1) * 11,
                        "record {} was published (watermark={w}) before its commit was visible",
                        i + 1
                    );
                }
            })
        };

        writer.join().unwrap();
        reader.join().unwrap();
    });
}
