//! §14.7 zero-allocation assertion: steady-state `append`+`commit` (no roll)
//! and `Reader::next` perform **zero** heap allocations after warm-up.
//!
//! A counting global allocator gates on an `ENABLED` flag so only the measured
//! region is counted (setup, `tempfile`, and warm-up allocations are excluded).
//! `alloc` and `realloc` (buffer growth) both bump the counter.
//!
//! The counter is **process-global**, so the two tests below must not measure
//! concurrently — `cargo test` runs them on parallel threads, and a sibling's
//! setup allocations would otherwise be counted inside an open measured window
//! (notably the `append+commit` window, which spans an `fdatasync`). `SERIAL`
//! serializes the whole body of each test so only the measuring thread is live
//! while counting is on. (Thread-id gating inside the allocator is avoided: it
//! risks reentrancy via `thread::current()`.)

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use open_wal::{Lsn, Wal, WalConfig};

struct CountingAlloc;

static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
/// Serializes the measured tests so the global counter reflects one thread.
static SERIAL: Mutex<()> = Mutex::new(());

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwarding to the system allocator with the same layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarding to the system allocator with the same pointer/layout.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwarding to the system allocator with the same pointer/layout.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Run `f` with allocation counting enabled and return the number of
/// allocations (including reallocations) it performed.
fn measure<F: FnOnce()>(f: F) -> usize {
    ALLOCS.store(0, Ordering::SeqCst);
    ENABLED.store(true, Ordering::SeqCst);
    f();
    ENABLED.store(false, Ordering::SeqCst);
    ALLOCS.load(Ordering::SeqCst)
}

fn config() -> WalConfig {
    WalConfig {
        segment_size: 1 << 20,
        max_record_size: 256,
    }
}

/// Uniform payloads so the reused buffers reach their final capacity during
/// warm-up and never grow inside the measured region.
fn batch() -> Vec<[u8; 64]> {
    (0..16u8).map(|i| [i; 64]).collect()
}

#[test]
fn append_commit_steady_state_is_zero_alloc() {
    // Held for the whole body so the sibling test is not running (and thus not
    // allocating) while this one measures against the process-global counter.
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(dir.path(), config()).unwrap();
    let batch = batch();

    // Warm up: grow the staging buffer to its steady-state capacity.
    for _ in 0..3 {
        for p in &batch {
            wal.append(p).unwrap();
        }
        wal.commit().unwrap();
    }

    let allocs = measure(|| {
        for p in &batch {
            wal.append(p).unwrap();
        }
        wal.commit().unwrap();
    });
    assert_eq!(allocs, 0, "append+commit must not allocate after warm-up");
}

#[test]
fn reader_next_steady_state_is_zero_alloc() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(dir.path(), config()).unwrap();
    let batch = batch();
    for p in &batch {
        wal.append(p).unwrap();
    }
    wal.commit().unwrap();

    let mut reader = wal.reader_from(Lsn(1)).unwrap();
    // Warm up: the first `next` grows the read buffer to a full record.
    reader.next().unwrap().unwrap();

    let mut count = 0usize;
    let allocs = measure(|| {
        while let Some(item) = reader.next() {
            item.unwrap();
            count += 1;
        }
    });
    assert_eq!(count, batch.len() - 1, "should drain the remaining records");
    assert_eq!(allocs, 0, "Reader::next must not allocate after warm-up");
}
