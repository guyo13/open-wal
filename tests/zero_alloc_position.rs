//! v7.2 §8.7 / §14.7: `Reader::position` is pure — it performs **zero** heap
//! allocations, before the first `next`, mid-stream and at the end of log.
//!
//! A separate file from `tests/zero_alloc.rs` (which is not edited): the
//! counting allocator is process-global, and this binary has a single test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use open_wal::{Lsn, Wal, WalConfig};

struct CountingAlloc;

static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

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

fn measure<T, F: FnOnce() -> T>(f: F) -> (T, usize) {
    ALLOCS.store(0, Ordering::SeqCst);
    ENABLED.store(true, Ordering::SeqCst);
    let out = f();
    ENABLED.store(false, Ordering::SeqCst);
    (out, ALLOCS.load(Ordering::SeqCst))
}

#[test]
fn position_is_zero_alloc() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        segment_size: 1 << 20,
        max_record_size: 256,
    };
    let (mut wal, _) = Wal::open(dir.path(), cfg).unwrap();
    for i in 0..16u8 {
        wal.append(&[i; 64]).unwrap();
    }
    wal.commit().unwrap();

    let mut reader = wal.reader_from(Lsn(1)).unwrap();

    // Before the first `next`.
    let (p, allocs) = measure(|| reader.position());
    assert_eq!(p.map(|p| p.next_lsn()), Some(Lsn(1)));
    assert_eq!(allocs, 0, "position() before any next must not allocate");

    // Mid-stream, after every record. `next` is called outside the counted
    // window (its own zero-alloc gate is `tests/zero_alloc.rs`).
    let mut yielded = 0u64;
    while let Some(item) = reader.next() {
        item.unwrap();
        yielded += 1;
        let (p, allocs) = measure(|| reader.position());
        assert_eq!(p.map(|p| p.next_lsn()), Some(Lsn(yielded + 1)));
        assert_eq!(
            allocs, 0,
            "position() after record {yielded} must not allocate"
        );
    }
    assert_eq!(yielded, 16);

    // At the clean end of log, many times over.
    let (p, allocs) = measure(|| {
        let mut last = None;
        for _ in 0..1000 {
            last = reader.position();
        }
        last
    });
    assert_eq!(p.map(|p| p.next_lsn()), Some(Lsn(17)));
    assert_eq!(allocs, 0, "position() at the end of log must not allocate");
}
