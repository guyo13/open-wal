//! Compiled mirrors of every code example in `README.md` and `book/`.
//!
//! Docs that don't compile are worse than none: each example shown to users is
//! kept here as a real test against the real public API, so an API change that
//! would silently rot the docs fails `cargo test` instead. If you edit an
//! example here, update the corresponding snippet in the README/book (and vice
//! versa) — the doc blocks are marked `rust,ignore` because the book cannot
//! link the crate, so THIS file is the compile/behavior check.

use open_wal::{DurabilityObserver, Lsn, Wal, WalConfig};

/// README + book "Getting started": open, append, commit, read back, reopen.
#[test]
fn quickstart() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // --- mirrored snippet: quickstart ---
    let (mut wal, report) = Wal::open(dir, WalConfig::default())?;
    assert_eq!(report.durable_lsn, Lsn(0)); // empty log: nothing durable yet

    // `append` is pure memory: it assigns an LSN and buffers the record.
    // Nothing is durable yet.
    let first = wal.append(b"order-created:42")?;
    let last = wal.append(b"order-paid:42")?;

    // `commit` writes the buffered records and fdatasyncs them. When it
    // returns Ok(w), every record with lsn <= w is durable.
    let durable = wal.commit()?;
    assert_eq!(durable, last);
    assert!(first <= durable);

    // Replay everything, in order, byte-identical.
    let mut reader = wal.reader_from(Lsn(0))?;
    let mut seen = Vec::new();
    while let Some(record) = reader.next() {
        let (lsn, payload) = record?;
        seen.push((lsn, payload.to_vec()));
    }
    assert_eq!(
        seen,
        vec![
            (Lsn(1), b"order-created:42".to_vec()),
            (Lsn(2), b"order-paid:42".to_vec()),
        ]
    );
    // --- end snippet ---

    // --- mirrored snippet: reopen/recover ---
    drop(wal); // releases the directory lock
    let (wal, report) = Wal::open(dir, WalConfig::default())?;
    assert_eq!(report.durable_lsn, Lsn(2));
    assert_eq!(wal.durable_lsn(), Lsn(2));
    // --- end snippet ---
    Ok(())
}

/// Book "The durability model": group commit — many appends, one fsync.
#[test]
fn group_commit() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(tmp.path(), WalConfig::default())?;

    // --- mirrored snippet: group commit ---
    for event in ["a", "b", "c", "d"] {
        wal.append(event.as_bytes())?; // pure memory, no syscall
    }
    let durable = wal.commit()?; // one write + one fdatasync for the batch
    assert_eq!(durable, wal.last_lsn());
    // --- end snippet ---
    Ok(())
}

/// Book "The durability model": the un-committed tail is not durable, and a
/// commit with nothing staged is a no-op returning the current watermark.
#[test]
fn append_is_not_durable() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // --- mirrored snippet: append vs commit ---
    let (mut wal, _) = Wal::open(dir, WalConfig::default())?;
    wal.append(b"durable")?;
    wal.commit()?;
    wal.append(b"buffered only")?; // never committed

    assert_eq!(wal.last_lsn(), Lsn(2)); // assigned...
    assert_eq!(wal.durable_lsn(), Lsn(1)); // ...but not durable

    // Dropping the handle without committing loses the buffered tail —
    // exactly what a crash would do.
    drop(wal);
    let (_, report) = Wal::open(dir, WalConfig::default())?;
    assert_eq!(report.durable_lsn, Lsn(1));
    // --- end snippet ---
    Ok(())
}

/// Book "The durability model": multi-event atomicity is one compound record.
#[test]
fn compound_record_for_atomicity() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(tmp.path(), WalConfig::default())?;

    // --- mirrored snippet: compound record ---
    // WRONG for atomicity: two appends in one commit batch. A crash (or a
    // split across segments) can keep the first and lose the second.
    //
    // RIGHT: if two events must be all-or-nothing, encode them into ONE
    // payload. A single record is the only atomicity primitive.
    let mut compound = Vec::new();
    compound.extend_from_slice(b"debit:alice:100;");
    compound.extend_from_slice(b"credit:bob:100");
    let lsn = wal.append(&compound)?;
    wal.commit()?;
    // --- end snippet ---
    assert_eq!(lsn, Lsn(1));
    Ok(())
}

/// Book "Writing & reading": the lending reader; `.to_vec()` to retain.
#[test]
fn reader_is_lending() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(tmp.path(), WalConfig::default())?;
    for i in 0..5u32 {
        wal.append(&i.to_le_bytes())?;
    }
    wal.commit()?;

    // --- mirrored snippet: lending reader ---
    let mut reader = wal.reader_from(Lsn(3))?; // start mid-log
    let mut retained: Vec<(Lsn, Vec<u8>)> = Vec::new();
    while let Some(record) = reader.next() {
        let (lsn, payload) = record?;
        // `payload` borrows the reader's internal buffer and is only valid
        // until the next `reader.next()` call. Copy it to keep it.
        retained.push((lsn, payload.to_vec()));
    }
    // --- end snippet ---
    assert_eq!(retained.len(), 3); // LSNs 3, 4, 5
    assert_eq!(retained[0].0, Lsn(3));
    Ok(())
}

/// Book "Recovery": inspecting the `RecoveryReport` and `TailState`.
#[test]
fn recovery_report() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    {
        let (mut wal, _) = Wal::open(dir, WalConfig::default())?;
        wal.append(b"survives")?;
        wal.commit()?;
    }

    // --- mirrored snippet: recovery report ---
    use open_wal::TailState;

    let (wal, report) = Wal::open(dir, WalConfig::default())?;
    println!(
        "recovered LSNs {}..={} across {} segment(s)",
        report.oldest_lsn, report.durable_lsn, report.segments_scanned
    );
    match report.tail_state {
        TailState::Clean => { /* the common case */ }
        TailState::TruncatedAt {
            segment_base,
            offset,
        } => {
            // A torn tail (crash mid-write) was truncated and durably zeroed.
            // Only un-committed records were lost.
            eprintln!("torn tail truncated in segment {segment_base} at byte {offset}");
        }
    }
    // --- end snippet ---
    assert_eq!(report.tail_state, TailState::Clean);
    assert_eq!(wal.durable_lsn(), Lsn(1));
    Ok(())
}

/// Book "Checkpointing": reclaim space up to a snapshot LSN.
#[test]
fn checkpoint_after_snapshot() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    // Tiny segments so a checkpoint actually has sealed segments to delete.
    let cfg = WalConfig {
        segment_size: 4096,
        max_record_size: 512,
    };
    let (mut wal, _) = Wal::open(tmp.path(), cfg)?;
    let mut snapshot_lsn = Lsn(0);
    for i in 0..64u32 {
        let lsn = wal.append(&[0u8; 128])?;
        wal.commit()?;
        if i == 40 {
            // Pretend the application persisted a snapshot covering
            // everything up to here.
            snapshot_lsn = lsn;
        }
    }

    // --- mirrored snippet: checkpoint ---
    // Only ever pass the LSN covered by your latest DURABLE SNAPSHOT —
    // never `wal.durable_lsn()`. Recovery is snapshot + replay of the log
    // after it; deleting the log past your snapshot caps recovery at the
    // stale snapshot. The WAL trusts the caller here.
    wal.checkpoint(snapshot_lsn)?;
    // --- end snippet ---

    // Records above the snapshot are still readable, from the new oldest LSN.
    let mut reader = wal.reader_from(snapshot_lsn.next())?;
    let (lsn, _) = reader.next().unwrap()?;
    assert_eq!(lsn, snapshot_lsn.next());

    // A reader below the retention floor is a loud error, never a silent skip.
    assert!(wal.reader_from(Lsn(1)).is_err());
    Ok(())
}

/// Book "Single-writer": the directory lock rejects a second writer.
#[test]
fn second_writer_is_rejected() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // --- mirrored snippet: dir lock ---
    let (wal, _) = Wal::open(dir, WalConfig::default())?;
    match Wal::open(dir, WalConfig::default()) {
        Err(open_wal::WalError::Locked) => { /* expected: one writer at a time */ }
        Err(other) => panic!("expected Locked, got {other:?}"),
        Ok(_) => panic!("expected Locked, got a second writer"),
    }
    drop(wal); // releases the lock; now a new writer may open
    let (_wal, _) = Wal::open(dir, WalConfig::default())?;
    // --- end snippet ---
    Ok(())
}

/// Book "External readers & the observer": publish the durable watermark
/// through a `DurabilityObserver`.
#[test]
fn observer_publishes_watermark() -> Result<(), open_wal::WalError> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let tmp = tempfile::tempdir().unwrap();

    // --- mirrored snippet: durability observer ---
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
        tmp.path(),
        WalConfig::default(),
        WatermarkPublisher(Arc::clone(&watermark)),
    )?;

    wal.append(b"event")?;
    wal.commit()?;
    // A consumer thread would now see watermark >= 1 and may ship records
    // up to it (and no further) via a `Reader`.
    assert_eq!(watermark.load(Ordering::Acquire), 1);
    // --- end snippet ---
    Ok(())
}

/// Book "Getting started": config validation is at `open`, not at roll time.
#[test]
fn invalid_config_is_rejected_at_open() {
    let tmp = tempfile::tempdir().unwrap();

    // --- mirrored snippet: invalid config ---
    // A record may not span segments, so max_record_size must fit a segment
    // alongside the segment header, record header, and padding:
    // max_record_size + 91 <= segment_size. Violations fail at open().
    let bad = WalConfig {
        segment_size: 1024,
        max_record_size: 1024,
    };
    assert!(matches!(
        Wal::open(tmp.path(), bad),
        Err(open_wal::WalError::InvalidConfig)
    ));
    // --- end snippet ---
}

/// Book "Writing & reading": oversized payloads are rejected at append.
#[test]
fn oversized_record_is_rejected() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        segment_size: 1 << 20,
        max_record_size: 1024,
    };
    let (mut wal, _) = Wal::open(tmp.path(), cfg)?;

    // --- mirrored snippet: record too large ---
    let big = vec![0u8; 2048]; // exceeds max_record_size
    assert!(matches!(
        wal.append(&big),
        Err(open_wal::WalError::RecordTooLarge)
    ));
    // --- end snippet ---
    Ok(())
}
