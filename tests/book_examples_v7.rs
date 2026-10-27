//! Compiled mirrors of the v7 code examples in `book/` (same discipline as
//! `tests/book_examples.rs`: the book's `rust,ignore` blocks cannot link the
//! crate, so THIS file is their compile/behavior check). If you edit an example
//! here, update the corresponding snippet in the book (and vice versa).

use open_wal::{DurabilityObserver, Lsn, Wal, WalConfig};

/// A stand-in for the reader's own observer in the snippet.
struct MyObserver;

impl DurabilityObserver for MyObserver {
    fn on_durable(&mut self, _durable_lsn: Lsn) {}
}

/// Book "Getting started → Open-time options": seed + observer + `oldest_lsn()`.
#[test]
fn open_time_options() -> Result<(), open_wal::WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // --- mirrored snippet: open-time options ---
    // A replica seeded from a snapshot at LSN 1000 starts its own log at 1001,
    // mirroring the primary's LSN space.
    let (mut wal, report) = Wal::options()
        .seed(Lsn(1001)) // cold-start origin; default Lsn(1)
        .observer(MyObserver) // optional; default NullObserver
        .open(dir, WalConfig::default())?;
    assert_eq!(report.oldest_lsn, Lsn(1001));
    assert_eq!(report.durable_lsn, Lsn(1000));
    assert_eq!(wal.append(b"first")?, Lsn(1001));
    // --- end snippet ---

    // The prose claims around it: `oldest_lsn()` equals the report at open, and
    // the seed is ignored once the directory holds a log.
    assert_eq!(wal.oldest_lsn(), report.oldest_lsn);
    wal.commit()?;
    drop(wal);
    let (wal, report) = Wal::options()
        .seed(Lsn(5))
        .open(dir, WalConfig::default())?;
    assert_eq!(report.oldest_lsn, Lsn(1001));
    assert_eq!(wal.durable_lsn(), Lsn(1001));
    // `Lsn(0)` is reserved and rejected.
    drop(wal);
    assert!(matches!(
        Wal::options().seed(Lsn(0)).open(dir, WalConfig::default()),
        Err(open_wal::WalError::InvalidConfig)
    ));
    Ok(())
}
