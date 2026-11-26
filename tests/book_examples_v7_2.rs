//! Compiled mirror of the v7.2 code example in `book/` ("Writing and reading →
//! Resuming a reader") and the README's "Resuming a reader" (same discipline as
//! `tests/book_examples_v7.rs`: the book's `rust,ignore` blocks cannot link the
//! crate, so THIS file is their compile/behavior check). If you edit the example
//! here, update the snippet in the book (and vice versa).

use open_wal::{Lsn, Wal, WalConfig, WalError};

#[test]
fn resuming_a_reader() -> Result<(), WalError> {
    let tmp = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(tmp.path(), WalConfig::default())?;
    for p in [&b"one"[..], b"two", b"three"] {
        wal.append(p)?;
    }
    wal.commit()?;

    let mut shipped: Vec<(Lsn, Vec<u8>)> = Vec::new();
    let mut ship = |lsn: Lsn, payload: &[u8]| shipped.push((lsn, payload.to_vec()));

    // --- mirrored snippet: resuming a reader ---
    // Step 1: read a bounded chunk, then remember where we stopped.
    let mut reader = wal.reader_from(Lsn(1))?;
    for _ in 0..2 {
        let (lsn, payload) = reader.next().expect("a record")?;
        ship(lsn, payload);
    }
    let pos = reader.position().expect("resumable");
    drop(reader); // release the borrow so the writer can run

    // The writer keeps going: appends, commits, rolls and checkpoints
    // do not invalidate the position.
    wal.append(b"more")?;
    wal.commit()?;

    // Step 2: pick up exactly where we left off — no prefix rescan.
    let mut reader = wal.reader_at(pos)?;
    while let Some(record) = reader.next() {
        let (lsn, payload) = record?;
        ship(lsn, payload);
    }
    let pos = reader.position(); // `Some` at a clean end of log: resume later
    // --- end snippet ---
    drop(reader);

    // The prose claims around it.
    assert_eq!(
        shipped,
        vec![
            (Lsn(1), b"one".to_vec()),
            (Lsn(2), b"two".to_vec()),
            (Lsn(3), b"three".to_vec()),
            (Lsn(4), b"more".to_vec()),
        ],
        "the two steps together ship every record exactly once, in order"
    );
    let end = pos.expect("`Some` at a clean end of log");
    assert_eq!(end.next_lsn(), Lsn(5));
    // Resuming at the end yields whatever was committed since.
    wal.append(b"later")?;
    wal.commit()?;
    let mut reader = wal.reader_at(end)?;
    let (lsn, payload) = reader.next().expect("the record committed since")?;
    assert_eq!((lsn, payload), (Lsn(5), &b"later"[..]));
    assert!(reader.next().is_none());
    drop(reader);

    // Before the first `next()`, the position is the containing segment's start.
    let reader = wal.reader_from(Lsn(3))?;
    assert_eq!(reader.position().map(|p| p.next_lsn()), Some(Lsn(1)));
    drop(reader);

    // A position belongs to the log it came from. On a different log, `end`
    // (offset 136) lands inside a record: `InvalidPosition`, which does not
    // poison the handle.
    let other = tempfile::tempdir().unwrap();
    let (mut other_wal, _) = Wal::open(other.path(), WalConfig::default())?;
    other_wal.append(&[7u8; 300])?;
    other_wal.commit()?;
    assert!(matches!(
        other_wal.reader_at(end),
        Err(WalError::InvalidPosition)
    ));
    other_wal.append(b"y")?;
    assert_eq!(other_wal.commit()?, Lsn(2));
    Ok(())
}
