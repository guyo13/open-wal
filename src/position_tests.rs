//! §14.1 unit tests for the v7.2 resumable reader position (§8.7):
//! [`Reader::position`], [`Wal::reader_at`] and `WalError::InvalidPosition`.
//!
//! Every oracle here is independent of the code under test: streams are compared
//! with the records the test itself appended, and positions with offsets the
//! test derives from the §5.3 framing (`20 + len`, padded to 8) — never with a
//! value `reader_at` reports about itself.

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::reader::ReaderPos;
use crate::{Lsn, Reader, Wal, WalConfig, WalError, record, segment};

/// 512-byte segments (448 usable), 256-byte max record: a 200-byte payload
/// frames to 224 bytes, so exactly two fill a segment to the brim.
fn tiny() -> WalConfig {
    WalConfig {
        segment_size: 512,
        max_record_size: 256,
    }
}

/// One roomy segment.
fn roomy() -> WalConfig {
    WalConfig {
        segment_size: 64 * 1024,
        max_record_size: 4096,
    }
}

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// The test's own §5.3 framing arithmetic.
fn framed(payload_len: usize) -> u64 {
    (20 + payload_len as u64).div_ceil(8) * 8
}

/// Drain a reader into owned `(lsn, payload)` pairs.
fn drain(mut r: Reader<'_>) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    while let Some(item) = r.next() {
        let (lsn, p) = item.unwrap();
        out.push((lsn.0, p.to_vec()));
    }
    out
}

/// The model's records `from..`, where `model[i]` is the payload of LSN `i + 1`.
fn model_from(model: &[Vec<u8>], from: u64) -> Vec<(u64, Vec<u8>)> {
    model
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64 + 1, p.clone()))
        .filter(|(l, _)| *l >= from)
        .collect()
}

/// Sorted `base_lsn`s of the `*.wal` files in `dir`, from the directory listing.
fn bases_on_disk(dir: &Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            name.strip_suffix(".wal")?.parse().ok()
        })
        .collect();
    v.sort_unstable();
    v
}

/// Five 200-byte records over three 512-byte segments: bases 1, 3, 5; records
/// at offsets 64 and 288 of each segment.
fn three_segment_log(dir: &Path) -> (Wal, Vec<Vec<u8>>) {
    let (mut wal, _) = Wal::open(dir, tiny()).unwrap();
    let model: Vec<Vec<u8>> = (1..=5u8).map(|i| vec![i; 200]).collect();
    for p in &model {
        wal.append(p).unwrap();
    }
    assert_eq!(wal.commit().unwrap(), Lsn(5));
    assert_eq!(bases_on_disk(dir), vec![1, 3, 5]);
    (wal, model)
}

/// The position after `reader_from(0)` has yielded `n` records.
fn pos_after(wal: &Wal, n: usize) -> ReaderPos {
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    for _ in 0..n {
        r.next().unwrap().unwrap();
    }
    r.position().unwrap()
}

fn pos(base: u64, offset: u64, next: u64) -> ReaderPos {
    ReaderPos::from_parts(Lsn(base), offset, Lsn(next))
}

#[test]
fn v72_position_after_each_record_resumes_identical_remainder() {
    // A multi-segment log of mixed sizes. Take the position before the first
    // `next`, after every record, and at the end; each must resume to exactly
    // the model's remainder (LSNs and bytes).
    let dir = tmp();
    let (mut wal, _) = Wal::open(dir.path(), tiny()).unwrap();
    let sizes = [0usize, 1, 8, 200, 37, 256, 100, 13, 250, 5, 64, 128, 256, 3];
    let model: Vec<Vec<u8>> = sizes
        .iter()
        .enumerate()
        .map(|(i, &n)| (0..n).map(|j| (i * 31 + j) as u8).collect())
        .collect();
    for (i, p) in model.iter().enumerate() {
        wal.append(p).unwrap();
        if i % 3 == 2 {
            wal.commit().unwrap();
        }
    }
    wal.commit().unwrap();
    assert!(bases_on_disk(dir.path()).len() >= 4, "must span segments");

    let mut positions = Vec::new();
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let start = r.position().unwrap();
    assert_eq!(start.next_lsn(), Lsn(1));
    positions.push(start);
    let mut yielded = 0u64;
    while let Some(item) = r.next() {
        let (lsn, payload) = item.unwrap();
        yielded += 1;
        assert_eq!(lsn.0, yielded);
        assert_eq!(payload, &model[yielded as usize - 1][..]);
        let p = r.position().expect("resumable after a yielded record");
        assert_eq!(
            p.next_lsn(),
            Lsn(yielded + 1),
            "after yielding {yielded}, position().next_lsn() must be {}",
            yielded + 1
        );
        positions.push(p);
    }
    assert_eq!(yielded, model.len() as u64);
    assert_eq!(r.position(), positions.last().copied(), "end position");
    drop(r);

    for (k, p) in positions.iter().enumerate() {
        assert_eq!(
            drain(wal.reader_at(*p).unwrap()),
            model_from(&model, k as u64 + 1),
            "resume at the position taken after {k} records"
        );
    }
}

#[test]
fn v72_sealed_segment_end_position_resumes_into_next_segment() {
    let dir = tmp();
    let (wal, model) = three_segment_log(dir.path());
    // After record 2 the reader sits at the end of sealed segment 1 (filled to
    // the brim: 64 + 2·224 = 512); the next record is segment 3's first.
    let p = pos_after(&wal, 2);
    assert_eq!(p.parts(), (Lsn(1), 64 + 2 * framed(200), Lsn(3)));
    assert_eq!(drain(wal.reader_at(p).unwrap()), model_from(&model, 3));
}

#[test]
fn v72_position_before_any_next_is_containing_segment_start() {
    let dir = tmp();
    let (wal, model) = three_segment_log(dir.path());
    // `reader_from(4)`: the containing segment is base 3. Before the first
    // `next` nothing was skipped, so the position is that segment's start.
    let mut r = wal.reader_from(Lsn(4)).unwrap();
    let p = r.position().unwrap();
    assert_eq!(p.parts(), (Lsn(3), segment::HEADER_SIZE, Lsn(3)));
    assert_eq!(p.next_lsn(), Lsn(3));
    // The original reader never yields below `from`…
    let (lsn, _) = r.next().unwrap().unwrap();
    assert_eq!(lsn, Lsn(4));
    assert_eq!(r.position().unwrap().next_lsn(), Lsn(5));
    drop(r);
    // …but a position does not carry `from`: resuming yields from its next_lsn.
    assert_eq!(drain(wal.reader_at(p).unwrap()), model_from(&model, 3));
}

#[test]
fn v72_end_of_log_position_resumes_after_further_commits() {
    // (a) Partly-filled active segment: the next commit writes at the end
    // position itself.
    let dir = tmp();
    let (mut wal, _) = Wal::open(dir.path(), roomy()).unwrap();
    let mut model = vec![b"a".to_vec(), b"bb".to_vec()];
    for p in &model {
        wal.append(p).unwrap();
    }
    wal.commit().unwrap();
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    while let Some(item) = r.next() {
        item.unwrap();
    }
    let end = r
        .position()
        .expect("a clean end of log must still have a position");
    drop(r);
    assert_eq!(end.parts(), (Lsn(1), 64 + framed(1) + framed(2), Lsn(3)));
    assert_eq!(drain(wal.reader_at(end).unwrap()), vec![]);
    // Staged-but-uncommitted records are not in the file: still nothing.
    wal.append(b"ccc").unwrap();
    assert_eq!(drain(wal.reader_at(end).unwrap()), vec![]);
    wal.append(b"dddd").unwrap();
    wal.commit().unwrap();
    model.extend([b"ccc".to_vec(), b"dddd".to_vec()]);
    assert_eq!(drain(wal.reader_at(end).unwrap()), model_from(&model, 3));

    // (b) The next commit rolls WITHOUT writing to the segment the end position
    // is in — once with room left (the record does not fit: the §7.3 empty
    // prefix), once with the segment filled to the brim.
    for first_batch in [vec![vec![7u8; 200]], vec![vec![7u8; 200], vec![8u8; 200]]] {
        let dir = tmp();
        let (mut wal, _) = Wal::open(dir.path(), tiny()).unwrap();
        let mut model = first_batch.clone();
        for p in &model {
            wal.append(p).unwrap();
        }
        wal.commit().unwrap();
        let n = model.len() as u64;
        let end = pos_after(&wal, model.len());
        assert_eq!(end.parts(), (Lsn(1), 64 + n * framed(200), Lsn(n + 1)));
        {
            let mut r = wal.reader_at(end).unwrap();
            assert!(r.next().is_none(), "nothing committed since");
            assert_eq!(r.position(), Some(end), "still the same end position");
        }

        model.extend([vec![9u8; 250], vec![10u8; 17]]);
        wal.append(&model[n as usize]).unwrap();
        wal.append(&model[n as usize + 1]).unwrap();
        wal.commit().unwrap();
        assert_eq!(
            bases_on_disk(dir.path()),
            vec![1, n + 1],
            "the commit must have rolled without writing to segment 1"
        );
        assert_eq!(
            drain(wal.reader_at(end).unwrap()),
            model_from(&model, n + 1)
        );
    }
}

#[test]
fn v72_position_is_none_after_max_lsn_yielded() {
    let dir = tmp();
    let (mut wal, _) = Wal::options()
        .seed(Lsn(u64::MAX - 1))
        .open(dir.path(), roomy())
        .unwrap();
    wal.append(b"a").unwrap();
    wal.append(b"last").unwrap();
    assert_eq!(wal.commit().unwrap(), Lsn(u64::MAX));

    let mut r = wal.reader_from(Lsn(0)).unwrap();
    assert_eq!(r.next().unwrap().unwrap().0, Lsn(u64::MAX - 1));
    assert_eq!(r.position().unwrap().next_lsn(), Lsn(u64::MAX));
    assert_eq!(r.next().unwrap().unwrap().0, Lsn(u64::MAX));
    assert_eq!(r.position(), None, "nothing can follow u64::MAX");
    assert!(r.next().is_none());
    assert_eq!(r.position(), None);
}

#[test]
fn v72_position_below_oldest_after_checkpoint_is_contiguity_violation() {
    let dir = tmp();
    let (mut wal, _) = three_segment_log(dir.path());
    // Mid-segment-1 position: record 2 at offset 288 of base 1.
    let p = pos_after(&wal, 1);
    assert_eq!(p.parts(), (Lsn(1), 64 + framed(200), Lsn(2)));
    wal.checkpoint(Lsn(2)).unwrap();
    assert_eq!(wal.oldest_lsn(), Lsn(3));
    // The exact variant: the same fatal gap as `reader_from` (§15.4), not
    // `InvalidPosition`.
    match wal.reader_at(p) {
        Err(WalError::ContiguityViolation) => {}
        Err(e) => panic!("expected ContiguityViolation, got {e:?}"),
        Ok(_) => panic!("expected ContiguityViolation, got Ok"),
    }
    assert!(matches!(
        wal.reader_from(p.next_lsn()),
        Err(WalError::ContiguityViolation)
    ));
}

#[test]
fn v72_sealed_end_position_survives_checkpoint_of_that_segment() {
    let dir = tmp();
    let (mut wal, model) = three_segment_log(dir.path());
    // End of sealed segment 1; the position still names base 1.
    let p = pos_after(&wal, 2);
    assert_eq!(p.parts().0, Lsn(1));
    assert_eq!(p.next_lsn(), Lsn(3));
    wal.checkpoint(Lsn(2)).unwrap();
    assert_eq!(bases_on_disk(dir.path()), vec![3, 5], "segment 1 deleted");
    // The segment-base rule: next_lsn is the base of live segment 3.
    match wal.reader_at(p) {
        Ok(r) => assert_eq!(drain(r), model_from(&model, 3)),
        Err(e) => panic!("sealed-end position must survive the checkpoint, got {e:?}"),
    }
}

#[test]
fn v72_position_is_none_after_error_and_after_invalid_end() {
    // Control: an untouched log ends cleanly ⇒ Some.
    let dir = tmp();
    let (wal, _) = three_segment_log(dir.path());
    assert!(pos_after(&wal, 5).next_lsn() == Lsn(6));
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    while let Some(item) = r.next() {
        item.unwrap();
    }
    assert!(r.position().is_some());
    drop(r);

    // An end on an INVALID record: overwrite the active segment's first record
    // (LSN 5, offset 64 of base 5) with garbage under the open handle.
    let f = OpenOptions::new()
        .write(true)
        .open(dir.path().join(segment::filename_for(Lsn(5))))
        .unwrap();
    f.write_all_at(&[0xA5u8; 64], segment::HEADER_SIZE).unwrap();
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    for want in 1..=4u64 {
        assert_eq!(r.next().unwrap().unwrap().0, Lsn(want));
        assert!(r.position().is_some());
    }
    assert!(r.next().is_none(), "the stream ends at the invalid record");
    assert_eq!(r.position(), None, "an invalid end is not resumable");
    drop(r);

    // `Some(Err(_))`: delete sealed segment 3 under the handle, so following
    // the roll out of segment 1 fails to open it.
    std::fs::remove_file(dir.path().join(segment::filename_for(Lsn(3)))).unwrap();
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    assert_eq!(r.next().unwrap().unwrap().0, Lsn(1));
    assert_eq!(r.next().unwrap().unwrap().0, Lsn(2));
    assert!(r.position().is_some());
    assert!(matches!(r.next(), Some(Err(WalError::Io(_)))));
    assert_eq!(r.position(), None, "not resumable after Some(Err(_))");
    assert!(r.next().is_none());
    assert_eq!(r.position(), None);
}

#[test]
fn v72_reader_ended_below_from_resumes_from_its_end() {
    let dir = tmp();
    let (mut wal, mut model) = three_segment_log(dir.path());
    // `from` is far past the end of the log (5): the reader ends at 6.
    let mut r = wal.reader_from(Lsn(100)).unwrap();
    assert!(r.next().is_none());
    let end = r.position().unwrap();
    drop(r);
    assert_eq!(end.next_lsn(), Lsn(6), "a position does not carry `from`");

    for i in 6..=8u8 {
        model.push(vec![i; 50]);
        wal.append(&model[i as usize - 1]).unwrap();
    }
    wal.commit().unwrap();
    assert_eq!(drain(wal.reader_at(end).unwrap()), model_from(&model, 6));
    assert_eq!(drain(wal.reader_from(Lsn(100)).unwrap()), vec![]);
}

#[test]
fn v72_reopen_after_torn_tail_position_above_durable_yields_nothing() {
    let dir = tmp();
    let (mut wal, _) = Wal::open(dir.path(), roomy()).unwrap();
    let mut model = vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()];
    for p in &model {
        wal.append(p).unwrap();
    }
    wal.commit().unwrap();
    let at3 = pos_after(&wal, 2); // next_lsn 3
    let end = pos_after(&wal, 3); // next_lsn 4
    drop(wal);

    // Fabricate a torn tail: flip a payload byte of record 3 (offset 112).
    let rec3 = 64 + framed(1) + framed(2);
    let f = OpenOptions::new()
        .write(true)
        .open(dir.path().join(segment::filename_for(Lsn(1))))
        .unwrap();
    f.write_all_at(&[0xFF], rec3 + 20).unwrap();
    drop(f);

    let (mut wal, report) = Wal::open(dir.path(), roomy()).unwrap();
    assert_eq!(report.durable_lsn, Lsn(2), "record 3 was truncated");
    model.truncate(2);

    // `end` (next_lsn 4 > durable + 1) pointed into the truncated, zeroed tail:
    // right after the reopen it reads zeros and yields nothing. The caller must
    // discard it now (§8.7 Reopen).
    assert!(end.next_lsn().0 > report.durable_lsn.0 + 1);
    assert_eq!(drain(wal.reader_at(end).unwrap()), vec![]);

    // `at3` (next_lsn == durable + 1) survives the reopen.
    assert_eq!(drain(wal.reader_at(at3).unwrap()), vec![]);
    model.push(b"new-3".to_vec());
    model.push(b"new-4".to_vec());
    wal.append(&model[2]).unwrap();
    wal.append(&model[3]).unwrap();
    wal.commit().unwrap();
    assert_eq!(drain(wal.reader_at(at3).unwrap()), model_from(&model, 3));
}

#[test]
fn v72_embedded_frame_forged_position_is_accepted() {
    // Documents the LIMIT of the forged-position guarantee (§8.7): payloads are
    // opaque, so a payload may itself contain a CRC-valid record at an 8-aligned
    // offset, and no local check can tell it from a real record boundary.
    //
    // Record 2's payload is 4 filler bytes followed by a complete framed record
    // carrying LSN 2. Payloads start 20 bytes into a frame, so the embedded
    // frame sits at an 8-aligned file offset and ends exactly where record 2
    // ends.
    let mut embedded = Vec::new();
    let embedded_len = record::encode_into(&mut embedded, Lsn(2), b"embedded") as u64;
    let mut payload2 = vec![0xEEu8; 4];
    payload2.extend_from_slice(&embedded);

    let dir = tmp();
    let (mut wal, _) = Wal::open(dir.path(), roomy()).unwrap();
    let model = vec![b"one".to_vec(), payload2, b"three".to_vec()];
    for p in &model {
        wal.append(p).unwrap();
    }
    wal.commit().unwrap();

    let rec2 = 64 + framed(3);
    let forged_offset = rec2 + 24;
    assert_eq!(forged_offset % 8, 0);
    assert_eq!(rec2 + framed(model[1].len()), forged_offset + embedded_len);

    // The forged position is accepted and yields a dense, CRC-valid run from
    // its next_lsn — but not the log's real record 2.
    let got = drain(wal.reader_at(pos(1, forged_offset, 2)).unwrap());
    assert_eq!(
        got,
        vec![(2, b"embedded".to_vec()), (3, b"three".to_vec())],
        "dense CRC-valid run starting at next_lsn"
    );
    assert_ne!(got, model_from(&model, 2), "…which is not the real stream");

    // No genuine position ever points there.
    let mut r = wal.reader_from(Lsn(0)).unwrap();
    let mut genuine = vec![r.position().unwrap()];
    while let Some(item) = r.next() {
        item.unwrap();
        genuine.push(r.position().unwrap());
    }
    let offsets: Vec<u64> = genuine.iter().map(|p| p.parts().1).collect();
    assert_eq!(
        offsets,
        vec![
            64,
            rec2,
            rec2 + framed(model[1].len()),
            rec2 + framed(model[1].len()) + framed(5)
        ]
    );
    assert!(!offsets.contains(&forged_offset));
}

/// `reader_at(p)` must be `Err(InvalidPosition)`.
#[track_caller]
fn assert_invalid(wal: &Wal, p: ReaderPos, why: &str) {
    match wal.reader_at(p) {
        Err(WalError::InvalidPosition) => {}
        Err(e) => panic!("{why}: expected InvalidPosition, got {e:?}"),
        Ok(_) => panic!("{why}: expected InvalidPosition, got Ok"),
    }
}

#[test]
fn v72_reader_at_rejects_forged_positions() {
    let dir = tmp();
    let (wal, model) = three_segment_log(dir.path());
    let f = framed(200);

    // Controls: the genuine mid-segment and end-of-log positions are accepted.
    assert_eq!(
        drain(wal.reader_at(pos(1, 64 + f, 2)).unwrap()),
        model_from(&model, 2)
    );
    assert_eq!(drain(wal.reader_at(pos(5, 64 + f, 6)).unwrap()), vec![]);

    // Rule 3: structural checks.
    assert_invalid(&wal, pos(2, 64 + f, 2), "segment_base is not a live base");
    assert_invalid(&wal, pos(7, 64, 8), "segment_base is not a live base");
    assert_invalid(&wal, pos(1, 0, 2), "offset inside the segment header");
    assert_invalid(&wal, pos(1, 56, 2), "offset below 64");
    assert_invalid(&wal, pos(1, 64 + f + 1, 2), "offset not 8-aligned");
    assert_invalid(&wal, pos(5, 520, 6), "offset past segment_size");
    assert_invalid(&wal, pos(5, u64::MAX - 7, 6), "offset far past the segment");
    assert_invalid(&wal, pos(3, 64 + f, 2), "next_lsn below segment_base");
    assert_invalid(&wal, pos(1, 64 + f, 4), "next_lsn in a later segment");
    assert_invalid(&wal, pos(1, 64 + f, 6), "next_lsn past the next base");

    // Rule 4: the one read.
    assert_invalid(&wal, pos(3, 64 + f, 6), "valid record, wrong LSN");
    assert_invalid(&wal, pos(1, 64 + 8, 2), "offset inside a record");
    assert_invalid(&wal, pos(1, 64 + 2 * f, 2), "clean end in a sealed segment");
}

#[test]
fn v72_invalid_position_leaves_handle_usable() {
    let dir = tmp();
    let (mut wal, _) = Wal::open(dir.path(), roomy()).unwrap();
    let mut model = vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()];
    for p in &model {
        wal.append(p).unwrap();
    }
    wal.commit().unwrap();

    // A clean end that is NOT a genuine end of log: the zeroed tail of the
    // active segment, claimed for an LSN that is already durable.
    let tail = 64 + framed(1) + framed(2) + framed(3);
    assert_invalid(&wal, pos(1, tail, 2), "clean end with next_lsn <= durable");
    assert_invalid(
        &wal,
        pos(1, tail + 800, 3),
        "clean end with next_lsn <= durable",
    );
    assert_invalid(&wal, pos(1, 64 + 8, 2), "offset inside a record");
    // Control: the same offset for durable + 1 is the genuine end of log.
    assert_eq!(drain(wal.reader_at(pos(1, tail, 4)).unwrap()), vec![]);

    // Non-poisoning (§12): append, commit, reads and checkpoint all still work.
    model.push(b"dddd".to_vec());
    assert_eq!(wal.append(&model[3]).unwrap(), Lsn(4));
    assert_eq!(wal.commit().unwrap(), Lsn(4));
    assert_eq!(
        drain(wal.reader_from(Lsn(0)).unwrap()),
        model_from(&model, 1)
    );
    assert_eq!(
        drain(wal.reader_at(pos(1, tail, 4)).unwrap()),
        model_from(&model, 4)
    );
    wal.checkpoint(Lsn(4)).unwrap();
    // …and the error repeats rather than latching anything.
    assert_invalid(&wal, pos(1, 64 + 8, 2), "offset inside a record");
    assert_eq!(wal.durable_lsn(), Lsn(4));
}
