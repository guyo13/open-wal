//! RM3 T8b — the serve ceiling on a **poisoned primary** (§9.1, §8.2).
//!
//! Run through `scripts/replica/poisoned-primary.sh`, which builds the
//! `LD_PRELOAD` EIO shim (`tests/fault/eio_preload.c`, shared with the WAL's
//! §14.8 H3 gate) and runs these `#[ignore]`d tests under it, single-threaded.
//! The shim makes one chosen `fdatasync` return EIO, so `wal.commit()` fails
//! and poisons the handle while the segment file already holds CRC-valid
//! records **above** anything a `commit() → Ok` covered.
//!
//! `Shipper::poll(&wal)` may be called on that poisoned handle; the crate
//! cannot detect poison through `&Wal`. The ceiling must still bind: nothing
//! above the released watermark is served, although `reader_from` exposes it.
//!
//! - **K = 0**: the batch's only sync fails. `durable` stays `R`; the records
//!   `R+1..` are in the file. A catch-up that read to the reader's end (or to
//!   `last_lsn`) would ship them.
//! - **K = 2**: a split batch — the first segment's sync and the roll pass, the
//!   second segment's sync fails. `durable = S > R` with **no** `Ok` returned,
//!   so `released` stays `R`. A ceiling of `wal.durable_lsn()` would ship
//!   `R+1..=S`.
//!
//! Scope (as for the H3 gate): the shim returns a *fake* EIO and drops no
//! data. This is a logic test of the ceiling, not a durability test.
//!
//! The fake replica has no WAL, so nothing but the primary can sync while the
//! shim is armed.
#![cfg(all(not(loom), target_os = "linux"))]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{Fake, Saw, measure_io, payload};
use open_wal::{Lsn, Wal, WalConfig};
use open_wal_replica::{ReplicaStatus, Shipper, ShipperConfig};

const R: u64 = 12;
const LEN: usize = 200;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "{name} is not set: T8b must run under the EIO shim — \
             use scripts/replica/poisoned-primary.sh"
        )
    })
}

fn arm(k: u64) {
    std::fs::write(PathBuf::from(env("WAL_FAULT_ARM")), format!("{k}\n")).unwrap();
}

fn injection_count() -> u64 {
    std::fs::read_to_string(PathBuf::from(env("WAL_FAULT_COUNT")))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn ship_cfg(fake: &Fake) -> ShipperConfig {
    let mut c = ShipperConfig::new(vec![fake.addr()]);
    c.ring_bytes = 64 * 256;
    c.slot_bytes = 256;
    c.heartbeat = Duration::from_millis(50);
    c.reconnect_backoff = Duration::from_millis(5);
    c.max_reconnect_backoff = Duration::from_millis(50);
    c
}

/// Read exactly the records `from..=to` (payloads from the test's own model).
fn expect(c: &mut common::FakeConn, from: u64, to: u64) {
    for l in from..=to {
        match c.next_data(Duration::from_secs(10)) {
            Saw::Record(got, bytes) => {
                assert_eq!(got, l, "dense");
                assert!(bytes == payload(l, LEN), "record {l} is not byte-identical");
            }
            other => panic!("expected RECORD {l}, got {other:?}"),
        }
    }
}

fn t8b(k: u64) {
    // 4 KiB segments, 200-byte records (224 framed): 17 per segment.
    let cfg = WalConfig {
        segment_size: 4096,
        max_record_size: 256,
    };
    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(dir.path(), cfg).unwrap();
    for l in 1..=R {
        wal.append(&payload(l, LEN)).unwrap();
    }
    assert_eq!(wal.commit().unwrap(), Lsn(R));

    // Structural: the shipper is created after 1..=R, so its ring starts at
    // R + 1 and the lagging replica can only be served from the log.
    let fake = Fake::new(256);
    let mut sh = Shipper::new(ship_cfg(&fake), &wal).unwrap();
    let d = 3;
    let mut c = fake.hello(d);
    common::wait_until(10, "the join request", || sh.request_pending(0));
    sh.poll(&wal).unwrap();
    assert_eq!(
        c.next_data(Duration::from_secs(5)),
        Saw::Serving {
            from: d + 1,
            oldest: 1
        }
    );
    common::wait_until(10, "the chunk request", || sh.request_pending(0));

    // The failing batch: R+1..=R+10 crosses the segment boundary (17 records
    // per segment), so the commit is a split batch.
    for l in R + 1..=R + 10 {
        let lsn = wal.append(&payload(l, LEN)).unwrap();
        sh.capture(lsn, &payload(l, LEN));
    }
    let before = injection_count();
    arm(k);
    let res = wal.commit();
    assert!(res.is_err(), "K={k}: the commit must fail, got {res:?}");
    // No on_commit: nothing was acknowledged.
    assert!(
        injection_count() > before,
        "K={k}: the shim injected nothing — the test verified NOTHING"
    );
    let (durable, last) = (wal.durable_lsn().0, wal.last_lsn().0);
    assert!(durable < last, "K={k}: durable {durable} < last {last}");
    match k {
        0 => assert_eq!(durable, R, "K=0: durable stays R"),
        _ => assert!(
            durable > R,
            "K={k}: the first segment was synced (durable {durable})"
        ),
    }
    let exposed = {
        let mut r = wal.reader_from(Lsn(durable + 1)).unwrap();
        let mut n = 0;
        while let Some(Ok(_)) = r.next() {
            n += 1;
        }
        n
    };
    assert!(
        exposed >= 1,
        "K={k}: precondition — the file must hold CRC-valid records above durable"
    );
    assert_eq!(sh.released(), Lsn(R));

    // Poll the poisoned handle: the chunk d+1..=R is read from the log.
    let ((), io) = measure_io(|| sh.poll(&wal).unwrap());
    assert!(io.rchar > 16, "positive control: a reader ran ({io:?})");
    expect(&mut c, d + 1, R); // positive control: exactly d+1..=R
    // The oracle: nothing above R, however long we keep polling.
    let until = std::time::Instant::now() + Duration::from_millis(300);
    while std::time::Instant::now() < until {
        sh.poll(&wal).unwrap();
        match c.next(Duration::from_millis(5)) {
            Saw::Timeout | Saw::Heartbeat(_) => {}
            other => panic!(
                "K={k}: R1 — nothing above the released watermark {R} may be served \
                 (durable {durable}, last {last}), got {other:?}"
            ),
        }
    }

    // §8.2 extension (liveness): reopen in-process, release what recovery
    // found durable, and the stream continues with exactly that prefix.
    drop(wal);
    let (mut wal, _) = Wal::open(dir.path(), cfg).unwrap();
    let recovered = wal.durable_lsn().0;
    assert!(recovered >= durable && recovered <= R + 10);
    let w = wal.commit().unwrap();
    assert_eq!(w.0, recovered, "an empty commit returns durable_lsn");
    sh.on_commit(w);
    sh.poll(&wal).unwrap();
    if recovered > R {
        expect(&mut c, R + 1, recovered);
    }
    // Then the stream stays dense across new appends.
    for l in recovered + 1..=recovered + 6 {
        let lsn = wal.append(&payload(l, LEN)).unwrap();
        assert_eq!(lsn.0, l);
        sh.capture(lsn, &payload(l, LEN));
    }
    let w = wal.commit().unwrap();
    sh.on_commit(w);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut next = recovered + 1;
    while next <= recovered + 6 {
        assert!(
            std::time::Instant::now() < deadline,
            "stream stalled at {next}"
        );
        sh.poll(&wal).unwrap();
        match c.next(Duration::from_millis(5)) {
            Saw::Record(l, bytes) => {
                assert_eq!(l, next, "dense after the reopen");
                assert!(bytes == payload(l, LEN));
                next += 1;
            }
            Saw::Timeout | Saw::Heartbeat(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(matches!(
        sh.replica_status(0),
        Some(ReplicaStatus::Streaming | ReplicaStatus::CatchingUp)
    ));
    eprintln!(
        "T8b K={k}: durable {durable} / last {last} after the failed commit, {exposed} \
         record(s) exposed above durable; served exactly {}..={R}; recovered to {recovered}",
        d + 1
    );
    sh.shutdown();
}

#[test]
#[ignore = "needs the LD_PRELOAD EIO shim: scripts/replica/poisoned-primary.sh"]
fn t8b_k0_a_failed_commit_leaves_readable_records_that_are_never_served() {
    t8b(0);
}

#[test]
#[ignore = "needs the LD_PRELOAD EIO shim: scripts/replica/poisoned-primary.sh"]
fn t8b_k2_a_failed_split_commit_does_not_raise_the_ceiling_to_durable() {
    t8b(2);
}
