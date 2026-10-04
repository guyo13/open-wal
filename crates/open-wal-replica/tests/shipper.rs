//! RM2 — `Shipper` integration tests (§15.2 P3, §15.3 ring overflow, §8.3
//! writer-never-blocks, §9 step 2 rejection, R7 restart).
//!
//! A real primary `Wal` + `Shipper` on the test (writer) thread ships over
//! loopback TCP to a real `Receiver` (`common::ReplicaServer`) or to a
//! hand-driven fake replica.
#![cfg(not(loom))] // the shipper (real std threads) is not built under `--cfg loom`

mod common;

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use common::{ReplicaServer, SMALL, payload, read_all, wait_until};
use open_wal::{Lsn, Wal};
use open_wal_replica::wire::{self, ErrCode, Frame, FrameReader, Limits, PROTO_VERSION};
use open_wal_replica::{ReceiverConfig, ReplicaStatus, Shipper, ShipperConfig};
use proptest::prelude::*;

fn ship_cfg(addr: SocketAddr) -> ShipperConfig {
    let mut c = ShipperConfig::new(vec![addr]);
    c.ring_bytes = 256 * 1024;
    c.slot_bytes = 256;
    c.heartbeat = Duration::from_millis(50);
    c.reconnect_backoff = Duration::from_millis(5);
    c.max_reconnect_backoff = Duration::from_millis(50);
    c.io_timeout = Duration::from_secs(2);
    c
}

fn rx_cfg() -> ReceiverConfig {
    let mut c = ReceiverConfig::new(SMALL);
    c.batch_records = 8;
    c.batch_interval = Duration::from_millis(1);
    c
}

#[derive(Clone, Debug)]
enum Op {
    Append(Vec<u8>),
    Commit,
    /// Let the network threads run while the primary sits on uncommitted
    /// captures — the window in which a mis-built shipper would ship early.
    Pause(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0usize..=SMALL.max_record_size as usize)
            .prop_flat_map(|n| proptest::collection::vec(any::<u8>(), n))
            .prop_map(Op::Append),
        2 => Just(Op::Commit),
        1 => (0u8..3).prop_map(Op::Pause),
    ]
}

/// Check the §15.2 P3 / R1 / R4 instant invariants. Samples are taken in an
/// order that makes each comparison sound against concurrent progress:
/// replica-side values first, the primary's durable watermark (owned by this
/// thread) last; acked before the replica's durable.
fn assert_never_ahead(wal: &Wal, sh: &Shipper, server: &ReplicaServer) {
    let acked = sh.replica_acked_lsn(0).unwrap();
    let r_durable = server.watermarks.durable_lsn();
    let r_last = server.watermarks.last_lsn();
    let p_durable = wal.durable_lsn();
    assert!(
        r_last <= p_durable,
        "R1: replica received lsn {r_last} beyond the primary's durable {p_durable}"
    );
    assert!(
        r_durable <= p_durable,
        "R1: replica durable {r_durable} > primary {p_durable}"
    );
    assert!(
        acked <= r_durable,
        "R4: acked {acked} > replica durable {r_durable}"
    );
}

/// No replica session ended with a protocol-class error (contiguity, CRC,
/// protocol, poison). A transport error when the primary closes the socket at
/// shutdown (a reset racing a final ACK) is not a replication fault.
fn assert_no_inband_errors(results: &[Result<(), open_wal_replica::ReplError>]) {
    for r in results {
        if let Err(e) = r {
            assert!(
                e.err_code().is_none(),
                "replica session failed in-band: {e:?}"
            );
        }
    }
}

/// Run `ops` on a primary shipping to one real replica, checking P3 after
/// every step; then converge and compare logs. If `release_on_capture`, the
/// shipper is deliberately misused (released at capture time — the §15.5
/// negative-control mis-build) and the first R1 violation is returned instead
/// of panicking.
fn run_primary(ops: &[Op], release_on_capture: bool) -> Option<String> {
    let pdir = tempfile::tempdir().unwrap();
    let rdir = tempfile::tempdir().unwrap();
    let server = ReplicaServer::start(rdir.path(), rx_cfg());
    let (mut wal, _) = Wal::open(pdir.path(), SMALL).unwrap();
    let mut sh = Shipper::new(ship_cfg(server.addr), &wal).unwrap();
    let mut violation = None;
    let mut check = |wal: &Wal, sh: &Shipper| {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_never_ahead(wal, sh, &server)
        }));
        if let Err(e) = r {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "R1/R4 violation".into());
            if release_on_capture {
                violation.get_or_insert(msg);
            } else {
                std::panic::resume_unwind(e);
            }
        }
    };
    for op in ops {
        match op {
            Op::Append(p) => {
                let lsn = wal.append(p).unwrap();
                sh.capture(lsn, p);
                if release_on_capture {
                    sh.on_commit(lsn); // WRONG on purpose: not durable yet.
                }
            }
            Op::Commit => {
                let w = wal.commit().unwrap();
                sh.on_commit(w);
            }
            Op::Pause(ms) => {
                let until = Instant::now() + Duration::from_millis(u64::from(*ms) + 1);
                while Instant::now() < until {
                    sh.poll();
                    check(&wal, &sh);
                    std::thread::sleep(Duration::from_micros(200));
                }
            }
        }
        sh.poll();
        check(&wal, &sh);
    }
    let w = wal.commit().unwrap();
    sh.on_commit(w);
    wait_until(30, "replica to ack the primary's durable watermark", || {
        sh.poll();
        check(&wal, &sh);
        sh.replica_acked_lsn(0) == Some(w)
    });
    assert_eq!(sh.replica_status(0), Some(ReplicaStatus::Streaming));
    sh.shutdown();
    let (rx, results) = server.stop();
    assert_no_inband_errors(&results);
    // R5/R9: the replica is the primary's log, byte-identical, same LSNs.
    assert_eq!(read_all(rx.wal()), read_all(&wal));
    violation
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// §15.2 P3 — never ahead (R1): at every instant the replica has neither
    /// received nor made durable anything beyond the primary's durable
    /// watermark, acks never exceed the replica's durable (R4), and the
    /// replica converges to a byte-identical copy (R5/R9).
    #[test]
    fn p3_replica_is_never_ahead_and_converges(
        ops in proptest::collection::vec(op_strategy(), 1..80),
    ) {
        prop_assert!(run_primary(&ops, false).is_none());
    }
}

/// Negative control for P3: a mis-built integration that releases at
/// `capture` (before `commit`) MUST be caught by the same R1 check — proving
/// the P3 oracle can fail. (The durability half — LazyFS power loss with the
/// real fsync in flight — is the RM7 §15.5 headline, not this test.)
#[test]
fn p3_negative_control_release_on_capture_is_detected() {
    let mut ops = Vec::new();
    for i in 0..40u8 {
        ops.push(Op::Append(vec![i; 32]));
        if i % 10 == 9 {
            ops.push(Op::Pause(2));
        }
    }
    let v = run_primary(&ops, true);
    assert!(
        v.as_deref().is_some_and(|m| m.contains("R1")),
        "release-on-capture went undetected: {v:?}"
    );
}

/// A fake replica that speaks the handshake by hand.
struct FakeReplica {
    listener: TcpListener,
}

impl FakeReplica {
    fn new() -> FakeReplica {
        FakeReplica {
            listener: TcpListener::bind("127.0.0.1:0").unwrap(),
        }
    }

    fn addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// Accept the primary and send `HELLO{durable}`.
    fn accept_hello(&self, durable: u64, proto_ver: u16) -> TcpStream {
        let (s, _) = self.listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        let mut b = Vec::new();
        wire::encode_frame(
            &Frame::Hello {
                proto_ver,
                replica_id: 9,
                durable_lsn: Lsn(durable),
            },
            &mut b,
        );
        (&s).write_all_checked(&b);
        s
    }
}

trait WriteAll {
    fn write_all_checked(self, b: &[u8]);
}
impl WriteAll for &TcpStream {
    fn write_all_checked(mut self, b: &[u8]) {
        std::io::Write::write_all(&mut self, b).unwrap();
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Serving(u64),
    Record(u64, Vec<u8>),
    Heartbeat,
    Err(ErrCode),
}

fn read_frames(s: TcpStream, max_payload: u32, stop_after_heartbeat: bool) -> Vec<Seen> {
    let mut rd = FrameReader::new(s, Limits::new(max_payload));
    let mut out = Vec::new();
    loop {
        match rd.read_frame() {
            Ok(Frame::Serving { from_lsn, .. }) => out.push(Seen::Serving(from_lsn.0)),
            Ok(Frame::Record { lsn, payload }) => out.push(Seen::Record(lsn.0, payload.to_vec())),
            Ok(Frame::Heartbeat { .. }) => {
                out.push(Seen::Heartbeat);
                if stop_after_heartbeat {
                    return out;
                }
            }
            Ok(Frame::Err { code, .. }) => out.push(Seen::Err(code)),
            Ok(other) => panic!("unexpected {other:?}"),
            Err(_) => return out,
        }
    }
}

/// §9 step 2 / §12.2: a replica claiming more than the primary durably has
/// (a former primary) is refused with `ERR(Contiguity)`, never served.
#[test]
fn replica_ahead_of_primary_is_rejected() {
    let pdir = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(pdir.path(), SMALL).unwrap();
    for i in 0..10u8 {
        wal.append(&[i]).unwrap();
    }
    wal.commit().unwrap();
    let fake = FakeReplica::new();
    let mut sh = Shipper::new(ship_cfg(fake.addr()), &wal).unwrap();
    let s = fake.accept_hello(100, PROTO_VERSION);
    wait_until(10, "join request", || {
        sh.poll();
        sh.replica_status(0) == Some(ReplicaStatus::Rejected)
    });
    assert_eq!(
        read_frames(s, 64, false),
        vec![Seen::Err(ErrCode::Contiguity)]
    );
    sh.shutdown();
}

#[test]
fn protocol_version_mismatch_is_refused() {
    let pdir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(pdir.path(), SMALL).unwrap();
    let fake = FakeReplica::new();
    let sh = Shipper::new(ship_cfg(fake.addr()), &wal).unwrap();
    let s = fake.accept_hello(0, PROTO_VERSION + 1);
    assert_eq!(
        read_frames(s, 64, false),
        vec![Seen::Err(ErrCode::Protocol)]
    );
    sh.shutdown();
}

/// §8.3 overflow + §4.2 "the writer never blocks": a replica that accepts and
/// then stops reading stalls only its own network thread. The writer keeps
/// appending/capturing/committing with bounded per-call latency, the ring
/// overflows past the stalled replica (`NeedsCatchUp`, repeatedly — once
/// mid-stream and again when it reconnects with a resume point the ring no
/// longer holds), and what *was* shipped is a dense, duplicate-free,
/// byte-identical prefix.
#[test]
fn stalled_replica_overflows_without_blocking_the_writer() {
    // ~25 MB of frames: more than loopback socket buffers can absorb, so the
    // network thread really blocks in write().
    const N: u64 = 200_000;
    const LEN: usize = 100;
    let pdir = tempfile::tempdir().unwrap();
    let big = open_wal::WalConfig {
        segment_size: 64 * 1024 * 1024,
        max_record_size: 256,
    };
    let (mut wal, _) = Wal::open(pdir.path(), big).unwrap();
    let fake = FakeReplica::new();
    let mut cfg = ship_cfg(fake.addr());
    // 1024 slots ≫ the 100-record commit interval: the ring alone never
    // overflows between commits, so the overflow below is caused by the
    // network thread blocking on the stalled socket — not by the writer.
    cfg.ring_bytes = 1024 * 128;
    cfg.slot_bytes = 128;
    cfg.io_timeout = Duration::from_secs(30);
    let mut sh = Shipper::new(cfg, &wal).unwrap();
    let s = fake.accept_hello(0, PROTO_VERSION);
    wait_until(10, "join", || {
        sh.poll();
        sh.replica_status(0) == Some(ReplicaStatus::Streaming)
    });

    // The replica now reads nothing; the socket buffers fill and the network
    // thread blocks in write().
    let mut worst_capture = Duration::ZERO;
    let mut worst_commit = Duration::ZERO;
    for l in 1..=N {
        let p = payload(l, LEN);
        let lsn = wal.append(&p).unwrap();
        let t = Instant::now();
        sh.capture(lsn, &p);
        worst_capture = worst_capture.max(t.elapsed());
        if l % 100 == 0 {
            let w = wal.commit().unwrap();
            let t = Instant::now();
            sh.on_commit(w);
            worst_commit = worst_commit.max(t.elapsed());
        }
    }
    let w = wal.commit().unwrap();
    sh.on_commit(w);
    // Generous bounds (shared CI runners): the point is "never waits on the
    // network", which would show up as ≥ seconds here (io_timeout = 30 s).
    assert!(
        worst_capture < Duration::from_millis(50) && worst_commit < Duration::from_millis(50),
        "writer blocked: worst capture {worst_capture:?}, worst on_commit {worst_commit:?}"
    );

    // Unstall: read everything the primary managed to send.
    let reader = std::thread::spawn(move || read_frames(s, 256, false));
    wait_until(30, "NeedsCatchUp mid-stream", || sh.catch_up_events(0) >= 1);
    let seen = reader.join().unwrap();
    assert_eq!(seen.first(), Some(&Seen::Serving(1)));
    let mut next = 1;
    for f in &seen[1..] {
        match f {
            Seen::Record(l, p) => {
                assert_eq!(*l, next, "dense, no gap, no duplicate");
                assert_eq!(*p, payload(*l, LEN), "byte-identical");
                next += 1;
            }
            Seen::Heartbeat => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    let got = next - 1;
    eprintln!(
        "stalled replica: received {got}/{N} records before falling behind; \
         worst capture {worst_capture:?}, worst on_commit {worst_commit:?}"
    );
    assert!(got > 0, "the replica must have streamed before stalling");
    assert!(got < N, "a stalled replica cannot have received everything");

    // The shipper reconnects at once; the replica resumes from what it got,
    // which the ring no longer holds ⇒ NeedsCatchUp again (served by RM3).
    let s2 = fake.accept_hello(got, PROTO_VERSION);
    wait_until(10, "second NeedsCatchUp", || {
        sh.poll();
        sh.catch_up_events(0) >= 2
    });
    assert_eq!(sh.replica_status(0), Some(ReplicaStatus::NeedsCatchUp));
    // Held with heartbeats only: never a record it cannot be served densely.
    let held = read_frames(s2, 256, true);
    assert_eq!(held, vec![Seen::Heartbeat]);
    sh.shutdown();
}

/// R7 — stateless primary: the primary restarts (shipper and WAL both
/// recreated); the replica re-HELLOs from its durable_lsn and shipping resumes
/// exactly after it, with no persisted shipping state.
#[test]
fn primary_restart_resumes_from_replica_hello() {
    let pdir = tempfile::tempdir().unwrap();
    let rdir = tempfile::tempdir().unwrap();
    let server = ReplicaServer::start(rdir.path(), rx_cfg());
    let mut expect = Vec::new();
    for phase in 0..3u64 {
        let (mut wal, _) = Wal::open(pdir.path(), SMALL).unwrap();
        let mut sh = Shipper::new(ship_cfg(server.addr), &wal).unwrap();
        for i in 0..40 {
            let p = payload(phase * 100 + i, 64);
            let lsn = wal.append(&p).unwrap();
            sh.capture(lsn, &p);
            expect.push((lsn, p));
            if i % 7 == 6 {
                let w = wal.commit().unwrap();
                sh.on_commit(w);
            }
        }
        let w = wal.commit().unwrap();
        sh.on_commit(w);
        wait_until(30, "replica to catch up", || {
            sh.poll();
            sh.replica_acked_lsn(0) == Some(w)
        });
        sh.shutdown(); // "crash": no state survives but the WAL itself
    }
    let (rx, results) = server.stop();
    assert_no_inband_errors(&results);
    assert_eq!(read_all(rx.wal()), expect);
}

#[test]
fn min_acked_and_unknown_ids() {
    let pdir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(pdir.path(), SMALL).unwrap();
    let a = FakeReplica::new();
    let b = FakeReplica::new();
    let mut cfg = ship_cfg(a.addr());
    cfg.replicas.push(b.addr());
    cfg.io_timeout = Duration::from_millis(200);
    let sh = Shipper::new(cfg, &wal).unwrap();
    assert_eq!(sh.min_replica_acked_lsn(), Some(Lsn(0)));
    assert_eq!(sh.replica_acked_lsn(0), Some(Lsn(0)));
    assert_eq!(sh.replica_acked_lsn(2), None);
    assert_eq!(sh.replica_status(2), None);
    sh.shutdown();
    drop((a, b));
}
