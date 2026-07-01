//! §14.10 — soak / endurance test (`#[ignore]`, env-driven).
//!
//! Drives a **single long-lived WAL** through a sustained randomized workload —
//! append / commit / `checkpoint(durable)` / crash-recover — for a configurable
//! duration, while watching the things that only break *over time*:
//!
//! - **fd count** (`/proc/self/fd`) — a handle not closed on roll/checkpoint/
//!   reopen leaks file descriptors;
//! - **disk usage** (segment-dir bytes) — a checkpoint that fails to reclaim
//!   sealed segments leaks disk. Caught by a deterministic **per-checkpoint floor**
//!   (right after `checkpoint(durable)`, exactly the active segment must remain, so
//!   a leak reds on the first bad cycle), backstopped by a peak-bytes ceiling for
//!   gross runaway between checkpoints;
//! - **RSS** (`/proc/self/statm`) — recovery never materializes payloads (§8.5),
//!   so steady-state memory must stay flat;
//! - **commit latency** (p50/p99/p999 via `hdrhistogram`) — must not run away;
//!
//! and a lean independent **oracle** (committed `BTreeMap` + watermarks) that
//! re-checks the durability envelope (D1/D2/D3/D6/D8) after every crash-recover,
//! exactly the §14.3 refinement the M6 harness uses — so a *correctness*
//! regression under sustained load is caught too, not just a resource leak.
//!
//! Deterministic: a seeded LCG (no RNG dep), so a failure reproduces from
//! `WAL_SOAK_SEED`. Config via env:
//!
//! - `WAL_SOAK_SECONDS` (default `3`) — wall-clock duration; the owner/CI long
//!   run sets this to hours.
//! - `WAL_SOAK_SEED` (default fixed) — LCG seed.
//! - `WAL_SOAK_EVIDENCE` — optional path; a one-line JSON summary is appended.
//!
//! Run: `cargo test --test soak -- --ignored --nocapture`
//! (or via `scripts/m9/soak.sh`).

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use open_wal::{Lsn, Wal, WalConfig};

/// Tiny deterministic PRNG (SplitMix-ish LCG) — no external RNG, reproducible
/// from the seed (recovery determinism discipline, §8.6).
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        // LCG step (same constants as the recovery unit tests' fill).
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // xorshift the high bits out for a usable stream.
        (self.0 >> 17) ^ self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }
}

/// Number of open fds the process holds right now.
fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

/// Resident set size in bytes, from `/proc/self/statm` (field 2 = resident
/// pages) × the page size.
fn rss_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // SAFETY: sysconf is a pure query; libc is a dev-dependency here.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = if page > 0 { page as u64 } else { 4096 };
    resident_pages * page
}

/// Total bytes of the WAL directory's segment files (`*.wal`).
fn dir_wal_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if e.file_name().to_string_lossy().ends_with(".wal") {
                if let Ok(m) = e.metadata() {
                    total += m.len();
                }
            }
        }
    }
    total
}

/// Count of segment files (`*.wal`) currently on disk.
fn dir_wal_count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
                .count()
        })
        .unwrap_or(0)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// The independent oracle (mirrors the §14.3 / M6 refinement, pruned to bound its
/// own memory so the oracle never trips the RSS watch).
struct Oracle {
    /// Committed records still expected to be retrievable (`lsn → payload`),
    /// pruned below `max_ckpt` after a checkpoint reclaims them.
    committed: BTreeMap<u64, Vec<u8>>,
    /// Appended-but-uncommitted, in order.
    staged: Vec<(u64, Vec<u8>)>,
    /// Highest committed LSN ever (monotonic) — the durable watermark.
    watermark: u64,
    /// Next LSN `append` will assign.
    next_lsn: u64,
    /// Highest `up_to` ever checkpointed (bounds authorized reclamation, D8).
    max_ckpt: u64,
    /// `P` from the last recovery (monotonic non-decreasing).
    oldest: u64,
}

fn assert_envelope(wal: &Wal, report: &open_wal::RecoveryReport, oracle: &Oracle) {
    // D1/D3: recovery never loses a committed record's watermark.
    assert!(
        report.durable_lsn.0 >= oracle.watermark,
        "D1/D3: recovered durable_lsn {} < committed watermark {}",
        report.durable_lsn.0,
        oracle.watermark
    );
    // D8: never reclaim past an authorized checkpoint.
    assert!(
        report.oldest_lsn.0 <= oracle.max_ckpt + 1,
        "D8: oldest_lsn {} exceeds max_ckpt+1 {}",
        report.oldest_lsn.0,
        oracle.max_ckpt + 1
    );
    assert!(
        report.oldest_lsn.0 >= oracle.oldest,
        "oldest_lsn regressed: {} < {}",
        report.oldest_lsn.0,
        oracle.oldest
    );

    // D2/D6: replay is a dense run oldest..=durable, byte-identical to what we
    // committed (for the records we still track).
    let mut reader = wal.reader_from(Lsn(0)).expect("reader_from(0)");
    let mut prev: Option<u64> = None;
    let mut last = None;
    let mut first = None;
    while let Some(item) = reader.next() {
        let (lsn, payload) = item.expect("replay item");
        if let Some(p) = prev {
            assert_eq!(lsn.0, p + 1, "D2: replay not dense ({p} -> {})", lsn.0);
        }
        first.get_or_insert(lsn.0);
        prev = Some(lsn.0);
        last = Some(lsn.0);
        if let Some(want) = oracle.committed.get(&lsn.0) {
            assert_eq!(
                payload,
                &want[..],
                "D6: record {} not byte-identical",
                lsn.0
            );
        }
    }
    if let Some(first) = first {
        assert_eq!(
            first, report.oldest_lsn.0,
            "D2: replay must start at oldest"
        );
        assert_eq!(
            last.unwrap(),
            report.durable_lsn.0,
            "D2: replay must reach durable_lsn"
        );
    } else {
        assert_eq!(
            report.durable_lsn.0 + 1,
            report.oldest_lsn.0,
            "empty suffix ⇒ durable == oldest-1"
        );
    }
}

#[test]
#[ignore = "soak: env-driven; run with `cargo test --test soak -- --ignored --nocapture`"]
fn soak() {
    let secs = env_u64("WAL_SOAK_SECONDS", 3);
    let seed = env_u64("WAL_SOAK_SEED", 0x5333_4441_4c00_0001);
    // Small config so rolls / splits / checkpoints happen constantly.
    let cfg = WalConfig {
        segment_size: 4096,
        max_record_size: 256,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path();
    let mut rng = Rng(seed);

    let (mut wal, _) = Wal::open(path, cfg).expect("initial open");
    let mut oracle = Oracle {
        committed: BTreeMap::new(),
        staged: Vec::new(),
        watermark: 0,
        next_lsn: 1,
        max_ckpt: 0,
        oldest: 1,
    };

    // Baselines + watches.
    let fd0 = fd_count();
    let rss0 = rss_bytes();
    let mut peak_fd = fd0;
    let mut peak_disk = dir_wal_bytes(path);
    let mut peak_rss = rss0;
    let mut commit_us = Histogram::<u64>::new(3).expect("histogram");

    let mut ops: u64 = 0;
    let mut commits: u64 = 0;
    let mut checkpoints: u64 = 0;
    let mut recoveries: u64 = 0;

    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        match rng.below(100) {
            // ~64% append a random payload (boundary-biased sizes).
            0..=63 => {
                let len = match rng.below(8) {
                    0 => 0,
                    1 => 1,
                    2 => 8,
                    3 => cfg.max_record_size as usize,
                    _ => rng.below(u64::from(cfg.max_record_size) + 1) as usize,
                };
                let payload = rng.bytes(len);
                let lsn = wal.append(&payload).expect("append within max");
                assert_eq!(lsn.0, oracle.next_lsn, "append LSN mismatch");
                oracle.staged.push((oracle.next_lsn, payload));
                oracle.next_lsn += 1;
            }
            // ~22% commit (timed).
            64..=85 => {
                let t = Instant::now();
                let w = wal.commit().expect("commit").0;
                commit_us
                    .record(t.elapsed().as_micros() as u64)
                    .expect("record latency");
                commits += 1;
                if let Some(&(last, _)) = oracle.staged.last() {
                    for (lsn, payload) in oracle.staged.drain(..) {
                        oracle.committed.insert(lsn, payload);
                    }
                    oracle.watermark = last;
                }
                assert_eq!(w, oracle.watermark, "commit watermark mismatch");
            }
            // ~7% checkpoint to the durable watermark (reclaim sealed segments).
            86..=92 => {
                // We checkpoint to exactly `durable_lsn` (== the committed
                // watermark; intervening appends only stage, never advancing
                // durability). Every record physically in a segment is therefore
                // <= up_to, so *every* sealed segment is fully superseded and must
                // be reclaimed — the correct post-checkpoint floor is exactly ONE
                // `*.wal` file, the never-deleted active segment (§9).
                let up = oracle.watermark;
                assert_eq!(
                    up,
                    wal.durable_lsn().0,
                    "checkpoint up_to must == durable_lsn"
                );
                wal.checkpoint(Lsn(up)).expect("checkpoint");
                oracle.max_ckpt = oracle.max_ckpt.max(up);
                // POST-CHECKPOINT FLOOR (D8): the disk-leak gate that fires on the
                // *first* leaked segment, deterministically — a checkpoint that
                // reclaims N-1 of N sealed segments leaves >= 2 files here on cycle
                // 1, instead of slowly accreting toward the 16x peak ceiling below.
                // This is what makes "no unreclaimed-segment leak" a real guarantee;
                // the peak ceiling is only the gross-runaway backstop.
                let live = dir_wal_count(path);
                assert_eq!(
                    live, 1,
                    "D8 disk floor: checkpoint(durable {up}) must reclaim all sealed \
                     segments, leaving exactly the active segment; found {live} *.wal \
                     files (unreclaimed-segment leak?) [{checkpoints} checkpoints]"
                );
                // Records at or below max_ckpt may now be reclaimed from disk;
                // prune them so the oracle's own memory stays bounded (else the
                // oracle would grow RSS and trip the watch).
                oracle.committed.retain(|&lsn, _| lsn > oracle.max_ckpt);
                checkpoints += 1;
            }
            // ~7% crash-recover: drop the handle (lose staged), reopen, re-check.
            _ => {
                drop(wal);
                oracle.staged.clear();
                oracle.next_lsn = oracle.watermark + 1;
                let (w2, report) = Wal::open(path, cfg).expect("reopen");
                assert_envelope(&w2, &report, &oracle);
                oracle.oldest = report.oldest_lsn.0;
                oracle.committed.retain(|&lsn, _| lsn >= oracle.oldest);
                wal = w2;
                recoveries += 1;
            }
        }
        ops += 1;

        // Sample the resource watches periodically (cheap, but not every op).
        if ops % 128 == 0 {
            peak_fd = peak_fd.max(fd_count());
            peak_disk = peak_disk.max(dir_wal_bytes(path));
            peak_rss = peak_rss.max(rss_bytes());
        }
    }

    // Terminal recover + envelope check (anchors the final state).
    let _ = wal.commit();
    if let Some(&(last, _)) = oracle.staged.last() {
        for (lsn, payload) in oracle.staged.drain(..) {
            oracle.committed.insert(lsn, payload);
        }
        oracle.watermark = last;
    }
    drop(wal);
    let (final_wal, final_report) = Wal::open(path, cfg).expect("terminal reopen");
    assert_envelope(&final_wal, &final_report, &oracle);

    let rss1 = rss_bytes();
    let p50 = commit_us.value_at_quantile(0.50);
    let p99 = commit_us.value_at_quantile(0.99);
    let p999 = commit_us.value_at_quantile(0.999);

    // ---- bounded-growth gates ----
    // fd: a handful (LOCK + active segment + the transient reader/dir handles).
    // A leak grows this without bound; 32 is generous headroom over the baseline.
    assert!(
        peak_fd <= fd0 + 32,
        "fd leak: peak {peak_fd} > baseline {fd0} + 32 ({ops} ops, {recoveries} recoveries)"
    );
    // disk: the *primary* leak detector is the per-checkpoint FLOOR assertion in
    // the checkpoint arm (live == 1 right after checkpoint(durable) — fires on the
    // first leaked segment, cycle 1). This peak ceiling is only the gross-runaway
    // BACKSTOP: it catches unbounded growth between checkpoints (e.g. rolls with no
    // checkpoint firing). 16 segments is generous headroom over the legitimate
    // working set; a runaway blows past it.
    let disk_bound = 16 * cfg.segment_size;
    assert!(
        peak_disk <= disk_bound,
        "disk runaway: peak {peak_disk} > {disk_bound} ({checkpoints} checkpoints)"
    );
    // RSS: steady-state recovery materializes no payloads (§8.5). Generous 64 MiB
    // slack absorbs allocator/arena noise; a real leak exceeds it on a long run.
    let rss_bound = rss0 + 64 * 1024 * 1024;
    assert!(
        rss1 <= rss_bound && peak_rss <= rss_bound + 64 * 1024 * 1024,
        "RSS growth: start {rss0} end {rss1} peak {peak_rss} > bound"
    );
    // latency: a generous absolute p999 ceiling (a real fdatasync is sub-ms on
    // honest storage; 2 s catches a pathological runaway without flaking on a
    // loaded CI box).
    assert!(
        p999 <= 2_000_000,
        "commit p999 {p999}us exceeds 2s ceiling (latency runaway?)"
    );

    let summary = format!(
        "{{\"seconds\":{secs},\"seed\":{seed},\"ops\":{ops},\"commits\":{commits},\
\"checkpoints\":{checkpoints},\"recoveries\":{recoveries},\
\"fd_baseline\":{fd0},\"fd_peak\":{peak_fd},\
\"disk_peak\":{peak_disk},\"disk_bound\":{disk_bound},\
\"rss_start\":{rss0},\"rss_end\":{rss1},\"rss_peak\":{peak_rss},\
\"commit_us_p50\":{p50},\"commit_us_p99\":{p99},\"commit_us_p999\":{p999}}}"
    );
    println!("soak summary: {summary}");
    if let Ok(evidence) = std::env::var("WAL_SOAK_EVIDENCE") {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&evidence)
        {
            let _ = writeln!(f, "{summary}");
        }
    }
}
