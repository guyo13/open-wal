//! §14.7 performance benchmarks (criterion).
//!
//! Four groups, driving only the public API against a **real `fdatasync`** (the
//! fsync cost is the point — durability is never mocked):
//!
//! 1. `throughput`     — records/s + MB/s for 64 B / 256 B / 4 KiB / 64 KiB payloads.
//! 2. `commit_latency` — p50/p99/p999 vs batch size 1/8/64/512/4096 (the
//!    group-commit self-regulation curve).
//! 3. `recovery`       — `Wal::open` time vs log size and segment count.
//! 4. `split_batch`    — commit latency for a batch that spans a segment boundary
//!    vs one that does not (quantifies the extra fsync).
//!
//! Fixtures (logs, batches) are built **outside** the measured closure
//! (`iter_batched*`/`iter_custom` setup) so we time the operation, not the setup.
//!
//! **Tail percentiles:** criterion reports only point estimates (mean/median), not
//! arbitrary percentiles. The `commit_latency` group therefore records per-iteration
//! timings into an `hdrhistogram` and emits p50/p99/p999 itself — both printed and
//! persisted to `target/perf/commit_latency_<batch>.json`, which `scripts/perf-gate.sh`
//! reads for the p999 regression delta (the throughput/median-time delta comes from
//! criterion's own `estimates.json`).
//!
//! **Tier:** these are NIGHTLY/manual (§14.11). Per-PR CI only compile-checks them
//! (`cargo bench --no-run`). Absolute numbers are device/filesystem-dependent — on
//! CI/tmpfs the fsync cost is unrepresentative, so these catch gross regressions and
//! show the curve *shape*, not headline throughput (real durability-throughput
//! numbers are §14.8 H1/H2 target-hardware territory).

use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hdrhistogram::Histogram;

use open_wal::{Wal, WalConfig};

fn cfg(segment_size: u64, max_record_size: u32) -> WalConfig {
    WalConfig {
        segment_size,
        max_record_size,
    }
}

/// On-disk framed size of a record carrying `payload` bytes (20-byte record
/// header + payload, padded to an 8-byte boundary, §5.3). Used only for sizing
/// segments so a batch provably does (or does not) roll.
fn framed_len(payload: usize) -> u64 {
    let pad = (8 - ((20 + payload) % 8)) % 8;
    (20 + payload + pad) as u64
}

/// Open a fresh WAL in a fresh temp dir. The `TempDir` is returned alongside so the
/// caller keeps it alive for the WAL's lifetime (dropping it removes the dir).
fn fresh_wal(segment_size: u64, max_record_size: u32) -> (tempfile::TempDir, Wal) {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(dir.path(), cfg(segment_size, max_record_size)).unwrap();
    (dir, wal)
}

fn make_batch(count: usize, payload: usize) -> Vec<Vec<u8>> {
    (0..count).map(|i| vec![i as u8; payload]).collect()
}

/// Group 1 — throughput for a range of payload sizes. A fresh WAL per iteration
/// keeps the log from growing (so no roll skews the steady-state write cost); the
/// segment is sized to hold exactly one batch. Only the append+commit is timed.
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    const N: usize = 32; // records per batch

    for &payload in &[64usize, 256, 4096, 64 * 1024] {
        // Segment large enough for one fresh batch (+ header + slack) so a single
        // measured commit never rolls.
        let segment_size = (N as u64 * framed_len(payload) + 4096).next_power_of_two();
        let max_record_size = payload as u32;
        let batch = make_batch(N, payload);

        group.throughput(Throughput::Bytes((N * payload) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(payload), &batch, |b, batch| {
            b.iter_batched_ref(
                || fresh_wal(segment_size, max_record_size),
                |(_dir, wal)| {
                    for p in batch.iter() {
                        wal.append(black_box(p)).unwrap();
                    }
                    black_box(wal.commit().unwrap());
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Group 2 — commit latency vs batch size, the group-commit amortization curve.
/// `iter_custom` times only `commit()` (the preceding appends are untimed) and
/// records each into an hdrhistogram, from which we emit p50/p99/p999.
///
/// The 8 MiB segment ensures even batch=4096 (~1.16 MiB framed) stays in one
/// segment — the curve reflects one `fdatasync` amortized over N records, never a
/// roll (that is `split_batch`'s job). The histogram includes criterion's warm-up
/// iterations, so the reported tail is mildly conservative — fine for a gate.
fn bench_commit_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit_latency");
    let payload = 256usize;
    let segment_size = 8u64 << 20;
    let max_record_size = payload as u32;
    fs::create_dir_all("target/perf").ok();

    for &batch_n in &[1usize, 8, 64, 512, 4096] {
        let batch = make_batch(batch_n, payload);
        let mut hist = Histogram::<u64>::new(3).unwrap();

        group.throughput(Throughput::Elements(batch_n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch_n), &batch, |b, batch| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (dir, mut wal) = fresh_wal(segment_size, max_record_size);
                    for p in batch.iter() {
                        wal.append(black_box(p)).unwrap();
                    }
                    let t0 = Instant::now();
                    black_box(wal.commit().unwrap());
                    let dt = t0.elapsed();
                    total += dt;
                    // ns, floored at 1 (hdrhistogram's low bound).
                    hist.record((dt.as_nanos() as u64).max(1)).ok();
                    drop(wal);
                    drop(dir);
                }
                total
            });
        });

        let p50 = hist.value_at_quantile(0.50);
        let p99 = hist.value_at_quantile(0.99);
        let p999 = hist.value_at_quantile(0.999);
        let samples = hist.len();
        println!(
            "commit_latency batch={batch_n}: p50={p50}ns p99={p99}ns p999={p999}ns (samples={samples})"
        );
        let json = format!(
            "{{\"batch\":{batch_n},\"p50_ns\":{p50},\"p99_ns\":{p99},\"p999_ns\":{p999},\"samples\":{samples}}}\n"
        );
        fs::write(format!("target/perf/commit_latency_{batch_n}.json"), json).unwrap();
    }
    group.finish();
}

/// Build a committed log of `n` records of `payload` bytes under `dir`/`config`.
/// Used as untimed setup for the recovery benchmark.
fn build_log(dir: &Path, config: WalConfig, n: usize, payload: usize) {
    let (mut wal, _) = Wal::open(dir, config).unwrap();
    let p = vec![0xABu8; payload];
    for _ in 0..n {
        wal.append(&p).unwrap();
    }
    wal.commit().unwrap();
}

/// Group 3 — recovery (`Wal::open`) time vs log size and segment count. The log is
/// built in untimed setup (a fresh dir per iteration); only the `open` is timed.
fn bench_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");
    let payload = 256usize;
    // (label, n_records, segment_size): contrast few-large-segments vs
    // many-small-segments, and scale the record count.
    let cases = [
        ("1k_fewseg", 1000usize, 8u64 << 20),
        ("1k_manyseg", 1000, 4096),
        ("8k_manyseg", 8000, 4096),
    ];

    for (label, n, seg) in cases {
        let config = cfg(seg, 256);
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    build_log(dir.path(), config, n, payload);
                    dir
                },
                |dir| {
                    let (wal, report) = Wal::open(dir.path(), config).unwrap();
                    black_box(report.durable_lsn);
                    black_box(wal);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// Group 4 — split-batch overhead: commit latency when a single commit spans a
/// segment boundary (seal + roll ⇒ a dir fsync and a second data fdatasync) vs when
/// it fits the active segment (one fdatasync). Tiny 512-byte segments (448 usable)
/// with 100-byte payloads (120-byte frames): 3 records fit; 5 records overflow and
/// force the split. Only `commit()` is timed.
fn bench_split_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("split_batch");
    let segment_size = 512u64;
    let max_record_size = 128u32;
    let payload = 100usize;

    for (label, n) in [("no_split", 3usize), ("split", 5usize)] {
        let batch = make_batch(n, payload);
        group.bench_with_input(BenchmarkId::from_parameter(label), &batch, |b, batch| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (dir, mut wal) = fresh_wal(segment_size, max_record_size);
                    for p in batch.iter() {
                        wal.append(black_box(p)).unwrap();
                    }
                    let t0 = Instant::now();
                    black_box(wal.commit().unwrap());
                    total += t0.elapsed();
                    drop(wal);
                    drop(dir);
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_throughput,
    bench_commit_latency,
    bench_recovery,
    bench_split_batch
);
criterion_main!(benches);
