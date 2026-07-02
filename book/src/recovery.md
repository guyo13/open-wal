# Recovery

Recovery is not an error path bolted on afterwards — it is the other half of
the durability contract, and most of this crate's engineering (and testing)
lives here. It runs inside every `Wal::open`, before the handle is returned.

## What `open` does

1. **Discover.** List the directory, collect `*.wal` segment files, parse each
   base LSN from the filename, and sort. (Never trusting directory iteration
   order — recovery must be deterministic.)
2. **Validate headers.** Each segment starts with a checksummed header written
   and synced at creation. A corrupt header on a sealed segment is fatal. One
   special case is forgiven: a highest-base file with an incomplete header is
   the residue of a crash *during* segment creation — it provably contains no
   durable records, so it is discarded.
3. **Scan records.** Each segment's records are walked in order, checking
   length bounds, CRC-32C, and LSN continuity, until the end-of-records
   sentinel (an all-zero header in the pre-allocated zero region).
4. **Handle the tail** — see below.
5. **Check cross-segment continuity.** The last LSN of each segment must be
   exactly followed by the next segment's base. An internal gap is fatal
   (`ContiguityViolation`).

The result is a `(Wal, RecoveryReport)` pair:

```rust,ignore
use open_wal::TailState;

let (wal, report) = Wal::open(dir, WalConfig::default())?;
println!(
    "recovered LSNs {}..={} across {} segment(s)",
    report.oldest_lsn, report.durable_lsn, report.segments_scanned
);
match report.tail_state {
    TailState::Clean => { /* the common case */ }
    TailState::TruncatedAt { segment_base, offset } => {
        // A torn tail (crash mid-write) was truncated and durably zeroed.
        // Only un-committed records were lost.
        eprintln!("torn tail truncated in segment {segment_base} at byte {offset}");
    }
}
```

A `TruncatedAt` tail is *normal* after a crash and worth logging, but requires
no action: nothing at or below any previously returned `durable_lsn` was lost.

## Torn tail vs. mid-log corruption — the crucial distinction

A crash mid-`commit` leaves a partially written record at the physical tail of
the active segment. That record was never covered by a returned `commit`, so
dropping it is correct. Recovery detects it (bad length or CRC), **truncates**
at that point, and then durably **zeroes everything from the truncation point
to the end of the segment**. The zeroing matters: without it, a *stale but
CRC-valid* record from an earlier generation could lurk past the tail and be
"resurrected" as if it were live data on some later recovery.

Corruption *before* the tail is a different animal entirely. If a record fails
validation but a valid record still exists after it, the bad record cannot be
a torn tail — data after it was genuinely written and acknowledged. Truncating
there would silently discard acknowledged records. Recovery instead fails
loudly with `TornMidLog` (or `Corruption` in a sealed segment, which can never
contain a torn tail). This is deliberate: **mid-log corruption means the
storage lied**, and the correct response is to stop and involve the operator —
restore from backup or accept explicit, visible data loss — not to quietly
shrink the log.

## Properties you can rely on

- **Deterministic and idempotent.** Recovery is a pure function of the on-disk
  bytes — no clocks, no environment. Opening the same directory repeatedly
  converges: the second recovery sees what the first left and changes nothing.
- **Bounded.** Recovery never panics, reads out of bounds, or scans/allocates
  unboundedly — for *any* input bytes, including adversarial ones. The parser
  is fuzzed continuously and cross-checked against an independent reference
  implementation.
- **Frugal with memory.** Recovery never loads payloads into memory; it keeps
  only a small per-segment index. Opening a multi-GB log does not OOM.
- **Read-back fidelity.** What replay yields after recovery is exactly what
  was committed: same records, same order, byte-identical.

These are user-facing distillations of invariants D1–D12; the precise
normative statements and their test mapping are in
[§4 of the design spec](https://github.com/guyo13/open-wal/blob/main/docs/wal_design_v6.md).

## One operational note: reopening after a crash

When a writer process dies, the OS releases its directory lock during process
teardown — which can complete slightly *after* the process is otherwise gone.
A supervisor that restarts the writer immediately may see a brief, spurious
`WalError::Locked` on the first `open`. Retry with a short backoff (up to
about a second) before treating `Locked` as a genuine concurrent writer.
