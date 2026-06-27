//! F1 — recovery-parser fuzz (§14.5, the highest-priority M9 fuzz target).
//!
//! Feeds arbitrary bytes as a **directory of segment files** to the recovery
//! parser and asserts it always terminates with `Ok(suffix)` or a clean `Err`,
//! never panicking, reading out of bounds, looping forever, or allocating
//! unboundedly — and that the bounded forward scan stays within its bound
//! (`scan_bound`). This is the executed proof of **D11** (bounded recovery
//! parsing for *any* input bytes).
//!
//! ## Primary surface: the real public `Wal::open`
//!
//! The directory-level mode is primary by design. The generator emits a *set* of
//! `*.wal` files with fuzzer-controlled filenames and `base_lsn`s — out-of-order,
//! duplicate, gapped, `base_lsn = 0`, malformed filenames — some with valid
//! headers + dense record bodies, some pure garbage. It then drives the real
//! `Wal::open`, so the whole production recovery state machine is in the blast
//! radius: filename parse → discovery → sort → header validation → the §8.4
//! incomplete-highest discard → cross-segment continuity → `recover_segment`.
//! The juicy D11/D2/contiguity bugs (empty-active, gap detection, duplicate /
//! out-of-order bases) live there, not in single-segment decoding. The
//! bounded-scan probe is reset before and asserted after the **production**
//! `open` call, so it measures production, not a harness copy.
//!
//! ## Secondary: direct `recover_segment` probe
//!
//! Each `*.wal` file is then also driven through `recover_segment` in isolation
//! via the `fuzzing` module, which asserts the bounded-scan probe directly. The
//! re-exported helpers exist for this secondary mode only — the thing under test
//! is always the public `open` above.

#![no_main]

use std::fs;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use open_wal::{Lsn, Wal, WalConfig};

/// A handful of small segment sizes that force rolls / commit-time splits during
/// any future write, and keep recovery cheap to fuzz. Each leaves `>= 165` bytes
/// of `max_record_size` headroom (`segment_size - 91`), so a valid config always
/// exists.
const SEG_SIZES: [u64; 5] = [256, 512, 1024, 4096, 65536];

/// The fuzzer-decoded recovery scenario: a config selector plus a set of files to
/// materialize in the WAL directory.
#[derive(Arbitrary, Debug)]
struct Scenario {
    /// Selects the segment size from [`SEG_SIZES`].
    seg_sel: u8,
    /// Reduced (mod the valid range) to a `max_record_size` satisfying §5.3.
    max_sel: u32,
    /// The files dropped into the directory before `Wal::open` runs.
    files: Vec<SegFile>,
    /// Whether to also run the secondary single-file `recover_segment` probe.
    probe_direct: bool,
}

/// One file to write into the WAL directory.
#[derive(Arbitrary, Debug)]
struct SegFile {
    name: Name,
    body: Body,
}

/// The file's name — a well-formed segment name for a chosen base, or arbitrary.
#[derive(Arbitrary, Debug)]
enum Name {
    /// A `{base:020}.wal` name (the production format) for this base.
    Wal(u64),
    /// An arbitrary name — may or may not parse as a segment name (the filename
    /// parser is itself an input surface).
    Raw(String),
}

/// The file's contents.
#[derive(Arbitrary, Debug)]
enum Body {
    /// Pure arbitrary bytes — the raw D11 surface (garbage header included).
    Raw(Vec<u8>),
    /// A valid header for `base` followed by a sequence of records (each
    /// optionally corrupted) and arbitrary trailing bytes. Exercises the real
    /// recovery state machine: dense runs, torn tails, mid-log corruption,
    /// sealed-vs-active classification, cross-segment continuity.
    Structured {
        base: u64,
        recs: Vec<Rec>,
        trailing: Vec<u8>,
    },
}

/// One record to encode into a structured body.
#[derive(Arbitrary, Debug)]
struct Rec {
    lsn: u64,
    payload: Vec<u8>,
    /// If set, flip a byte so the record is invalid (a candidate torn/corrupt
    /// boundary for the classifier).
    corrupt: bool,
}

/// Turn a (possibly arbitrary) name into a filesystem-safe single path component,
/// or `None` to skip it. Keeps `/`, NUL, `.`/`..` and over-long names out.
fn safe_name(name: &Name) -> Option<String> {
    match name {
        Name::Wal(base) => Some(format!("{base:020}.wal")),
        Name::Raw(s) => {
            if s.is_empty() || s.len() > 64 || s == "." || s == ".." {
                return None;
            }
            if s.bytes().any(|b| b == b'/' || b == 0) {
                return None;
            }
            Some(s.clone())
        }
    }
}

/// Build the on-disk bytes for a body under `cfg` (payloads clamped to
/// `max_record_size` so structured records are genuinely valid).
fn body_bytes(body: &Body, max_record_size: u32) -> Vec<u8> {
    match body {
        Body::Raw(bytes) => bytes.clone(),
        Body::Structured {
            base,
            recs,
            trailing,
        } => {
            let mut buf = open_wal::fuzzing::segment_header_bytes(*base);
            for r in recs {
                let clamp = (max_record_size as usize).min(r.payload.len());
                let start = buf.len();
                let framed = open_wal::fuzzing::encode_record_into(&mut buf, r.lsn, &r.payload[..clamp]);
                if r.corrupt && framed > 0 {
                    // Flip the CRC field of the just-written record ⇒ invalid.
                    buf[start] ^= 0xFF;
                }
            }
            buf.extend_from_slice(trailing);
            buf
        }
    }
}

fuzz_target!(|scenario: Scenario| {
    let seg = SEG_SIZES[(scenario.seg_sel as usize) % SEG_SIZES.len()];
    // Valid `max_record_size`: §5.3 requires `max + 91 <= seg`, so the range is
    // `[0, seg - 91]`. `seg >= 256` ⇒ the upper bound is `>= 165`.
    let max_hdr = (seg - 91) as u32;
    let max_record_size = scenario.max_sel % (max_hdr + 1);
    let cfg = WalConfig {
        segment_size: seg,
        max_record_size,
    };

    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(_) => return,
    };

    // Materialize the fuzzer's files.
    for f in &scenario.files {
        let Some(name) = safe_name(&f.name) else {
            continue;
        };
        let bytes = body_bytes(&f.body, max_record_size);
        let _ = fs::write(dir.path().join(name), &bytes);
    }

    // ---- Primary: drive the real public recovery path, instrumented. ----
    open_wal::fuzzing::scan_probe_reset();
    let res = Wal::open(dir.path(), cfg);
    let peak = open_wal::fuzzing::scan_probe_peak();
    assert!(
        peak <= open_wal::fuzzing::scan_bound(cfg.max_record_size),
        "Wal::open forward scan exceeded its bound: peak {peak} > {} (max_record_size {})",
        open_wal::fuzzing::scan_bound(cfg.max_record_size),
        cfg.max_record_size,
    );

    if let Ok((wal, _report)) = res {
        // Exercise the read path too: replay the recovered suffix. A gap / clean
        // end is fine; only a panic / OOB is a bug. The dense surviving suffix is
        // finite, but cap defensively so a hypothetical loop is caught as a
        // timeout, not an OOM.
        if let Ok(mut reader) = wal.reader_from(Lsn(0)) {
            let mut seen = 0u64;
            while let Some(item) = reader.next() {
                if item.is_err() {
                    break;
                }
                seen += 1;
                if seen > 10_000_000 {
                    break;
                }
            }
        }
    }

    // ---- Secondary: direct single-file `recover_segment` probe. ----
    if scenario.probe_direct {
        let Ok(entries) = fs::read_dir(dir.path()) else {
            return;
        };
        let mut wal_files: Vec<(u64, std::path::PathBuf)> = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(digits) = name.strip_suffix(".wal") {
                if digits.len() == 20 {
                    if let Ok(base) = digits.parse::<u64>() {
                        wal_files.push((base, entry.path()));
                    }
                }
            }
        }
        wal_files.sort_unstable();
        let last = wal_files.len().saturating_sub(1);
        for (i, (base, path)) in wal_files.iter().enumerate() {
            if let Ok(file) = fs::File::open(path) {
                // Asserts the bounded-scan probe internally; both Ok and a clean
                // Err are acceptable (D11).
                let _ = open_wal::fuzzing::recover_segment_probe(
                    &file,
                    *base,
                    i == last,
                    cfg.segment_size,
                    cfg.max_record_size,
                );
            }
        }
    }
});