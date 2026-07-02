# The on-disk format

This chapter is a readable summary — enough to understand what the files are,
reason about recovery, and write an external reader. The normative byte-level
layout is
[§5 of the design spec](https://github.com/guyo13/open-wal/blob/main/docs/wal_design_v6.md),
which wins on any disagreement.

## Directory layout

```text
<wal_dir>/
├── 00000000000000000001.wal    # segment whose first record has LSN 1
├── 00000000000000100001.wal    # base_lsn = 100001
├── 00000000000000200001.wal    # active segment (highest base_lsn)
└── LOCK                        # advisory exclusive-writer lock file
```

- Each segment file is named `{base_lsn:020}.wal` — its first record's LSN,
  zero-padded to 20 digits (the width of `u64::MAX`), so lexicographic and
  numeric order agree.
- **There is no manifest or `CURRENT` file.** The log's state is derived
  entirely by listing the directory, sorting by base LSN, and scanning. A
  manifest would be one more thing that could disagree with reality.
- The **active** segment is the one with the highest base LSN; every other
  segment is **sealed** and immutable until a checkpoint deletes it whole.
- Segments are **pre-allocated** to `segment_size` at creation, so the
  unwritten remainder reads as zeros. During a scan, an all-zero record header
  is the end-of-records sentinel.

## Segments

Every segment starts with a fixed 64-byte header: a magic string
(`WAL\0SEG1`), a format version, the segment's `base_lsn`, a creation
timestamp (informational only — recovery never uses it), and a CRC over the
header. The header is written and synced when the segment is created, before
any record — so a corrupt header is never a torn write, and recovery treats it
as fatal (with one carve-out for a crash mid-creation; see
[Recovery](recovery.md)).

Records follow the header back-to-back, 8-byte aligned.

## Records

Each record is framed as:

| Field | Size | Notes |
|---|---|---|
| `crc` | 4 | CRC-32C over everything after this field, **including padding** |
| `length` | 4 | payload length in bytes |
| `lsn` | 8 | the record's LSN |
| `rec_type` | 1 | `1` = full record; other values reserved |
| flags/reserved | 3 | must be zero |
| payload | `length` | your opaque bytes |
| padding | 0–7 | zeros to the next 8-byte boundary |

Details that carry weight:

- **The checksum is CRC-32C (Castagnoli)** — hardware-accelerated on modern
  CPUs, and the same polynomial ext4, RocksDB, and iSCSI use. The crate
  re-exports the primitive as `open_wal::crc32c` so an external reader can
  match it exactly. (Beware: the popular `crc32fast` crate implements a
  *different* polynomial and will reject every record.)
- **Padding is inside CRC coverage**, so tampered or corrupted padding is
  detected — there is nowhere to hide bytes in a record.
- **A record never spans segments.** This is why
  `max_record_size + 91 <= segment_size` is enforced at `open` (64-byte
  segment header + 20-byte record header + up to 7 bytes padding), and it is
  what makes each segment independently parseable.
- **The end-of-records sentinel is an *all-zero 20-byte header*** — not merely
  `rec_type == 0`. A header with a zero `rec_type` but nonzero CRC is a
  *corrupt record*, not the end of the log; the distinction is what lets
  recovery tell a clean end from a single flipped byte in the middle of
  acknowledged data.

## What a scan looks like

Reading a segment is: skip the 64-byte header, then repeatedly — check for the
sentinel, bounds-check `length`, verify the CRC, verify the LSN is the
expected next one, yield the payload, advance by the padded record size. The
in-crate `Reader`, recovery, and any external tailer all follow this same
logic; [Recovery](recovery.md) adds the tail-classification rules on top.

That's the whole format: sorted filenames, a checksummed header, checksummed
LSN-stamped records, zeros at the end. Boring on purpose.
