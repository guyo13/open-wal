//! §12 fsync-failure poison gate — M8 / §14.8 H3 (state-machine half).
//!
//! These exercise the §12 *poison state machine* on real storage by making the
//! WAL's commit data sync (libc `fdatasync`) return EIO via an `LD_PRELOAD` shim
//! (`tests/fault/eio_preload.c`). They assert that a failed `fdatasync`:
//!   - surfaces `FsyncFailed` (or `Io` for the roll's segment-create sync);
//!   - does **not** advance `durable_lsn` past the last successfully-synced
//!     segment (the split-batch partial-advance — D-/§12);
//!   - **poisons** the handle, so every subsequent `append`/`commit` is `Poisoned`.
//!
//! SCOPE — this is an APPLICATION-LOGIC test, NOT a durability test, and NOT a
//! substitute for the §14.8 H3 dm-flakey / power-pull gold path. The shim returns
//! a *fake* EIO with the data still in cache; it proves "we poison on EIO", not
//! "we correctly treat the data as already-gone" (the fsyncgate property only real
//! hardware / dm-flakey validates). It also only intercepts the libc `fdatasync`
//! symbol — the WAL's directory fsync uses rustix raw syscalls and is not
//! interceptable here (so the dir-fsync poison path and §14.4d stay dm-flakey-only,
//! OPEN-pending-owner-hardware). See `docs/m8-runbook.md`.
//!
//! `#[ignore]` by default — they require the shim preloaded and two env vars, set
//! by the harness. Run them with:
//! ```text
//! scripts/m8/fsync-fault.sh        # builds the shim + runs this suite
//! ```
//! Running them WITHOUT the shim is not a vacuous pass: the anti-vacuous guard
//! (`assert_injection_fired`) fails loudly because no EIO was injected.
//!
//! Env (set by the harness):
//!   - `WAL_FAULT_ARM`   — arm-file path; write `K` to let the next `K` fdatasync
//!     calls pass then fail the `(K+1)`th with EIO (one-shot).
//!   - `WAL_FAULT_COUNT` — counter-file path the shim bumps on each injected EIO.

use std::path::PathBuf;

use open_wal::{Lsn, Wal, WalConfig, WalError};

fn arm_path() -> PathBuf {
    PathBuf::from(env("WAL_FAULT_ARM"))
}

fn count_path() -> PathBuf {
    PathBuf::from(env("WAL_FAULT_COUNT"))
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} must be set; run this gate via scripts/m8/fsync-fault.sh (not bare `cargo test`)")
    })
}

/// Clear any stale arm and zero the injection counter. Called at the start of
/// each test; with `--test-threads=1` the shared arm/count files are race-free.
fn reset() {
    let _ = std::fs::remove_file(arm_path());
    std::fs::write(count_path(), b"0\n").unwrap();
}

/// Arm the shim to let `k` fdatasync calls pass, then fail the next with EIO.
fn arm(k: u64) {
    std::fs::write(arm_path(), format!("{k}\n")).unwrap();
}

/// Number of EIOs the shim has injected since the last `reset`.
fn injection_count() -> u64 {
    std::fs::read_to_string(count_path())
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

/// The anti-vacuous guard: prove the shim actually fired. A zero count means
/// interception silently failed (e.g. the gate was run without `LD_PRELOAD`) and
/// the test verified NOTHING — that must be a loud failure, never a pass.
fn assert_injection_fired(expected_at_least: u64) {
    let got = injection_count();
    assert!(
        got >= expected_at_least,
        "ANTI-VACUOUS GUARD: the EIO shim injected {got} faults (expected >= {expected_at_least}). \
         The fsync-failure was never actually injected, so this test verified nothing. \
         Run via scripts/m8/fsync-fault.sh so LD_PRELOAD={{eio_preload.so}} is set."
    );
}

fn tmp_wal_dir(tag: &str) -> PathBuf {
    // H3 is a logic test of the §12 state machine, so the WAL dir does not need
    // durable storage — std temp (ext4 here) is fine. Unique per test/pid.
    let dir = std::env::temp_dir().join(format!("wal-m8-h3-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Single-segment commit whose data `fdatasync` fails ⇒ `FsyncFailed`, no
/// durable advance, handle poisoned (§12).
#[test]
#[ignore = "requires the eio_preload shim via scripts/m8/fsync-fault.sh"]
fn single_segment_fsync_failure_poisons() {
    reset();
    let dir = tmp_wal_dir("single");
    let cfg = WalConfig {
        segment_size: 4096,
        max_record_size: 256,
    };
    let (mut wal, _) = Wal::open(&dir, cfg).unwrap();
    wal.append(b"only-record").unwrap();

    // Arm AFTER open (so the cold-start segment-create sync already passed), to
    // fail the very next fdatasync — the commit's record data sync.
    arm(0);
    let r = wal.commit();

    assert!(
        matches!(r, Err(WalError::FsyncFailed)),
        "expected FsyncFailed, got {r:?}"
    );
    assert_eq!(wal.durable_lsn(), Lsn(0), "durable_lsn must not advance");
    assert!(matches!(wal.append(b"x"), Err(WalError::Poisoned)));
    assert!(matches!(wal.commit(), Err(WalError::Poisoned)));
    assert_injection_fired(1);
}

/// Split-batch: a single commit spans two segments; the **second segment's record
/// `fdatasync` fails** while the first already synced. Asserts the §14.8 H3
/// wording: `durable_lsn` rests at the **first segment's max** (partial advance —
/// not 0, not the full batch), then the handle is poisoned.
///
/// Config: `segment_size = 112` ⇒ 48 usable bytes after the 64-byte header = exactly
/// one 32-byte framed record (20 header + 9 payload + 3 pad). Two records ⇒ the
/// commit writes record 1 to segment 1, rolls, writes record 2 to segment 2. The
/// fdatasync sequence is: (1) seg1 record sync, (2) seg2 create sync, (3) seg2
/// record sync. Arming `K = 2` passes (1) and (2) and fails (3).
#[test]
#[ignore = "requires the eio_preload shim via scripts/m8/fsync-fault.sh"]
fn split_batch_second_segment_fsync_failure_rests_at_seg1_max() {
    reset();
    let dir = tmp_wal_dir("split");
    let cfg = WalConfig {
        segment_size: 112,
        max_record_size: 21,
    };
    let (mut wal, _) = Wal::open(&dir, cfg).unwrap();
    assert_eq!(wal.append(b"rec-00001").unwrap(), Lsn(1)); // 9-byte payload ⇒ 32 framed
    assert_eq!(wal.append(b"rec-00002").unwrap(), Lsn(2));

    arm(2); // pass seg1-record-sync + seg2-create-sync, fail seg2-record-sync
    let r = wal.commit();

    assert!(
        matches!(r, Err(WalError::FsyncFailed)),
        "expected FsyncFailed at seg2 record sync, got {r:?}"
    );
    assert_eq!(
        wal.durable_lsn(),
        Lsn(1),
        "durable_lsn must rest at seg1's max (partial advance), not 0 and not 2"
    );
    assert!(matches!(wal.append(b"x"), Err(WalError::Poisoned)));
    assert!(matches!(wal.commit(), Err(WalError::Poisoned)));
    assert_injection_fired(1);
}

/// Split-batch variant: the **roll's segment-create `fdatasync` fails** (the sync
/// that makes the new segment's header + pre-allocated zeros durable). The first
/// segment is already durable, so `durable_lsn` again rests at seg1's max; the
/// create error surfaces as `Io` and the handle is poisoned (§12). Same config as
/// above; arming `K = 1` passes (1) seg1 record sync and fails (2) seg2 create sync.
#[test]
#[ignore = "requires the eio_preload shim via scripts/m8/fsync-fault.sh"]
fn split_batch_roll_create_fsync_failure_rests_at_seg1_max() {
    reset();
    let dir = tmp_wal_dir("roll");
    let cfg = WalConfig {
        segment_size: 112,
        max_record_size: 21,
    };
    let (mut wal, _) = Wal::open(&dir, cfg).unwrap();
    wal.append(b"rec-00001").unwrap();
    wal.append(b"rec-00002").unwrap();

    arm(1); // pass seg1-record-sync, fail seg2-create-sync (during roll)
    let r = wal.commit();

    assert!(
        matches!(r, Err(WalError::Io(_))),
        "expected Io from the failed segment-create sync, got {r:?}"
    );
    assert_eq!(
        wal.durable_lsn(),
        Lsn(1),
        "durable_lsn must rest at seg1's max even when the roll's create sync fails"
    );
    assert!(matches!(wal.append(b"x"), Err(WalError::Poisoned)));
    assert!(matches!(wal.commit(), Err(WalError::Poisoned)));
    assert_injection_fired(1);
}
