//! §8.4.1 — `open` makes a WAL directory it creates (or cold-starts in) durable
//! by fsyncing the parent directory.
//!
//! - **F1 (functional):** a missing nested path, a pre-existing empty directory
//!   and a bare relative name all open, append, commit and reopen.
//! - **F3 (the one tolerated failure):** a *pre-existing* empty directory whose
//!   parent cannot be opened still opens; a directory this call *creates* under
//!   such a parent is an error. Needs a non-root process (root bypasses
//!   directory permissions) — under root it prints a loud skip and proves
//!   nothing.
//!
//! These tests cannot see whether an fsync was issued: that is F2,
//! `scripts/m8/parent-dirfsync-presence.sh` (strace), the primary guard.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

use open_wal::{Lsn, Wal, WalConfig, WalError};

const CHILD_ENV: &str = "OPEN_WAL_PARENT_DIR_CHILD";

fn cfg() -> WalConfig {
    WalConfig {
        segment_size: 64 * 1024,
        max_record_size: 4096,
    }
}

/// Open (cold start), write three records, commit, close; reopen and replay.
fn write_reopen_replay(dir: &Path) {
    {
        let (mut wal, report) = Wal::open(dir, cfg()).expect("first open (cold start)");
        assert_eq!(report.durable_lsn, Lsn(0));
        for i in 1..=3u64 {
            let lsn = wal.append(format!("rec-{i}").as_bytes()).expect("append");
            assert_eq!(lsn, Lsn(i));
        }
        assert_eq!(wal.commit().expect("commit"), Lsn(3));
    }
    let (wal, report) = Wal::open(dir, cfg()).expect("reopen");
    assert_eq!(report.durable_lsn, Lsn(3));
    assert_eq!(report.oldest_lsn, Lsn(1));
    let mut reader = wal.reader_from(Lsn(1)).expect("reader");
    let mut got: Vec<(Lsn, Vec<u8>)> = Vec::new();
    while let Some(record) = reader.next() {
        let (lsn, payload) = record.expect("record");
        got.push((lsn, payload.to_vec()));
    }
    let want: Vec<(Lsn, Vec<u8>)> = (1..=3u64)
        .map(|i| (Lsn(i), format!("rec-{i}").into_bytes()))
        .collect();
    assert_eq!(got, want);
}

#[test]
fn f1_missing_nested_path_opens_commits_and_reopens() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("a").join("b").join("c");
    assert!(!tmp.path().join("a").exists());
    write_reopen_replay(&dir);
    assert!(dir.join("00000000000000000001.wal").is_file());
}

#[test]
fn f1_pre_existing_empty_directory_opens_commits_and_reopens() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wal");
    std::fs::create_dir(&dir).unwrap();
    write_reopen_replay(&dir);
}

#[test]
fn f1_path_that_is_a_file_is_an_error_not_a_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    assert!(matches!(Wal::open(&file, cfg()), Err(WalError::Io(_))));
    assert!(matches!(
        Wal::open(&file.join("below"), cfg()),
        Err(WalError::Io(_))
    ));
}

/// Child half of the bare-relative-name test: a no-op unless re-executed by
/// [`f1_bare_relative_directory_name`] with `CHILD_ENV` set and the current
/// directory pointed at a scratch directory (so the test binary's own cwd is
/// never changed).
#[test]
fn f1_bare_relative_child() {
    let Ok(name) = std::env::var(CHILD_ENV) else {
        return;
    };
    assert!(Path::new(&name).is_relative());
    write_reopen_replay(Path::new(&name));
}

#[test]
fn f1_bare_relative_directory_name() {
    let tmp = tempfile::tempdir().unwrap();
    for name in ["wal", "x/y"] {
        let exe = std::env::current_exe().expect("current test binary");
        let out = Command::new(exe)
            .args(["--exact", "f1_bare_relative_child", "--test-threads=1"])
            .env(CHILD_ENV, name)
            .current_dir(tmp.path())
            .output()
            .expect("spawn child");
        assert!(
            out.status.success(),
            "child failed for relative dir {name:?}:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        // The child ran the real test (not zero tests) and the log landed under
        // the child's cwd, i.e. the relative name was resolved there.
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("1 passed"),
            "child did not run the test for {name:?}"
        );
        assert!(
            tmp.path()
                .join(name)
                .join("00000000000000000001.wal")
                .is_file()
        );
    }
}

/// Restores a directory's mode on drop so the tempdir can be cleaned up even
/// when an assertion fails.
struct RestoreMode<'a>(&'a Path);
impl Drop for RestoreMode<'_> {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o700));
    }
}

/// Make `parent` writable + searchable but **not readable** (`0o300`): entries
/// can be created and reached, but the directory itself cannot be opened, so it
/// cannot be fsynced. Returns `false` (after a loud note) when this process can
/// open it anyway — i.e. it runs as root / with `CAP_DAC_OVERRIDE`.
fn make_unopenable(parent: &Path) -> bool {
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o300)).unwrap();
    if std::fs::File::open(parent).is_ok() {
        eprintln!(
            "\n*** F3 NOT EXERCISED: this process can open a mode-0300 directory \
             (running as root / CAP_DAC_OVERRIDE). The tolerated-failure rule of \
             §8.4.1 is UNVERIFIED by this run — re-run as a non-root user. ***\n"
        );
        return false;
    }
    true
}

#[test]
fn f3_pre_existing_dir_under_an_unopenable_parent_opens() {
    let tmp = tempfile::tempdir().unwrap();
    let parent = tmp.path().join("parent");
    let dir = parent.join("wal");
    std::fs::create_dir_all(&dir).unwrap();
    let _restore = RestoreMode(&parent);
    if !make_unopenable(&parent) {
        return;
    }
    // Pre-provisioned directory, parent cannot be opened ⇒ the parent fsync is
    // skipped and `open` succeeds (the deployment owns that entry's durability).
    write_reopen_replay(&dir);
}

#[test]
fn f3_dir_created_under_an_unopenable_parent_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let parent = tmp.path().join("parent");
    std::fs::create_dir(&parent).unwrap();
    let dir = parent.join("wal");
    let _restore = RestoreMode(&parent);
    if !make_unopenable(&parent) {
        return;
    }
    // `mkdir` succeeds (the parent is writable) but the parent cannot be opened
    // to fsync the new entry. This call created the level, so nothing is
    // tolerated: `open` must fail rather than return a log whose directory a
    // power loss may remove.
    match Wal::open(&dir, cfg()) {
        Err(WalError::Io(e)) => assert_eq!(
            e.kind(),
            std::io::ErrorKind::PermissionDenied,
            "expected PermissionDenied from the parent fsync, got {e:?}"
        ),
        Err(e) => panic!("expected WalError::Io(PermissionDenied), got {e:?}"),
        Ok(_) => panic!(
            "open succeeded for a directory it created whose parent could not be \
             fsynced: PermissionDenied must not be tolerated for a created level"
        ),
    }
    // No segment was created: the failure came before the cold start.
    assert!(
        !dir.join("00000000000000000001.wal").exists(),
        "open went on to cold-start a directory it created without fsyncing its \
         parent: the failure must come from the creation walk, before any segment"
    );
}
