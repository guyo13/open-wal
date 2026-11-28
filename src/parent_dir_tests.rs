//! §8.4.1 unit tests: which directory holds a path's own entry (the one whose
//! fsync makes a newly created WAL directory durable). The syscall-level guard is
//! `scripts/m8/parent-dirfsync-presence.sh`; the functional and tolerated-failure
//! tests are in `tests/parent_dir_fsync.rs`.

use std::path::{Path, PathBuf};

use crate::wal::parent_dir_of;

fn parent(p: &str) -> Option<PathBuf> {
    parent_dir_of(Path::new(p))
}

#[test]
fn parent_of_a_bare_relative_name_is_dot() {
    assert_eq!(parent("wal"), Some(PathBuf::from(".")));
    assert_eq!(parent("wal/"), Some(PathBuf::from(".")));
}

#[test]
fn parent_of_nested_paths_is_the_lexical_parent() {
    assert_eq!(parent("a/b/c"), Some(PathBuf::from("a/b")));
    assert_eq!(parent("/a/b/c"), Some(PathBuf::from("/a/b")));
    assert_eq!(parent("/a"), Some(PathBuf::from("/")));
    assert_eq!(parent("./a"), Some(PathBuf::from(".")));
}

#[test]
fn the_filesystem_root_has_no_parent() {
    assert_eq!(parent("/"), None);
}

#[test]
fn a_path_with_no_final_name_is_held_by_its_dot_dot() {
    // `Path::parent` would answer "" / "a" here, which is the directory itself
    // (or a child of it), not the directory holding its entry.
    assert_eq!(parent("."), Some(PathBuf::from("./..")));
    assert_eq!(parent(".."), Some(PathBuf::from("../..")));
    assert_eq!(parent("a/.."), Some(PathBuf::from("a/../..")));
    assert_eq!(parent(""), Some(PathBuf::from("..")));
}
