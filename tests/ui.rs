//! Compile-fail proof that the single-writer `Wal` handle is **`!Sync`** (§6.2 /
//! §14.6): sharing it across threads must not compile, so concurrent writers
//! cannot exist. `Wal` IS `Send` (it can be moved to another thread) — that
//! positive direction is asserted in `src/wal.rs` (`handle_is_send`); here we
//! pin the negative.
//!
//! trybuild compiles each `tests/ui/*.rs` and diffs the compiler output against
//! the committed `tests/ui/*.stderr`. That diagnostic is **toolchain-sensitive**;
//! regenerate after a rustc bump that changes the wording with:
//!
//! ```text
//! TRYBUILD=overwrite cargo test --test ui
//! ```
//!
//! This test runs only in the per-PR `test` job (stable) — the MSRV job is
//! `cargo check`, which builds this runner but does not execute it, so the
//! `.stderr` is compared against exactly one toolchain.

#[test]
fn ui() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
