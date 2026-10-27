//! `Wal` is `!Sync` (single-writer enforcement, §6.2): requiring `Sync` must fail
//! to compile. The handle holds a `PhantomData<Cell<()>>` marker so it can never
//! be shared (`&Wal`) across threads, making concurrent writers a compile error.

use open_wal::{NullObserver, Wal};

fn assert_sync<T: Sync>() {}

fn main() {
    // Wal<NullObserver> is the default handle type. It is Send but NOT Sync.
    assert_sync::<Wal<NullObserver>>();
}
