use std::sync::Arc;

use arc_swap::ArcSwap;

/// Fallible Read-Copy-Update of a value contained in an [`ArcSwap`].
///
/// See also: [`ArcSwap::rcu`].
pub fn try_rcu<T, E, F>(swap: &ArcSwap<T>, mut f: F) -> Result<(), E>
where
    F: FnMut(&Arc<T>) -> Result<Arc<T>, E>,
{
    let mut cur = swap.load_full();
    loop {
        let new = f(&cur)?;
        let prev = swap.compare_and_swap(&cur, new);
        if Arc::ptr_eq(&cur, &prev) {
            return Ok(());
        }
        // Someone else updated the value before us, retry with the latest version.
        cur = Arc::clone(&prev);
    }
}
