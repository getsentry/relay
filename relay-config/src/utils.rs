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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_rcu_replaces_value() {
        let swap = ArcSwap::from_pointee(1);
        let old = swap.load_full();

        try_rcu(&swap, |value| Ok::<_, ()>(Arc::new(**value + 1))).unwrap();

        let new = swap.load_full();
        assert_eq!(*new, 2);
        assert!(!Arc::ptr_eq(&old, &new));
    }

    #[test]
    fn test_try_rcu_keeps_unchanged_value() {
        let swap = ArcSwap::from_pointee(1);
        let old = swap.load_full();

        try_rcu(&swap, |value| Ok::<_, ()>(Arc::clone(value))).unwrap();

        let new = swap.load_full();
        assert_eq!(*new, 1);
        assert!(Arc::ptr_eq(&old, &new));
    }
}
