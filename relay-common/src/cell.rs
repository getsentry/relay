use std::ops::Deref;

use lazycell::AtomicLazyCell;

/// An `AtomicLazyCell` acting as a kind of cached property.
#[derive(Debug, Clone)]
pub struct UpsertingLazyCell<T>(AtomicLazyCell<T>);

/// A wrapper around the lazycell's value. This can be `Owned` if we enter race conditions when
/// initializing the lazycell state and compute `T` multiple times.
pub enum LazyCellRef<'a, T> {
    /// We borrowed data from the underlying lazycell.
    Borrowed(&'a T),

    /// We entered a race condition when trying to initialize the lazycell and are forced to return
    /// owned data.
    Owned(T),
}

impl<'a, T> Deref for LazyCellRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match *self {
            LazyCellRef::Owned(ref t) => t,
            LazyCellRef::Borrowed(t) => t,
        }
    }
}

impl<'a, T> AsRef<T> for LazyCellRef<'a, T> {
    fn as_ref(&self) -> &T {
        match *self {
            LazyCellRef::Owned(ref t) => t,
            LazyCellRef::Borrowed(t) => t,
        }
    }
}

impl<T> Default for UpsertingLazyCell<T> {
    fn default() -> Self {
        UpsertingLazyCell::new()
    }
}

impl<T> UpsertingLazyCell<T> {
    /// Create a new empty cell.
    pub fn new() -> Self {
        UpsertingLazyCell(AtomicLazyCell::new())
    }

    /// Try to get a cached value or create one by calling `f()`.
    pub fn get_or_insert_with(&self, f: impl FnOnce() -> T) -> LazyCellRef<'_, T> {
        if let Some(value) = self.0.borrow() {
            return LazyCellRef::Borrowed(value);
        }

        let value = f();

        // If filling the lazy cell fails, another thread has already computed `f()`. Since we
        // already went through the effort of computing the value for `f()` in this invocation, we
        // might as well return it.
        if let Err(value) = self.0.fill(value) {
            return LazyCellRef::Owned(value);
        }

        // The lazy cell was filled successfully, so the `borrow()` should never fail.
        match self.0.borrow() {
            Some(value) => LazyCellRef::Borrowed(value),
            None => unreachable!(),
        }
    }
}
