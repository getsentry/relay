use std::cell::Cell;
use std::rc::Rc;

use hashbrown::HashMap;
use relay_event_schema::protocol::{Addr, DebugId, NativeImagePath};

use crate::ProfileError;
use crate::debug_image::{DebugImage, ImageType};
use crate::perfetto::proto;
use crate::sample::Frame;
use crate::sample::v2::{FrameId, Sample, StackId};

/// Use as an error when the configured [`MemoryBudget`] limit is exceeded.
#[derive(Debug, Clone, Copy)]
pub struct BudgetExceeded;

impl From<BudgetExceeded> for ProfileError {
    fn from(_: BudgetExceeded) -> Self {
        Self::ExceedSizeLimit
    }
}

/// A convenient tracker for tracking memory limits.
///
/// The tracker can be cloned, all of the cloned instances will share the same limit.
#[derive(Debug, Clone)]
pub struct MemoryBudget {
    remaining: Rc<Cell<usize>>,
}

impl MemoryBudget {
    /// Creates a new tracker, allowing up to `limit` bytes to be used.
    pub fn new(limit: usize) -> Self {
        Self {
            remaining: Rc::new(Cell::new(limit)),
        }
    }

    /// Derives a new [`LocalBudget`].
    ///
    /// The local budget will contribute to the budget of this tracker.
    pub fn local(&self) -> LocalBudget {
        LocalBudget {
            budget: self.clone(),
            local_usage: 0,
        }
    }

    /// Creates a budgeted vector.
    pub fn vec<T>(&self) -> BudgetVec<T> {
        BudgetVec {
            budget: self.local(),
            inner: Default::default(),
        }
    }

    /// Creates a budgeted map.
    pub fn map<K, V>(&self) -> BudgetMap<K, V> {
        BudgetMap {
            budget: self.local(),
            inner: Default::default(),
        }
    }

    /// Attempts to add `size` to the current usage, if the usage is exceeded returns [`BudgetExceeded`].
    pub fn try_add(&self, size: usize) -> Result<(), BudgetExceeded> {
        let current = self.remaining.get();
        let new = current.checked_sub(size).ok_or(BudgetExceeded)?;
        self.remaining.set(new);
        Ok(())
    }

    /// Removes `size` from the current usage.
    pub fn remove(&self, size: usize) {
        self.remaining.set(self.remaining.get() + size);
    }
}

/// A [`LocalBudget`] is like a [`MemoryBudget`], but it also keeps track of its local usage
/// and refunds it on drop.
#[derive(Debug)]
pub struct LocalBudget {
    budget: MemoryBudget,
    local_usage: usize,
}

impl LocalBudget {
    /// Attempts to add `size` to the current usage, if the usage is exceeded returns [`BudgetExceeded`].
    pub fn try_add(&mut self, size: usize) -> Result<(), BudgetExceeded> {
        self.budget.try_add(size)?;
        self.local_usage += size;
        Ok(())
    }

    /// Removes `size` from the current usage.
    pub fn remove(&mut self, size: usize) {
        self.budget.remove(size);
        self.local_usage = self.local_usage.saturating_sub(size);
    }
}

impl Drop for LocalBudget {
    fn drop(&mut self) {
        self.budget.remove(self.local_usage);
    }
}

/// A minimal [`Vec`] which keeps track of its internal memory usage.
#[derive(Debug)]
pub struct BudgetVec<T> {
    budget: LocalBudget,
    inner: Vec<T>,
}

impl<T> BudgetVec<T> {
    /// Returns the inner [`Vec`].
    pub fn into_inner(self) -> Vec<T> {
        self.inner
    }
}

impl<T> BudgetVec<T>
where
    T: BudgetSize,
{
    /// Appends an `item` to the vector.
    ///
    /// Returns [`BudgetExceeded`] if the memory budget is exhausted.
    pub fn push(&mut self, item: T) -> Result<(), BudgetExceeded> {
        self.budget.try_add(item.size())?;
        self.inner.push(item);
        Ok(())
    }
}

impl<T> std::ops::Deref for BudgetVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct BudgetMap<K, V> {
    budget: LocalBudget,
    inner: HashMap<K, V>,
}

impl<K, V> BudgetMap<K, V>
where
    K: Eq + std::hash::Hash + BudgetSize,
    V: BudgetSize,
{
    /// Inserts an entry into the map.
    ///
    /// Returns [`BudgetExceeded`] if the memory budget is exhausted.
    pub fn insert(&mut self, k: K, v: V) -> Result<Option<V>, BudgetExceeded> {
        let key_size = k.size();
        self.budget.try_add(key_size + v.size())?;
        Ok(match self.inner.insert(k, v) {
            Some(prev) => {
                // Key was already stored, remove one count to not double count it.
                self.budget.remove(key_size + prev.size());
                Some(prev)
            }
            None => None,
        })
    }

    /// Inserts an iterator of entries into the map.
    ///
    /// Returns [`BudgetExceeded`] if the memory budget is exhausted.
    pub fn extend<T: IntoIterator<Item = (K, V)>>(
        &mut self,
        iter: T,
    ) -> Result<(), BudgetExceeded> {
        for (k, v) in iter {
            self.insert(k, v)?;
        }

        Ok(())
    }
}

impl<K, V> std::ops::Deref for BudgetMap<K, V> {
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Helper trait which returns an estimated memory usage of an item.
pub trait BudgetSize {
    fn size(&self) -> usize;
}

// Various trait impls, hidden behind a module for convenience.
mod impls {
    use super::*;

    macro_rules! impl_budget_size_copy {
        ($T:ty) => {
            impl BudgetSize for $T
            where
                $T: Copy,
            {
                fn size(&self) -> usize {
                    std::mem::size_of::<$T>()
                }
            }
        };
    }

    impl_budget_size_copy!(bool);
    impl_budget_size_copy!(u8);
    impl_budget_size_copy!(u32);
    impl_budget_size_copy!(u64);
    impl_budget_size_copy!(proto::Frame);
    impl_budget_size_copy!(Addr);
    impl_budget_size_copy!(uuid::Uuid);
    impl_budget_size_copy!(DebugId);
    impl_budget_size_copy!(ImageType);
    impl_budget_size_copy!(StackId);
    impl_budget_size_copy!(FrameId);

    macro_rules! impl_budget_size_vec_copy {
        ($T:ty) => {
            impl BudgetSize for Vec<$T>
            where
                $T: Copy,
            {
                fn size(&self) -> usize {
                    std::mem::size_of::<Vec<$T>>() + (self.len() * std::mem::size_of::<$T>())
                }
            }
        };
    }

    impl_budget_size_vec_copy!(u8);
    impl_budget_size_vec_copy!(u64);
    impl_budget_size_vec_copy!(FrameId);

    impl BudgetSize for String {
        fn size(&self) -> usize {
            std::mem::size_of::<String>() + self.len()
        }
    }

    impl<T> BudgetSize for Option<T>
    where
        T: BudgetSize,
    {
        fn size(&self) -> usize {
            let total = std::mem::size_of::<Self>();
            let inner = std::mem::size_of::<T>();
            match self {
                Some(v) => v.size() + total - inner,
                None => total,
            }
        }
    }

    impl BudgetSize for Sample {
        fn size(&self) -> usize {
            let Sample {
                timestamp,
                stack_id,
                thread_id,
            } = self;

            std::mem::size_of_val(timestamp) + stack_id.size() + thread_id.size()
        }
    }

    impl BudgetSize for NativeImagePath {
        fn size(&self) -> usize {
            self.0.size()
        }
    }

    impl BudgetSize for DebugImage {
        fn size(&self) -> usize {
            let DebugImage {
                code_file,
                debug_id,
                image_type,
                image_addr,
                image_vmaddr,
                image_size,
                uuid,
            } = self;

            code_file.size()
                + debug_id.size()
                + image_type.size()
                + image_addr.size()
                + image_vmaddr.size()
                + image_size.size()
                + uuid.size()
        }
    }

    impl BudgetSize for Frame {
        fn size(&self) -> usize {
            let Frame {
                abs_path,
                colno,
                filename,
                function,
                in_app,
                instruction_addr,
                lineno,
                module,
                package,
                platform,
            } = self;

            abs_path.size()
                + colno.size()
                + filename.size()
                + function.size()
                + in_app.size()
                + instruction_addr.size()
                + lineno.size()
                + module.size()
                + package.size()
                + platform.size()
        }
    }

    impl BudgetSize for proto::Callstack {
        fn size(&self) -> usize {
            let ty = std::mem::size_of::<Self>();
            let array = self.frame_ids.len() * std::mem::size_of::<u64>();
            ty + array
        }
    }

    impl BudgetSize for proto::Mapping {
        fn size(&self) -> usize {
            let ty = std::mem::size_of::<Self>();
            let array = self.path_string_ids.len() * std::mem::size_of::<u64>();
            ty + array
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    #[test]
    fn test_memory_budget() {
        let b = MemoryBudget::new(100);
        assert_matches!(b.try_add(99), Ok(_));
        assert_matches!(b.try_add(1), Ok(_));
        assert_matches!(b.try_add(1), Err(_));
        b.remove(1);
        assert_matches!(b.try_add(1), Ok(_));
        assert_matches!(b.try_add(1), Err(_));
        b.remove(200);
        assert_matches!(b.try_add(200), Ok(_));
        assert_matches!(b.try_add(1), Err(_));
    }

    #[test]
    fn test_memory_vec() {
        let b = MemoryBudget::new(3);
        let mut v = b.vec();
        assert_matches!(v.push(1u8), Ok(_));
        assert_matches!(v.push(1u8), Ok(_));
        assert_matches!(v.push(1u8), Ok(_));

        assert_matches!(v.push(1u8), Err(_));
        assert_matches!(b.try_add(1), Err(_));

        drop(v);

        assert_matches!(b.try_add(3), Ok(_));
        assert_matches!(b.try_add(1), Err(_));
    }

    #[test]
    fn test_memory_map() {
        let b = MemoryBudget::new(5);
        let mut m = b.map();
        assert_matches!(m.insert(1u8, 1u8), Ok(_));
        // Doesn't count, as the entry already existed.
        assert_matches!(m.insert(1u8, 1u8), Ok(_));
        assert_matches!(m.insert(2u8, 2u8), Ok(_));

        // While technically this would fit (as the entry already exists),
        // the implementation only knows that after insertion and the check
        // happens before.
        assert_matches!(m.insert(1u8, 1u8), Err(_));
        assert_matches!(m.insert(3u8, 3u8), Err(_));

        assert_matches!(b.try_add(1), Ok(_));
        assert_matches!(b.try_add(1), Err(_));

        drop(m);

        assert_matches!(b.try_add(4), Ok(_));
        assert_matches!(b.try_add(1), Err(_));
    }
}
