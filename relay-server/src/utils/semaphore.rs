use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// RAII guard for reserved resources in a [`Semaphore`].
///
/// Returned by [`Semaphore::try_acquire`].
#[must_use]
#[derive(Debug)]
pub struct SemaphorePermit {
    handle: Arc<AtomicUsize>,
}

impl Drop for SemaphorePermit {
    fn drop(&mut self) {
        self.handle.fetch_add(1, Ordering::Relaxed);
    }
}

/// A thread-safe, counting semaphore.
///
/// Semaphores control concurrent access to a protected resource. They only grant access up to a
/// certain number of holders at the same time.
///
/// This semaphore is sync and can be shared across threads using [`Arc`].
#[derive(Debug)]
pub struct Semaphore {
    capacity: Arc<AtomicUsize>,
}

impl Semaphore {
    /// Creates a new `Semaphore` with the given capacity.
    ///
    /// The capacity denotes how many times a permit can be issued before a permit has to be
    /// reclaimed.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: Arc::new(AtomicUsize::new(capacity)),
        }
    }

    /// Returns the number of available resources at the current time.
    ///
    /// Note that this number may change any time during or after the call if the semaphore is
    /// shared across threads.
    pub fn available(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Acquires a resource of this semaphore, returning a RAII guard.
    ///
    /// Returns `Some` if the semaphore has available resources. Once the permit is dropped, the
    /// resource is reclaimed and can be reused on another call to `try_acquire`.
    ///
    /// Returns `None` if there are no resources available.
    pub fn try_acquire(&self) -> Option<SemaphorePermit> {
        let result = self
            .capacity
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                val.checked_sub(1)
            });

        match result {
            Ok(_) => Some(SemaphorePermit {
                handle: self.capacity.clone(),
            }),
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let semaphore = Semaphore::new(0);
        assert!(semaphore.try_acquire().is_none());
        assert_eq!(semaphore.available(), 0);
    }

    #[test]
    fn test_single_thread() {
        let semaphore = Semaphore::new(2);
        let permit1 = semaphore.try_acquire().unwrap();
        let permit2 = semaphore.try_acquire().unwrap();
        assert!(semaphore.try_acquire().is_none());
        assert_eq!(semaphore.available(), 0);

        drop(permit1);
        assert_eq!(semaphore.available(), 1);
        let permit3 = semaphore.try_acquire().unwrap();
        assert_eq!(semaphore.available(), 0);

        drop(permit2);
        drop(permit3);
        assert_eq!(semaphore.available(), 2);
    }

    #[test]
    fn test_multi_thread() {
        let semaphore1 = Arc::new(Semaphore::new(2));
        let semaphore2 = Arc::clone(&semaphore1);

        let thread1 = std::thread::spawn(move || {
            for _ in 0..1000 {
                let _guard = semaphore1.try_acquire().unwrap();
            }
        });

        let thread2 = std::thread::spawn(move || {
            for _ in 0..1000 {
                let _guard = semaphore2.try_acquire().unwrap();
            }
        });

        thread1.join().unwrap();
        thread2.join().unwrap();
    }
}
