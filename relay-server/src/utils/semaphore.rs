use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// TODO(ja): Doc
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

/// TODO(ja): Doc
#[derive(Debug)]
pub struct Semaphore {
    capacity: Arc<AtomicUsize>,
}

impl Semaphore {
    /// TODO(ja): Doc
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: Arc::new(AtomicUsize::new(capacity)),
        }
    }

    /// TODO(ja): Doc
    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    /// TODO(ja): Doc RAII guard
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
