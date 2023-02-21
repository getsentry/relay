use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

pub mod mem;

// TODO: docs on all public types and functions.

/// Combine multiple logical queues into a single structure so we can efficiently handle 10.000s of queues.
pub trait MultiQueue<K, T> {
    fn push_back(&mut self, queue_id: &K, item: T);
    fn pop_front(&mut self, queue_id: &K) -> Option<T>;
    fn len(&self, queue_id: &K) -> usize;
}

/// Behaves like a queue, but defers actual work to a MultiQueue backend.
#[derive(Debug)]
pub struct QueueView<K, T, Q: MultiQueue<K, T>> {
    backend: Arc<Mutex<Q>>,
    queue_id: K,
    _t: PhantomData<T>,
}

impl<K: Copy, T, Q: MultiQueue<K, T>> QueueView<K, T, Q> {
    pub fn new(backend: Arc<Mutex<Q>>, queue_id: K) -> Self {
        Self {
            backend,
            queue_id,
            _t: PhantomData::<T>,
        }
    }
    pub fn push_back(&mut self, item: T) {
        let queue_id = self.queue_id;
        self.backend_mut().push_back(&queue_id, item);
    }
    pub fn pop_front(&mut self) -> Option<T> {
        let queue_id = self.queue_id;
        self.backend_mut().pop_front(&queue_id)
    }
    pub fn len(&self) -> usize {
        let queue_id = self.queue_id;
        self.backend().len(&queue_id)
    }

    fn backend(&self) -> MutexGuard<'_, Q> {
        (*self.backend).lock().expect("Lock was poisoned")
    }

    fn backend_mut(&mut self) -> MutexGuard<'_, Q> {
        (*self.backend).lock().expect("Lock was poisoned")
    }
}
