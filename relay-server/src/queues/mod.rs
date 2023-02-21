use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

pub mod mem;

/// TODO: better docs.
/// Combine multiple logical queues into a single structure so we can efficiently handle 10.000s of queues.
pub trait MultiQueue<K, T> {
    fn push_back(&mut self, queue_id: &K, item: T);
    fn pop_front(&mut self, queue_id: &K) -> Option<T>;
    fn len(&self, queue_id: &K) -> usize;
}

/// Behaves like a queue, but defers actual work to a MultiQueue backend.
pub struct Queue<K, T, Q: MultiQueue<K, T>> {
    backend: Rc<RefCell<Q>>,
    queue_id: K,
    _t: PhantomData<T>,
}

impl<K, T, Q: MultiQueue<K, T>> Queue<K, T, Q> {
    pub fn new(backend: Rc<RefCell<Q>>, queue_id: K) -> Self {
        Self {
            backend,
            queue_id,
            _t: PhantomData::<T>,
        }
    }

    pub fn push_back(&mut self, item: T) {
        (*self.backend).borrow_mut().push_back(&self.queue_id, item);
    }
    pub fn pop_front(&mut self) -> Option<T> {
        (*self.backend).borrow_mut().pop_front(&self.queue_id)
    }
    pub fn len(&self) -> usize {
        (*self.backend).borrow().len(&self.queue_id)
    }
}
