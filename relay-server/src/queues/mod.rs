pub mod mem;

/// Combine multiple logical queues into a single structure so we can efficiently handle 10.000s of queues.
pub trait MultiQueue<K, T> {
    fn push_back(&mut self, queue_id: &K, item: T);
    fn pop_front(&mut self, queue_id: &K) -> Option<T>;
    fn len(&self, queue_id: &K) -> usize;
}

// pub struct Queue<Q: MultiQueue<K, T>> {
//     queue_id: K,
//     backend: Q,
// }

// impl<K, T, MQ: MultiQueue<K, T>> Queue<K, T, MQ> {
//     fn push_back(&mut self, item: T) {
//         self.backend.push_back(self.queue_id, item);
//     }
//     fn pop_front(&mut self) -> Option<T> {
//         self.backend.pop_front(self.queue_id)
//     }
//     fn len(&self) -> usize {
//         self.backend.len(self.queue_id)
//     }
// }
