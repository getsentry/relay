use std::collections::{BTreeMap, VecDeque};

use crate::queues::MultiQueue;

pub struct MemQueue<K: Sized + std::cmp::Ord + Copy, T>(BTreeMap<K, VecDeque<T>>);

impl<K: Sized + std::cmp::Ord + Copy, T> MemQueue<K, T> {
    fn new() -> Self {
        MemQueue(BTreeMap::new())
    }
}

impl<K: Sized + std::cmp::Ord + Copy, T> MultiQueue<K, T> for MemQueue<K, T> {
    fn push_back(&mut self, queue_id: &K, item: T) {
        self.0.entry(*queue_id).or_default().push_back(item)
    }

    fn pop_front(&mut self, queue_id: &K) -> Option<T> {
        self.0.get_mut(queue_id).and_then(|q| q.pop_front())
    }

    fn len(&self, queue_id: &K) -> usize {
        match self.0.get(queue_id) {
            Some(q) => q.len(),
            None => 0usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_basics() {
        let mut q = MemQueue::new();
        assert_eq!(q.pop_front(&123), None);
        q.push_back(&1, "a");
        q.push_back(&2, "b");
        assert_eq!(q.pop_front(&1), Some("a"));
        assert_eq!(q.pop_front(&2), Some("b"));
        assert_eq!(q.pop_front(&1), None);
        assert_eq!(q.pop_front(&2), None);
    }

    // fn test_single_queues() {
    //     let mut q = Arc::new(MemQueue::new());

    //     q1 = Queue
    //     assert_eq!(q.pop_front(&123), None);
    //     q.push_back(&1, "a");
    //     q.push_back(&2, "b");
    //     assert_eq!(q.pop_front(&1), Some("a"));
    //     assert_eq!(q.pop_front(&2), Some("b"));
    //     assert_eq!(q.pop_front(&1), None);
    //     assert_eq!(q.pop_front(&2), None);
    // }
}
