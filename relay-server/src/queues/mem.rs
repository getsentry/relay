use std::collections::{BTreeMap, VecDeque};

use crate::queues::MultiQueue;

pub struct MemQueue<K, T>(BTreeMap<K, VecDeque<T>>);

impl<K, T> MultiQueue<K, T> for MemQueue<K, T> {
    fn push_back(&mut self, queue_id: K, item: T) {
        let x = &mut self.0;
        x.entry(queue_id).or_default().push_back(item);
    }

    fn pop_front(&mut self, queue_id: K) -> Option<T> {
        todo!()
    }

    fn len(&self, queue_id: K) -> usize {
        todo!()
    }
}
