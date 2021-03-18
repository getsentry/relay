use crate::Metric;
use relay_common::UnixTimestamp;
use std::collections::BTreeMap;

pub struct Aggregator<SendFn: Fn(Bucket) -> ()> {
    bucket_size: UnixTimestamp,
    retention: u64, // TODO use duration
    send_fn: SendFn,
    buckets: BTreeMap<UnixTimestamp, Bucket>,
}

impl<SendFn: Fn(Bucket) -> ()> Aggregator<SendFn> {
    pub fn new(bucket_size: UnixTimestamp, retention: u64, send_fn: SendFn) -> Self {
        Self {
            bucket_size,
            retention,
            send_fn,
            buckets: BTreeMap::new(),
        }
    }

    pub fn push(&mut self, timestamp: UnixTimestamp, metric: Metric) {
        self.cleanup();

        let bucket_index = timestamp.as_secs() / self.bucket_size.as_secs();
        let bucket_index = UnixTimestamp::from_secs(bucket_index);
        let bucket = self
            .buckets
            .entry(bucket_index)
            .or_insert(Bucket::default());
        bucket.push(metric);
    }

    fn cleanup(&mut self) {
        let min_timestamp =
            UnixTimestamp::from_secs(UnixTimestamp::now().as_secs() - self.retention);
        let mut to_be_sent = vec![];
        for timestamp in self.buckets.keys() {
            if timestamp >= &min_timestamp {
                break;
            }
            to_be_sent.push(timestamp.clone());
        }
        for timestamp in to_be_sent {
            // TODO: prevent double lookup
            let bucket = self
                .buckets
                .remove(&timestamp)
                .expect("Key should still be there");

            (self.send_fn)(bucket); // Might be delegated to async worker later
        }
    }
}

#[derive(Default)]
pub struct Bucket {}

impl Bucket {
    pub fn push(&mut self, metric: Metric) {
        todo!();
    }
}
