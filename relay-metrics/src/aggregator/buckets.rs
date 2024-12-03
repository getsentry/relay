use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use fnv::FnvHasher;
use priority_queue::PriorityQueue;
use relay_base_schema::project::ProjectKey;
use relay_common::time::UnixTimestamp;
use tokio::time::Instant;

use crate::aggregator::{cost, AggregateMetricsError};
use crate::bucket::BucketValue;
use crate::{BucketMetadata, MetricName, MetricNamespace};

/// Holds a collection of buckets and maintains flush order.
#[derive(Debug, Default)]
pub struct Buckets {
    queue: PriorityQueue<BucketKey, QueuedBucket>,
}

impl Buckets {
    /// Returns the current amount of buckets stored.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Merge a new bucket into the existing collection of buckets.
    pub fn merge<T>(
        &mut self,
        key: BucketKey,
        value: T,
        merge: impl FnOnce(&BucketKey, T, &mut QueuedBucket) -> Result<usize, AggregateMetricsError>,
        create: impl FnOnce(&BucketKey, T) -> QueuedBucket,
    ) -> Result<usize, AggregateMetricsError> {
        let mut added_cost = 0;

        // Need to do a bunch of shenanigans to work around the limitations of the priority queue
        // API. There is no `entry` like API which would allow us to return errors on update
        // as well as make the compiler recognize control flow to recognize that `bucket`
        // is only used once.
        //
        // The following code never panics and prefers `expect` over a silent `if let Some(..)`
        // to prevent errors in future refactors.
        let mut value = Some(value);
        let mut error = None;

        let updated = self.queue.change_priority_by(&key, |existing| {
            let value = value.take().expect("expected bucket to not be used");

            match merge(&key, value, existing) {
                Ok(cost) => added_cost = cost,
                Err(err) => error = Some(err),
            }
        });

        if let Some(error) = error {
            return Err(error);
        }

        if !updated {
            let value = create(
                &key,
                value.expect("expected bucket not to be used when the value was not updated"),
            );

            added_cost = key.cost() + value.value.cost();
            self.queue.push(key, value);
        }

        Ok(added_cost)
    }

    /// Tries to pop the next element bucket to be flushed.
    ///
    /// Returns `None` if there are no more metric buckets or
    /// if the passed filter `f` returns `False`.
    pub fn try_pop(
        &mut self,
        f: impl FnOnce(&BucketKey, &QueuedBucket) -> bool,
    ) -> Option<(BucketKey, QueuedBucket)> {
        let (key, value) = self.queue.peek()?;
        if !f(key, value) {
            return None;
        }
        self.queue.pop()
    }
}

impl IntoIterator for Buckets {
    type Item = (BucketKey, QueuedBucket);
    type IntoIter = priority_queue::core_iterators::IntoIter<BucketKey, QueuedBucket>;

    fn into_iter(self) -> Self::IntoIter {
        self.queue.into_iter()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BucketKey {
    pub project_key: ProjectKey,
    pub timestamp: UnixTimestamp,
    pub metric_name: MetricName,
    pub tags: BTreeMap<String, String>,
    pub extracted_from_indexed: bool,
}

impl BucketKey {
    /// Creates a 64-bit hash of the bucket key using FnvHasher.
    ///
    /// This is used for partition key computation and statsd logging.
    pub fn hash64(&self) -> u64 {
        let mut hasher = FnvHasher::default();
        std::hash::Hash::hash(self, &mut hasher);
        hasher.finish()
    }

    /// Estimates the number of bytes needed to encode the bucket key.
    ///
    /// Note that this does not necessarily match the exact memory footprint of the key,
    /// because data structures have a memory overhead.
    pub fn cost(&self) -> usize {
        std::mem::size_of::<Self>() + self.metric_name.len() + cost::tags_cost(&self.tags)
    }

    /// Returns the namespace of this bucket.
    pub fn namespace(&self) -> MetricNamespace {
        self.metric_name.namespace()
    }

    /// Computes a stable partitioning key for this [`crate::Bucket`].
    ///
    /// The partitioning key is inherently producing collisions, since the output of the hasher is
    /// reduced into an interval of size `partitions`. This means that buckets with totally
    /// different values might end up in the same partition.
    ///
    /// The role of partitioning is to let Relays forward the same metric to the same upstream
    /// instance with the goal of increasing bucketing efficiency.
    pub fn partition_key(&self, partitions: u64) -> u64 {
        let key = (self.project_key, &self.metric_name, &self.tags);

        let mut hasher = fnv::FnvHasher::default();
        key.hash(&mut hasher);

        let partitions = partitions.max(1);
        hasher.finish() % partitions
    }
}

/// Bucket in the [`crate::aggregator::Aggregator`] with a defined flush time.
///
/// This type implements an inverted total ordering. The maximum queued bucket has the lowest flush
/// time, which is suitable for using it in a [`BinaryHeap`].
///
/// [`BinaryHeap`]: std::collections::BinaryHeap
#[derive(Debug)]
pub struct QueuedBucket {
    pub flush_at: Instant,
    pub value: BucketValue,
    pub metadata: BucketMetadata,
}

impl QueuedBucket {
    /// Creates a new `QueuedBucket` with a given flush time.
    pub fn new(flush_at: Instant, value: BucketValue, metadata: BucketMetadata) -> Self {
        Self {
            flush_at,
            value,
            metadata,
        }
    }

    /// Returns `true` if the flush time has elapsed.
    pub fn elapsed(&self, now: Instant) -> bool {
        now > self.flush_at
    }

    /// Merges a bucket into the current queued bucket.
    ///
    /// Returns the value cost increase on success,
    /// otherwise returns an error if the passed bucket value type does not match
    /// the contained type.
    pub fn merge(
        &mut self,
        value: BucketValue,
        metadata: BucketMetadata,
    ) -> Result<usize, AggregateMetricsError> {
        let cost_before = self.value.cost();

        self.value
            .merge(value)
            .map_err(|_| AggregateMetricsError::InvalidTypes)?;
        self.metadata.merge(metadata);

        Ok(self.value.cost().saturating_sub(cost_before))
    }
}

impl PartialEq for QueuedBucket {
    fn eq(&self, other: &Self) -> bool {
        self.flush_at.eq(&other.flush_at)
    }
}

impl Eq for QueuedBucket {}

impl PartialOrd for QueuedBucket {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Comparing order is reversed to convert the max heap into a min heap
        Some(other.flush_at.cmp(&self.flush_at))
    }
}

impl Ord for QueuedBucket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Comparing order is reversed to convert the max heap into a min heap
        other.flush_at.cmp(&self.flush_at)
    }
}
