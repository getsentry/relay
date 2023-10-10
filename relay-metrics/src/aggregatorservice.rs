use std::error::Error;
use std::iter::FusedIterator;
use std::time::Duration;

use relay_base_schema::project::ProjectKey;
use relay_system::{
    AsyncResponse, Controller, FromMessage, Interface, NoResponse, Recipient, Sender, Service,
    Shutdown,
};

use crate::aggregator;
use crate::bucket::Bucket;
use crate::statsd::{MetricCounters, MetricHistograms};
use crate::{aggregator::AggregatorServiceConfig, BucketValue, DistributionValue};

/// Interval for the flush cycle of the [`AggregatorService`].
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// The fraction of [`AggregatorServiceConfig::max_flush_bytes`] at which buckets will be split. A value of
/// `2` means that all buckets smaller than half of max_flush_bytes will be moved in their entirety,
/// and buckets larger will be split up.
const BUCKET_SPLIT_FACTOR: usize = 32;

/// The average size of values when serialized.
const AVG_VALUE_SIZE: usize = 8;

/// Aggregator service interface.
#[derive(Debug)]
pub enum Aggregator {
    /// The health check message which makes sure that the service can accept the requests now.
    AcceptsMetrics(AcceptsMetrics, Sender<bool>),
    /// Merge the buckets.
    MergeBuckets(MergeBuckets),

    /// Message is used only for tests to get the current number of buckets in `AggregatorService`.
    #[cfg(test)]
    BucketCountInquiry(BucketCountInquiry, Sender<usize>),
}

impl Interface for Aggregator {}

impl FromMessage<AcceptsMetrics> for Aggregator {
    type Response = AsyncResponse<bool>;
    fn from_message(message: AcceptsMetrics, sender: Sender<bool>) -> Self {
        Self::AcceptsMetrics(message, sender)
    }
}

impl FromMessage<MergeBuckets> for Aggregator {
    type Response = NoResponse;
    fn from_message(message: MergeBuckets, _: ()) -> Self {
        Self::MergeBuckets(message)
    }
}

#[cfg(test)]
impl FromMessage<BucketCountInquiry> for Aggregator {
    type Response = AsyncResponse<usize>;
    fn from_message(message: BucketCountInquiry, sender: Sender<usize>) -> Self {
        Self::BucketCountInquiry(message, sender)
    }
}

/// Check whether the aggregator has not (yet) exceeded its total limits. Used for health checks.
#[derive(Debug)]
pub struct AcceptsMetrics;

/// Used only for testing the `AggregatorService`.
#[cfg(test)]
#[derive(Debug)]
pub struct BucketCountInquiry;

/// A message containing a vector of buckets to be flushed.
///
/// Handlers must respond to this message with a `Result`:
/// - If flushing has succeeded or the buckets should be dropped for any reason, respond with `Ok`.
/// - If flushing fails and should be retried at a later time, respond with `Err` containing the
///   failed buckets. They will be merged back into the aggregator and flushed at a later time.
#[derive(Clone, Debug)]
pub struct FlushBuckets {
    /// The project key.
    pub project_key: ProjectKey,
    /// The logical partition to send this batch to.
    pub partition_key: Option<u64>,
    /// The buckets to be flushed.
    pub buckets: Vec<Bucket>,
}

enum AggregatorState {
    Running,
    ShuttingDown,
}

/// A service for aggregationg metric buckets.
///
/// Buckets are flushed to a receiver after their time window and a grace period have passed.
/// Metrics with a recent timestamp are given a longer grace period than backdated metrics, which
/// are flushed after a shorter debounce delay. See [`AggregatorServiceConfig`] for configuration options.
///
/// Internally, the aggregator maintains a continuous flush cycle every 100ms. It guarantees that
/// all elapsed buckets belonging to the same [`ProjectKey`] are flushed together.
///
/// Receivers must implement a handler for the [`FlushBuckets`] message.
pub struct AggregatorService {
    aggregator: aggregator::Aggregator,
    state: AggregatorState,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    max_flush_bytes: usize,
    max_total_bucket_bytes: Option<usize>,
    flush_partitions: Option<u64>,
}

impl AggregatorService {
    /// Create a new aggregator service and connect it to `receiver`.
    ///
    /// The aggregator will flush a list of buckets to the receiver in regular intervals based on
    /// the given `config`.
    pub fn new(
        config: AggregatorServiceConfig,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    ) -> Self {
        Self::named("default".to_owned(), config, receiver)
    }

    /// Like [`Self::new`], but with a provided name.
    pub(crate) fn named(
        name: String,
        config: AggregatorServiceConfig,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    ) -> Self {
        Self {
            receiver,
            state: AggregatorState::Running,
            max_flush_bytes: config.max_flush_bytes,
            max_total_bucket_bytes: config.max_total_bucket_bytes,
            flush_partitions: config.flush_partitions,
            aggregator: aggregator::Aggregator::named(name, config.aggregator),
        }
    }

    fn handle_accepts_metrics(&self, sender: Sender<bool>) {
        let result = !self
            .aggregator
            .totals_cost_exceeded(self.max_total_bucket_bytes);

        sender.send(result);
    }

    /// Split the provided buckets into batches and process each batch with the given function.
    ///
    /// For each batch, log a histogram metric.
    fn process_batches<F>(&self, buckets: impl IntoIterator<Item = Bucket>, mut process: F)
    where
        F: FnMut(Vec<Bucket>),
    {
        let capped_batches = CappedBucketIter::new(buckets.into_iter(), self.max_flush_bytes);
        let num_batches = capped_batches
            .map(|batch| {
                relay_statsd::metric!(
                    histogram(MetricHistograms::BucketsPerBatch) = batch.len() as f64,
                    aggregator = self.aggregator.name(),
                );
                process(batch);
            })
            .count();

        relay_statsd::metric!(
            histogram(MetricHistograms::BatchesPerPartition) = num_batches as f64,
            aggregator = self.aggregator.name(),
        );
    }

    /// Sends the [`FlushBuckets`] message to the receiver in the fire and forget fashion. It is up
    /// to the receiver to send the [`MergeBuckets`] message back if buckets could not be flushed
    /// and we require another re-try.
    ///
    /// If `force` is true, flush all buckets unconditionally and do not attempt to merge back.
    fn try_flush(&mut self) {
        let flush_buckets = {
            let force_flush = matches!(&self.state, AggregatorState::ShuttingDown);
            self.aggregator.pop_flush_buckets(force_flush)
        };

        if flush_buckets.is_empty() {
            return;
        }

        relay_log::trace!("flushing {} projects to receiver", flush_buckets.len());

        let mut total_bucket_count = 0u64;
        for (project_key, project_buckets) in flush_buckets.into_iter() {
            let bucket_count = project_buckets.len() as u64;
            relay_statsd::metric!(
                histogram(MetricHistograms::BucketsFlushedPerProject) = bucket_count,
                aggregator = self.aggregator.name(),
            );
            total_bucket_count += bucket_count;

            let partitioned_buckets = self
                .aggregator
                .partition_buckets(project_buckets, self.flush_partitions);
            for (partition_key, buckets) in partitioned_buckets {
                self.process_batches(buckets, |batch| {
                    if let Some(ref receiver) = self.receiver {
                        receiver.send(FlushBuckets {
                            project_key,
                            partition_key,
                            buckets: batch,
                        });
                    }
                });
            }
        }
        relay_statsd::metric!(
            histogram(MetricHistograms::BucketsFlushed) = total_bucket_count,
            aggregator = self.aggregator.name(),
        );
    }

    fn handle_merge_buckets(&mut self, msg: MergeBuckets) {
        let MergeBuckets {
            project_key,
            buckets,
        } = msg;
        if let Err(err) =
            self.aggregator
                .merge_all(project_key, buckets, self.max_total_bucket_bytes)
        {
            relay_log::error!(
                tags.aggregator = &self.aggregator.name(),
                error = &err as &dyn Error,
                "failed to merge buckets"
            );
        }
    }

    fn handle_message(&mut self, msg: Aggregator) {
        match msg {
            Aggregator::AcceptsMetrics(_, sender) => self.handle_accepts_metrics(sender),
            Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, sender) => {
                sender.send(self.aggregator.bucket_count())
            }
        }
    }

    fn handle_shutdown(&mut self, message: Shutdown) {
        if message.timeout.is_some() {
            self.state = AggregatorState::ShuttingDown;
        }
    }
}

impl Service for AggregatorService {
    type Interface = Aggregator;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
            let mut shutdown = Controller::shutdown_handle();

            // Note that currently this loop never exits and will run till the tokio runtime shuts
            // down. This is about to change with the refactoring for the shutdown process.
            loop {
                tokio::select! {
                    biased;

                    _ = ticker.tick() => self.try_flush(),
                    Some(message) = rx.recv() => self.handle_message(message),
                    shutdown = shutdown.notified() => self.handle_shutdown(shutdown),

                    else => break,
                }
            }
        });
    }
}

impl Drop for AggregatorService {
    fn drop(&mut self) {
        let remaining_buckets = self.aggregator.bucket_count();
        if remaining_buckets > 0 {
            relay_log::error!(
                tags.aggregator = self.aggregator.name(),
                "metrics aggregator dropping {remaining_buckets} buckets"
            );
            relay_statsd::metric!(
                counter(MetricCounters::BucketsDropped) += remaining_buckets as i64,
                aggregator = self.aggregator.name(),
            );
        }
    }
}

/// An iterator returning batches of buckets fitting into a size budget.
///
/// The size budget is given through `max_flush_bytes`, though this is an approximate number. On
/// every iteration, this iterator returns a `Vec<Bucket>` which serializes into a buffer of the
/// specified size. Buckets at the end of each batch may be split to fit into the batch.
///
/// Since this uses an approximate function to estimate the size of buckets, the actual serialized
/// payload may exceed the size. The estimation function is built in a way to guarantee the same
/// order of magnitude.
struct CappedBucketIter<T: Iterator<Item = Bucket>> {
    buckets: T,
    next_bucket: Option<Bucket>,
    max_flush_bytes: usize,
}

impl<T: Iterator<Item = Bucket>> CappedBucketIter<T> {
    /// Creates a new `CappedBucketIter`.
    pub fn new(mut buckets: T, max_flush_bytes: usize) -> Self {
        let next_bucket = buckets.next();

        Self {
            buckets,
            next_bucket,
            max_flush_bytes,
        }
    }
}

impl<T: Iterator<Item = Bucket>> Iterator for CappedBucketIter<T> {
    type Item = Vec<Bucket>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current_batch = Vec::new();
        let mut remaining_bytes = self.max_flush_bytes;

        while let Some(bucket) = self.next_bucket.take() {
            let bucket_size = estimate_size(&bucket);
            if bucket_size <= remaining_bytes {
                // the bucket fits
                remaining_bytes -= bucket_size;
                current_batch.push(bucket);
                self.next_bucket = self.buckets.next();
            } else if bucket_size < self.max_flush_bytes / BUCKET_SPLIT_FACTOR {
                // the bucket is too small to split, move it entirely
                self.next_bucket = Some(bucket);
                break;
            } else {
                // the bucket is big enough to split
                let (left, right) = split_at(bucket, remaining_bytes);
                if let Some(left) = left {
                    current_batch.push(left);
                }

                self.next_bucket = right;
                break;
            }
        }

        if current_batch.is_empty() {
            // There is still leftover data not returned by the iterator after it has ended.
            if self.next_bucket.take().is_some() {
                relay_log::error!("CappedBucketIter swallowed bucket");
            }
            None
        } else {
            Some(current_batch)
        }
    }
}

impl<T: Iterator<Item = Bucket>> FusedIterator for CappedBucketIter<T> {}

/// Splits this bucket if its estimated serialization size exceeds a threshold.
///
/// There are three possible return values:
///  - `(Some, None)` if the bucket fits entirely into the size budget. There is no split.
///  - `(None, Some)` if the size budget cannot even hold the bucket name and tags. There is no
///    split, the entire bucket is moved.
///  - `(Some, Some)` if the bucket fits partially. Remaining values are moved into a new bucket
///    with all other information cloned.
///
/// This is an approximate function. The bucket is not actually serialized, but rather its
/// footprint is estimated through the number of data points contained. See
/// `estimate_size` for more information.
fn split_at(mut bucket: Bucket, size: usize) -> (Option<Bucket>, Option<Bucket>) {
    // If there's enough space for the entire bucket, do not perform a split.
    if size >= estimate_size(&bucket) {
        return (Some(bucket), None);
    }

    // If the bucket key can't even fit into the remaining length, move the entire bucket into
    // the right-hand side.
    let own_size = estimate_base_size(&bucket);
    if size < (own_size + AVG_VALUE_SIZE) {
        // split_at must not be zero
        return (None, Some(bucket));
    }

    // Perform a split with the remaining space after adding the key. We assume an average
    // length of 8 bytes per value and compute the number of items fitting into the left side.
    let split_at = (size - own_size) / AVG_VALUE_SIZE;

    match bucket.value {
        BucketValue::Counter(_) => (None, Some(bucket)),
        BucketValue::Distribution(ref mut distribution) => {
            let mut org = std::mem::take(distribution);

            let mut new_bucket = bucket.clone();
            new_bucket.value =
                BucketValue::Distribution(DistributionValue::from_slice(&org[split_at..]));

            org.truncate(split_at);
            bucket.value = BucketValue::Distribution(org);

            (Some(bucket), Some(new_bucket))
        }
        BucketValue::Set(ref mut set) => {
            let org = std::mem::take(set);
            let mut new_bucket = bucket.clone();

            let mut iter = org.into_iter();
            bucket.value = BucketValue::Set((&mut iter).take(split_at).collect());
            new_bucket.value = BucketValue::Set(iter.collect());

            (Some(bucket), Some(new_bucket))
        }
        BucketValue::Gauge(_) => (None, Some(bucket)),
    }
}

/// Estimates the number of bytes needed to serialize the bucket without value.
///
/// Note that this does not match the exact size of the serialized payload. Instead, the size is
/// approximated through tags and a static overhead.
fn estimate_base_size(bucket: &Bucket) -> usize {
    50 + bucket.name.len() + aggregator::tags_cost(&bucket.tags)
}

/// Estimates the number of bytes needed to serialize the bucket.
///
/// Note that this does not match the exact size of the serialized payload. Instead, the size is
/// approximated through the number of contained values, assuming an average size of serialized
/// values.
fn estimate_size(bucket: &Bucket) -> usize {
    estimate_base_size(bucket) + bucket.value.len() * AVG_VALUE_SIZE
}

/// A message containing a list of [`Bucket`]s to be inserted into the aggregator.
#[derive(Debug)]
pub struct MergeBuckets {
    pub(crate) project_key: ProjectKey,
    pub(crate) buckets: Vec<Bucket>,
}

impl MergeBuckets {
    /// Creates a new message containing a list of [`Bucket`]s.
    pub fn new(project_key: ProjectKey, buckets: Vec<Bucket>) -> Self {
        Self {
            project_key,
            buckets,
        }
    }

    /// Returns the `ProjectKey` for the the current `MergeBuckets` message.
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
    }

    /// Returns the list of the buckets in the current `MergeBuckets` message, consuming the
    /// message itself.
    pub fn buckets(self) -> Vec<Bucket> {
        self.buckets
    }
}

/*

#[cfg(test)]
mod tests {

    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, RwLock};

    use relay_common::time::UnixTimestamp;
    use relay_system::{FromMessage, Interface};

    use crate::{BucketCountInquiry, BucketValue};

    use super::*;

    #[derive(Default)]
    struct ReceivedData {
        buckets: Vec<Bucket>,
    }

    struct TestInterface(FlushBuckets);

    impl Interface for TestInterface {}

    impl FromMessage<FlushBuckets> for TestInterface {
        type Response = NoResponse;

        fn from_message(message: FlushBuckets, _: ()) -> Self {
            Self(message)
        }
    }

    #[derive(Clone, Default)]
    struct TestReceiver {
        data: Arc<RwLock<ReceivedData>>,
        reject_all: bool,
    }

    impl TestReceiver {
        fn add_buckets(&self, buckets: Vec<Bucket>) {
            self.data.write().unwrap().buckets.extend(buckets);
        }

        fn bucket_count(&self) -> usize {
            self.data.read().unwrap().buckets.len()
        }
    }

    impl Service for TestReceiver {
        type Interface = TestInterface;

        fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
            tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    let buckets = message.0.buckets;
                    relay_log::debug!(?buckets, "received buckets");
                    if !self.reject_all {
                        self.add_buckets(buckets);
                    }
                }
            });
        }
    }

    fn some_bucket() -> Bucket {
        Bucket {
            timestamp: UnixTimestamp::from_secs(999994711),
            width: 0,
            name: "c:transactions/foo".to_owned(),
            value: BucketValue::counter(42.),
            tags: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn test_flush_bucket() {
        relay_test::setup();
        tokio::time::pause();

        let receiver = TestReceiver::default();
        let recipient = receiver.clone().start().recipient();

        let config = AggregatorServiceConfig {
            bucket_interval: 1,
            initial_delay: 0,
            debounce_delay: 0,
            ..Default::default()
        };
        let aggregator = AggregatorService::new(config, Some(recipient)).start();

        let mut bucket = some_bucket();
        bucket.timestamp = UnixTimestamp::now();

        aggregator.send(MergeBuckets {
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            buckets: vec![bucket],
        });

        let buckets_count = aggregator.send(BucketCountInquiry).await.unwrap();
        // Let's check the number of buckets in the aggregator just after sending a
        // message.
        assert_eq!(buckets_count, 1);

        // Wait until flush delay has passed. It is up to 2s: 1s for the current bucket
        // and 1s for the flush shift. Adding 100ms buffer.
        tokio::time::sleep(Duration::from_millis(2100)).await;
        // receiver must have 1 bucket flushed
        assert_eq!(receiver.bucket_count(), 1);
    }

    #[tokio::test]
    async fn test_merge_back() {
        relay_test::setup();
        tokio::time::pause();

        // Create a receiver which accepts nothing:
        let receiver = TestReceiver {
            reject_all: true,
            ..TestReceiver::default()
        };
        let recipient = receiver.clone().start().recipient();

        let config = AggregatorServiceConfig {
            bucket_interval: 1,
            initial_delay: 0,
            debounce_delay: 0,
            ..Default::default()
        };
        let aggregator = AggregatorService::new(config, Some(recipient)).start();

        let mut bucket = some_bucket();
        bucket.timestamp = UnixTimestamp::now();

        aggregator.send(MergeBuckets {
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            buckets: vec![bucket],
        });

        assert_eq!(receiver.bucket_count(), 0);

        tokio::time::sleep(Duration::from_millis(1100)).await;
        let bucket_count = aggregator.send(BucketCountInquiry).await.unwrap();
        assert_eq!(bucket_count, 1);
        assert_eq!(receiver.bucket_count(), 0);
    }

    fn test_config() -> AggregatorServiceConfig {
        AggregatorServiceConfig {
            bucket_interval: 1,
            initial_delay: 0,
            debounce_delay: 0,
            max_flush_bytes: 50_000_000,
            max_secs_in_past: 50 * 365 * 24 * 60 * 60,
            max_secs_in_future: 50 * 365 * 24 * 60 * 60,
            max_name_length: 200,
            max_tag_key_length: 200,
            max_tag_value_length: 200,
            max_project_key_bucket_bytes: None,
            max_total_bucket_bytes: None,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn run_test_bucket_partitioning(flush_partitions: Option<u64>) -> Vec<String> {
        let config = AggregatorServiceConfig {
            max_flush_bytes: 1000,
            flush_partitions,
            ..test_config()
        };

        let bucket1 = Bucket {
            timestamp: UnixTimestamp::from_secs(999994711),
            width: 0,
            name: "c:transactions/foo".to_owned(),
            value: BucketValue::counter(42.),
            tags: BTreeMap::new(),
        };

        let bucket2 = Bucket {
            timestamp: UnixTimestamp::from_secs(999994711),
            width: 0,
            name: "c:transactions/bar".to_owned(),
            value: BucketValue::counter(43.),
            tags: BTreeMap::new(),
        };

        let mut aggregator = AggregatorService::new(config, None);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let captures = relay_statsd::with_capturing_test_client(|| {
            aggregator.aggregator.merge(project_key, bucket1).ok();
            aggregator.aggregator.merge(project_key, bucket2).ok();
            aggregator.try_flush();
        });
        captures
            .into_iter()
            .filter(|x| {
                [
                    "metrics.buckets.batches_per_partition",
                    "metrics.buckets.per_batch",
                    "metrics.buckets.partition_keys",
                ]
                .contains(&x.split_once(':').unwrap().0)
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_bucket_partitioning_dummy() {
        let output = run_test_bucket_partitioning(None);
        insta::assert_debug_snapshot!(output, @r#"
        [
            "metrics.buckets.per_batch:2|h|#aggregator:default",
            "metrics.buckets.batches_per_partition:1|h|#aggregator:default",
        ]
        "#);
    }

    #[test]
    fn test_bucket_partitioning_128() {
        let output = run_test_bucket_partitioning(Some(128));
        // Because buckets are stored in a HashMap, we do not know in what order the buckets will
        // be processed, so we need to convert them to a set:
        let (partition_keys, tail) = output.split_at(2);
        insta::assert_debug_snapshot!(BTreeSet::from_iter(partition_keys), @r#"
        {
            "metrics.buckets.partition_keys:59|h|#aggregator:default",
            "metrics.buckets.partition_keys:62|h|#aggregator:default",
        }
        "#);

        insta::assert_debug_snapshot!(tail, @r#"
        [
            "metrics.buckets.per_batch:1|h|#aggregator:default",
            "metrics.buckets.batches_per_partition:1|h|#aggregator:default",
            "metrics.buckets.per_batch:1|h|#aggregator:default",
            "metrics.buckets.batches_per_partition:1|h|#aggregator:default",
        ]
        "#);
    }

    #[test]
    fn test_capped_iter_empty() {
        let buckets = vec![];

        let mut iter = CappedBucketIter::new(buckets.into_iter(), 200);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_capped_iter_single() {
        let json = r#"[
          {
            "name": "endpoint.response_time",
            "unit": "millisecond",
            "value": [36, 49, 57, 68],
            "type": "d",
            "timestamp": 1615889440,
            "width": 10,
            "tags": {
                "route": "user_index"
            }
          }
        ]"#;

        let buckets = serde_json::from_str::<Vec<Bucket>>(json).unwrap();

        let mut iter = CappedBucketIter::new(buckets.into_iter(), 200);
        let batch = iter.next().unwrap();
        assert_eq!(batch.len(), 1);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_capped_iter_split() {
        let json = r#"[
          {
            "name": "endpoint.response_time",
            "unit": "millisecond",
            "value": [1, 1, 1, 1],
            "type": "d",
            "timestamp": 1615889440,
            "width": 10,
            "tags": {
                "route": "user_index"
            }
          }
        ]"#;

        let buckets = serde_json::from_str::<Vec<Bucket>>(json).unwrap();

        // 58 is a magic number obtained by experimentation that happens to split this bucket
        let mut iter = CappedBucketIter::new(buckets.into_iter(), 108);
        let batch1 = iter.next().unwrap();
        assert_eq!(batch1.len(), 1);

        match batch1.first().unwrap().value {
            BucketValue::Distribution(ref dist) => assert_eq!(dist.len(), 2),
            _ => unreachable!(),
        }

        let batch2 = iter.next().unwrap();
        assert_eq!(batch2.len(), 1);

        match batch2.first().unwrap().value {
            BucketValue::Distribution(ref dist) => assert_eq!(dist.len(), 2),
            _ => unreachable!(),
        }

        assert!(iter.next().is_none());
    }

    fn test_capped_iter_completeness(max_flush_bytes: usize, expected_elements: usize) {
        let json = r#"[
          {
            "name": "endpoint.response_time",
            "unit": "millisecond",
            "value": [1, 1, 1, 1],
            "type": "d",
            "timestamp": 1615889440,
            "width": 10,
            "tags": {
                "route": "user_index"
            }
          }
        ]"#;

        let buckets = serde_json::from_str::<Vec<Bucket>>(json).unwrap();

        let mut iter = CappedBucketIter::new(buckets.into_iter(), max_flush_bytes);
        let batches = iter
            .by_ref()
            .take(expected_elements + 1)
            .collect::<Vec<_>>();
        assert!(
            batches.len() <= expected_elements,
            "Cannot have more buckets than individual values"
        );
        let total_elements: usize = batches.into_iter().flatten().map(|x| x.value.len()).sum();
        assert_eq!(total_elements, expected_elements);
    }

    #[test]
    fn test_capped_iter_completeness_0() {
        test_capped_iter_completeness(0, 0);
    }

    #[test]
    fn test_capped_iter_completeness_90() {
        // This would cause an infinite loop.
        test_capped_iter_completeness(90, 0);
    }

    #[test]
    fn test_capped_iter_completeness_100() {
        test_capped_iter_completeness(100, 4);
    }
}

*/
