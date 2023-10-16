//! Aggregation logic for metrics. Metrics from different namespaces may be routed to different aggregators,
//! with their own limits, bucket intervals, etc.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use itertools::Itertools;
use relay_base_schema::project::ProjectKey;
use relay_system::{
    AsyncResponse, Controller, FromMessage, Interface, NoResponse, Recipient, Sender, Service,
    Shutdown,
};
use serde::{Deserialize, Serialize};

use crate::aggregator::{self, AggregatorConfig, CappedBucketIter};
use crate::statsd::MetricHistograms;
use crate::{Bucket, MetricNamespace, MetricResourceIdentifier};

/// Interval for the flush cycle of the [`AggregatorService`].
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// A Bucket and its hashed key.
/// This is cheaper to pass around than a (BucketKey, Bucket) pair.
pub struct HashedBucket {
    hashed_key: u64,
    bucket: Bucket,
}

impl HashedBucket {
    /// Creates a new [`HashedBucket`] instance.
    pub fn new(hashed_key: u64, bucket: Bucket) -> Self {
        Self { hashed_key, bucket }
    }
}

/// Aggregator service interface.
#[derive(Debug)]
pub enum Aggregator {
    /// The health check message which makes sure that the service can accept the requests now.
    AcceptsMetrics(AcceptsMetrics, Sender<bool>),
    /// Merge the buckets.
    MergeBuckets(MergeBuckets),
    /// Message is used only for tests to get the current number of buckets in the default [`AggregatorService`].
    #[cfg(test)]
    BucketCountInquiry(BucketCountInquiry, Sender<usize>),
}

impl Interface for Aggregator {}

/// Check whether the aggregator has not (yet) exceeded its total limits. Used for health checks.
#[derive(Debug)]
pub struct AcceptsMetrics;

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

/// Used only for testing the `AggregatorService`.
#[cfg(test)]
#[derive(Debug)]
pub struct BucketCountInquiry;

#[cfg(test)]
impl FromMessage<BucketCountInquiry> for Aggregator {
    type Response = AsyncResponse<usize>;
    fn from_message(message: BucketCountInquiry, sender: Sender<usize>) -> Self {
        Self::BucketCountInquiry(message, sender)
    }
}

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

/// Parameters used by the [`AggregatorService`].
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AggregatorServiceConfig {
    /// Parameters used by the [`Aggregator`].
    #[serde(flatten)]
    pub aggregator: AggregatorConfig,
    /// The approximate maximum number of bytes submitted within one flush cycle.
    ///
    /// This controls how big flushed batches of buckets get, depending on the number of buckets,
    /// the cumulative length of their keys, and the number of raw values. Since final serialization
    /// adds some additional overhead, this number is approxmate and some safety margin should be
    /// left to hard limits.
    pub max_flush_bytes: usize,

    /// Maximum amount of bytes used for metrics aggregation.
    ///
    /// When aggregating metrics, Relay keeps track of how many bytes a metric takes in memory.
    /// This is only an approximation and does not take into account things such as pre-allocation
    /// in hashmaps.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_total_bucket_bytes: Option<usize>,

    /// The number of logical partitions that can receive flushed buckets.
    ///
    /// If set, buckets are partitioned by (bucket key % flush_partitions), and routed
    /// by setting the header `X-Sentry-Relay-Shard`.
    pub flush_partitions: Option<u64>,
}

impl Default for AggregatorServiceConfig {
    fn default() -> Self {
        Self {
            max_flush_bytes: 5_000_000, // 5 MB
            flush_partitions: None,
            max_total_bucket_bytes: None,
            aggregator: AggregatorConfig::default(),
        }
    }
}

/// Contains an [`AggregatorServiceConfig`] for a specific scope.
///
/// For now, the only way to scope an aggregator is by [`MetricNamespace`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScopedAggregatorConfig {
    /// Name of the aggregator, used to tag statsd metrics.
    pub name: String,
    /// Condition that needs to be met for a metric or bucket to be routed to a
    /// secondary aggregator.
    pub condition: Condition,
    /// The configuration of the secondary aggregator.
    pub config: AggregatorServiceConfig,
}

/// Condition that needs to be met for a metric or bucket to be routed to a
/// secondary aggregator.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Condition {
    /// Checks for equality on a specific field.
    Eq(Field),
}

/// Defines a field and a field value to compare to when a [`Condition`] is evaluated.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "field", content = "value", rename_all = "lowercase")]
pub enum Field {
    /// Field that allows comparison to a metric or bucket's namespace.
    Namespace(MetricNamespace),
}

enum AggregatorState {
    Running,
    ShuttingDown,
}

/// Service that routes metrics & metric buckets to the appropriate aggregator.
///
/// Each aggregator gets its own configuration.
/// Metrics are routed to the first aggregator which matches the configuration's [`Condition`].
/// If no condition matches, the metric/bucket is routed to the `default_aggregator`.
pub struct AggregatorService {
    default_aggregator: aggregator::Aggregator,
    secondary_aggregators: BTreeMap<MetricNamespace, aggregator::Aggregator>,
    max_flush_bytes: usize,
    max_total_bucket_bytes: Option<usize>,
    flush_partitions: Option<u64>,
    state: AggregatorState,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
}

impl AggregatorService {
    /// Create a new aggregator service.
    pub fn new(
        aggregator_config: AggregatorServiceConfig,
        secondary_aggregators: Vec<ScopedAggregatorConfig>,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    ) -> Self {
        let max_total_bucket_bytes = aggregator_config.max_total_bucket_bytes;
        let max_flush_bytes = aggregator_config.max_flush_bytes;
        let flush_partitions = aggregator_config.flush_partitions;

        if !secondary_aggregators.iter().all(|agg| {
            agg.config.flush_partitions == flush_partitions
                && agg.config.max_total_bucket_bytes == max_total_bucket_bytes
                && agg.config.max_flush_bytes == max_flush_bytes
        }) {
            relay_log::error!("aggregatorserviceconfigs are inconsistent");
        }

        Self {
            default_aggregator: aggregator::Aggregator::new(aggregator_config.aggregator),
            secondary_aggregators: secondary_aggregators
                .into_iter()
                .map(|c| {
                    let Condition::Eq(Field::Namespace(namespace)) = c.condition;
                    (
                        namespace,
                        aggregator::Aggregator::named(c.name, c.config.aggregator),
                    )
                })
                .collect(),
            state: AggregatorState::Running,
            receiver,
            max_total_bucket_bytes,
            max_flush_bytes,
            flush_partitions,
        }
    }

    /// Split buckets into N logical partitions, determined by the bucket key.
    fn partition_buckets(
        &self,
        aggregator_name: &str,
        buckets: Vec<HashedBucket>,
    ) -> BTreeMap<Option<u64>, Vec<Bucket>> {
        let flush_partitions = match self.flush_partitions {
            None => {
                return BTreeMap::from([(None, buckets.into_iter().map(|x| x.bucket).collect())]);
            }
            Some(x) => x.max(1), // handle 0,
        };
        let mut partitions = BTreeMap::<_, Vec<Bucket>>::new();
        for bucket in buckets {
            let partition_key = bucket.hashed_key % flush_partitions;
            partitions
                .entry(Some(partition_key))
                .or_default()
                .push(bucket.bucket);

            // Log the distribution of buckets over partition key
            relay_statsd::metric!(
                histogram(MetricHistograms::PartitionKeys) = partition_key as f64,
                aggregator = aggregator_name,
            );
        }
        partitions
    }

    fn try_flush(
        &self,
        aggregator_name: &str,
        flush_buckets: HashMap<ProjectKey, Vec<HashedBucket>>,
    ) {
        relay_log::trace!("flushing {} projects to receiver", flush_buckets.len());

        let mut total_bucket_count = 0u64;
        for (project_key, project_buckets) in flush_buckets.into_iter() {
            let bucket_count = project_buckets.len() as u64;
            relay_statsd::metric!(
                histogram(MetricHistograms::BucketsFlushedPerProject) = bucket_count,
                aggregator = aggregator_name,
            );
            total_bucket_count += bucket_count;

            let partitioned_buckets = self.partition_buckets(aggregator_name, project_buckets);

            for (partition_key, buckets) in partitioned_buckets {
                let capped_batches =
                    CappedBucketIter::new(buckets.into_iter(), self.max_flush_bytes);

                let num_batches = capped_batches
                    .map(|batch| {
                        relay_statsd::metric!(
                            histogram(MetricHistograms::BucketsPerBatch) = batch.len() as f64,
                            aggregator = aggregator_name,
                        );
                        if let Some(receiver) = self.receiver.as_ref() {
                            receiver.send(FlushBuckets {
                                project_key,
                                partition_key,
                                buckets: batch,
                            });
                        };
                    })
                    .count();

                relay_statsd::metric!(
                    histogram(MetricHistograms::BatchesPerPartition) = num_batches as f64,
                    aggregator = aggregator_name,
                );
            }
        }
        relay_statsd::metric!(
            histogram(MetricHistograms::BucketsFlushed) = total_bucket_count,
            aggregator = aggregator_name,
        );
    }

    /// Flushes all the aggregators and sends the [`FlushBuckets`] message to the receiver
    /// in the fire and forget fashion. It is up to the receiver to send the [`MergeBuckets`]
    /// message back if buckets could not be flushed and we require another re-try.
    ///
    /// If `force` is true, flush all buckets unconditionally and do not attempt to merge back.
    fn try_flush_all(&mut self) {
        let force_flush = matches!(&self.state, &AggregatorState::ShuttingDown);

        let flush_buckets: Vec<_> = self
            .aggregators_mut()
            .map(|agg| agg.pop_flush_buckets(force_flush))
            .collect_vec();

        // Was unable to do it in one line due to borrow checker constraints.
        for (name, buckets) in self
            .aggregators_ref()
            .map(|agg| agg.name())
            .zip(flush_buckets.into_iter())
        {
            if buckets.is_empty() {
                continue;
            };

            self.try_flush(name, buckets);
        }
    }

    fn aggregators_ref(&self) -> impl Iterator<Item = &aggregator::Aggregator> {
        std::iter::once(&self.default_aggregator).chain(self.secondary_aggregators.values())
    }

    fn aggregators_mut(&mut self) -> impl Iterator<Item = &mut aggregator::Aggregator> {
        std::iter::once(&mut self.default_aggregator).chain(self.secondary_aggregators.values_mut())
    }

    fn handle_message(&mut self, msg: Aggregator) {
        match msg {
            Aggregator::AcceptsMetrics(_, sender) => self.handle_accepts_metrics(sender),
            Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, sender) => {
                sender.send(self.default_aggregator.bucket_count())
            }
        }
    }

    fn handle_accepts_metrics(&mut self, sender: Sender<bool>) {
        let max_bytes = self.max_total_bucket_bytes;

        let accepts = self
            .aggregators_mut()
            .all(|agg| agg.totals_cost_exceeded(max_bytes));

        sender.send(accepts);
    }

    fn handle_merge_buckets(&mut self, message: MergeBuckets) {
        let MergeBuckets {
            project_key,
            buckets,
        } = message;

        let metrics_by_namespace = buckets.into_iter().group_by(|bucket| {
            MetricResourceIdentifier::parse(&bucket.name)
                .map(|mri| mri.namespace)
                .ok()
        });

        // TODO: Parse MRI only once, move validation from Aggregator here.
        for (namespace, group) in metrics_by_namespace.into_iter() {
            let aggregator = namespace
                .and_then(|ns| self.secondary_aggregators.get_mut(&ns))
                .unwrap_or(&mut self.default_aggregator);

            aggregator.merge_all(
                project_key,
                group.collect::<Vec<Bucket>>(),
                self.max_total_bucket_bytes,
            );
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
            relay_log::info!("metrics router started");

            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
            let mut shutdown = Controller::shutdown_handle();

            // Note that currently this loop never exists and will run till the tokio runtime shuts
            // down. This is about to change with the refactoring for the shutdown process.
            loop {
                tokio::select! {
                    biased;


                    _ = ticker.tick() => self.try_flush_all(),
                    Some(message) = rx.recv() => self.handle_message(message),
                    shutdown = shutdown.notified() => self.handle_shutdown(shutdown),

                    else => break,
                }
            }
            relay_log::info!("metrics router stopped");
        });
    }
}

/// A message containing a list of [`Bucket`]s to be inserted into the aggregator.
#[derive(Debug)]
pub struct MergeBuckets {
    project_key: ProjectKey,
    buckets: Vec<Bucket>,
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

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use serde_json::json;

    use super::*;

    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use relay_common::time::UnixTimestamp;
    use relay_system::{FromMessage, Interface};

    use crate::aggregator::AggregatorConfig;
    use crate::{BucketCountInquiry, BucketValue};

    #[test]
    fn condition_roundtrip() {
        let json = json!({"op": "eq", "field": "namespace", "value": "spans"});
        assert_debug_snapshot!(
            serde_json::from_value::<Condition>(json).unwrap(),
            @r###"
        Eq(
            Namespace(
                Spans,
            ),
        )
        "###
        );
    }

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
            aggregator: AggregatorConfig {
                bucket_interval: 1,
                initial_delay: 0,
                debounce_delay: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let aggregator = AggregatorService::new(config, vec![], Some(recipient)).start();

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
            aggregator: AggregatorConfig {
                bucket_interval: 1,
                initial_delay: 0,
                debounce_delay: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let aggregator = AggregatorService::new(config, vec![], Some(recipient)).start();

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
            aggregator: {
                AggregatorConfig {
                    bucket_interval: 1,
                    initial_delay: 0,
                    debounce_delay: 0,
                    max_secs_in_past: 50 * 365 * 24 * 60 * 60,
                    max_secs_in_future: 50 * 365 * 24 * 60 * 60,
                    max_name_length: 200,
                    max_tag_key_length: 200,
                    max_tag_value_length: 200,
                    max_project_key_bucket_bytes: None,
                    ..Default::default()
                }
            },
            max_total_bucket_bytes: None,
            max_flush_bytes: 50_000_000,
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

        let mut aggregator = AggregatorService::new(config.clone(), vec![], None);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let captures = relay_statsd::with_capturing_test_client(|| {
            aggregator
                .default_aggregator
                .merge(project_key, bucket1, config.max_total_bucket_bytes)
                .ok();
            aggregator
                .default_aggregator
                .merge(project_key, bucket2, config.max_total_bucket_bytes)
                .ok();
            aggregator.try_flush_all();
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
}
