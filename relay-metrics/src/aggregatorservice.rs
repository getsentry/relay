use std::error::Error;
use std::time::Duration;

use relay_base_schema::project::ProjectKey;
use relay_system::{Controller, NoResponse, Recipient, Sender, Service, Shutdown};

use crate::bucket::Bucket;
use crate::statsd::{MetricCounters, MetricHistograms};
use crate::{Aggregator, AggregatorConfig, AggregatorManager, MergeBuckets};

/// Interval for the flush cycle of the [`AggregatorService`].
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

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
/// are flushed after a shorter debounce delay. See [`AggregatorConfig`] for configuration options.
///
/// Internally, the aggregator maintains a continuous flush cycle every 100ms. It guarantees that
/// all elapsed buckets belonging to the same [`ProjectKey`] are flushed together.
///
/// Receivers must implement a handler for the [`FlushBuckets`] message.
pub struct AggregatorService {
    aggregator: Aggregator,
    state: AggregatorState,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
}

impl AggregatorService {
    /// Create a new aggregator service and connect it to `receiver`.
    ///
    /// The aggregator will flush a list of buckets to the receiver in regular intervals based on
    /// the given `config`.
    pub fn new(
        config: AggregatorConfig,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    ) -> Self {
        Self::named("default".to_owned(), config, receiver)
    }

    /// Like [`Self::new`], but with a provided name.
    pub(crate) fn named(
        name: String,
        config: AggregatorConfig,
        receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    ) -> Self {
        Self {
            aggregator: Aggregator::named(name, config),
            receiver,
            state: AggregatorState::Running,
        }
    }

    fn handle_accepts_metrics(&self, sender: Sender<bool>) {
        let result = !self
            .aggregator
            .totals_cost_exceeded(self.aggregator.config().max_total_bucket_bytes);

        sender.send(result);
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

            let num_partitions = self.aggregator.config().flush_partitions;
            let partitioned_buckets = self
                .aggregator
                .partition_buckets(project_buckets, num_partitions);
            for (partition_key, buckets) in partitioned_buckets {
                self.aggregator.process_batches(buckets, |batch| {
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
        if let Err(err) = self.aggregator.merge_all(project_key, buckets) {
            relay_log::error!(
                tags.aggregator = &self.aggregator.name(),
                error = &err as &dyn Error,
                "failed to merge buckets"
            );
        }
    }

    fn handle_message(&mut self, msg: AggregatorManager) {
        match msg {
            AggregatorManager::AcceptsMetrics(_, sender) => self.handle_accepts_metrics(sender),
            AggregatorManager::MergeBuckets(msg) => self.handle_merge_buckets(msg),
            #[cfg(test)]
            AggregatorManager::BucketCountInquiry(_, sender) => {
                sender.send(self.aggregator.bucket_qty())
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
    type Interface = AggregatorManager;

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
        let remaining_buckets = self.aggregator.bucket_qty();
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

        let config = AggregatorConfig {
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

        let config = AggregatorConfig {
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

    fn test_config() -> AggregatorConfig {
        AggregatorConfig {
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
        let config = AggregatorConfig {
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
}
