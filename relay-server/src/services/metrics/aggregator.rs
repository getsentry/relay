use hashbrown::HashMap;
use std::time::Duration;

use relay_base_schema::project::ProjectKey;
use relay_config::AggregatorServiceConfig;
use relay_system::{
    AsyncResponse, Controller, FromMessage, Interface, NoResponse, Recipient, Sender, Service,
    Shutdown,
};

use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use relay_metrics::{aggregator, Bucket};

/// Aggregator for metric buckets.
///
/// Buckets are flushed to a receiver after their time window and a grace period have passed.
/// Metrics with a recent timestamp are given a longer grace period than backdated metrics, which
/// are flushed after a shorter debounce delay. See [`AggregatorServiceConfig`] for configuration options.
///
/// Internally, the aggregator maintains a continuous flush cycle every 100ms. It guarantees that
/// all elapsed buckets belonging to the same [`ProjectKey`] are flushed together.
///
/// Receivers must implement a handler for the [`FlushBuckets`] message.
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

impl Aggregator {
    /// Returns the name of the message variant.
    pub fn variant(&self) -> &'static str {
        match self {
            Aggregator::AcceptsMetrics(_, _) => "AcceptsMetrics",
            Aggregator::MergeBuckets(_) => "MergeBuckets",
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, _) => "BucketCountInquiry",
        }
    }
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
    /// The partition to which the buckets belong.
    ///
    /// When set to `Some` it means that partitioning was enabled in the [`Aggregator`].
    pub partition_key: Option<u64>,
    /// The buckets to be flushed.
    pub buckets: HashMap<ProjectKey, Vec<Bucket>>,
}

enum AggregatorState {
    Running,
    ShuttingDown,
}

/// Service implementing the [`Aggregator`] interface.
pub struct AggregatorService {
    aggregator: aggregator::Aggregator,
    state: AggregatorState,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    max_total_bucket_bytes: Option<usize>,
    flush_interval_ms: u64,
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
            max_total_bucket_bytes: config.max_total_bucket_bytes,
            aggregator: aggregator::Aggregator::named(name, config.aggregator),
            flush_interval_ms: config.flush_interval_ms,
        }
    }

    fn handle_accepts_metrics(&self, sender: Sender<bool>) {
        let result = !self
            .aggregator
            .totals_cost_exceeded(self.max_total_bucket_bytes);

        sender.send(result);
    }

    /// Sends the [`FlushBuckets`] message to the receiver in the fire and forget fashion. It is up
    /// to the receiver to send the [`MergeBuckets`] message back if buckets could not be flushed
    /// and we require another re-try.
    ///
    /// If `force` is true, flush all buckets unconditionally and do not attempt to merge back.
    fn try_flush(&mut self) {
        let force_flush = matches!(&self.state, AggregatorState::ShuttingDown);
        let partitions = self.aggregator.pop_flush_buckets(force_flush);

        if partitions.is_empty() {
            return;
        }

        let partitions_count = partitions.len() as u64;
        relay_log::trace!("flushing {} partitions to receiver", partitions_count);
        relay_statsd::metric!(
            histogram(RelayHistograms::PartitionsFlushed) = partitions_count,
            aggregator = self.aggregator.name(),
        );

        let mut total_bucket_count = 0u64;
        for buckets_by_project in partitions.values() {
            for buckets in buckets_by_project.values() {
                let bucket_count = buckets.len() as u64;
                total_bucket_count += bucket_count;

                relay_statsd::metric!(
                    histogram(RelayHistograms::BucketsFlushedPerProject) = bucket_count,
                    aggregator = self.aggregator.name(),
                );
            }
        }

        relay_statsd::metric!(
            histogram(RelayHistograms::BucketsFlushed) = total_bucket_count,
            aggregator = self.aggregator.name(),
        );

        if let Some(ref receiver) = self.receiver {
            for (partition_key, buckets_by_project) in partitions {
                receiver.send(FlushBuckets {
                    partition_key,
                    buckets: buckets_by_project,
                })
            }
        }
    }

    fn handle_merge_buckets(&mut self, msg: MergeBuckets) {
        let MergeBuckets {
            project_key,
            buckets,
        } = msg;
        self.aggregator
            .merge_all(project_key, buckets, self.max_total_bucket_bytes);
    }

    fn handle_message(&mut self, message: Aggregator) {
        let ty = message.variant();
        relay_statsd::metric!(
            timer(RelayTimers::AggregatorServiceDuration),
            message = ty,
            {
                match message {
                    Aggregator::AcceptsMetrics(_, sender) => self.handle_accepts_metrics(sender),
                    Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
                    #[cfg(test)]
                    Aggregator::BucketCountInquiry(_, sender) => {
                        sender.send(self.aggregator.bucket_count())
                    }
                }
            }
        )
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
            let mut ticker = tokio::time::interval(Duration::from_millis(self.flush_interval_ms));
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
                counter(RelayCounters::BucketsDropped) += remaining_buckets as i64,
                aggregator = self.aggregator.name(),
            );
        }
    }
}

/// A message containing a list of [`Bucket`]s to be inserted into the aggregator.
#[derive(Debug)]
pub struct MergeBuckets {
    pub project_key: ProjectKey,
    pub buckets: Vec<Bucket>,
}

impl MergeBuckets {
    /// Creates a new message containing a list of [`Bucket`]s.
    pub fn new(project_key: ProjectKey, buckets: Vec<Bucket>) -> Self {
        Self {
            project_key,
            buckets,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};

    use relay_common::time::UnixTimestamp;
    use relay_metrics::{aggregator::AggregatorConfig, BucketMetadata, BucketValue};

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
        fn add_buckets(&self, buckets: HashMap<ProjectKey, Vec<Bucket>>) {
            let buckets = buckets.into_values().flatten();
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
        let timestamp = UnixTimestamp::from_secs(999994711);
        Bucket {
            timestamp,
            width: 0,
            name: "c:transactions/foo".into(),
            value: BucketValue::counter(42.into()),
            tags: BTreeMap::new(),
            metadata: BucketMetadata::new(timestamp),
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
                ..Default::default()
            },
            ..Default::default()
        };
        let aggregator = AggregatorService::new(config, Some(recipient)).start();

        let mut bucket = some_bucket();
        bucket.timestamp = UnixTimestamp::now();

        aggregator.send(MergeBuckets::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            vec![bucket],
        ));

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
}
