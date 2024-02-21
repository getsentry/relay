use std::collections::HashMap;
use std::time::Duration;

use relay_base_schema::project::ProjectKey;
use relay_system::{
    AsyncResponse, Controller, FromMessage, Interface, NoResponse, Recipient, Sender, Service,
    Shutdown,
};
use serde::{Deserialize, Serialize};

use crate::aggregator::{self, AggregatorConfig, ShiftKey};
use crate::bucket::Bucket;
use crate::statsd::{MetricCounters, MetricHistograms, MetricTimers};

/// Interval for the flush cycle of the [`AggregatorService`].
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Parameters used by the [`AggregatorService`].
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AggregatorServiceConfig {
    /// Maximum amount of bytes used for metrics aggregation.
    ///
    /// When aggregating metrics, Relay keeps track of how many bytes a metric takes in memory.
    /// This is only an approximation and does not take into account things such as pre-allocation
    /// in hashmaps.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_total_bucket_bytes: Option<usize>,

    /// Determines the wall clock time interval for buckets in seconds.
    ///
    /// Defaults to `10` seconds. Every metric is sorted into a bucket of this size based on its
    /// timestamp. This defines the minimum granularity with which metrics can be queried later.
    pub bucket_interval: u64,

    /// The initial delay in seconds to wait before flushing a bucket.
    ///
    /// Defaults to `30` seconds. Before sending an aggregated bucket, this is the time Relay waits
    /// for buckets that are being reported in real time. This should be higher than the
    /// `debounce_delay`.
    ///
    /// Relay applies up to a full `bucket_interval` of additional jitter after the initial delay to spread out flushing real time buckets.
    pub initial_delay: u64,

    /// The delay in seconds to wait before flushing a backdated buckets.
    ///
    /// Defaults to `10` seconds. Metrics can be sent with a past timestamp. Relay wait this time
    /// before sending such a backdated bucket to the upsteam. This should be lower than
    /// `initial_delay`.
    ///
    /// Unlike `initial_delay`, the debounce delay starts with the exact moment the first metric
    /// is added to a backdated bucket.
    pub debounce_delay: u64,

    /// The age in seconds of the oldest allowed bucket timestamp.
    ///
    /// Defaults to 5 days.
    pub max_secs_in_past: u64,

    /// The time in seconds that a timestamp may be in the future.
    ///
    /// Defaults to 1 minute.
    pub max_secs_in_future: u64,

    /// The length the name of a metric is allowed to be.
    ///
    /// Defaults to `200` bytes.
    pub max_name_length: usize,

    /// The length the tag key is allowed to be.
    ///
    /// Defaults to `200` bytes.
    pub max_tag_key_length: usize,

    /// The length the tag value is allowed to be.
    ///
    /// Defaults to `200` chars.
    pub max_tag_value_length: usize,

    /// Maximum amount of bytes used for metrics aggregation per project key.
    ///
    /// Similar measuring technique to `max_total_bucket_bytes`, but instead of a
    /// global/process-wide limit, it is enforced per project key.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_project_key_bucket_bytes: Option<usize>,

    /// Key used to shift the flush time of a bucket.
    ///
    /// This prevents flushing all buckets from a bucket interval at the same
    /// time by computing an offset from the hash of the given key.
    pub shift_key: ShiftKey,

    // TODO(dav1dde): move these config values to a better spot
    /// The approximate maximum number of bytes submitted within one flush cycle.
    ///
    /// This controls how big flushed batches of buckets get, depending on the number of buckets,
    /// the cumulative length of their keys, and the number of raw values. Since final serialization
    /// adds some additional overhead, this number is approxmate and some safety margin should be
    /// left to hard limits.
    pub max_flush_bytes: usize,
    /// The number of logical partitions that can receive flushed buckets.
    ///
    /// If set, buckets are partitioned by (bucket key % flush_partitions), and routed
    /// by setting the header `X-Sentry-Relay-Shard`.
    pub flush_partitions: Option<u64>,
}

impl Default for AggregatorServiceConfig {
    fn default() -> Self {
        Self {
            max_total_bucket_bytes: None,
            bucket_interval: 10,
            initial_delay: 30,
            debounce_delay: 10,
            max_secs_in_past: 5 * 24 * 60 * 60, // 5 days, as for sessions
            max_secs_in_future: 60,             // 1 minute
            max_name_length: 200,
            max_tag_key_length: 200,
            max_tag_value_length: 200,
            max_project_key_bucket_bytes: None,
            shift_key: ShiftKey::default(),
            max_flush_bytes: 5_000_000, // 5 MB
            flush_partitions: None,
        }
    }
}

impl From<&AggregatorServiceConfig> for AggregatorConfig {
    fn from(value: &AggregatorServiceConfig) -> Self {
        Self {
            bucket_interval: value.bucket_interval,
            initial_delay: value.initial_delay,
            debounce_delay: value.debounce_delay,
            max_secs_in_past: value.max_secs_in_past,
            max_secs_in_future: value.max_secs_in_future,
            max_name_length: value.max_name_length,
            max_tag_key_length: value.max_tag_key_length,
            max_tag_value_length: value.max_tag_value_length,
            max_project_key_bucket_bytes: value.max_project_key_bucket_bytes,
            shift_key: value.shift_key,
        }
    }
}

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
    fn variant(&self) -> &'static str {
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
            aggregator: aggregator::Aggregator::named(name, AggregatorConfig::from(&config)),
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
        let buckets = self.aggregator.pop_flush_buckets(force_flush);

        if buckets.is_empty() {
            return;
        }

        relay_log::trace!("flushing {} projects to receiver", buckets.len());

        let mut total_bucket_count = 0u64;
        for buckets in buckets.values() {
            let bucket_count = buckets.len() as u64;
            total_bucket_count += bucket_count;

            relay_statsd::metric!(
                histogram(MetricHistograms::BucketsFlushedPerProject) = bucket_count,
                aggregator = self.aggregator.name(),
            );
        }

        relay_statsd::metric!(
            histogram(MetricHistograms::BucketsFlushed) = total_bucket_count,
            aggregator = self.aggregator.name(),
        );

        if let Some(ref receiver) = self.receiver {
            receiver.send(FlushBuckets { buckets })
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
            timer(MetricTimers::AggregatorServiceDuration),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
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
        Bucket {
            timestamp: UnixTimestamp::from_secs(999994711),
            width: 0,
            name: "c:transactions/foo".to_owned(),
            value: BucketValue::counter(42.into()),
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
