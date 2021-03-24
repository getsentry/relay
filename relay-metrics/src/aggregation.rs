use std::collections::BinaryHeap;
use std::collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use actix::prelude::*;

use serde::{Deserialize, Serialize};

use relay_common::{MonotonicResult, UnixTimestamp};

use crate::{Metric, MetricValue};

/// Anything that can be merged into a [`BucketValue`].
/// Currently either a [`MetricValue`] or another BucketValue.
trait MergeValue: Into<BucketValue> {
    fn merge_into(self, bucket_value: &mut BucketValue) -> Result<(), AggregateMetricsError>;
}

impl MergeValue for MetricValue {
    fn merge_into(self, bucket_value: &mut BucketValue) -> Result<(), AggregateMetricsError> {
        match (bucket_value, self) {
            (BucketValue::Counter(counter), MetricValue::Counter(value)) => {
                *counter += value;
            }
            (BucketValue::Distribution(distribution), MetricValue::Distribution(value)) => {
                distribution.push(value)
            }
            (BucketValue::Set(set), MetricValue::Set(value)) => {
                set.insert(value);
            }
            (BucketValue::Gauge(gauge), MetricValue::Gauge(value)) => {
                *gauge = value;
            }
            _ => {
                return Err(AggregateMetricsError);
            }
        }

        Ok(())
    }
}

/// The aggregated value stored in a bucket.
#[derive(Debug, Clone)]
pub enum BucketValue {
    /// Aggregates [`MetricValue::Counter`] values by adding them.
    Counter(f64),
    /// Aggregates [`MetricValue::Distribution`] values by collecting their values.
    Distribution(Vec<f64>),
    /// Aggregates [`MetricValue::Set`] values by storing their values in a set.
    Set(BTreeSet<u32>),
    /// Aggregates [`MetricValue::Gauge`] values by overwriting the previous value.
    Gauge(f64),
}

impl From<MetricValue> for BucketValue {
    fn from(value: MetricValue) -> Self {
        match value {
            MetricValue::Counter(value) => Self::Counter(value),
            MetricValue::Distribution(value) => Self::Distribution(vec![value]),
            MetricValue::Set(value) => Self::Set(std::iter::once(value).collect()),
            MetricValue::Gauge(value) => Self::Gauge(value),
        }
    }
}

impl MergeValue for BucketValue {
    fn merge_into(self, bucket_value: &mut BucketValue) -> Result<(), AggregateMetricsError> {
        match (bucket_value, self) {
            (BucketValue::Counter(lhs), BucketValue::Counter(rhs)) => *lhs += rhs,
            (BucketValue::Distribution(lhs), BucketValue::Distribution(rhs)) => lhs.extend(rhs),
            (BucketValue::Set(lhs), BucketValue::Set(rhs)) => lhs.extend(rhs),
            (BucketValue::Gauge(lhs), BucketValue::Gauge(rhs)) => *lhs = rhs,
            _ => return Err(AggregateMetricsError),
        }

        Ok(())
    }
}

/// A bucket collecting metric values for a given metric name, time window, and tag combination.
#[derive(Clone, Debug)]
pub struct Bucket {
    /// The start time of the time window. The length of the time window is stored in the
    /// [`Aggregator`].
    pub timestamp: UnixTimestamp,
    /// Name of the metric. See [`Metric::name`].
    pub metric_name: String,
    /// The value of the bucket.
    pub value: BucketValue,
    /// See [`Metric::tags``]. Every combination of tags results in a different bucket.
    pub tags: BTreeMap<String, String>,
}

impl Bucket {
    fn from_parts(key: BucketKey, value: BucketValue) -> Self {
        Self {
            timestamp: key.timestamp,
            metric_name: key.metric_name,
            value,
            tags: key.tags,
        }
    }
}

/// Any error that may occur during aggregation.
#[derive(Debug)]
pub struct AggregateMetricsError;

impl fmt::Display for AggregateMetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("failed to aggregate metrics")
    }
}

impl Error for AggregateMetricsError {}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BucketKey {
    timestamp: UnixTimestamp,
    metric_name: String,
    tags: BTreeMap<String, String>,
}
// TODO: use better types and names
/// Parameters used by the [`Aggregator`].
///
/// `initial_delay` and `debounce_delay` should be multiples of `bucket_interval`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AggregatorConfig {
    bucket_interval: u64,

    initial_delay: u64,

    debounce_delay: u64,
}

impl AggregatorConfig {
    /// The wall clock time width of each bucket in seconds.
    pub fn bucket_interval(&self) -> Duration {
        Duration::from_secs(self.bucket_interval)
    }

    /// The initial flush delay after the end of a bucket's original time window.
    pub fn initial_delay(&self) -> Duration {
        Duration::from_secs(self.initial_delay)
    }

    /// The delay to debounce backdated flushes.
    pub fn debounce_delay(&self) -> Duration {
        Duration::from_secs(self.debounce_delay)
    }

    /// Gets the instant at which this bucket will be flushed, i.e. removed from the aggregator and
    /// sent onwards.
    ///
    /// Recent buckets are flushed after a grace period of `initial_delay`. Backdated buckets (i.e.
    /// buckets that lie in the past) are flushed after the shorter `debounce_delay`.
    fn get_flush_time(&self, bucket_timestamp: UnixTimestamp) -> Instant {
        let now = Instant::now();

        if let MonotonicResult::Instant(instant) = bucket_timestamp.to_monotonic() {
            let bucket_end = instant + self.bucket_interval();
            let initial_flush = bucket_end + self.initial_delay();
            if initial_flush > now {
                // If the initial flush is still pending, use that.
                return initial_flush;
            }
        }

        // If the initial flush time has passed or cannot be represented, debounce future flushes
        // with the `debounce_delay` starting now.
        now + self.debounce_delay()
    }
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            bucket_interval: 10,
            initial_delay: 30,
            debounce_delay: 10,
        }
    }
}

/// Reference to a bucket in the [`Aggregator`] with a defined flush time.
///
/// This type implements an inverted total ordering. The maximum queued bucket has the lowest flush
/// time, which is suitable for using it in a [`BinaryHeap`].
#[derive(Debug)]
struct QueuedBucket {
    flush_at: Instant,
    key: BucketKey,
}

impl QueuedBucket {
    /// Creates a new `QueuedBucket` with a given flush time.
    fn new(flush_at: Instant, key: BucketKey) -> Self {
        Self { flush_at, key }
    }

    /// Returns `true` if the flush time has elapsed.
    fn elapsed(&self) -> bool {
        Instant::now() > self.flush_at
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
        other.flush_at.partial_cmp(&self.flush_at)
    }
}

impl Ord for QueuedBucket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Comparing order is reversed to convert the max heap into a min heap
        other.flush_at.cmp(&self.flush_at)
    }
}

/// A message containing a vector of buckets to be flushed.
#[derive(Clone, Debug)]
pub struct FlushBuckets {
    buckets: Vec<Bucket>,
}

impl FlushBuckets {
    /// Creates a new message by consuming a vector of buckets.
    pub fn new(buckets: Vec<Bucket>) -> Self {
        Self { buckets }
    }

    /// Consumes the buckets contained in this message.
    pub fn into_buckets(self) -> Vec<Bucket> {
        self.buckets
    }
}

impl Message for FlushBuckets {
    type Result = Result<(), Vec<Bucket>>;
}

/// A collector of [`Metric`] submissions. Every project has one aggregator.
///
/// # Aggregation
///
/// Each metric is dispatched into the correct [`Bucket`] depending on their name, tags and timestamp.
/// Each bucket stores the accumulated value of submitted metrics:
///
/// - `Counter`: sum of values
/// - `Distribution`: a vector of values
/// - `Set`: a set of values
/// - `Gauge`: the latest submitted value
///
/// # Flushing
///
/// Buckets are flushed (i.e. sent onwards to a receiver) after its time window and a grace period
/// have passed. Metrics with a recent timestamp are given a longer grace period than backdated metrics,
/// which are flushed after a shorter debounce delay (see [`AggregatorConfig`]).
pub struct Aggregator {
    config: AggregatorConfig,
    buckets: HashMap<BucketKey, BucketValue>,
    queue: BinaryHeap<QueuedBucket>,
    receiver: Recipient<FlushBuckets>,
}

impl Aggregator {
    /// Create a new aggregator and connect it to `receiver`, to which batches of buckets will
    /// be sent in intervals based on `config`.
    pub fn new(config: AggregatorConfig, receiver: Recipient<FlushBuckets>) -> Self {
        Self {
            config,
            buckets: HashMap::new(),
            queue: BinaryHeap::new(),
            receiver,
        }
    }

    /// Determines which bucket a timestamp is assigned to. The bucket timestamp is the input timestamp
    /// rounded by the configured `bucket_interval`.
    fn get_bucket_timestamp(&self, timestamp: UnixTimestamp) -> UnixTimestamp {
        // We know this must be UNIX timestamp because we need reliable match even with system
        // clock skew over time.
        let ts = (timestamp.as_secs() / self.config.bucket_interval) * self.config.bucket_interval;
        UnixTimestamp::from_secs(ts)
    }

    fn merge_in<T: MergeValue>(
        &mut self,
        key: BucketKey,
        value: T,
    ) -> Result<(), AggregateMetricsError> {
        let timestamp = key.timestamp;

        match self.buckets.entry(key) {
            Entry::Occupied(mut entry) => {
                value.merge_into(entry.get_mut())?;
            }
            Entry::Vacant(entry) => {
                let flush_at = self.config.get_flush_time(timestamp);
                self.queue
                    .push(QueuedBucket::new(flush_at, entry.key().clone()));
                entry.insert(value.into());
            }
        }

        Ok(())
    }

    /// Inserts a metric into the corresponding bucket. If no bucket exists for the given
    /// bucket key, a new bucket will be created.
    ///
    /// Fails if a bucket for this metric name already exists with a different type.
    pub fn insert(&mut self, metric: Metric) -> Result<(), AggregateMetricsError> {
        let key = BucketKey {
            timestamp: self.get_bucket_timestamp(metric.timestamp),
            metric_name: metric.name,
            tags: metric.tags,
        };
        self.merge_in(key, metric.value)
    }

    /// Merge a bucket with the existing buckets. If no bucket exists for the given
    /// bucket key, a new bucket will be created.
    pub fn merge(&mut self, bucket: Bucket) -> Result<(), AggregateMetricsError> {
        let key = BucketKey {
            timestamp: bucket.timestamp,
            metric_name: bucket.metric_name,
            tags: bucket.tags,
        };
        self.merge_in(key, bucket.value)
    }

    /// Iterates over `buckets` and merges them using [`Self::merge`].
    pub fn merge_all<I>(&mut self, buckets: I) -> Result<(), AggregateMetricsError>
    where
        I: IntoIterator<Item = Bucket>,
    {
        for bucket in buckets.into_iter() {
            self.merge(bucket)?;
        }

        Ok(())
    }

    fn try_flush(&mut self, context: &mut <Self as Actor>::Context) {
        let mut buckets = Vec::new();

        while self.queue.peek().map_or(false, |flush| flush.elapsed()) {
            let flush = self.queue.pop().unwrap();
            let value = self.buckets.remove(&flush.key).unwrap();
            buckets.push(Bucket::from_parts(flush.key, value));
        }

        if buckets.is_empty() {
            return;
        }

        self.receiver
            .send(FlushBuckets::new(buckets))
            .into_actor(self)
            .and_then(|result, slf, _ctx| {
                if let Err(buckets) = result {
                    // TODO: Decide if we need to log errors
                    slf.merge_all(buckets).ok();
                }
                fut::ok(())
            })
            .drop_err()
            .spawn(context);
    }
}

impl fmt::Debug for Aggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("config", &self.config)
            .field("buckets", &self.buckets)
            .field("queue", &self.queue)
            .field("receiver", &format_args!("Recipient<FlushBuckets>"))
            .finish()
    }
}

impl Actor for Aggregator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        relay_log::info!("aggregator started");

        // TODO: Consider a better approach than busy polling
        ctx.run_interval(Duration::from_millis(500), Self::try_flush);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("aggregator stopped");
    }
}

/// Message containing a [`Metric`] to be inserted.
#[derive(Debug)]
pub struct InsertMetric {
    metric: Metric,
}

impl InsertMetric {
    /// Create a new message containing a [`Metric`].
    pub fn new(metric: Metric) -> Self {
        Self { metric }
    }
}

impl Message for InsertMetric {
    type Result = Result<(), AggregateMetricsError>;
}

impl Handler<InsertMetric> for Aggregator {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, message: InsertMetric, _context: &mut Self::Context) -> Self::Result {
        let InsertMetric { metric } = message;
        self.insert(metric)
    }
}

#[cfg(test)]
mod tests {
    use futures::future::Future;
    use std::sync::{Arc, RwLock};

    use super::*;
    use crate::MetricUnit;

    struct BucketCountInquiry;

    impl Message for BucketCountInquiry {
        type Result = usize;
    }

    impl Handler<BucketCountInquiry> for Aggregator {
        type Result = usize;

        fn handle(&mut self, _: BucketCountInquiry, _: &mut Self::Context) -> Self::Result {
            self.buckets.len()
        }
    }

    #[derive(Default)]
    struct ReceivedData {
        buckets: Vec<Bucket>,
    }

    #[derive(Clone, Default)]
    struct TestReceiver {
        // TODO: Better way to communicate with Actor after it's started?
        // Messages, maybe?
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

    impl Actor for TestReceiver {
        type Context = Context<Self>;
    }

    impl Handler<FlushBuckets> for TestReceiver {
        type Result = Result<(), Vec<Bucket>>;

        fn handle(&mut self, msg: FlushBuckets, _ctx: &mut Self::Context) -> Self::Result {
            let buckets = msg.into_buckets();
            relay_log::debug!("received buckets: {:#?}", buckets);
            if self.reject_all {
                return Err(buckets);
            }
            self.add_buckets(buckets);
            Ok(())
        }
    }

    fn some_metric() -> Metric {
        Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::None,
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(4711),
            tags: BTreeMap::new(),
        }
    }

    #[test]
    fn test_merge_counters() {
        relay_test::setup();

        let config = AggregatorConfig::default();
        let receiver = TestReceiver::start_default().recipient();
        let mut aggregator = Aggregator::new(config, receiver);

        let metric1 = some_metric();

        let mut metric2 = metric1.clone();
        metric2.value = MetricValue::Counter(43.);
        aggregator.insert(metric1).unwrap();
        aggregator.insert(metric2).unwrap();

        insta::assert_debug_snapshot!(aggregator.buckets, @r###"
        {
            BucketKey {
                timestamp: UnixTimestamp(4710),
                metric_name: "foo",
                tags: {},
            }: Counter(
                85.0,
            ),
        }
        "###);
    }

    #[test]
    fn test_merge_similar_timestamps() {
        relay_test::setup();
        let config = AggregatorConfig {
            bucket_interval: 10,
            ..AggregatorConfig::default()
        };
        let receiver = TestReceiver::start_default().recipient();
        let mut aggregator = Aggregator::new(config, receiver);

        let metric1 = some_metric();

        let mut metric2 = metric1.clone();
        metric2.timestamp = UnixTimestamp::from_secs(4712);

        let mut metric3 = metric1.clone();
        metric3.timestamp = UnixTimestamp::from_secs(4721);
        aggregator.insert(metric1).unwrap();
        aggregator.insert(metric2).unwrap();
        aggregator.insert(metric3).unwrap();

        let mut buckets: Vec<(BucketKey, BucketValue)> = aggregator.buckets.into_iter().collect();
        buckets.sort_by(|a, b| a.0.timestamp.cmp(&b.0.timestamp));
        insta::assert_debug_snapshot!(buckets, @r###"
        [
            (
                BucketKey {
                    timestamp: UnixTimestamp(4710),
                    metric_name: "foo",
                    tags: {},
                },
                Counter(
                    84.0,
                ),
            ),
            (
                BucketKey {
                    timestamp: UnixTimestamp(4720),
                    metric_name: "foo",
                    tags: {},
                },
                Counter(
                    42.0,
                ),
            ),
        ]
        "###);
    }

    #[test]
    fn test_mixup_types() {
        relay_test::setup();
        let config = AggregatorConfig {
            bucket_interval: 10,
            ..AggregatorConfig::default()
        };
        let receiver = TestReceiver::start_default().recipient();
        let mut aggregator = Aggregator::new(config, receiver);

        let metric1 = some_metric();

        let mut metric2 = metric1.clone();
        metric2.value = MetricValue::Set(123);

        aggregator.insert(metric1).unwrap();
        assert!(matches!(aggregator.insert(metric2), Result::Err(_)));

        insta::assert_debug_snapshot!(aggregator.buckets, @r###"
        {
            BucketKey {
                timestamp: UnixTimestamp(4710),
                metric_name: "foo",
                tags: {},
            }: Counter(
                42.0,
            ),
        }
        "###);
    }

    #[test]
    fn test_mixup_types_with_different_tags() {
        relay_test::setup();
        let config = AggregatorConfig {
            bucket_interval: 10,
            ..AggregatorConfig::default()
        };
        let receiver = TestReceiver::start_default().recipient();
        let mut aggregator = Aggregator::new(config, receiver);

        let metric1 = some_metric();

        let mut metric2 = metric1.clone();
        metric2.value = MetricValue::Set(123);
        metric2.tags.insert("foo".to_owned(), "bar".to_owned());

        aggregator.insert(metric1).unwrap();

        // Should not be able to insert conflicting type with the same name, even if
        // tags are different:
        assert!(matches!(aggregator.insert(metric2), Result::Err(_)));
    }

    #[test]
    fn test_flush_bucket() {
        relay_test::setup();
        let receiver = TestReceiver::default();
        relay_test::block_fn(|| {
            let config = AggregatorConfig {
                bucket_interval: 1,
                initial_delay: 0,
                debounce_delay: 0,
            };
            let recipient = receiver.clone().start().recipient();
            let aggregator = Aggregator::new(config, recipient).start();

            let mut metric = some_metric();
            metric.timestamp = UnixTimestamp::now();
            aggregator
                .send(InsertMetric { metric })
                .and_then(move |_| aggregator.send(BucketCountInquiry))
                .map_err(|_| ())
                .and_then(|bucket_count| {
                    // Immediately after sending the metric, nothing has been flushed:
                    assert_eq!(bucket_count, 1);
                    assert_eq!(receiver.bucket_count(), 0);
                    Ok(())
                })
                .and_then(|_| {
                    // Wait until flush delay has passed
                    relay_test::delay(Duration::from_millis(1100)).map_err(|_| ())
                })
                .and_then(|_| {
                    // After the flush delay has passed, the receiver should have the bucket:
                    assert_eq!(receiver.bucket_count(), 1);
                    Ok(())
                })
        })
        .ok();
    }

    #[test]
    fn test_merge_back() {
        relay_test::setup();

        // Create a receiver which accepts nothing:
        let receiver = TestReceiver {
            reject_all: true,
            ..TestReceiver::default()
        };

        relay_test::block_fn(|| {
            let config = AggregatorConfig {
                bucket_interval: 1,
                initial_delay: 0,
                debounce_delay: 0,
            };
            let recipient = receiver.clone().start().recipient();
            let aggregator = Aggregator::new(config, recipient).start();

            let mut metric = some_metric();
            metric.timestamp = UnixTimestamp::now();
            aggregator
                .send(InsertMetric { metric })
                .map_err(|_| ())
                .and_then(|_| {
                    // Immediately after sending the metric, nothing has been flushed:
                    assert_eq!(receiver.bucket_count(), 0);
                    Ok(())
                })
                .map_err(|_| ())
                .and_then(|_| {
                    // Wait until flush delay has passed
                    relay_test::delay(Duration::from_millis(1100)).map_err(|_| ())
                })
                .and_then(move |_| aggregator.send(BucketCountInquiry).map_err(|_| ()))
                .and_then(|bucket_count| {
                    // After the flush delay has passed, the receiver should still not have the
                    // bucket
                    assert_eq!(bucket_count, 1);
                    assert_eq!(receiver.bucket_count(), 0);
                    Ok(())
                })
        })
        .ok();
    }
}
