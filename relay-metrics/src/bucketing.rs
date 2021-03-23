use std::collections::BinaryHeap;
use std::collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use actix::prelude::*;

use serde::{Deserialize, Serialize};

use relay_common::{UnboundedInstant, UnixTimestamp};

use crate::{Metric, MetricValue};

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

impl BucketValue {
    fn insert(&mut self, value: MetricValue) -> Result<(), AggregateMetricsError> {
        match (self, value) {
            (Self::Counter(counter), MetricValue::Counter(value)) => {
                *counter += value;
            }
            (Self::Distribution(distribution), MetricValue::Distribution(value)) => {
                distribution.push(value)
            }
            (Self::Set(set), MetricValue::Set(value)) => {
                set.insert(value);
            }
            (Self::Gauge(gauge), MetricValue::Gauge(value)) => {
                *gauge = value;
            }
            _ => {
                return Err(AggregateMetricsError);
            }
        }

        Ok(())
    }

    fn merge(&mut self, other: Self) -> Result<(), AggregateMetricsError> {
        match (self, other) {
            (Self::Counter(lhs), Self::Counter(rhs)) => *lhs += rhs,
            (Self::Distribution(lhs), Self::Distribution(rhs)) => lhs.extend(rhs),
            (Self::Set(lhs), Self::Set(rhs)) => lhs.extend(rhs),
            (Self::Gauge(lhs), Self::Gauge(rhs)) => *lhs = rhs,
            _ => return Err(AggregateMetricsError),
        }

        Ok(())
    }
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
    /// Recent buckets are flushed after a grace period of `initial_delay`. Backdated
    /// buckets (i.e. buckets that lie in the past) are flushed after the shorter `debounce_delay`.
    fn get_flush_time(&self, bucket_timestamp: UnixTimestamp) -> Instant {
        let now = Instant::now();

        match bucket_timestamp.to_instant() {
            // For a bucket from the distance past, use debounce delay
            UnboundedInstant::Earlier => now + self.debounce_delay(),
            UnboundedInstant::Instant(instant) => {
                let bucket_end = instant + self.bucket_interval();
                let initial_flush = bucket_end + self.initial_delay();
                if initial_flush > now {
                    // If the initial flush is still pending, use that.
                    initial_flush
                } else {
                    // If the initial flush time has passed, debounce future flushes with the
                    // `debounce_delay` starting now.
                    now + self.debounce_delay()
                }
            }
            // Bucket from distant future. Best we can do is schedule it after initial delay
            // TODO: or refuse metric altogether?
            UnboundedInstant::Later => now + self.initial_delay(),
        }
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

/// Collector of submitted [`Metric`]s.
/// Each metric is added to its corresponding [`Bucket`], which is identified by the metric's
/// name, tags and timestamp.
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

    /// Inserts a metric into the corresponding bucket. If no bucket exists for the given
    /// bucket key, a new bucket will be created.
    pub fn insert(&mut self, metric: Metric) -> Result<(), AggregateMetricsError> {
        let bucket_timestamp = self.get_bucket_timestamp(metric.timestamp);

        let key = BucketKey {
            timestamp: bucket_timestamp,
            metric_name: metric.name,
            tags: metric.tags,
        };

        match self.buckets.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(metric.value)?;
            }
            Entry::Vacant(entry) => {
                let flush_at = self.config.get_flush_time(bucket_timestamp);
                self.queue
                    .push(QueuedBucket::new(flush_at, entry.key().clone()));
                entry.insert(metric.value.into());
            }
        }

        Ok(())
    }

    /// Add the values of `bucket` to this aggregator.
    /// This assumes that the bucket was created
    /// by the same aggregator, i.e. with the same `bucket_interval`. If a matching bucket already
    /// exists, the buckets are merged. Else, the new bucket is simply added.
    pub fn merge(&mut self, bucket: Bucket) -> Result<(), AggregateMetricsError> {
        let key = BucketKey {
            timestamp: bucket.timestamp,
            metric_name: bucket.metric_name,
            tags: bucket.tags,
        };

        match self.buckets.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge(bucket.value)?;
            }
            Entry::Vacant(entry) => {
                let flush_at = self.config.get_flush_time(bucket.timestamp);
                self.queue
                    .push(QueuedBucket::new(flush_at, entry.key().clone()));
                entry.insert(bucket.value);
            }
        }

        Ok(())
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
    use super::*;
    use crate::MetricUnit;

    struct TestReceiver;

    impl Actor for TestReceiver {
        type Context = Context<Self>;
    }

    impl Handler<FlushBuckets> for TestReceiver {
        type Result = Result<(), Vec<Bucket>>;

        fn handle(&mut self, msg: FlushBuckets, _ctx: &mut Self::Context) -> Self::Result {
            relay_log::debug!("received buckets: {:#?}", msg.into_buckets());
            Ok(())
        }
    }

    #[test]
    fn test_merge_counters() {
        // TODO: Setup tests

        let config = AggregatorConfig::default();
        let receiver = TestReceiver.start().recipient();
        let mut aggregator = Aggregator::new(config, receiver);

        let metric1 = Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::None,
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(4711),
            tags: BTreeMap::new(),
        };

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
        let config = AggregatorConfig {
            bucket_interval: 10,
            ..AggregatorConfig::default()
        };
        let receiver = TestReceiver.start().recipient();
        let mut aggregator = Aggregator::new(config, receiver);

        let metric1 = Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::None,
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(4711),
            tags: BTreeMap::new(),
        };

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
        let config = AggregatorConfig {
            bucket_interval: 10,
            ..AggregatorConfig::default()
        };
        let receiver = TestReceiver.start().recipient();
        let mut aggregator = Aggregator::new(config, receiver);

        let metric1 = Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::None,
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(4711),
            tags: BTreeMap::new(),
        };

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
}
