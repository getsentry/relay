#![allow(missing_docs)]
use std::collections::BinaryHeap;
use std::collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use actix::prelude::*;
use serde::Deserialize;

use relay_common::UnixTimestamp;

use crate::{Metric, MetricValue};

#[derive(Debug, Clone)]
pub enum BucketValue {
    Counter(f64),
    Distribution(Vec<f64>),
    Set(BTreeSet<u32>),
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

#[derive(Clone, Debug)]
pub struct Bucket {
    pub timestamp: UnixTimestamp,
    pub metric_name: String,
    pub value: BucketValue,
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
#[derive(Clone, Debug, Deserialize)]
pub struct AggregatorConfig {
    /// The wall clock time width of each bucket in seconds.
    bucket_interval: u64,
    /// The initial flush delay after the end of a bucket's original time window.
    initial_delay: u64,
    /// The delay to debounce backdated flushes.
    debounce_delay: u64,
}

impl AggregatorConfig {
    pub fn bucket_interval(&self) -> Duration {
        Duration::from_secs(self.bucket_interval)
    }

    pub fn initial_delay(&self) -> Duration {
        Duration::from_secs(self.initial_delay)
    }

    pub fn debounce_delay(&self) -> Duration {
        Duration::from_secs(self.debounce_delay)
    }

    /// TODO: Doc
    fn get_flush_time(&self, bucket_timestamp: UnixTimestamp) -> Instant {
        let now = Instant::now();

        let bucket_end = bucket_timestamp.to_instant() + self.bucket_interval();
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
    /// Creates a new `QueuedBucket` witha  given flush time.
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

#[derive(Clone, Debug)]
pub struct FlushBuckets {
    buckets: Vec<Bucket>,
}

impl FlushBuckets {
    pub fn new(buckets: Vec<Bucket>) -> Self {
        Self { buckets }
    }

    pub fn into_buckets(self) -> Vec<Bucket> {
        self.buckets
    }
}

impl Message for FlushBuckets {
    type Result = Result<(), Vec<Bucket>>;
}

/// TODO
pub struct Aggregator {
    config: AggregatorConfig,
    buckets: HashMap<BucketKey, BucketValue>,
    queue: BinaryHeap<QueuedBucket>,
    receiver: Recipient<FlushBuckets>,
}

impl Aggregator {
    pub fn new(config: AggregatorConfig, receiver: Recipient<FlushBuckets>) -> Self {
        Self {
            config,
            buckets: HashMap::new(),
            queue: BinaryHeap::new(),
            receiver,
        }
    }

    /// TODO: Doc
    fn get_bucket_timestamp(&self, timestamp: UnixTimestamp) -> UnixTimestamp {
        // TODO: We know this must be UNIX timestamp because we need reliable match even with system
        // clock skew over time.
        let ts = (timestamp.as_secs() / self.config.bucket_interval) * self.config.bucket_interval;
        UnixTimestamp::from_secs(ts)
    }

    /// TODO: doc
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

#[derive(Debug)]
pub struct InsertMetric {
    metric: Metric,
}

impl InsertMetric {
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

    /*
    #[test]
    fn test_merge_counters() {
        let mut aggregator = Aggregator::new(AggregatorConfig::default());

        let metric1 = Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::None,
            value: MetricValue::Integer(42),
            ty: MetricType::Counter,
            timestamp: UnixTimestamp::from_secs(4711),
            tags: BTreeMap::new(),
        };

        let mut metric2 = metric1.clone();
        metric2.value = MetricValue::Integer(43);
        aggregator.insert(metric1).unwrap();
        aggregator.insert(metric2).unwrap();

        insta::assert_debug_snapshot!(aggregator.buckets, @r###"
        {
            BucketKey {
                timestamp: UnixTimestamp(4710),
                metric_name: "foo",
                tags: {},
            }: Counter(
                Integer(
                    85,
                ),
            ),
        }
        "###);
    }

    #[test]
    fn test_merge_similar_timestamps() {
        let mut aggregator = Aggregator::new(AggregatorConfig {
            bucket_interval: 10,
            ..AggregatorConfig::default()
        });

        let metric1 = Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::None,
            value: MetricValue::Integer(42),
            ty: MetricType::Counter,
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

        insta::assert_debug_snapshot!(aggregator.buckets, @r###"
        {
            BucketKey {
                timestamp: UnixTimestamp(4710),
                metric_name: "foo",
                tags: {},
            }: Counter(
                Integer(
                    84,
                ),
            ),
            BucketKey {
                timestamp: UnixTimestamp(4720),
                metric_name: "foo",
                tags: {},
            }: Counter(
                Integer(
                    42,
                ),
            ),
        }
        "###);
    }
    */
}
