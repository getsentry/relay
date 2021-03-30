use std::collections::BinaryHeap;
use std::collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use actix::prelude::*;

use serde::{Deserialize, Serialize};

use relay_common::{MonotonicResult, UnixTimestamp};

use crate::{Metric, MetricType, MetricUnit, MetricValue};

/// The [aggregated value](Bucket::value) of a metric bucket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum BucketValue {
    /// Aggregates [`MetricValue::Counter`] values by adding them into a single value.
    ///
    /// ```notrust
    /// 2, 1, 3, 2 => 8
    /// ```
    #[serde(rename = "c")]
    Counter(f64),
    /// Aggregates [`MetricValue::Distribution`] values by collecting their values.
    ///
    /// ```notrust
    /// 2, 1, 3, 2 => [1, 2, 2, 3]
    /// ```
    #[serde(rename = "d")]
    Distribution(Vec<f64>),
    /// Aggregates [`MetricValue::Set`] values by storing their hash values in a set.
    ///
    /// ```notrust
    /// 2, 1, 3, 2 => {1, 2, 3}
    /// ```
    #[serde(rename = "s")]
    Set(BTreeSet<u32>),
    /// Aggregates [`MetricValue::Gauge`] values always retaining the single last value.
    ///
    /// **Note**: This aggregation is not commutative.
    ///
    /// ```notrust
    /// 1, 2, 3, 2 => 2
    /// ```
    #[serde(rename = "g")]
    Gauge(f64),
}

impl BucketValue {
    /// Returns the type of this value.
    pub fn ty(&self) -> MetricType {
        match self {
            Self::Counter(_) => MetricType::Counter,
            Self::Distribution(_) => MetricType::Distribution,
            Self::Set(_) => MetricType::Set,
            Self::Gauge(_) => MetricType::Gauge,
        }
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

/// A value that can be merged into a [`BucketValue`].
///
/// Currently either a [`MetricValue`] or another `BucketValue`.
trait MergeValue: Into<BucketValue> {
    /// Merges `self` into the given `bucket_value`.
    ///
    /// Aggregation is performed according to the rules documented in [`BucketValue`].
    fn merge_into(self, bucket_value: &mut BucketValue) -> Result<(), AggregateMetricsError>;
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

/// Error returned when parsing or serializing a [`Bucket`].
#[derive(Debug)]
pub struct ParseBucketError {
    inner: serde_json::Error,
}

impl fmt::Display for ParseBucketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("failed to parse metric bucket")
    }
}

impl Error for ParseBucketError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.inner)
    }
}

/// An aggregation of metric values by the [`Aggregator`].
///
/// As opposed to single metric values, bucket aggregations can carry multiple values. See
/// [`MetricType`] for a description on how values are aggregated in buckets. Values are aggregated
/// by metric name, type, time window, and all tags. Particularly, this allows metrics to have the
/// same name even if their types differ.
///
/// See the [crate documentation](crate) for general information on Metrics.
///
/// # Submission Protocol
///
/// Buckets are always represented as JSON. The type of the `value` field is determined by the
/// metric type, see [`BucketValue`] for a description of all valid values.
///
/// ```json
/// [
///   {
///     "name": "endpoint.response_time",
///     "unit": "ms",
///     "value": [36, 49, 57, 68],
///     "type": "d",
///     "timestamp": 1615889440,
///     "tags": {
///       "route": "user_index"
///     }
///   },
///   {
///     "name": "endpoint.hits",
///     "value": 4,
///     "type": "c",
///     "timestamp": 1615889440,
///     "tags": {
///       "route": "user_index"
///     }
///   }
/// ]
/// ```
///
/// To parse a submission payload, use [`Bucket::parse_all`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Bucket {
    /// The start time of the time window. The length of the time window is stored in the
    /// [`Aggregator`].
    pub timestamp: UnixTimestamp,
    /// The name of the metric without its unit.
    ///
    /// See [`Metric::name`].
    pub name: String,
    /// The unit of the metric value.
    ///
    /// See [`Metric::unit`].
    #[serde(default, skip_serializing_if = "MetricUnit::is_none")]
    pub unit: MetricUnit,
    /// The aggregated values in this bucket.
    ///
    /// [Counters](BucketValue::Counter) and [gauges](BucketValue::Gauge) store a single value while
    /// [distributions](MetricType::Distribution) and [sets](MetricType::Set) store the full set of
    /// reported values.
    #[serde(flatten)]
    pub value: BucketValue,
    /// A list of tags adding dimensions to the metric for filtering and aggregation.
    ///
    /// See [`Metric::tags`]. Every combination of tags results in a different bucket.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl Bucket {
    fn from_parts(key: BucketKey, value: BucketValue) -> Self {
        Self {
            timestamp: key.timestamp,
            name: key.metric_name,
            unit: key.metric_unit,
            value,
            tags: key.tags,
        }
    }

    /// Parses a single metric bucket from the JSON protocol.
    pub fn parse(slice: &[u8]) -> Result<Self, ParseBucketError> {
        serde_json::from_slice(slice).map_err(|inner| ParseBucketError { inner })
    }

    /// Parses a set of metric bucket from the JSON protocol.
    pub fn parse_all(slice: &[u8]) -> Result<Vec<Bucket>, ParseBucketError> {
        serde_json::from_slice(slice).map_err(|inner| ParseBucketError { inner })
    }

    /// Serializes the given buckets to the JSON protocol.
    pub fn serialize_all(buckets: &[Self]) -> Result<String, ParseBucketError> {
        serde_json::to_string(&buckets).map_err(|inner| ParseBucketError { inner })
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
    metric_type: MetricType,
    metric_unit: MetricUnit,
    tags: BTreeMap<String, String>,
}
/// Parameters used by the [`Aggregator`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AggregatorConfig {
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
    pub initial_delay: u64,

    /// The delay in seconds to wait before flushing a backdated buckets.
    ///
    /// Defaults to `10` seconds. Metrics can be sent with a past timestamp. Relay wait this time
    /// before sending such a backdated bucket to the upsteam. This should be lower than
    /// `initial_delay`.
    pub debounce_delay: u64,
}

impl AggregatorConfig {
    /// Returns the time width buckets.
    fn bucket_interval(&self) -> Duration {
        Duration::from_secs(self.bucket_interval)
    }

    /// Returns the initial flush delay after the end of a bucket's original time window.
    fn initial_delay(&self) -> Duration {
        Duration::from_secs(self.initial_delay)
    }

    /// The delay to debounce backdated flushes.
    fn debounce_delay(&self) -> Duration {
        Duration::from_secs(self.debounce_delay)
    }

    /// Returns the instant at which a bucket should be flushed.
    ///
    /// Recent buckets are flushed after a grace period of `initial_delay`. Backdated buckets, that
    /// is, buckets that lie in the past, are flushed after the shorter `debounce_delay`.
    fn get_flush_time(&self, bucket_timestamp: UnixTimestamp) -> Instant {
        let now = Instant::now();

        if let MonotonicResult::Instant(instant) = bucket_timestamp.to_instant() {
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
///
/// Use [`into_buckets`](Self::into_buckets) to access the raw [`Bucket`]s. Handlers must respond to
/// this message with a `Result`:
/// - If flushing has succeeded or the buckets should be dropped for any reason, respond with `Ok`.
/// - If flushing fails and should be retried at a later time, respond with `Err` containing the
///   failed buckets. They will be merged back into the aggregator and flushed at a later time.
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

/// A collector of [`Metric`] submissions. Every project key (DSN) has one Aggregator.
///
/// # Aggregation
///
/// Each metric is dispatched into the a [`Bucket`] depending on its name, type, unit, tags and
/// timestamp. The bucket timestamp is rounded to the precision declared by the
/// [`bucket_interval`](AggregatorConfig::bucket_interval) configuration.
///
/// Each bucket stores the accumulated value of submitted metrics:
///
/// - `Counter`: Sum of values.
/// - `Distribution`: A list of values.
/// - `Set`: A unique set of hashed values.
/// - `Gauge`: The latest submitted value.
///
/// # Conflicts
///
/// Metrics are uniquely identified by the combination of their name, type and unit. It is allowed
/// to send metrics of different types and units under the same name. For example, sending a metric
/// once as set and once as distribution will result in two actual metrics being recorded.
///
/// # Flushing
///
/// Buckets are flushed to a receiver after their time window and a grace period have passed.
/// Metrics with a recent timestamp are given a longer grace period than backdated metrics, which
/// are flushed after a shorter debounce delay. See [`AggregatorConfig`] for configuration options.
///
/// Receivers must implement a handler for the [`FlushBuckets`] message:
///
/// ```
/// use actix::prelude::*;
/// use relay_metrics::{Bucket, FlushBuckets};
///
/// struct BucketReceiver;
///
/// impl Actor for BucketReceiver {
///     type Context = Context<Self>;
/// }
///
/// impl Handler<FlushBuckets> for BucketReceiver {
///     type Result = Result<(), Vec<Bucket>>;
///
///     fn handle(&mut self, msg: FlushBuckets, _ctx: &mut Self::Context) -> Self::Result {
///         // Return `Ok` to consume the buckets or `Err` to send them back
///         Err(msg.into_buckets())
///     }
/// }
/// ```
pub struct Aggregator {
    config: AggregatorConfig,
    buckets: HashMap<BucketKey, BucketValue>,
    queue: BinaryHeap<QueuedBucket>,
    receiver: Recipient<FlushBuckets>,
}

impl Aggregator {
    /// Create a new aggregator and connect it to `receiver`.
    ///
    /// The aggregator will flush a list of buckets to the receiver in regular intervals based on
    /// the given `config`.
    pub fn new(config: AggregatorConfig, receiver: Recipient<FlushBuckets>) -> Self {
        Self {
            config,
            buckets: HashMap::new(),
            queue: BinaryHeap::new(),
            receiver,
        }
    }

    /// Determines which bucket a timestamp is assigned to.
    ///
    /// The bucket timestamp is the input timestamp rounded by the configured `bucket_interval`.
    fn get_bucket_timestamp(&self, timestamp: UnixTimestamp) -> UnixTimestamp {
        // We know this must be UNIX timestamp because we need reliable match even with system
        // clock skew over time.
        let ts = (timestamp.as_secs() / self.config.bucket_interval) * self.config.bucket_interval;
        UnixTimestamp::from_secs(ts)
    }

    /// Merges any mergeable value into the bucket at the given `key`.
    ///
    /// If no bucket exists for the given bucket key, a new bucket will be created.
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

    /// Inserts a metric into the corresponding bucket in this aggregator.
    ///
    /// If no bucket exists for the given bucket key, a new bucket will be created.
    pub fn insert(&mut self, metric: Metric) -> Result<(), AggregateMetricsError> {
        let key = BucketKey {
            timestamp: self.get_bucket_timestamp(metric.timestamp),
            metric_name: metric.name,
            metric_type: metric.value.ty(),
            metric_unit: metric.unit,
            tags: metric.tags,
        };
        self.merge_in(key, metric.value)
    }

    /// Merge a preaggregated bucket into this aggregator.
    ///
    /// If no bucket exists for the given bucket key, a new bucket will be created.
    pub fn merge(&mut self, bucket: Bucket) -> Result<(), AggregateMetricsError> {
        let key = BucketKey {
            timestamp: bucket.timestamp,
            metric_name: bucket.name,
            metric_type: bucket.value.ty(),
            metric_unit: bucket.unit,
            tags: bucket.tags,
        };
        self.merge_in(key, bucket.value)
    }

    /// Merges all given `buckets` into this aggregator.
    ///
    /// Buckets that do not exist yet will be created.
    pub fn merge_all<I>(&mut self, buckets: I) -> Result<(), AggregateMetricsError>
    where
        I: IntoIterator<Item = Bucket>,
    {
        for bucket in buckets.into_iter() {
            self.merge(bucket)?;
        }

        Ok(())
    }

    /// Sends the [`FlushBuckets`] message to the receiver.
    ///
    /// If the receiver returns buckets, they are merged back into the cache.
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

        relay_log::trace!("flushing {} buckets to receiver", buckets.len());

        self.receiver
            .send(FlushBuckets::new(buckets))
            .into_actor(self)
            .and_then(|result, slf, _ctx| {
                if let Err(buckets) = result {
                    relay_log::trace!(
                        "returned {} buckets from receiver, merging back",
                        buckets.len()
                    );
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

/// A message containing a list of [`Metric`]s to be inserted into the aggregator.
#[derive(Debug)]
pub struct InsertMetrics {
    metrics: Vec<Metric>,
}

impl InsertMetrics {
    /// Creates a new message containing a list of [`Metric`]s.
    pub fn new<I>(metrics: I) -> Self
    where
        I: IntoIterator<Item = Metric>,
    {
        Self {
            metrics: metrics.into_iter().collect(),
        }
    }
}

impl Message for InsertMetrics {
    type Result = Result<(), AggregateMetricsError>;
}

impl Handler<InsertMetrics> for Aggregator {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, msg: InsertMetrics, _ctx: &mut Self::Context) -> Self::Result {
        for metric in msg.metrics {
            self.insert(metric)?;
        }

        Ok(())
    }
}

/// A message containing a list of [`Bucket`]s to be inserted into the aggregator.
#[derive(Debug)]
pub struct MergeBuckets {
    buckets: Vec<Bucket>,
}

impl MergeBuckets {
    /// Creates a new message containing a list of [`Bucket`]s.
    pub fn new(buckets: Vec<Bucket>) -> Self {
        Self { buckets }
    }
}

impl Message for MergeBuckets {
    type Result = Result<(), AggregateMetricsError>;
}

impl Handler<MergeBuckets> for Aggregator {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, msg: MergeBuckets, _ctx: &mut Self::Context) -> Self::Result {
        self.merge_all(msg.buckets)
    }
}

#[cfg(test)]
mod tests {
    use futures::future::Future;
    use std::sync::{Arc, RwLock};

    use super::*;

    use crate::{DurationPrecision, MetricUnit};

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
    fn test_parse_buckets() {
        let json = r#"[
          {
            "name": "endpoint.response_time",
            "unit": "ms",
            "value": [36, 49, 57, 68],
            "type": "d",
            "timestamp": 1615889440,
            "tags": {
                "route": "user_index"
            }
          }
        ]"#;

        let buckets = Bucket::parse_all(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(buckets, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                name: "endpoint.response_time",
                unit: Duration(
                    MilliSecond,
                ),
                value: Distribution(
                    [
                        36.0,
                        49.0,
                        57.0,
                        68.0,
                    ],
                ),
                tags: {
                    "route": "user_index",
                },
            },
        ]
        "###);
    }

    #[test]
    fn test_parse_bucket_defaults() {
        let json = r#"[
          {
            "name": "endpoint.hits",
            "value": 4,
            "type": "c",
            "timestamp": 1615889440
          }
        ]"#;

        let buckets = Bucket::parse_all(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(buckets, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                name: "endpoint.hits",
                unit: None,
                value: Counter(
                    4.0,
                ),
                tags: {},
            },
        ]
        "###);
    }

    #[test]
    fn test_buckets_roundtrip() {
        let json = r#"[
  {
    "timestamp": 1615889440,
    "name": "endpoint.response_time",
    "type": "d",
    "value": [
      36.0,
      49.0,
      57.0,
      68.0
    ],
    "tags": {
      "route": "user_index"
    }
  },
  {
    "timestamp": 1615889440,
    "name": "endpoint.hits",
    "type": "c",
    "value": 4.0,
    "tags": {
      "route": "user_index"
    }
  }
]"#;

        let buckets = Bucket::parse_all(json.as_bytes()).unwrap();
        let serialized = serde_json::to_string_pretty(&buckets).unwrap();
        assert_eq!(json, serialized);
    }

    #[test]
    fn test_bucket_value_merge_counter() {
        let mut value = BucketValue::Counter(42.);
        BucketValue::Counter(43.).merge_into(&mut value).unwrap();
        assert_eq!(value, BucketValue::Counter(85.));
    }

    #[test]
    fn test_bucket_value_merge_distribution() {
        let mut value = BucketValue::Distribution(vec![1., 2., 3.]);
        BucketValue::Distribution(vec![2., 4.])
            .merge_into(&mut value)
            .unwrap();
        // TODO: This should be ordered
        assert_eq!(value, BucketValue::Distribution(vec![1., 2., 3., 2., 4.]));
    }

    #[test]
    fn test_bucket_value_merge_set() {
        let mut value = BucketValue::Set(vec![1, 2].into_iter().collect());
        BucketValue::Set(vec![2, 3].into_iter().collect())
            .merge_into(&mut value)
            .unwrap();
        assert_eq!(value, BucketValue::Set(vec![1, 2, 3].into_iter().collect()));
    }

    #[test]
    fn test_bucket_value_merge_gauge() {
        let mut value = BucketValue::Gauge(42.);
        BucketValue::Gauge(43.).merge_into(&mut value).unwrap();
        assert_eq!(value, BucketValue::Gauge(43.));
    }

    #[test]
    fn test_bucket_value_insert_counter() {
        let mut value = BucketValue::Counter(42.);
        MetricValue::Counter(43.).merge_into(&mut value).unwrap();
        assert_eq!(value, BucketValue::Counter(85.));
    }

    #[test]
    fn test_bucket_value_insert_distribution() {
        let mut value = BucketValue::Distribution(vec![1., 2., 3.]);
        MetricValue::Distribution(2.0)
            .merge_into(&mut value)
            .unwrap();
        // TODO: This should be ordered
        assert_eq!(value, BucketValue::Distribution(vec![1., 2., 3., 2.]));
    }

    #[test]
    fn test_bucket_value_insert_set() {
        let mut value = BucketValue::Set(vec![1, 2].into_iter().collect());
        MetricValue::Set(3).merge_into(&mut value).unwrap();
        assert_eq!(value, BucketValue::Set(vec![1, 2, 3].into_iter().collect()));
        MetricValue::Set(2).merge_into(&mut value).unwrap();
        assert_eq!(value, BucketValue::Set(vec![1, 2, 3].into_iter().collect()));
    }

    #[test]
    fn test_bucket_value_insert_gauge() {
        let mut value = BucketValue::Gauge(42.);
        MetricValue::Gauge(43.).merge_into(&mut value).unwrap();
        assert_eq!(value, BucketValue::Gauge(43.));
    }

    #[test]
    fn test_aggregator_merge_counters() {
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
                metric_type: Counter,
                metric_unit: None,
                tags: {},
            }: Counter(
                85.0,
            ),
        }
        "###);
    }

    #[test]
    fn test_aggregator_merge_timestamps() {
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
                    metric_type: Counter,
                    metric_unit: None,
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
                    metric_type: Counter,
                    metric_unit: None,
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
    fn test_aggregator_mixed_types() {
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

        // It's OK to have same name for different types:
        aggregator.insert(metric1).unwrap();
        aggregator.insert(metric2).unwrap();
        assert_eq!(aggregator.buckets.len(), 2);
    }

    #[test]
    fn test_aggregator_mixed_units() {
        relay_test::setup();

        let config = AggregatorConfig {
            bucket_interval: 10,
            ..AggregatorConfig::default()
        };

        let receiver = TestReceiver::start_default().recipient();
        let mut aggregator = Aggregator::new(config, receiver);

        let metric1 = some_metric();

        let mut metric2 = metric1.clone();
        metric2.unit = MetricUnit::Duration(DurationPrecision::Second);

        // It's OK to have same metric with different units:
        aggregator.insert(metric1).unwrap();
        aggregator.insert(metric2).unwrap();

        // TODO: This should convert if units are convertible
        assert_eq!(aggregator.buckets.len(), 2);
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
                .send(InsertMetrics {
                    metrics: vec![metric],
                })
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
                .send(InsertMetrics {
                    metrics: vec![metric],
                })
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
