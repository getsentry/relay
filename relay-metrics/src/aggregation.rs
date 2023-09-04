use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::hash::Hasher;
use std::iter::{FromIterator, FusedIterator};
use std::time::Duration;
use std::{fmt, mem};

use fnv::FnvHasher;
use relay_base_schema::project::ProjectKey;
use relay_common::time::{MonotonicResult, UnixTimestamp};
use relay_system::{
    AsyncResponse, Controller, FromMessage, Interface, NoResponse, Recipient, Sender, Service,
    Shutdown,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;

use crate::statsd::{
    metric_name_tag, metric_type_tag, MetricCounters, MetricGauges, MetricHistograms, MetricSets,
    MetricTimers,
};
use crate::{
    protocol, Bucket, BucketValue, Metric, MetricNamespace, MetricResourceIdentifier, MetricValue,
};

/// Interval for the flush cycle of the [`AggregatorService`].
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// The average size of values when serialized.
const AVG_VALUE_SIZE: usize = 8;

/// The fraction of [`AggregatorConfig::max_flush_bytes`] at which buckets will be split. A value of
/// `2` means that all buckets smaller than half of max_flush_bytes will be moved in their entirety,
/// and buckets larger will be split up.
const BUCKET_SPLIT_FACTOR: usize = 32;

/// A value that can be merged into a [`BucketValue`].
///
/// Currently either a [`MetricValue`] or another `BucketValue`.
trait MergeValue: Into<BucketValue> {
    /// Merges `self` into the given `bucket_value` and returns the additional cost for storing this value.
    ///
    /// Aggregation is performed according to the rules documented in [`BucketValue`].
    fn merge_into(self, bucket_value: &mut BucketValue) -> Result<(), AggregateMetricsError>;
}

impl MergeValue for BucketValue {
    fn merge_into(self, bucket_value: &mut BucketValue) -> Result<(), AggregateMetricsError> {
        match (bucket_value, self) {
            (BucketValue::Counter(lhs), BucketValue::Counter(rhs)) => *lhs += rhs,
            (BucketValue::Distribution(lhs), BucketValue::Distribution(rhs)) => lhs.extend(&rhs),
            (BucketValue::Set(lhs), BucketValue::Set(rhs)) => lhs.extend(rhs),
            (BucketValue::Gauge(lhs), BucketValue::Gauge(rhs)) => lhs.merge(rhs),
            _ => return Err(AggregateMetricsErrorKind::InvalidTypes.into()),
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
                distribution.insert(value);
            }
            (BucketValue::Set(set), MetricValue::Set(value)) => {
                set.insert(value);
            }
            (BucketValue::Gauge(gauge), MetricValue::Gauge(value)) => {
                gauge.insert(value);
            }
            _ => {
                return Err(AggregateMetricsErrorKind::InvalidTypes.into());
            }
        }

        Ok(())
    }
}

/// Estimates the number of bytes needed to encode the tags.
///
/// Note that this does not necessarily match the exact memory footprint of the tags,
/// because data structures or their serialization have overheads.
fn tags_cost(tags: &BTreeMap<String, String>) -> usize {
    tags.iter().map(|(k, v)| k.capacity() + v.capacity()).sum()
}

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
/// [`estimated_size`](Self::estimated_size) for more information.
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
            let org = std::mem::take(distribution);
            let mut new_bucket = bucket.clone();

            let mut iter = org.iter_values();
            bucket.value = BucketValue::Distribution((&mut iter).take(split_at).collect());
            new_bucket.value = BucketValue::Distribution(iter.collect());

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
    50 + bucket.name.len() + tags_cost(&bucket.tags)
}

/// Estimates the number of bytes needed to serialize the bucket.
///
/// Note that this does not match the exact size of the serialized payload. Instead, the size is
/// approximated through the number of contained values, assuming an average size of serialized
/// values.
fn estimate_size(bucket: &Bucket) -> usize {
    estimate_base_size(bucket) + bucket.value.len() * AVG_VALUE_SIZE
}

/// Any error that may occur during aggregation.
#[derive(Debug, Error, PartialEq)]
#[error("failed to aggregate metrics: {kind}")]
pub struct AggregateMetricsError {
    kind: AggregateMetricsErrorKind,
}

impl From<AggregateMetricsErrorKind> for AggregateMetricsError {
    fn from(kind: AggregateMetricsErrorKind) -> Self {
        AggregateMetricsError { kind }
    }
}

#[derive(Debug, Error, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum AggregateMetricsErrorKind {
    /// A metric bucket had invalid characters in the metric name.
    #[error("found invalid characters: {0}")]
    InvalidCharacters(String),
    /// A metric bucket had an unknown namespace in the metric name.
    #[error("found unsupported namespace: {0}")]
    UnsupportedNamespace(MetricNamespace),
    /// A metric bucket's timestamp was out of the configured acceptable range.
    #[error("found invalid timestamp: {0}")]
    InvalidTimestamp(UnixTimestamp),
    /// Internal error: Attempted to merge two metric buckets of different types.
    #[error("found incompatible metric types")]
    InvalidTypes,
    /// A metric bucket had a too long string (metric name or a tag key/value).
    #[error("found invalid string: {0}")]
    InvalidStringLength(String),
    /// A metric bucket is too large for the global bytes limit.
    #[error("total metrics limit exceeded")]
    TotalLimitExceeded,
    /// A metric bucket is too large for the per-project bytes limit.
    #[error("project metrics limit exceeded")]
    ProjectLimitExceeded,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BucketKey {
    project_key: ProjectKey,
    timestamp: UnixTimestamp,
    metric_name: String,
    tags: BTreeMap<String, String>,
}

impl BucketKey {
    // Create a 64-bit hash of the bucket key using FnvHasher.
    // This is used for partition key computation and statsd logging.
    fn hash64(&self) -> u64 {
        let mut hasher = FnvHasher::default();
        std::hash::Hash::hash(self, &mut hasher);
        hasher.finish()
    }

    /// Estimates the number of bytes needed to encode the bucket key.
    ///
    /// Note that this does not necessarily match the exact memory footprint of the key,
    /// because data structures have a memory overhead.
    fn cost(&self) -> usize {
        mem::size_of::<Self>() + self.metric_name.capacity() + tags_cost(&self.tags)
    }
}

/// Configuration value for [`AggregatorConfig::shift_key`].
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ShiftKey {
    /// Shifts the flush time by an offset based on the [`ProjectKey`].
    ///
    /// This allows buckets from the same project to be flushed together.
    #[default]
    Project,

    /// Shifts the flush time by an offset based on the bucket key itself.
    ///
    /// This allows for a completely random distribution of bucket flush times.
    ///
    /// Only for use in processing Relays.
    Bucket,
}

/// Parameters used by the [`AggregatorService`].
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
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

    /// Maximum amount of bytes used for metrics aggregation.
    ///
    /// When aggregating metrics, Relay keeps track of how many bytes a metric takes in memory.
    /// This is only an approximation and does not take into account things such as pre-allocation
    /// in hashmaps.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_total_bucket_bytes: Option<usize>,

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

    /// Returns the valid range for metrics timestamps.
    ///
    /// Metrics or buckets outside of this range should be discarded.
    pub fn timestamp_range(&self) -> std::ops::Range<UnixTimestamp> {
        let now = UnixTimestamp::now().as_secs();
        let min_timestamp = UnixTimestamp::from_secs(now.saturating_sub(self.max_secs_in_past));
        let max_timestamp = UnixTimestamp::from_secs(now.saturating_add(self.max_secs_in_future));
        min_timestamp..max_timestamp
    }

    /// Determines the target bucket for an incoming bucket timestamp and bucket width.
    ///
    /// We select the output bucket which overlaps with the center of the incoming bucket.
    /// Fails if timestamp is too old or too far into the future.
    fn get_bucket_timestamp(
        &self,
        timestamp: UnixTimestamp,
        bucket_width: u64,
    ) -> Result<UnixTimestamp, AggregateMetricsError> {
        // Find middle of the input bucket to select a target
        let ts = timestamp.as_secs().saturating_add(bucket_width / 2);
        // Align target_timestamp to output bucket width
        let ts = (ts / self.bucket_interval) * self.bucket_interval;
        let output_timestamp = UnixTimestamp::from_secs(ts);

        if !self.timestamp_range().contains(&output_timestamp) {
            let delta = (ts as i64) - (UnixTimestamp::now().as_secs() as i64);
            relay_statsd::metric!(
                histogram(MetricHistograms::InvalidBucketTimestamp) = delta as f64,
            );
            return Err(AggregateMetricsErrorKind::InvalidTimestamp(timestamp).into());
        }

        Ok(output_timestamp)
    }

    /// Returns the instant at which a bucket should be flushed.
    ///
    /// Recent buckets are flushed after a grace period of `initial_delay`. Backdated buckets, that
    /// is, buckets that lie in the past, are flushed after the shorter `debounce_delay`.
    fn get_flush_time(&self, bucket_key: &BucketKey) -> Instant {
        let now = Instant::now();
        let mut flush = None;

        if let MonotonicResult::Instant(instant) = bucket_key.timestamp.to_instant() {
            let instant = Instant::from_std(instant);
            let bucket_end = instant + self.bucket_interval();
            let initial_flush = bucket_end + self.initial_delay();
            // If the initial flush is still pending, use that.
            if initial_flush > now {
                flush = Some(initial_flush + self.flush_time_shift(bucket_key));
            }
        }

        let delay = UnixTimestamp::now().as_secs() as i64 - bucket_key.timestamp.as_secs() as i64;
        relay_statsd::metric!(
            histogram(MetricHistograms::BucketsDelay) = delay as f64,
            backdated = if flush.is_none() { "true" } else { "false" },
        );

        // If the initial flush time has passed or cannot be represented, debounce future flushes
        // with the `debounce_delay` starting now.
        match flush {
            Some(initial_flush) => initial_flush,
            None => now + self.debounce_delay(),
        }
    }

    // Shift deterministically within one bucket interval based on the project or bucket key.
    //
    // This distributes buckets over time to prevent peaks.
    fn flush_time_shift(&self, bucket: &BucketKey) -> Duration {
        let hash_value = match self.shift_key {
            ShiftKey::Project => {
                let mut hasher = FnvHasher::default();
                hasher.write(bucket.project_key.as_str().as_bytes());
                hasher.finish()
            }
            ShiftKey::Bucket => bucket.hash64(),
        };
        let shift_millis = hash_value % (self.bucket_interval * 1000);

        Duration::from_millis(shift_millis)
    }
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            bucket_interval: 10,
            initial_delay: 30,
            debounce_delay: 10,
            max_flush_bytes: 5_000_000, // 5 MB
            flush_partitions: None,
            max_secs_in_past: 5 * 24 * 60 * 60, // 5 days, as for sessions
            max_secs_in_future: 60,             // 1 minute
            max_name_length: 200,
            max_tag_key_length: 200,
            max_tag_value_length: 200,
            max_total_bucket_bytes: None,
            max_project_key_bucket_bytes: None,
            shift_key: ShiftKey::default(),
        }
    }
}

/// Bucket in the [`Aggregator`] with a defined flush time.
///
/// This type implements an inverted total ordering. The maximum queued bucket has the lowest flush
/// time, which is suitable for using it in a [`BinaryHeap`].
///
/// [`BinaryHeap`]: std::collections::BinaryHeap
#[derive(Debug)]
struct QueuedBucket {
    flush_at: Instant,
    value: BucketValue,
}

impl QueuedBucket {
    /// Creates a new `QueuedBucket` with a given flush time.
    fn new(flush_at: Instant, value: BucketValue) -> Self {
        Self { flush_at, value }
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

/// A Bucket and its hashed key.
/// This is cheaper to pass around than a (BucketKey, Bucket) pair.
pub struct HashedBucket {
    // This is only public because pop_flush_buckets is used in benchmark.
    hashed_key: u64,
    bucket: Bucket,
}

enum AggregatorState {
    Running,
    ShuttingDown,
}

#[derive(Default)]
struct CostTracker {
    total_cost: usize,
    cost_per_project_key: HashMap<ProjectKey, usize>,
}

impl CostTracker {
    fn totals_cost_exceeded(&self, max_total_cost: Option<usize>) -> bool {
        if let Some(max_total_cost) = max_total_cost {
            if self.total_cost >= max_total_cost {
                return true;
            }
        }

        false
    }

    fn check_limits_exceeded(
        &self,
        project_key: ProjectKey,
        max_total_cost: Option<usize>,
        max_project_cost: Option<usize>,
    ) -> Result<(), AggregateMetricsError> {
        if self.totals_cost_exceeded(max_total_cost) {
            relay_log::configure_scope(|scope| {
                scope.set_extra("bucket.project_key", project_key.as_str().to_owned().into());
            });
            return Err(AggregateMetricsErrorKind::TotalLimitExceeded.into());
        }

        if let Some(max_project_cost) = max_project_cost {
            let project_cost = self
                .cost_per_project_key
                .get(&project_key)
                .cloned()
                .unwrap_or(0);
            if project_cost >= max_project_cost {
                relay_log::configure_scope(|scope| {
                    scope.set_extra("bucket.project_key", project_key.as_str().to_owned().into());
                });
                return Err(AggregateMetricsErrorKind::ProjectLimitExceeded.into());
            }
        }

        Ok(())
    }

    fn add_cost(&mut self, project_key: ProjectKey, cost: usize) {
        self.total_cost += cost;
        let project_cost = self.cost_per_project_key.entry(project_key).or_insert(0);
        *project_cost += cost;
    }

    fn subtract_cost(&mut self, project_key: ProjectKey, cost: usize) {
        match self.cost_per_project_key.entry(project_key) {
            Entry::Vacant(_) => {
                relay_log::error!("cost subtracted for an untracked project key");
            }
            Entry::Occupied(mut entry) => {
                // Handle per-project cost:
                let project_cost = entry.get_mut();
                if cost > *project_cost {
                    relay_log::error!("underflow while subtracing project cost");
                    self.total_cost = self.total_cost.saturating_sub(*project_cost);
                    *project_cost = 0;
                } else {
                    *project_cost -= cost;
                    self.total_cost = self.total_cost.saturating_sub(cost);
                }
                if *project_cost == 0 {
                    // Remove this project_key from the map
                    entry.remove();
                }
            }
        };
    }
}

impl fmt::Debug for CostTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CostTracker")
            .field("total_cost", &self.total_cost)
            .field(
                "cost_per_project_key",
                &BTreeMap::from_iter(self.cost_per_project_key.iter()),
            )
            .finish()
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

/// A message containing a list of [`Metric`]s to be inserted into the aggregator.
#[derive(Debug)]
pub struct InsertMetrics {
    pub(crate) project_key: ProjectKey,
    pub(crate) metrics: Vec<Metric>,
}

impl InsertMetrics {
    /// Creates a new message containing a list of [`Metric`]s.
    pub fn new<I>(project_key: ProjectKey, metrics: I) -> Self
    where
        I: IntoIterator<Item = Metric>,
    {
        Self {
            project_key,
            metrics: metrics.into_iter().collect(),
        }
    }

    /// Returns the `ProjectKey` for the the current `InsertMetrics` message.
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
    }

    /// Returns the list of the metrics in the current `InsertMetrics` message, consuming the
    /// message itself.
    pub fn metrics(self) -> Vec<Metric> {
        self.metrics
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

/// Aggregator service interface.
#[derive(Debug)]
pub enum Aggregator {
    /// The health check message which makes sure that the service can accept the requests now.
    AcceptsMetrics(AcceptsMetrics, Sender<bool>),
    /// Insert metrics.
    InsertMetrics(InsertMetrics),
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

#[cfg(test)]
impl FromMessage<BucketCountInquiry> for Aggregator {
    type Response = AsyncResponse<usize>;
    fn from_message(message: BucketCountInquiry, sender: Sender<usize>) -> Self {
        Self::BucketCountInquiry(message, sender)
    }
}

impl FromMessage<InsertMetrics> for Aggregator {
    type Response = NoResponse;
    fn from_message(message: InsertMetrics, _: ()) -> Self {
        Self::InsertMetrics(message)
    }
}

impl FromMessage<MergeBuckets> for Aggregator {
    type Response = NoResponse;
    fn from_message(message: MergeBuckets, _: ()) -> Self {
        Self::MergeBuckets(message)
    }
}

/// A collector of [`Metric`] submissions.
///
/// # Aggregation
///
/// Each metric is dispatched into the a [`Bucket`] depending on its project key (DSN), name, type,
/// unit, tags and timestamp. The bucket timestamp is rounded to the precision declared by the
/// `bucket_interval` field on the [AggregatorConfig] configuration.
///
/// Each bucket stores the accumulated value of submitted metrics:
///
/// - `Counter`: Sum of values.
/// - `Distribution`: A list of values.
/// - `Set`: A unique set of hashed values.
/// - `Gauge`: A summary of the reported values, see [`GaugeValue`].
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
/// Internally, the aggregator maintains a continuous flush cycle every 100ms. It guarantees that
/// all elapsed buckets belonging to the same [`ProjectKey`] are flushed together.
///
/// Receivers must implement a handler for the [`FlushBuckets`] message.
pub struct AggregatorService {
    name: String,
    config: AggregatorConfig,
    buckets: HashMap<BucketKey, QueuedBucket>,
    receiver: Option<Recipient<FlushBuckets, NoResponse>>,
    state: AggregatorState,
    cost_tracker: CostTracker,
}

impl AggregatorService {
    /// Create a new aggregator and connect it to `receiver`.
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
            name,
            config,
            buckets: HashMap::new(),
            receiver,
            state: AggregatorState::Running,
            cost_tracker: CostTracker::default(),
        }
    }

    /// Validates the metric name and its tags are correct.
    ///
    /// Returns `Err` if the metric should be dropped.
    fn validate_bucket_key(
        mut key: BucketKey,
        aggregator_config: &AggregatorConfig,
    ) -> Result<BucketKey, AggregateMetricsError> {
        key = Self::validate_metric_name(key, aggregator_config)?;
        key = Self::validate_metric_tags(key, aggregator_config);
        Ok(key)
    }

    /// Removes invalid characters from metric names.
    ///
    /// Returns `Err` if the metric must be dropped.
    fn validate_metric_name(
        mut key: BucketKey,
        aggregator_config: &AggregatorConfig,
    ) -> Result<BucketKey, AggregateMetricsError> {
        let metric_name_length = key.metric_name.len();
        if metric_name_length > aggregator_config.max_name_length {
            relay_log::configure_scope(|scope| {
                scope.set_extra(
                    "bucket.project_key",
                    key.project_key.as_str().to_owned().into(),
                );
                scope.set_extra(
                    "bucket.metric_name.length",
                    metric_name_length.to_string().into(),
                );
                scope.set_extra(
                    "aggregator_config.max_name_length",
                    aggregator_config.max_name_length.to_string().into(),
                );
            });
            return Err(AggregateMetricsErrorKind::InvalidStringLength(key.metric_name).into());
        }

        if let Err(err) = Self::normalize_metric_name(&mut key) {
            relay_log::configure_scope(|scope| {
                scope.set_extra(
                    "bucket.project_key",
                    key.project_key.as_str().to_owned().into(),
                );
                scope.set_extra("bucket.metric_name", key.metric_name.into());
            });
            return Err(err);
        }

        Ok(key)
    }

    fn normalize_metric_name(key: &mut BucketKey) -> Result<(), AggregateMetricsError> {
        key.metric_name = match MetricResourceIdentifier::parse(&key.metric_name) {
            Ok(mri) => {
                if matches!(mri.namespace, MetricNamespace::Unsupported) {
                    relay_log::debug!("invalid metric namespace {:?}", &key.metric_name);
                    return Err(
                        AggregateMetricsErrorKind::UnsupportedNamespace(mri.namespace).into(),
                    );
                }

                let mut metric_name = mri.to_string();
                // do this so cost tracking still works accurately.
                metric_name.shrink_to_fit();
                metric_name
            }
            Err(_) => {
                relay_log::debug!("invalid metric name {:?}", &key.metric_name);
                return Err(
                    AggregateMetricsErrorKind::InvalidCharacters(key.metric_name.clone()).into(),
                );
            }
        };

        Ok(())
    }

    /// Removes tags with invalid characters in the key, and validates tag values.
    ///
    /// Tag values are validated with `protocol::validate_tag_value`.
    fn validate_metric_tags(mut key: BucketKey, aggregator_config: &AggregatorConfig) -> BucketKey {
        let proj_key = key.project_key.as_str();
        key.tags.retain(|tag_key, tag_value| {
            if tag_key.len() > aggregator_config.max_tag_key_length {
                relay_log::configure_scope(|scope| {
                    scope.set_extra("bucket.project_key", proj_key.to_owned().into());
                    scope.set_extra("bucket.metric.tag_key", tag_key.to_owned().into());
                    scope.set_extra(
                        "aggregator_config.max_tag_key_length",
                        aggregator_config.max_tag_key_length.to_string().into(),
                    );
                });
                relay_log::debug!("Invalid metric tag key");
                return false;
            }
            if bytecount::num_chars(tag_value.as_bytes()) > aggregator_config.max_tag_value_length {
                relay_log::configure_scope(|scope| {
                    scope.set_extra("bucket.project_key", proj_key.to_owned().into());
                    scope.set_extra("bucket.metric.tag_value", tag_value.to_owned().into());
                    scope.set_extra(
                        "aggregator_config.max_tag_value_length",
                        aggregator_config.max_tag_value_length.to_string().into(),
                    );
                });
                relay_log::debug!("Invalid metric tag value");
                return false;
            }

            if protocol::is_valid_tag_key(tag_key) {
                true
            } else {
                relay_log::debug!("invalid metric tag key {tag_key:?}");
                false
            }
        });
        for (_, tag_value) in key.tags.iter_mut() {
            protocol::validate_tag_value(tag_value);
        }
        key
    }

    /// Merges any mergeable value into the bucket at the given `key`.
    ///
    /// If no bucket exists for the given bucket key, a new bucket will be created.
    fn merge_in<T: MergeValue>(
        &mut self,
        key: BucketKey,
        value: T,
    ) -> Result<(), AggregateMetricsError> {
        let project_key = key.project_key;

        let key = Self::validate_bucket_key(key, &self.config)?;

        // XXX: This is not a great implementation of cost enforcement.
        //
        // * it takes two lookups of the project key in the cost tracker to merge a bucket: once in
        //   `check_limits_exceeded` and once in `add_cost`.
        //
        // * the limits are not actually enforced consistently
        //
        //   A bucket can be merged that exceeds the cost limit, and only the next bucket will be
        //   limited because the limit is now reached. This implementation was chosen because it's
        //   currently not possible to determine cost accurately upfront: The bucket values have to
        //   be merged together to figure out how costly the merge was. Changing that would force
        //   us to unravel a lot of abstractions that we have already built.
        //
        //   As a result of that, it is possible to exceed the bucket cost limit significantly
        //   until we have guaranteed upper bounds on the cost of a single bucket (which we
        //   currently don't, because a metric can have arbitrary amount of tag values).
        //
        //   Another consequence is that a MergeValue that adds zero cost (such as an existing
        //   counter bucket being incremented) is currently rejected even though it doesn't have to
        //   be.
        //
        // The flipside of this approach is however that there's more optimization potential: If
        // the limit is already exceeded, we could implement an optimization that drops envelope
        // items before they are parsed, as we can be sure that the new metric bucket will be
        // rejected in the aggregator regardless of whether it is merged into existing buckets,
        // whether it is just a counter, etc.
        self.cost_tracker.check_limits_exceeded(
            project_key,
            self.config.max_total_bucket_bytes,
            self.config.max_project_key_bucket_bytes,
        )?;

        let added_cost;
        match self.buckets.entry(key) {
            Entry::Occupied(mut entry) => {
                relay_statsd::metric!(
                    counter(MetricCounters::MergeHit) += 1,
                    aggregator = &self.name,
                    metric_name = metric_name_tag(&entry.key().metric_name),
                );
                let bucket_value = &mut entry.get_mut().value;
                let cost_before = bucket_value.cost();
                value.merge_into(bucket_value)?;
                let cost_after = bucket_value.cost();
                added_cost = cost_after.saturating_sub(cost_before);
            }
            Entry::Vacant(entry) => {
                relay_statsd::metric!(
                    counter(MetricCounters::MergeMiss) += 1,
                    aggregator = &self.name,
                    metric_name = metric_name_tag(&entry.key().metric_name),
                );
                relay_statsd::metric!(
                    set(MetricSets::UniqueBucketsCreated) = entry.key().hash64() as i64, // 2-complement
                    aggregator = &self.name,
                    metric_name = metric_name_tag(&entry.key().metric_name),
                );

                let flush_at = self.config.get_flush_time(entry.key());
                let bucket = value.into();
                added_cost = entry.key().cost() + bucket.cost();
                entry.insert(QueuedBucket::new(flush_at, bucket));
            }
        }

        self.cost_tracker.add_cost(project_key, added_cost);

        Ok(())
    }

    /// Inserts a metric into the corresponding bucket in this aggregator.
    ///
    /// If no bucket exists for the given bucket key, a new bucket will be created.
    pub fn insert(
        &mut self,
        project_key: ProjectKey,
        metric: Metric,
    ) -> Result<(), AggregateMetricsError> {
        relay_statsd::metric!(
            counter(MetricCounters::InsertMetric) += 1,
            aggregator = &self.name,
            metric_type = metric.value.ty().as_str(),
        );
        let key = BucketKey {
            project_key,
            timestamp: self.config.get_bucket_timestamp(metric.timestamp, 0)?,
            metric_name: metric.name,
            tags: metric.tags,
        };
        self.merge_in(key, metric.value)
    }

    /// Merge a preaggregated bucket into this aggregator.
    ///
    /// If no bucket exists for the given bucket key, a new bucket will be created.
    pub fn merge(
        &mut self,
        project_key: ProjectKey,
        bucket: Bucket,
    ) -> Result<(), AggregateMetricsError> {
        let key = BucketKey {
            project_key,
            timestamp: self
                .config
                .get_bucket_timestamp(bucket.timestamp, bucket.width)?,
            metric_name: bucket.name,
            tags: bucket.tags,
        };
        self.merge_in(key, bucket.value)
    }

    /// Merges all given `buckets` into this aggregator.
    ///
    /// Buckets that do not exist yet will be created.
    pub fn merge_all<I>(
        &mut self,
        project_key: ProjectKey,
        buckets: I,
    ) -> Result<(), AggregateMetricsError>
    where
        I: IntoIterator<Item = Bucket>,
    {
        for bucket in buckets.into_iter() {
            if let Err(error) = self.merge(project_key, bucket) {
                relay_log::error!(error = &error as &dyn Error);
            }
        }

        Ok(())
    }

    /// Pop and return the buckets that are eligible for flushing out according to bucket interval.
    ///
    /// Note that this function is primarily intended for tests.
    pub fn pop_flush_buckets(&mut self) -> HashMap<ProjectKey, Vec<HashedBucket>> {
        relay_statsd::metric!(
            gauge(MetricGauges::Buckets) = self.buckets.len() as u64,
            aggregator = &self.name,
        );

        // We only emit statsd metrics for the cost on flush (and not when merging the buckets),
        // assuming that this gives us more than enough data points.
        relay_statsd::metric!(
            gauge(MetricGauges::BucketsCost) = self.cost_tracker.total_cost as u64
        );

        let mut buckets = HashMap::<ProjectKey, Vec<HashedBucket>>::new();

        let force = matches!(&self.state, AggregatorState::ShuttingDown);

        let mut stats = BTreeMap::new();

        relay_statsd::metric!(
            timer(MetricTimers::BucketsScanDuration),
            aggregator = &self.name,
            {
                let bucket_interval = self.config.bucket_interval;
                let cost_tracker = &mut self.cost_tracker;
                self.buckets.retain(|key, entry| {
                    if force || entry.elapsed() {
                        // Take the value and leave a placeholder behind. It'll be removed right after.
                        let value = mem::replace(&mut entry.value, BucketValue::Counter(0.0));
                        cost_tracker.subtract_cost(key.project_key, key.cost());
                        cost_tracker.subtract_cost(key.project_key, value.cost());

                        let (bucket_count, item_count) = stats
                            .entry((metric_type_tag(&value), metric_name_tag(&key.metric_name)))
                            .or_insert((0usize, 0usize));
                        *bucket_count += 1;
                        *item_count += value.len();

                        let bucket = Bucket {
                            timestamp: key.timestamp,
                            width: bucket_interval,
                            name: key.metric_name.clone(),
                            value,
                            tags: key.tags.clone(),
                        };

                        buckets
                            .entry(key.project_key)
                            .or_default()
                            .push(HashedBucket {
                                hashed_key: key.hash64(),
                                bucket,
                            });

                        false
                    } else {
                        true
                    }
                });
            }
        );

        for ((ty, name), (bucket_count, item_count)) in stats.into_iter() {
            relay_statsd::metric!(
                gauge(MetricGauges::AvgBucketSize) = item_count as f64 / bucket_count as f64,
                metric_type = ty,
                metric_name = name
            );
        }

        buckets
    }

    /// Split buckets into N logical partitions, determined by the bucket key.
    fn partition_buckets(
        &self,
        buckets: Vec<HashedBucket>,
        flush_partitions: Option<u64>,
    ) -> BTreeMap<Option<u64>, Vec<Bucket>> {
        let flush_partitions = match flush_partitions {
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
                aggregator = &self.name,
            );
        }
        partitions
    }

    /// Split the provided buckets into batches and process each batch with the given function.
    ///
    /// For each batch, log a histogram metric.
    fn process_batches<F>(&self, buckets: impl IntoIterator<Item = Bucket>, mut process: F)
    where
        F: FnMut(Vec<Bucket>),
    {
        let capped_batches =
            CappedBucketIter::new(buckets.into_iter(), self.config.max_flush_bytes);
        let num_batches = capped_batches
            .map(|batch| {
                relay_statsd::metric!(
                    histogram(MetricHistograms::BucketsPerBatch) = batch.len() as f64,
                    aggregator = &self.name,
                );
                process(batch);
            })
            .count();

        relay_statsd::metric!(
            histogram(MetricHistograms::BatchesPerPartition) = num_batches as f64,
            aggregator = &self.name,
        );
    }

    /// Sends the [`FlushBuckets`] message to the receiver in the fire and forget fashion. It is up
    /// to the receiver to send the [`MergeBuckets`] message back if buckets could not be flushed
    /// and we require another re-try.
    ///
    /// If `force` is true, flush all buckets unconditionally and do not attempt to merge back.
    fn try_flush(&mut self) {
        let flush_buckets = self.pop_flush_buckets();

        if flush_buckets.is_empty() {
            return;
        }

        relay_log::trace!("flushing {} projects to receiver", flush_buckets.len());

        let mut total_bucket_count = 0u64;
        for (project_key, project_buckets) in flush_buckets.into_iter() {
            let bucket_count = project_buckets.len() as u64;
            relay_statsd::metric!(
                histogram(MetricHistograms::BucketsFlushedPerProject) = bucket_count,
                aggregator = &self.name,
            );
            total_bucket_count += bucket_count;

            let num_partitions = self.config.flush_partitions;
            let partitioned_buckets = self.partition_buckets(project_buckets, num_partitions);
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
            aggregator = &self.name,
        );
    }

    fn handle_accepts_metrics(&self, sender: Sender<bool>) {
        let result = !self
            .cost_tracker
            .totals_cost_exceeded(self.config.max_total_bucket_bytes);
        sender.send(result);
    }

    fn handle_insert_metrics(&mut self, msg: InsertMetrics) {
        let InsertMetrics {
            project_key,
            metrics,
        } = msg;
        for metric in metrics {
            if let Err(err) = self.insert(project_key, metric) {
                relay_log::error!(error = &err as &dyn Error, "failed to insert metrics",);
            }
        }
    }

    fn handle_merge_buckets(&mut self, msg: MergeBuckets) {
        let MergeBuckets {
            project_key,
            buckets,
        } = msg;
        if let Err(err) = self.merge_all(project_key, buckets) {
            relay_log::error!(error = &err as &dyn Error, "failed to merge buckets");
        }
    }

    fn handle_message(&mut self, msg: Aggregator) {
        match msg {
            Aggregator::AcceptsMetrics(_, sender) => self.handle_accepts_metrics(sender),
            Aggregator::InsertMetrics(msg) => self.handle_insert_metrics(msg),
            Aggregator::MergeBuckets(msg) => self.handle_merge_buckets(msg),
            #[cfg(test)]
            Aggregator::BucketCountInquiry(_, sender) => sender.send(self.buckets.len()),
        }
    }

    fn handle_shutdown(&mut self, message: Shutdown) {
        if message.timeout.is_some() {
            self.state = AggregatorState::ShuttingDown;
        }
    }
}

impl fmt::Debug for AggregatorService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("config", &self.config)
            .field("buckets", &self.buckets)
            .field("receiver", &format_args!("Recipient<FlushBuckets>"))
            .finish()
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
        let remaining_buckets = self.buckets.len();
        if remaining_buckets > 0 {
            relay_log::error!("metrics aggregator dropping {remaining_buckets} buckets");
            relay_statsd::metric!(
                counter(MetricCounters::BucketsDropped) += remaining_buckets as i64,
                aggregator = &self.name,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, RwLock};

    use similar_asserts::assert_eq;

    use super::*;
    use crate::{dist, GaugeValue};

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

    fn some_metric() -> Metric {
        Metric {
            name: "c:transactions/foo".to_owned(),
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(999994711),
            tags: BTreeMap::new(),
        }
    }

    #[test]
    fn test_bucket_value_merge_counter() {
        let mut value = BucketValue::Counter(42.);
        BucketValue::Counter(43.).merge_into(&mut value).unwrap();
        assert_eq!(value, BucketValue::Counter(85.));
    }

    #[test]
    fn test_bucket_value_merge_distribution() {
        let mut value = BucketValue::Distribution(dist![1., 2., 3.]);
        BucketValue::Distribution(dist![2., 4.])
            .merge_into(&mut value)
            .unwrap();
        assert_eq!(value, BucketValue::Distribution(dist![1., 2., 2., 3., 4.]));
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
        let mut value = BucketValue::Gauge(GaugeValue::single(42.));
        BucketValue::Gauge(GaugeValue::single(43.))
            .merge_into(&mut value)
            .unwrap();

        assert_eq!(
            value,
            BucketValue::Gauge(GaugeValue {
                max: 43.,
                min: 42.,
                sum: 85.,
                last: 43.,
                count: 2,
            })
        );
    }

    #[test]
    fn test_bucket_value_insert_counter() {
        let mut value = BucketValue::Counter(42.);
        MetricValue::Counter(43.).merge_into(&mut value).unwrap();
        assert_eq!(value, BucketValue::Counter(85.));
    }

    #[test]
    fn test_bucket_value_insert_distribution() {
        let mut value = BucketValue::Distribution(dist![1., 2., 3.]);
        MetricValue::Distribution(2.0)
            .merge_into(&mut value)
            .unwrap();
        assert_eq!(value, BucketValue::Distribution(dist![1., 2., 3., 2.]));
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
        let mut value = BucketValue::Gauge(GaugeValue::single(42.));
        MetricValue::Gauge(43.).merge_into(&mut value).unwrap();
        assert_eq!(
            value,
            BucketValue::Gauge(GaugeValue {
                max: 43.,
                min: 42.,
                sum: 85.,
                last: 43.,
                count: 2,
            })
        );
    }

    #[test]
    fn test_bucket_value_cost() {
        // When this test fails, it means that the cost model has changed.
        // Check dimensionality limits.
        let expected_bucket_value_size = 48;
        let expected_set_entry_size = 4;

        let counter = BucketValue::Counter(123.0);
        assert_eq!(counter.cost(), expected_bucket_value_size);
        let set = BucketValue::Set([1, 2, 3, 4, 5].into());
        assert_eq!(
            set.cost(),
            expected_bucket_value_size + 5 * expected_set_entry_size
        );
        let distribution = BucketValue::Distribution(dist![1., 2., 3.]);
        assert_eq!(
            distribution.cost(),
            expected_bucket_value_size + 3 * (8 + 4)
        );
        let gauge = BucketValue::Gauge(GaugeValue {
            max: 43.,
            min: 42.,
            sum: 85.,
            last: 43.,
            count: 2,
        });
        assert_eq!(gauge.cost(), expected_bucket_value_size);
    }

    #[test]
    fn test_bucket_key_cost() {
        // When this test fails, it means that the cost model has changed.
        // Check dimensionality limits.
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let name = "12345".to_owned();
        let bucket_key = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: name,
            tags: BTreeMap::from([
                ("hello".to_owned(), "world".to_owned()),
                ("answer".to_owned(), "42".to_owned()),
            ]),
        };
        assert_eq!(
            bucket_key.cost(),
            88 + // BucketKey
            5 + // name
            (5 + 5 + 6 + 2) // tags
        );
    }

    #[test]
    fn test_aggregator_merge_counters() {
        relay_test::setup();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut aggregator = AggregatorService::new(test_config(), None);

        let metric1 = some_metric();

        let mut metric2 = metric1.clone();
        metric2.value = MetricValue::Counter(43.);
        aggregator.insert(project_key, metric1).unwrap();
        aggregator.insert(project_key, metric2).unwrap();

        let buckets: Vec<_> = aggregator
            .buckets
            .iter()
            .map(|(k, e)| (k, &e.value)) // skip flush times, they are different every time
            .collect();

        insta::assert_debug_snapshot!(buckets, @r#"
        [
            (
                BucketKey {
                    project_key: ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"),
                    timestamp: UnixTimestamp(999994711),
                    metric_name: "c:transactions/foo@none",
                    tags: {},
                },
                Counter(
                    85.0,
                ),
            ),
        ]
        "#);
    }

    #[test]
    fn test_aggregator_merge_timestamps() {
        relay_test::setup();
        let config = AggregatorConfig {
            bucket_interval: 10,
            ..test_config()
        };
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut aggregator = AggregatorService::new(config, None);

        let metric1 = some_metric();

        let mut metric2 = metric1.clone();
        metric2.timestamp = UnixTimestamp::from_secs(999994712);

        let mut metric3 = metric1.clone();
        metric3.timestamp = UnixTimestamp::from_secs(999994721);
        aggregator.insert(project_key, metric1).unwrap();
        aggregator.insert(project_key, metric2).unwrap();
        aggregator.insert(project_key, metric3).unwrap();

        let mut buckets: Vec<_> = aggregator
            .buckets
            .iter()
            .map(|(k, e)| (k, &e.value)) // skip flush times, they are different every time
            .collect();

        buckets.sort_by(|a, b| a.0.timestamp.cmp(&b.0.timestamp));
        insta::assert_debug_snapshot!(buckets, @r#"
        [
            (
                BucketKey {
                    project_key: ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"),
                    timestamp: UnixTimestamp(999994710),
                    metric_name: "c:transactions/foo@none",
                    tags: {},
                },
                Counter(
                    84.0,
                ),
            ),
            (
                BucketKey {
                    project_key: ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"),
                    timestamp: UnixTimestamp(999994720),
                    metric_name: "c:transactions/foo@none",
                    tags: {},
                },
                Counter(
                    42.0,
                ),
            ),
        ]
        "#);
    }

    #[test]
    fn test_aggregator_mixed_projects() {
        relay_test::setup();

        let config = AggregatorConfig {
            bucket_interval: 10,
            ..test_config()
        };

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let mut aggregator = AggregatorService::new(config, None);

        // It's OK to have same metric with different projects:
        aggregator.insert(project_key1, some_metric()).unwrap();
        aggregator.insert(project_key2, some_metric()).unwrap();

        assert_eq!(aggregator.buckets.len(), 2);
    }

    #[test]
    fn test_cost_tracker() {
        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let project_key3 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();
        let mut cost_tracker = CostTracker::default();
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 0,
            cost_per_project_key: {},
        }
        "#);
        cost_tracker.add_cost(project_key1, 100);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 100,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
            },
        }
        "#);
        cost_tracker.add_cost(project_key2, 200);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 300,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
        }
        "#);
        // Unknown project: Will log error, but not crash
        cost_tracker.subtract_cost(project_key3, 666);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 300,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
        }
        "#);
        // Subtract too much: Will log error, but not crash
        cost_tracker.subtract_cost(project_key1, 666);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 200,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
        }
        "#);
        cost_tracker.subtract_cost(project_key2, 20);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 180,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 180,
            },
        }
        "#);
        cost_tracker.subtract_cost(project_key2, 180);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 0,
            cost_per_project_key: {},
        }
        "#);
    }

    #[test]
    fn test_aggregator_cost_tracking() {
        // Make sure that the right cost is added / subtracted
        let mut aggregator = AggregatorService::new(test_config(), None);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        let metric = Metric {
            name: "c:transactions/foo@none".to_owned(),
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(999994711),
            tags: BTreeMap::new(),
        };
        let bucket_key = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/foo@none".to_owned(),
            tags: BTreeMap::new(),
        };
        let fixed_cost = bucket_key.cost() + mem::size_of::<BucketValue>();
        for (metric_name, metric_value, expected_added_cost) in [
            (
                "c:transactions/foo@none",
                MetricValue::Counter(42.),
                fixed_cost,
            ),
            ("c:transactions/foo@none", MetricValue::Counter(42.), 0), // counters have constant size
            (
                "s:transactions/foo@none",
                MetricValue::Set(123),
                fixed_cost + 4,
            ), // Added a new bucket + 1 element
            ("s:transactions/foo@none", MetricValue::Set(123), 0), // Same element in set, no change
            ("s:transactions/foo@none", MetricValue::Set(456), 4), // Different element in set -> +4
            (
                "d:transactions/foo@none",
                MetricValue::Distribution(1.0),
                fixed_cost + 12,
            ), // New bucket + 1 element
            ("d:transactions/foo@none", MetricValue::Distribution(1.0), 0), // no new element
            (
                "d:transactions/foo@none",
                MetricValue::Distribution(2.0),
                12,
            ), // 1 new element
            (
                "g:transactions/foo@none",
                MetricValue::Gauge(0.3),
                fixed_cost,
            ), // New bucket
            ("g:transactions/foo@none", MetricValue::Gauge(0.2), 0), // gauge has constant size
        ] {
            let mut metric = metric.clone();
            metric.value = metric_value;
            metric.name = metric_name.to_string();

            let current_cost = aggregator.cost_tracker.total_cost;
            aggregator.insert(project_key, metric).unwrap();
            let total_cost = aggregator.cost_tracker.total_cost;
            assert_eq!(total_cost, current_cost + expected_added_cost);
        }

        aggregator.pop_flush_buckets();
        assert_eq!(aggregator.cost_tracker.total_cost, 0);
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

        let buckets = Bucket::parse_all(json.as_bytes()).unwrap();

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

        let buckets = Bucket::parse_all(json.as_bytes()).unwrap();

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

        let mut metric = some_metric();
        metric.timestamp = UnixTimestamp::now();

        aggregator.send(InsertMetrics {
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            metrics: vec![metric],
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

        let mut metric = some_metric();
        metric.timestamp = UnixTimestamp::now();

        aggregator.send(InsertMetrics {
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            metrics: vec![metric],
        });

        assert_eq!(receiver.bucket_count(), 0);

        tokio::time::sleep(Duration::from_millis(1100)).await;
        let bucket_count = aggregator.send(BucketCountInquiry).await.unwrap();
        assert_eq!(bucket_count, 1);
        assert_eq!(receiver.bucket_count(), 0);
    }

    #[test]
    fn test_get_bucket_timestamp_overflow() {
        let config = AggregatorConfig {
            bucket_interval: 10,
            initial_delay: 0,
            debounce_delay: 0,
            ..Default::default()
        };

        assert!(matches!(
            config
                .get_bucket_timestamp(UnixTimestamp::from_secs(u64::MAX), 2)
                .unwrap_err()
                .kind,
            AggregateMetricsErrorKind::InvalidTimestamp(_)
        ));
    }

    #[test]
    fn test_get_bucket_timestamp_zero() {
        let config = AggregatorConfig {
            bucket_interval: 10,
            initial_delay: 0,
            debounce_delay: 0,
            ..Default::default()
        };

        let now = UnixTimestamp::now().as_secs();
        let rounded_now = UnixTimestamp::from_secs(now / 10 * 10);
        assert_eq!(
            config
                .get_bucket_timestamp(UnixTimestamp::from_secs(now), 0)
                .unwrap(),
            rounded_now
        );
    }

    #[test]
    fn test_get_bucket_timestamp_multiple() {
        let config = AggregatorConfig {
            bucket_interval: 10,
            initial_delay: 0,
            debounce_delay: 0,
            ..Default::default()
        };

        let rounded_now = UnixTimestamp::now().as_secs() / 10 * 10;
        let now = rounded_now + 3;
        assert_eq!(
            config
                .get_bucket_timestamp(UnixTimestamp::from_secs(now), 20)
                .unwrap()
                .as_secs(),
            rounded_now + 10
        );
    }

    #[test]
    fn test_get_bucket_timestamp_non_multiple() {
        let config = AggregatorConfig {
            bucket_interval: 10,
            initial_delay: 0,
            debounce_delay: 0,
            ..Default::default()
        };

        let rounded_now = UnixTimestamp::now().as_secs() / 10 * 10;
        let now = rounded_now + 3;
        assert_eq!(
            config
                .get_bucket_timestamp(UnixTimestamp::from_secs(now), 23)
                .unwrap()
                .as_secs(),
            rounded_now + 10
        );
    }

    #[test]
    fn test_validate_bucket_key_chars() {
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let bucket_key = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/hergus.bergus".to_owned(),
            tags: {
                let mut tags = BTreeMap::new();
                // There are some SDKs which mess up content encodings, and interpret the raw bytes
                // of an UTF-16 string as UTF-8. Leading to ASCII
                // strings getting null-bytes interleaved.
                //
                // Somehow those values end up as release tag in sessions, while in error events we
                // haven't observed this malformed encoding. We believe it's slightly better to
                // strip out NUL-bytes instead of dropping the tag such that those values line up
                // again across sessions and events. Should that cause too high cardinality we'll
                // have to drop tags.
                //
                // Note that releases are validated separately against much stricter character set,
                // but the above idea should still apply to other tags.
                tags.insert(
                    "is_it_garbage".to_owned(),
                    "a\0b\0s\0o\0l\0u\0t\0e\0l\0y".to_owned(),
                );
                tags.insert("another\0garbage".to_owned(), "bye".to_owned());
                tags
            },
        };
        let aggregator_config = test_config();

        let mut bucket_key =
            AggregatorService::validate_bucket_key(bucket_key, &aggregator_config).unwrap();

        assert_eq!(bucket_key.tags.len(), 1);
        assert_eq!(
            bucket_key.tags.get("is_it_garbage"),
            Some(&"absolutely".to_owned())
        );
        assert_eq!(bucket_key.tags.get("another\0garbage"), None);

        bucket_key.metric_name = "hergus\0bergus".to_owned();
        AggregatorService::validate_bucket_key(bucket_key, &aggregator_config).unwrap_err();
    }

    #[test]
    fn test_validate_bucket_key_str_lens() {
        relay_test::setup();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let aggregator_config = test_config();

        let short_metric = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric".to_owned(),
            tags: BTreeMap::new(),
        };
        assert!(AggregatorService::validate_bucket_key(short_metric, &aggregator_config).is_ok());

        let long_metric = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/long_name_a_very_long_name_its_super_long_really_but_like_super_long_probably_the_longest_name_youve_seen_and_even_the_longest_name_ever_its_extremly_long_i_cant_tell_how_long_it_is_because_i_dont_have_that_many_fingers_thus_i_cant_count_the_many_characters_this_long_name_is".to_owned(),
            tags: BTreeMap::new(),
        };
        let validation = AggregatorService::validate_bucket_key(long_metric, &aggregator_config);

        assert!(matches!(
            validation.unwrap_err(),
            AggregateMetricsError {
                kind: AggregateMetricsErrorKind::InvalidStringLength(_)
            }
        ));

        let short_metric_long_tag_key = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric_with_long_tag_key".to_owned(),
            tags: BTreeMap::from([("i_run_out_of_creativity_so_here_we_go_Lorem_Ipsum_is_simply_dummy_text_of_the_printing_and_typesetting_industry_Lorem_Ipsum_has_been_the_industrys_standard_dummy_text_ever_since_the_1500s_when_an_unknown_printer_took_a_galley_of_type_and_scrambled_it_to_make_a_type_specimen_book".into(), "tag_value".into())]),
        };
        let validation =
            AggregatorService::validate_bucket_key(short_metric_long_tag_key, &aggregator_config)
                .unwrap();
        assert_eq!(validation.tags.len(), 0);

        let short_metric_long_tag_value = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric_with_long_tag_value".to_owned(),
            tags: BTreeMap::from([("tag_key".into(), "i_run_out_of_creativity_so_here_we_go_Lorem_Ipsum_is_simply_dummy_text_of_the_printing_and_typesetting_industry_Lorem_Ipsum_has_been_the_industrys_standard_dummy_text_ever_since_the_1500s_when_an_unknown_printer_took_a_galley_of_type_and_scrambled_it_to_make_a_type_specimen_book".into())]),
        };
        let validation =
            AggregatorService::validate_bucket_key(short_metric_long_tag_value, &aggregator_config)
                .unwrap();
        assert_eq!(validation.tags.len(), 0);
    }

    #[test]
    fn test_validate_tag_values_special_chars() {
        relay_test::setup();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let aggregator_config = test_config();

        let tag_value = "x".repeat(199) + "ø";
        assert_eq!(tag_value.chars().count(), 200); // Should be allowed
        let short_metric = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric".to_owned(),
            tags: BTreeMap::from([("foo".into(), tag_value.clone())]),
        };
        let validated_bucket =
            AggregatorService::validate_metric_tags(short_metric, &aggregator_config);
        assert_eq!(validated_bucket.tags["foo"], tag_value);
    }

    #[test]
    fn test_aggregator_cost_enforcement_total() {
        let config = AggregatorConfig {
            max_total_bucket_bytes: Some(1),
            ..test_config()
        };

        let metric = Metric {
            name: "c:transactions/foo".to_owned(),
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(999994711),
            tags: BTreeMap::new(),
        };

        let mut aggregator = AggregatorService::new(config, None);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        aggregator.insert(project_key, metric.clone()).unwrap();
        assert_eq!(
            aggregator.insert(project_key, metric).unwrap_err().kind,
            AggregateMetricsErrorKind::TotalLimitExceeded
        );
    }

    #[test]
    fn test_aggregator_cost_enforcement_project() {
        relay_test::setup();
        let config = AggregatorConfig {
            max_project_key_bucket_bytes: Some(1),
            ..test_config()
        };

        let metric = Metric {
            name: "c:transactions/foo".to_owned(),
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(999994711),
            tags: BTreeMap::new(),
        };

        let mut aggregator = AggregatorService::new(config, None);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        aggregator.insert(project_key, metric.clone()).unwrap();
        assert_eq!(
            aggregator.insert(project_key, metric).unwrap_err().kind,
            AggregateMetricsErrorKind::ProjectLimitExceeded
        );
    }

    #[must_use]
    fn run_test_bucket_partitioning(flush_partitions: Option<u64>) -> Vec<String> {
        let config = AggregatorConfig {
            max_flush_bytes: 1000,
            flush_partitions,
            ..test_config()
        };

        let metric1 = Metric {
            name: "c:transactions/foo".to_owned(),
            value: MetricValue::Counter(42.),
            timestamp: UnixTimestamp::from_secs(999994711),
            tags: BTreeMap::new(),
        };

        let metric2 = Metric {
            name: "c:transactions/bar".to_owned(),
            value: MetricValue::Counter(43.),
            timestamp: UnixTimestamp::from_secs(999994711),
            tags: BTreeMap::new(),
        };

        let mut aggregator = AggregatorService::new(config, None);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let captures = relay_statsd::with_capturing_test_client(|| {
            aggregator.insert(project_key, metric1).ok();
            aggregator.insert(project_key, metric2).ok();
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

        let buckets = Bucket::parse_all(json.as_bytes()).unwrap();

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

    #[test]
    fn test_parse_shift_key() {
        let json = r#"{"shift_key": "bucket"}"#;
        let parsed: AggregatorConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(parsed.shift_key, ShiftKey::Bucket));
    }
}
