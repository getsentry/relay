use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::hash::Hasher;
use std::iter::FromIterator;
use std::time::Duration;
use std::{fmt, mem};

use fnv::FnvHasher;
use relay_base_schema::project::ProjectKey;
use relay_common::time::{MonotonicResult, UnixTimestamp};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;

use crate::bucket::{Bucket, BucketValue};
use crate::protocol::{self, MetricNamespace, MetricResourceIdentifier};
use crate::statsd::{MetricCounters, MetricGauges, MetricHistograms, MetricSets, MetricTimers};

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

    /// Returns the namespace of this bucket.
    fn namespace(&self) -> MetricNamespace {
        match MetricResourceIdentifier::parse(&self.metric_name) {
            Ok(mri) => mri.namespace,
            Err(_) => MetricNamespace::Unsupported,
        }
    }
}

/// Estimates the number of bytes needed to encode the tags.
///
/// Note that this does not necessarily match the exact memory footprint of the tags,
/// because data structures or their serialization have overheads.
pub fn tags_cost(tags: &BTreeMap<String, String>) -> usize {
    tags.iter().map(|(k, v)| k.capacity() + v.capacity()).sum()
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

/// Parameters used by the [`Aggregator`].
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
}

impl AggregatorConfig {
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

    /// The delay to debounce backdated flushes.
    fn debounce_delay(&self) -> Duration {
        Duration::from_secs(self.debounce_delay)
    }

    /// Returns the time width buckets.
    fn bucket_interval(&self) -> Duration {
        Duration::from_secs(self.bucket_interval)
    }

    /// Returns the initial flush delay after the end of a bucket's original time window.
    fn initial_delay(&self) -> Duration {
        Duration::from_secs(self.initial_delay)
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
            return Err(AggregateMetricsErrorKind::InvalidTimestamp(timestamp).into());
        }

        Ok(output_timestamp)
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
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
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
        Some(other.flush_at.cmp(&self.flush_at))
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

/// A collector of [`Bucket`] submissions.
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
/// - `Gauge`: A summary of the reported values, see [`GaugeValue`](crate::GaugeValue).
///
/// # Conflicts
///
/// Metrics are uniquely identified by the combination of their name, type and unit. It is allowed
/// to send metrics of different types and units under the same name. For example, sending a metric
/// once as set and once as distribution will result in two actual metrics being recorded.
pub struct Aggregator {
    name: String,
    config: AggregatorConfig,
    buckets: HashMap<BucketKey, QueuedBucket>,
    cost_tracker: CostTracker,
}

impl Aggregator {
    /// Returns the name of the aggregator.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the number of buckets in the aggregator.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Returns `true` if the cost trackers value is larger than the given max cost.
    pub fn totals_cost_exceeded(&self, max_total_cost: Option<usize>) -> bool {
        self.cost_tracker.totals_cost_exceeded(max_total_cost)
    }

    /// Converts this aggregator into a vector of [`Bucket`].
    pub fn into_buckets(self) -> Vec<Bucket> {
        let bucket_interval = self.config.bucket_interval;

        self.buckets
            .into_iter()
            .map(|(key, entry)| Bucket {
                timestamp: key.timestamp,
                width: bucket_interval,
                name: key.metric_name,
                value: entry.value,
                tags: key.tags,
            })
            .collect()
    }

    /// Pop and return the buckets that are eligible for flushing out according to bucket interval.
    ///
    /// Note that this function is primarily intended for tests.
    pub fn pop_flush_buckets(&mut self, force: bool) -> HashMap<ProjectKey, Vec<HashedBucket>> {
        relay_statsd::metric!(
            gauge(MetricGauges::Buckets) = self.bucket_count() as u64,
            aggregator = &self.name,
        );

        // We only emit statsd metrics for the cost on flush (and not when merging the buckets),
        // assuming that this gives us more than enough data points.
        relay_statsd::metric!(
            gauge(MetricGauges::BucketsCost) = self.cost_tracker.total_cost as u64,
            aggregator = &self.name,
        );

        let mut buckets = HashMap::<ProjectKey, Vec<HashedBucket>>::new();
        let mut stats = HashMap::new();

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
                            .entry((value.ty(), key.namespace()))
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

        for ((ty, namespace), (bucket_count, item_count)) in stats.into_iter() {
            relay_statsd::metric!(
                gauge(MetricGauges::AvgBucketSize) = item_count as f64 / bucket_count as f64,
                metric_type = ty.as_str(),
                namespace = namespace.as_str(),
                aggregator = self.name(),
            );
        }

        buckets
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

    // Wrapper for [`AggregatorConfig::get_bucket_timestamp`].
    // Logs a statsd metric for invalid timestamps.
    fn get_bucket_timestamp(
        &self,
        timestamp: UnixTimestamp,
        bucket_width: u64,
    ) -> Result<UnixTimestamp, AggregateMetricsError> {
        let res = self.config.get_bucket_timestamp(timestamp, bucket_width);
        if let Err(AggregateMetricsError {
            kind: AggregateMetricsErrorKind::InvalidTimestamp(ts),
        }) = res
        {
            let delta = (ts.as_secs() as i64) - (UnixTimestamp::now().as_secs() as i64);
            relay_statsd::metric!(
                histogram(MetricHistograms::InvalidBucketTimestamp) = delta as f64,
                aggregator = &self.name,
            );
        }

        res
    }

    /// Merge a preaggregated bucket into this aggregator.
    ///
    /// If no bucket exists for the given bucket key, a new bucket will be created.
    pub fn merge(
        &mut self,
        project_key: ProjectKey,
        bucket: Bucket,
        max_total_bucket_bytes: Option<usize>,
    ) -> Result<(), AggregateMetricsError> {
        let timestamp = self.get_bucket_timestamp(bucket.timestamp, bucket.width)?;
        let key = BucketKey {
            project_key,
            timestamp,
            metric_name: bucket.name,
            tags: bucket.tags,
        };
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
            max_total_bucket_bytes,
            self.config.max_project_key_bucket_bytes,
        )?;

        let added_cost;
        match self.buckets.entry(key) {
            Entry::Occupied(mut entry) => {
                relay_statsd::metric!(
                    counter(MetricCounters::MergeHit) += 1,
                    aggregator = &self.name,
                    namespace = entry.key().namespace().as_str(),
                );
                let bucket_value = &mut entry.get_mut().value;
                let cost_before = bucket_value.cost();
                bucket_value
                    .merge(bucket.value)
                    .map_err(|_| AggregateMetricsErrorKind::InvalidTypes)?;
                let cost_after = bucket_value.cost();
                added_cost = cost_after.saturating_sub(cost_before);
            }
            Entry::Vacant(entry) => {
                relay_statsd::metric!(
                    counter(MetricCounters::MergeMiss) += 1,
                    aggregator = &self.name,
                    namespace = entry.key().namespace().as_str(),
                );
                relay_statsd::metric!(
                    set(MetricSets::UniqueBucketsCreated) = entry.key().hash64() as i64, // 2-complement
                    aggregator = &self.name,
                    namespace = entry.key().namespace().as_str(),
                );

                let flush_at = self.config.get_flush_time(entry.key());
                let value = bucket.value;
                added_cost = entry.key().cost() + value.cost();
                entry.insert(QueuedBucket::new(flush_at, value));
            }
        }

        self.cost_tracker.add_cost(project_key, added_cost);

        Ok(())
    }

    /// Merges all given `buckets` into this aggregator.
    ///
    /// Buckets that do not exist yet will be created.
    pub fn merge_all<I>(
        &mut self,
        project_key: ProjectKey,
        buckets: I,
        max_total_bucket_bytes: Option<usize>,
    ) where
        I: IntoIterator<Item = Bucket>,
    {
        for bucket in buckets.into_iter() {
            if let Err(error) = self.merge(project_key, bucket, max_total_bucket_bytes) {
                match &error.kind {
                    // Ignore invalid timestamp errors.
                    AggregateMetricsErrorKind::InvalidTimestamp(_) => {}
                    _other => {
                        relay_log::error!(
                            tags.aggregator = self.name,
                            bucket.error = &error as &dyn Error
                        );
                    }
                }
            }
        }
    }

    /// Split buckets into N logical partitions, determined by the bucket key.
    pub fn partition_buckets(
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

    /// Create a new aggregator.
    pub fn new(config: AggregatorConfig) -> Self {
        Self::named("default".to_owned(), config)
    }

    /// Like [`Self::new`], but with a provided name.
    pub fn named(name: String, config: AggregatorConfig) -> Self {
        Self {
            name,
            config,
            buckets: HashMap::new(),
            cost_tracker: CostTracker::default(),
        }
    }
}

impl fmt::Debug for Aggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("config", &self.config)
            .field("buckets", &self.buckets)
            .field("receiver", &format_args!("Recipient<FlushBuckets>"))
            .finish()
    }
}

#[cfg(test)]
mod tests {

    use similar_asserts::assert_eq;

    use super::*;
    use crate::{dist, GaugeValue};

    fn test_config() -> AggregatorConfig {
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

    #[test]
    fn test_aggregator_merge_counters() {
        relay_test::setup();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut aggregator = Aggregator::new(test_config());

        let bucket1 = some_bucket();

        let mut bucket2 = bucket1.clone();
        bucket2.value = BucketValue::counter(43.);
        aggregator.merge(project_key, bucket1, None).unwrap();
        aggregator.merge(project_key, bucket2, None).unwrap();

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
        assert_eq!(distribution.cost(), expected_bucket_value_size + 3 * 8);
        let gauge = BucketValue::Gauge(GaugeValue {
            last: 43.,
            min: 42.,
            max: 43.,
            sum: 85.,
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
    fn test_aggregator_merge_timestamps() {
        relay_test::setup();
        let mut config = test_config();
        config.bucket_interval = 10;

        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut aggregator = Aggregator::new(config);

        let bucket1 = some_bucket();

        let mut bucket2 = bucket1.clone();
        bucket2.timestamp = UnixTimestamp::from_secs(999994712);

        let mut bucket3 = bucket1.clone();
        bucket3.timestamp = UnixTimestamp::from_secs(999994721);
        aggregator.merge(project_key, bucket1, None).unwrap();
        aggregator.merge(project_key, bucket2, None).unwrap();
        aggregator.merge(project_key, bucket3, None).unwrap();

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

        let mut config = test_config();
        config.bucket_interval = 10;

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let mut aggregator = Aggregator::new(config);

        // It's OK to have same metric with different projects:
        aggregator.merge(project_key1, some_bucket(), None).unwrap();
        aggregator.merge(project_key2, some_bucket(), None).unwrap();

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
        let mut aggregator = Aggregator::new(test_config());
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        let bucket = Bucket {
            timestamp: UnixTimestamp::from_secs(999994711),
            width: 0,
            name: "c:transactions/foo@none".to_owned(),
            value: BucketValue::counter(42.),
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
                BucketValue::counter(42.),
                fixed_cost,
            ),
            ("c:transactions/foo@none", BucketValue::counter(42.), 0), // counters have constant size
            (
                "s:transactions/foo@none",
                BucketValue::set(123),
                fixed_cost + 4,
            ), // Added a new bucket + 1 element
            ("s:transactions/foo@none", BucketValue::set(123), 0), // Same element in set, no change
            ("s:transactions/foo@none", BucketValue::set(456), 4), // Different element in set -> +4
            (
                "d:transactions/foo@none",
                BucketValue::distribution(1.0),
                fixed_cost + 8,
            ), // New bucket + 1 element
            ("d:transactions/foo@none", BucketValue::distribution(1.0), 8), // duplicate element
            ("d:transactions/foo@none", BucketValue::distribution(2.0), 8), // 1 new element
            (
                "g:transactions/foo@none",
                BucketValue::gauge(0.3),
                fixed_cost,
            ), // New bucket
            ("g:transactions/foo@none", BucketValue::gauge(0.2), 0), // gauge has constant size
        ] {
            let mut bucket = bucket.clone();
            bucket.value = metric_value;
            bucket.name = metric_name.to_string();

            let current_cost = aggregator.cost_tracker.total_cost;
            aggregator.merge(project_key, bucket, None).unwrap();
            let total_cost = aggregator.cost_tracker.total_cost;
            assert_eq!(total_cost, current_cost + expected_added_cost);
        }

        aggregator.pop_flush_buckets(false);
        assert_eq!(aggregator.cost_tracker.total_cost, 0);
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

        let mut bucket_key = Aggregator::validate_bucket_key(bucket_key, &test_config()).unwrap();

        assert_eq!(bucket_key.tags.len(), 1);
        assert_eq!(
            bucket_key.tags.get("is_it_garbage"),
            Some(&"absolutely".to_owned())
        );
        assert_eq!(bucket_key.tags.get("another\0garbage"), None);

        bucket_key.metric_name = "hergus\0bergus".to_owned();
        Aggregator::validate_bucket_key(bucket_key, &test_config()).unwrap_err();
    }

    #[test]
    fn test_validate_bucket_key_str_lens() {
        relay_test::setup();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let short_metric = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric".to_owned(),
            tags: BTreeMap::new(),
        };
        assert!(Aggregator::validate_bucket_key(short_metric, &test_config()).is_ok());

        let long_metric = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/long_name_a_very_long_name_its_super_long_really_but_like_super_long_probably_the_longest_name_youve_seen_and_even_the_longest_name_ever_its_extremly_long_i_cant_tell_how_long_it_is_because_i_dont_have_that_many_fingers_thus_i_cant_count_the_many_characters_this_long_name_is".to_owned(),
            tags: BTreeMap::new(),
        };
        let validation = Aggregator::validate_bucket_key(long_metric, &test_config());

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
            Aggregator::validate_bucket_key(short_metric_long_tag_key, &test_config()).unwrap();
        assert_eq!(validation.tags.len(), 0);

        let short_metric_long_tag_value = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric_with_long_tag_value".to_owned(),
            tags: BTreeMap::from([("tag_key".into(), "i_run_out_of_creativity_so_here_we_go_Lorem_Ipsum_is_simply_dummy_text_of_the_printing_and_typesetting_industry_Lorem_Ipsum_has_been_the_industrys_standard_dummy_text_ever_since_the_1500s_when_an_unknown_printer_took_a_galley_of_type_and_scrambled_it_to_make_a_type_specimen_book".into())]),
        };
        let validation =
            Aggregator::validate_bucket_key(short_metric_long_tag_value, &test_config()).unwrap();
        assert_eq!(validation.tags.len(), 0);
    }

    #[test]
    fn test_validate_tag_values_special_chars() {
        relay_test::setup();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let tag_value = "x".repeat(199) + "ø";
        assert_eq!(tag_value.chars().count(), 200); // Should be allowed
        let short_metric = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric".to_owned(),
            tags: BTreeMap::from([("foo".into(), tag_value.clone())]),
        };
        let validated_bucket = Aggregator::validate_metric_tags(short_metric, &test_config());
        assert_eq!(validated_bucket.tags["foo"], tag_value);
    }

    #[test]
    fn test_aggregator_cost_enforcement_total() {
        let bucket = Bucket {
            timestamp: UnixTimestamp::from_secs(999994711),
            width: 0,
            name: "c:transactions/foo".to_owned(),
            value: BucketValue::counter(42.),
            tags: BTreeMap::new(),
        };

        let mut aggregator = Aggregator::new(test_config());
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        aggregator
            .merge(project_key, bucket.clone(), Some(1))
            .unwrap();

        assert_eq!(
            aggregator
                .merge(project_key, bucket, Some(1))
                .unwrap_err()
                .kind,
            AggregateMetricsErrorKind::TotalLimitExceeded
        );
    }

    #[test]
    fn test_aggregator_cost_enforcement_project() {
        relay_test::setup();
        let mut config = test_config();
        config.max_project_key_bucket_bytes = Some(1);

        let bucket = Bucket {
            timestamp: UnixTimestamp::from_secs(999994711),
            width: 0,
            name: "c:transactions/foo".to_owned(),
            value: BucketValue::counter(42.),
            tags: BTreeMap::new(),
        };

        let mut aggregator = Aggregator::new(config);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        aggregator.merge(project_key, bucket.clone(), None).unwrap();
        assert_eq!(
            aggregator
                .merge(project_key, bucket, None)
                .unwrap_err()
                .kind,
            AggregateMetricsErrorKind::ProjectLimitExceeded
        );
    }

    #[test]
    fn test_parse_shift_key() {
        let json = r#"{"shift_key": "bucket"}"#;
        let parsed: AggregatorConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(parsed.shift_key, ShiftKey::Bucket));
    }
}
