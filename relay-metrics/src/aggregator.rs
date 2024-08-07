//! Core functionality of metrics aggregation.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use std::{fmt, mem};

use fnv::FnvHasher;
use priority_queue::PriorityQueue;
use relay_base_schema::project::ProjectKey;
use relay_common::time::UnixTimestamp;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;

use crate::bucket::{Bucket, BucketValue};
use crate::statsd::{MetricCounters, MetricGauges, MetricHistograms, MetricSets, MetricTimers};
use crate::{BucketMetadata, FiniteF64, MetricName, MetricNamespace};

use hashbrown::HashMap;

/// Any error that may occur during aggregation.
#[derive(Debug, Error, PartialEq)]
pub enum AggregateMetricsError {
    /// A metric bucket's timestamp was out of the configured acceptable range.
    #[error("found invalid timestamp: {0}")]
    InvalidTimestamp(UnixTimestamp),
    /// Internal error: Attempted to merge two metric buckets of different types.
    #[error("found incompatible metric types")]
    InvalidTypes,
    /// A metric bucket had a too long string (metric name or a tag key/value).
    #[error("found invalid string: {0}")]
    InvalidStringLength(MetricName),
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
    metric_name: MetricName,
    tags: BTreeMap<String, String>,
    extracted_from_indexed: bool,
}

impl BucketKey {
    /// Creates a 64-bit hash of the bucket key using FnvHasher.
    ///
    /// This is used for partition key computation and statsd logging.
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
        mem::size_of::<Self>() + self.metric_name.len() + tags_cost(&self.tags)
    }

    /// Returns the namespace of this bucket.
    fn namespace(&self) -> MetricNamespace {
        self.metric_name.namespace()
    }

    /// Computes a stable partitioning key for this [`Bucket`].
    ///
    /// The partitioning key is inherently producing collisions, since the output of the hasher is
    /// reduced into an interval of size `partitions`. This means that buckets with totally
    /// different values might end up in the same partition.
    ///
    /// The role of partitioning is to let Relays forward the same metric to the same upstream
    /// instance with the goal of increasing bucketing efficiency.
    fn partition_key(&self, partitions: u64) -> u64 {
        let key = (self.project_key, &self.metric_name, &self.tags);

        let mut hasher = fnv::FnvHasher::default();
        key.hash(&mut hasher);

        let partitions = partitions.max(1);
        hasher.finish() % partitions
    }
}

/// Estimates the number of bytes needed to encode the tags.
///
/// Note that this does not necessarily match the exact memory footprint of the tags,
/// because data structures or their serialization have overheads.
pub fn tags_cost(tags: &BTreeMap<String, String>) -> usize {
    tags.iter().map(|(k, v)| k.len() + v.len()).sum()
}

/// Configuration value for [`AggregatorConfig::flush_batching`].
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FlushBatching {
    /// Shifts the flush time by an offset based on the [`ProjectKey`].
    ///
    /// This allows buckets from the same project to be flushed together.
    #[default]
    Project,

    /// Shifts the flush time by an offset based on the bucket key itself.
    ///
    /// This allows for a completely random distribution of bucket flush times.
    ///
    /// It should only be used in processing Relays since this flushing behavior it's better
    /// suited for how Relay emits metrics to Kafka.
    Bucket,

    /// Shifts the flush time by an offset based on the partition key.
    ///
    /// This allows buckets belonging to the same partition to be flushed together. Requires
    /// [`flush_partitions`](AggregatorConfig::flush_partitions) to be set, otherwise this will fall
    /// back to [`FlushBatching::Project`].
    ///
    /// It should only be used in Relays with the `http.global_metrics` option set since when
    /// encoding metrics via the global endpoint we can leverage partitioning.
    Partition,

    /// Do not apply shift.
    None,
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
    /// for buckets that are being reported in real time.
    ///
    /// Relay applies up to a full `bucket_interval` of additional jitter after the initial delay to spread out flushing real time buckets.
    pub initial_delay: u64,

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

    /// The number of logical partitions that can receive flushed buckets.
    ///
    /// If set, buckets are partitioned by (bucket key % flush_partitions), and routed
    /// by setting the header `X-Sentry-Relay-Shard`.
    ///
    /// This setting will take effect only when paired with [`FlushBatching::Partition`].
    pub flush_partitions: Option<u64>,

    /// The batching mode for the flushing of the aggregator.
    ///
    /// Batching is applied via shifts to the flushing time that is determined when the first bucket
    /// is inserted. Thanks to the shifts, Relay is able to prevent flushing all buckets from a
    /// bucket interval at the same time.
    ///
    /// For example, the aggregator can choose to shift by the same value all buckets within a given
    /// partition, effectively allowing all the elements of that partition to be flushed together.
    #[serde(alias = "shift_key")]
    pub flush_batching: FlushBatching,
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

    /// Shift deterministically within one bucket interval based on the configured [`FlushBatching`].
    ///
    /// This distributes buckets over time to prevent peaks.
    fn flush_time_shift(&self, bucket: &BucketKey) -> Duration {
        let shift_range = self.bucket_interval * 1000;

        // Fall back to default flushing by project if no partitioning is configured.
        let (batching, partitions) = match (self.flush_batching, self.flush_partitions) {
            (FlushBatching::Partition, None | Some(0)) => (FlushBatching::Project, 0),
            (flush_batching, partitions) => (flush_batching, partitions.unwrap_or(0)),
        };

        let shift_millis = match batching {
            FlushBatching::Project => {
                let mut hasher = FnvHasher::default();
                hasher.write(bucket.project_key.as_str().as_bytes());
                hasher.finish() % shift_range
            }
            FlushBatching::Bucket => bucket.hash64() % shift_range,
            FlushBatching::Partition => shift_range * bucket.partition_key(partitions) / partitions,
            FlushBatching::None => 0,
        };

        Duration::from_millis(shift_millis)
    }

    /// Determines the target bucket for an incoming bucket timestamp and bucket width.
    ///
    /// We select the output bucket which overlaps with the center of the incoming bucket.
    /// Fails if timestamp is too old or too far into the future.
    fn get_bucket_timestamp(&self, timestamp: UnixTimestamp, bucket_width: u64) -> UnixTimestamp {
        // Find middle of the input bucket to select a target
        let ts = timestamp.as_secs().saturating_add(bucket_width / 2);
        // Align target_timestamp to output bucket width
        let ts = (ts / self.bucket_interval) * self.bucket_interval;
        UnixTimestamp::from_secs(ts)
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
            max_secs_in_past: 5 * 24 * 60 * 60, // 5 days, as for sessions
            max_secs_in_future: 60,             // 1 minute
            max_name_length: 200,
            max_tag_key_length: 200,
            max_tag_value_length: 200,
            max_project_key_bucket_bytes: None,
            flush_batching: FlushBatching::default(),
            flush_partitions: None,
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
    metadata: BucketMetadata,
}

impl QueuedBucket {
    /// Creates a new `QueuedBucket` with a given flush time.
    fn new(flush_at: Instant, value: BucketValue, metadata: BucketMetadata) -> Self {
        Self {
            flush_at,
            value,
            metadata,
        }
    }

    /// Returns `true` if the flush time has elapsed.
    fn elapsed(&self, now: Instant) -> bool {
        now > self.flush_at
    }

    /// Merges a bucket into the current queued bucket.
    ///
    /// Returns the value cost increase on success,
    /// otherwise returns an error if the passed bucket value type does not match
    /// the contained type.
    fn merge(
        &mut self,
        value: BucketValue,
        metadata: BucketMetadata,
    ) -> Result<usize, AggregateMetricsError> {
        let cost_before = self.value.cost();

        self.value
            .merge(value)
            .map_err(|_| AggregateMetricsError::InvalidTypes)?;
        self.metadata.merge(metadata);

        Ok(self.value.cost().saturating_sub(cost_before))
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

#[derive(Default)]
struct CostTracker {
    total_cost: usize,
    cost_per_project_key: HashMap<ProjectKey, usize>,
    cost_per_namespace: BTreeMap<MetricNamespace, usize>,
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
            return Err(AggregateMetricsError::TotalLimitExceeded);
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
                return Err(AggregateMetricsError::ProjectLimitExceeded);
            }
        }

        Ok(())
    }

    fn add_cost(&mut self, namespace: MetricNamespace, project_key: ProjectKey, cost: usize) {
        *self.cost_per_project_key.entry(project_key).or_insert(0) += cost;
        *self.cost_per_namespace.entry(namespace).or_insert(0) += cost;
        self.total_cost += cost;
    }

    fn subtract_cost(&mut self, namespace: MetricNamespace, project_key: ProjectKey, cost: usize) {
        let project_cost = self.cost_per_project_key.get_mut(&project_key);
        let namespace_cost = self.cost_per_namespace.get_mut(&namespace);
        match (project_cost, namespace_cost) {
            (Some(project_cost), Some(namespace_cost))
                if *project_cost >= cost && *namespace_cost >= cost =>
            {
                *project_cost -= cost;
                if *project_cost == 0 {
                    self.cost_per_project_key.remove(&project_key);
                }
                *namespace_cost -= cost;
                if *namespace_cost == 0 {
                    self.cost_per_namespace.remove(&namespace);
                }
                self.total_cost -= cost;
            }
            _ => {
                relay_log::error!(
                    namespace = namespace.as_str(),
                    project_key = project_key.to_string(),
                    cost = cost.to_string(),
                    "Cost mismatch",
                );
            }
        }
    }
}

impl fmt::Debug for CostTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CostTracker")
            .field("total_cost", &self.total_cost)
            // Convert HashMap to BTreeMap to guarantee order:
            .field(
                "cost_per_project_key",
                &BTreeMap::from_iter(self.cost_per_project_key.iter()),
            )
            .field("cost_per_namespace", &self.cost_per_namespace)
            .finish()
    }
}

/// Returns the instant at which a bucket should be flushed.
///
/// All buckets are flushed after a grace period of `initial_delay`.
fn get_flush_time(
    config: &AggregatorConfig,
    reference_time: Instant,
    bucket_key: &BucketKey,
) -> Instant {
    let initial_flush = bucket_key.timestamp + config.bucket_interval() + config.initial_delay();

    let now = UnixTimestamp::now();
    let backdated = initial_flush <= now;

    let delay = now.as_secs() as i64 - bucket_key.timestamp.as_secs() as i64;
    relay_statsd::metric!(
        histogram(MetricHistograms::BucketsDelay) = delay as f64,
        backdated = if backdated { "true" } else { "false" },
    );

    let flush_timestamp = if backdated {
        // If the initial flush time has passed or can't be represented, we want to treat the
        // flush of the bucket as if it came in with the timestamp of current bucket based on
        // the now timestamp.
        //
        // The rationale behind this is that we want to flush this bucket in the earliest slot
        // together with buckets that have similar characteristics (e.g., same partition,
        // project...).
        let floored_timestamp = (now.as_secs() / config.bucket_interval) * config.bucket_interval;
        UnixTimestamp::from_secs(floored_timestamp)
            + config.bucket_interval()
            + config.initial_delay()
    } else {
        // If the initial flush is still pending, use that.
        initial_flush
    };

    let instant = if flush_timestamp > now {
        Instant::now().checked_add(flush_timestamp - now)
    } else {
        Instant::now().checked_sub(now - flush_timestamp)
    }
    .unwrap_or_else(Instant::now);

    // Since `Instant` doesn't allow to get directly how many seconds elapsed, we leverage the
    // diffing to get a duration and round it to the smallest second to get consistent times.
    let instant = reference_time + Duration::from_secs((instant - reference_time).as_secs());
    instant + config.flush_time_shift(bucket_key)
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
    buckets: PriorityQueue<BucketKey, QueuedBucket>,
    cost_tracker: CostTracker,
    reference_time: Instant,
}

impl Aggregator {
    /// Create a new aggregator.
    pub fn new(config: AggregatorConfig) -> Self {
        Self::named("default".to_owned(), config)
    }

    /// Like [`Self::new`], but with a provided name.
    pub fn named(name: String, config: AggregatorConfig) -> Self {
        Self {
            name,
            config,
            buckets: Default::default(),
            cost_tracker: CostTracker::default(),
            reference_time: Instant::now(),
        }
    }

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
        relay_statsd::metric!(
            gauge(MetricGauges::Buckets) = self.bucket_count() as u64,
            aggregator = &self.name,
        );

        let bucket_interval = self.config.bucket_interval;

        self.buckets
            .into_iter()
            .map(|(key, entry)| Bucket {
                timestamp: key.timestamp,
                width: bucket_interval,
                name: key.metric_name,
                value: entry.value,
                tags: key.tags,
                metadata: entry.metadata,
            })
            .collect()
    }

    /// Pop and return the partitions with buckets that are eligible for flushing out according to
    /// bucket interval.
    ///
    /// If no partitioning is enabled, the function will return a single `None` partition.
    ///
    /// Note that this function is primarily intended for tests.
    pub fn pop_flush_buckets(
        &mut self,
        force: bool,
    ) -> HashMap<Option<u64>, HashMap<ProjectKey, Vec<Bucket>>> {
        relay_statsd::metric!(
            gauge(MetricGauges::Buckets) = self.bucket_count() as u64,
            aggregator = &self.name,
        );

        // We only emit statsd metrics for the cost on flush (and not when merging the buckets),
        // assuming that this gives us more than enough data points.
        for (namespace, cost) in &self.cost_tracker.cost_per_namespace {
            relay_statsd::metric!(
                gauge(MetricGauges::BucketsCost) = *cost as u64,
                aggregator = &self.name,
                namespace = namespace.as_str()
            );
        }

        let mut partitions = HashMap::new();
        let mut stats = HashMap::new();

        let now = Instant::now();

        relay_statsd::metric!(
            timer(MetricTimers::BucketsScanDuration),
            aggregator = &self.name,
            {
                let bucket_interval = self.config.bucket_interval;
                let cost_tracker = &mut self.cost_tracker;

                while let Some((_, entry)) = self.buckets.peek() {
                    if !entry.elapsed(now) && !force {
                        break;
                    }

                    let (key, entry) = self.buckets.pop().expect("pop after peek");
                    cost_tracker.subtract_cost(key.namespace(), key.project_key, key.cost());
                    cost_tracker.subtract_cost(
                        key.namespace(),
                        key.project_key,
                        entry.value.cost(),
                    );

                    let (bucket_count, item_count) = stats
                        .entry((entry.value.ty(), key.namespace()))
                        .or_insert((0usize, 0usize));
                    *bucket_count += 1;
                    *item_count += entry.value.len();

                    let partition = self.config.flush_partitions.map(|p| key.partition_key(p));

                    let bucket = Bucket {
                        timestamp: key.timestamp,
                        width: bucket_interval,
                        name: key.metric_name,
                        value: entry.value,
                        tags: key.tags,
                        metadata: entry.metadata,
                    };

                    partitions
                        .entry(partition)
                        .or_insert_with(HashMap::new)
                        .entry(key.project_key)
                        .or_insert_with(Vec::new)
                        .push(bucket);
                }
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

        partitions
    }

    /// Wrapper for [`AggregatorConfig::get_bucket_timestamp`].
    ///
    /// Logs a statsd metric for invalid timestamps.
    fn get_bucket_timestamp(
        &self,
        timestamp: UnixTimestamp,
        bucket_width: u64,
    ) -> Result<UnixTimestamp, AggregateMetricsError> {
        let bucket_ts = self.config.get_bucket_timestamp(timestamp, bucket_width);

        if !self.config.timestamp_range().contains(&bucket_ts) {
            let delta = (bucket_ts.as_secs() as i64) - (UnixTimestamp::now().as_secs() as i64);
            relay_statsd::metric!(
                histogram(MetricHistograms::InvalidBucketTimestamp) = delta as f64,
                aggregator = &self.name,
            );

            return Err(AggregateMetricsError::InvalidTimestamp(timestamp));
        }

        Ok(bucket_ts)
    }

    /// Merge a preaggregated bucket into this aggregator.
    ///
    /// If no bucket exists for the given bucket key, a new bucket will be created.
    pub fn merge(
        &mut self,
        project_key: ProjectKey,
        mut bucket: Bucket,
        max_total_bucket_bytes: Option<usize>,
    ) -> Result<(), AggregateMetricsError> {
        let timestamp = self.get_bucket_timestamp(bucket.timestamp, bucket.width)?;
        let key = BucketKey {
            project_key,
            timestamp,
            metric_name: bucket.name,
            tags: bucket.tags,
            extracted_from_indexed: bucket.metadata.extracted_from_indexed,
        };
        let key = validate_bucket_key(key, &self.config)?;
        let namespace = key.namespace();

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

        let mut added_cost = 0;

        let mut error = None;
        let updated = self.buckets.change_priority_by(&key, |value| {
            relay_statsd::metric!(
                counter(MetricCounters::MergeHit) += 1,
                aggregator = &self.name,
                namespace = key.namespace().as_str(),
            );

            let bv = std::mem::replace(
                &mut bucket.value,
                BucketValue::Counter(FiniteF64::default()),
            );
            match value.merge(bv, bucket.metadata) {
                Ok(ac) => added_cost = ac,
                Err(err) => error = Some(err),
            }
        });

        if let Some(error) = error {
            return Err(error);
        }

        if !updated {
            relay_statsd::metric!(
                counter(MetricCounters::MergeMiss) += 1,
                aggregator = &self.name,
                namespace = key.namespace().as_str(),
            );
            relay_statsd::metric!(
                set(MetricSets::UniqueBucketsCreated) = key.hash64() as i64, // 2-complement
                aggregator = &self.name,
                namespace = key.namespace().as_str(),
            );

            let flush_at = get_flush_time(&self.config, self.reference_time, &key);
            let value = bucket.value;
            added_cost = key.cost() + value.cost();

            self.buckets
                .push(key, QueuedBucket::new(flush_at, value, bucket.metadata));
        }

        self.cost_tracker
            .add_cost(namespace, project_key, added_cost);

        Ok(())
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

/// Validates the metric name and its tags are correct.
///
/// Returns `Err` if the metric should be dropped.
fn validate_bucket_key(
    mut key: BucketKey,
    aggregator_config: &AggregatorConfig,
) -> Result<BucketKey, AggregateMetricsError> {
    key = validate_metric_name(key, aggregator_config)?;
    key = validate_metric_tags(key, aggregator_config);
    Ok(key)
}

/// Validates metric name against [`AggregatorConfig`].
///
/// Returns `Err` if the metric must be dropped.
fn validate_metric_name(
    key: BucketKey,
    aggregator_config: &AggregatorConfig,
) -> Result<BucketKey, AggregateMetricsError> {
    let metric_name_length = key.metric_name.len();
    if metric_name_length > aggregator_config.max_name_length {
        relay_log::debug!(
            "Invalid metric name, too long (> {}): {:?}",
            aggregator_config.max_name_length,
            key.metric_name
        );
        return Err(AggregateMetricsError::InvalidStringLength(key.metric_name));
    }

    Ok(key)
}

/// Validates metric tags against [`AggregatorConfig`].
///
/// Invalid tags are removed.
fn validate_metric_tags(mut key: BucketKey, aggregator_config: &AggregatorConfig) -> BucketKey {
    key.tags.retain(|tag_key, tag_value| {
        if tag_key.len() > aggregator_config.max_tag_key_length {
            relay_log::debug!("Invalid metric tag key");
            return false;
        }
        if bytecount::num_chars(tag_value.as_bytes()) > aggregator_config.max_tag_value_length {
            relay_log::debug!("Invalid metric tag value");
            return false;
        }

        true
    });
    key
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use similar_asserts::assert_eq;

    use super::*;
    use crate::{dist, GaugeValue};

    fn test_config() -> AggregatorConfig {
        AggregatorConfig {
            bucket_interval: 1,
            initial_delay: 0,
            max_secs_in_past: 50 * 365 * 24 * 60 * 60,
            max_secs_in_future: 50 * 365 * 24 * 60 * 60,
            max_name_length: 200,
            max_tag_key_length: 200,
            max_tag_value_length: 200,
            max_project_key_bucket_bytes: None,
            flush_batching: FlushBatching::default(),
            flush_partitions: None,
        }
    }

    fn some_bucket(timestamp: Option<UnixTimestamp>) -> Bucket {
        let timestamp = timestamp.map_or(UnixTimestamp::from_secs(999994711), |t| t);
        Bucket {
            timestamp,
            width: 0,
            name: "c:transactions/foo@none".into(),
            value: BucketValue::counter(42.into()),
            tags: BTreeMap::new(),
            metadata: BucketMetadata::new(timestamp),
        }
    }

    #[test]
    fn test_aggregator_merge_counters() {
        relay_test::setup();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut aggregator: Aggregator = Aggregator::new(test_config());

        let bucket1 = some_bucket(None);

        let mut bucket2 = bucket1.clone();
        bucket2.value = BucketValue::counter(43.into());
        aggregator.merge(project_key, bucket1, None).unwrap();
        aggregator.merge(project_key, bucket2, None).unwrap();

        let buckets: Vec<_> = aggregator
            .buckets
            .iter()
            .map(|(k, e)| (k, &e.value)) // skip flush times, they are different every time
            .collect();

        insta::assert_debug_snapshot!(buckets, @r###"
        [
            (
                BucketKey {
                    project_key: ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"),
                    timestamp: UnixTimestamp(999994711),
                    metric_name: MetricName(
                        "c:transactions/foo@none",
                    ),
                    tags: {},
                    extracted_from_indexed: false,
                },
                Counter(
                    85.0,
                ),
            ),
        ]
        "###);
    }

    #[test]
    fn test_bucket_value_cost() {
        // When this test fails, it means that the cost model has changed.
        // Check dimensionality limits.
        let expected_bucket_value_size = 48;
        let expected_set_entry_size = 4;

        let counter = BucketValue::Counter(123.into());
        assert_eq!(counter.cost(), expected_bucket_value_size);
        let set = BucketValue::Set([1, 2, 3, 4, 5].into());
        assert_eq!(
            set.cost(),
            expected_bucket_value_size + 5 * expected_set_entry_size
        );
        let distribution = BucketValue::Distribution(dist![1, 2, 3]);
        assert_eq!(distribution.cost(), expected_bucket_value_size + 3 * 8);
        let gauge = BucketValue::Gauge(GaugeValue {
            last: 43.into(),
            min: 42.into(),
            max: 43.into(),
            sum: 85.into(),
            count: 2,
        });
        assert_eq!(gauge.cost(), expected_bucket_value_size);
    }

    #[test]
    fn test_bucket_key_cost() {
        // When this test fails, it means that the cost model has changed.
        // Check dimensionality limits.
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let metric_name = "12345".into();
        let bucket_key = BucketKey {
            timestamp: UnixTimestamp::now(),
            project_key,
            metric_name,
            tags: BTreeMap::from([
                ("hello".to_owned(), "world".to_owned()),
                ("answer".to_owned(), "42".to_owned()),
            ]),
            extracted_from_indexed: false,
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
        let mut aggregator: Aggregator = Aggregator::new(config);

        let bucket1 = some_bucket(None);

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
        insta::assert_debug_snapshot!(buckets, @r###"
        [
            (
                BucketKey {
                    project_key: ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"),
                    timestamp: UnixTimestamp(999994710),
                    metric_name: MetricName(
                        "c:transactions/foo@none",
                    ),
                    tags: {},
                    extracted_from_indexed: false,
                },
                Counter(
                    84.0,
                ),
            ),
            (
                BucketKey {
                    project_key: ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"),
                    timestamp: UnixTimestamp(999994720),
                    metric_name: MetricName(
                        "c:transactions/foo@none",
                    ),
                    tags: {},
                    extracted_from_indexed: false,
                },
                Counter(
                    42.0,
                ),
            ),
        ]
        "###);
    }

    #[test]
    fn test_aggregator_mixed_projects() {
        relay_test::setup();

        let mut config = test_config();
        config.bucket_interval = 10;

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let mut aggregator: Aggregator = Aggregator::new(config);

        // It's OK to have same metric with different projects:
        aggregator
            .merge(project_key1, some_bucket(None), None)
            .unwrap();
        aggregator
            .merge(project_key2, some_bucket(None), None)
            .unwrap();

        assert_eq!(aggregator.buckets.len(), 2);
    }

    #[test]
    fn test_cost_tracker() {
        let namespace = MetricNamespace::Custom;
        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let project_key3 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();
        let mut cost_tracker = CostTracker::default();
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 0,
            cost_per_project_key: {},
            cost_per_namespace: {},
        }
        "#);
        cost_tracker.add_cost(MetricNamespace::Custom, project_key1, 50);
        cost_tracker.add_cost(MetricNamespace::Profiles, project_key1, 50);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 100,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 50,
            },
        }
        "#);
        cost_tracker.add_cost(namespace, project_key2, 200);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 300,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 250,
            },
        }
        "#);
        // Unknown project: bail
        cost_tracker.subtract_cost(namespace, project_key3, 666);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 300,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 250,
            },
        }
        "#);
        // Subtract too much: bail
        cost_tracker.subtract_cost(namespace, project_key1, 666);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 300,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 200,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 250,
            },
        }
        "#);
        cost_tracker.subtract_cost(namespace, project_key2, 20);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 280,
            cost_per_project_key: {
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fed"): 100,
                ProjectKey("a94ae32be2584e0bbd7a4cbb95971fee"): 180,
            },
            cost_per_namespace: {
                Profiles: 50,
                Custom: 230,
            },
        }
        "#);

        // Subtract all
        cost_tracker.subtract_cost(MetricNamespace::Profiles, project_key1, 50);
        cost_tracker.subtract_cost(MetricNamespace::Custom, project_key1, 50);
        cost_tracker.subtract_cost(MetricNamespace::Custom, project_key2, 180);
        insta::assert_debug_snapshot!(cost_tracker, @r#"
        CostTracker {
            total_cost: 0,
            cost_per_project_key: {},
            cost_per_namespace: {},
        }
        "#);
    }

    #[test]
    fn test_aggregator_cost_tracking() {
        // Make sure that the right cost is added / subtracted
        let mut aggregator: Aggregator = Aggregator::new(test_config());
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        let timestamp = UnixTimestamp::from_secs(999994711);
        let bucket = Bucket {
            timestamp,
            width: 0,
            name: "c:transactions/foo@none".into(),
            value: BucketValue::counter(42.into()),
            tags: BTreeMap::new(),
            metadata: BucketMetadata::new(timestamp),
        };
        let bucket_key = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/foo@none".into(),
            tags: BTreeMap::new(),
            extracted_from_indexed: false,
        };
        let fixed_cost = bucket_key.cost() + mem::size_of::<BucketValue>();
        for (metric_name, metric_value, expected_added_cost) in [
            (
                "c:transactions/foo@none",
                BucketValue::counter(42.into()),
                fixed_cost,
            ),
            (
                "c:transactions/foo@none",
                BucketValue::counter(42.into()),
                0,
            ), // counters have constant size
            (
                "s:transactions/foo@none",
                BucketValue::set(123),
                fixed_cost + 4,
            ), // Added a new bucket + 1 element
            ("s:transactions/foo@none", BucketValue::set(123), 0), // Same element in set, no change
            ("s:transactions/foo@none", BucketValue::set(456), 4), // Different element in set -> +4
            (
                "d:transactions/foo@none",
                BucketValue::distribution(1.into()),
                fixed_cost + 8,
            ), // New bucket + 1 element
            (
                "d:transactions/foo@none",
                BucketValue::distribution(1.into()),
                8,
            ), // duplicate element
            (
                "d:transactions/foo@none",
                BucketValue::distribution(2.into()),
                8,
            ), // 1 new element
            (
                "g:transactions/foo@none",
                BucketValue::gauge(3.into()),
                fixed_cost,
            ), // New bucket
            ("g:transactions/foo@none", BucketValue::gauge(2.into()), 0), // gauge has constant size
        ] {
            let mut bucket = bucket.clone();
            bucket.value = metric_value;
            bucket.name = metric_name.into();

            let current_cost = aggregator.cost_tracker.total_cost;
            aggregator.merge(project_key, bucket, None).unwrap();
            let total_cost = aggregator.cost_tracker.total_cost;
            assert_eq!(total_cost, current_cost + expected_added_cost);
        }

        aggregator.pop_flush_buckets(true);
        assert_eq!(aggregator.cost_tracker.total_cost, 0);
    }

    #[tokio::test]
    async fn test_aggregator_flush() {
        // Make sure that the right cost is added / subtracted
        let mut aggregator: Aggregator = Aggregator::new(AggregatorConfig {
            bucket_interval: 10,
            ..test_config()
        });
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        let now = UnixTimestamp::now();
        tokio::time::pause();

        for i in 0..3u32 {
            for (name, offset) in [("foo", 30), ("bar", 15)] {
                let timestamp = now + Duration::from_secs(60 * u64::from(i) + offset);
                let bucket = Bucket {
                    timestamp,
                    width: 0,
                    name: format!("c:transactions/{name}_{i}@none").into(),
                    value: BucketValue::counter(i.into()),
                    tags: BTreeMap::new(),
                    metadata: BucketMetadata::new(timestamp),
                };

                aggregator.merge(project_key, bucket, None).unwrap();
            }
        }

        let mut flush_buckets = || {
            let mut result = Vec::new();
            for (partition, v) in aggregator.pop_flush_buckets(false) {
                assert!(partition.is_none());
                for (pk, buckets) in v {
                    assert_eq!(pk, project_key);
                    for bucket in buckets {
                        result.push(bucket.name.to_string());
                    }
                }
            }
            result.sort();
            result
        };

        assert!(flush_buckets().is_empty());

        tokio::time::advance(Duration::from_secs(60)).await;
        assert_debug_snapshot!(flush_buckets(), @r###"
        [
            "c:transactions/bar_0@none",
            "c:transactions/foo_0@none",
        ]
        "###);
        assert!(flush_buckets().is_empty());

        tokio::time::advance(Duration::from_secs(60)).await;
        assert_debug_snapshot!(flush_buckets(), @r###"
        [
            "c:transactions/bar_1@none",
            "c:transactions/foo_1@none",
        ]
        "###);
        assert!(flush_buckets().is_empty());

        tokio::time::advance(Duration::from_secs(60)).await;
        assert_debug_snapshot!(flush_buckets(), @r###"
        [
            "c:transactions/bar_2@none",
            "c:transactions/foo_2@none",
        ]
        "###);
        assert!(flush_buckets().is_empty());

        tokio::time::advance(Duration::from_secs(3600)).await;
        assert!(flush_buckets().is_empty());

        assert!(aggregator.into_buckets().is_empty());
    }

    #[test]
    fn test_get_bucket_timestamp_overflow() {
        let config = AggregatorConfig {
            bucket_interval: 10,
            initial_delay: 0,
            ..Default::default()
        };

        let aggregator: Aggregator = Aggregator::new(config);

        assert!(matches!(
            aggregator
                .get_bucket_timestamp(UnixTimestamp::from_secs(u64::MAX), 2)
                .unwrap_err(),
            AggregateMetricsError::InvalidTimestamp(_)
        ));
    }

    #[test]
    fn test_get_bucket_timestamp_zero() {
        let config = AggregatorConfig {
            bucket_interval: 10,
            initial_delay: 0,
            ..Default::default()
        };

        let now = UnixTimestamp::now().as_secs();
        let rounded_now = UnixTimestamp::from_secs(now / 10 * 10);
        assert_eq!(
            config.get_bucket_timestamp(UnixTimestamp::from_secs(now), 0),
            rounded_now
        );
    }

    #[test]
    fn test_get_bucket_timestamp_multiple() {
        let config = AggregatorConfig {
            bucket_interval: 10,
            initial_delay: 0,
            ..Default::default()
        };

        let rounded_now = UnixTimestamp::now().as_secs() / 10 * 10;
        let now = rounded_now + 3;
        assert_eq!(
            config
                .get_bucket_timestamp(UnixTimestamp::from_secs(now), 20)
                .as_secs(),
            rounded_now + 10
        );
    }

    #[test]
    fn test_get_bucket_timestamp_non_multiple() {
        let config = AggregatorConfig {
            bucket_interval: 10,
            initial_delay: 0,
            ..Default::default()
        };

        let rounded_now = UnixTimestamp::now().as_secs() / 10 * 10;
        let now = rounded_now + 3;
        assert_eq!(
            config
                .get_bucket_timestamp(UnixTimestamp::from_secs(now), 23)
                .as_secs(),
            rounded_now + 10
        );
    }

    #[test]
    fn test_validate_bucket_key_str_length() {
        relay_test::setup();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let short_metric = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric".into(),
            tags: BTreeMap::new(),
            extracted_from_indexed: false,
        };
        assert!(validate_bucket_key(short_metric, &test_config()).is_ok());

        let long_metric = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/long_name_a_very_long_name_its_super_long_really_but_like_super_long_probably_the_longest_name_youve_seen_and_even_the_longest_name_ever_its_extremly_long_i_cant_tell_how_long_it_is_because_i_dont_have_that_many_fingers_thus_i_cant_count_the_many_characters_this_long_name_is".into(),
            tags: BTreeMap::new(),
            extracted_from_indexed: false,
        };
        let validation = validate_bucket_key(long_metric, &test_config());

        assert!(matches!(
            validation.unwrap_err(),
            AggregateMetricsError::InvalidStringLength(_),
        ));

        let short_metric_long_tag_key = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric_with_long_tag_key".into(),
            tags: BTreeMap::from([("i_run_out_of_creativity_so_here_we_go_Lorem_Ipsum_is_simply_dummy_text_of_the_printing_and_typesetting_industry_Lorem_Ipsum_has_been_the_industrys_standard_dummy_text_ever_since_the_1500s_when_an_unknown_printer_took_a_galley_of_type_and_scrambled_it_to_make_a_type_specimen_book".into(), "tag_value".into())]),
            extracted_from_indexed: false,
        };
        let validation = validate_bucket_key(short_metric_long_tag_key, &test_config()).unwrap();
        assert_eq!(validation.tags.len(), 0);

        let short_metric_long_tag_value = BucketKey {
            project_key,
            timestamp: UnixTimestamp::now(),
            metric_name: "c:transactions/a_short_metric_with_long_tag_value".into(),
            tags: BTreeMap::from([("tag_key".into(), "i_run_out_of_creativity_so_here_we_go_Lorem_Ipsum_is_simply_dummy_text_of_the_printing_and_typesetting_industry_Lorem_Ipsum_has_been_the_industrys_standard_dummy_text_ever_since_the_1500s_when_an_unknown_printer_took_a_galley_of_type_and_scrambled_it_to_make_a_type_specimen_book".into())]),
            extracted_from_indexed: false,
        };
        let validation = validate_bucket_key(short_metric_long_tag_value, &test_config()).unwrap();
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
            metric_name: "c:transactions/a_short_metric".into(),
            tags: BTreeMap::from([("foo".into(), tag_value.clone())]),
            extracted_from_indexed: false,
        };
        let validated_bucket = validate_metric_tags(short_metric, &test_config());
        assert_eq!(validated_bucket.tags["foo"], tag_value);
    }

    #[test]
    fn test_aggregator_cost_enforcement_total() {
        let timestamp = UnixTimestamp::from_secs(999994711);
        let bucket = Bucket {
            timestamp,
            width: 0,
            name: "c:transactions/foo".into(),
            value: BucketValue::counter(42.into()),
            tags: BTreeMap::new(),
            metadata: BucketMetadata::new(timestamp),
        };

        let mut aggregator: Aggregator = Aggregator::new(test_config());
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        aggregator
            .merge(project_key, bucket.clone(), Some(1))
            .unwrap();

        assert_eq!(
            aggregator.merge(project_key, bucket, Some(1)).unwrap_err(),
            AggregateMetricsError::TotalLimitExceeded
        );
    }

    #[test]
    fn test_aggregator_cost_enforcement_project() {
        relay_test::setup();
        let mut config = test_config();
        config.max_project_key_bucket_bytes = Some(1);

        let timestamp = UnixTimestamp::from_secs(999994711);
        let bucket = Bucket {
            timestamp,
            width: 0,
            name: "c:transactions/foo".into(),
            value: BucketValue::counter(42.into()),
            tags: BTreeMap::new(),
            metadata: BucketMetadata::new(timestamp),
        };

        let mut aggregator: Aggregator = Aggregator::new(config);
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        aggregator.merge(project_key, bucket.clone(), None).unwrap();
        assert_eq!(
            aggregator.merge(project_key, bucket, None).unwrap_err(),
            AggregateMetricsError::ProjectLimitExceeded
        );
    }

    #[test]
    fn test_parse_flush_batching() {
        let json = r#"{"shift_key": "partition"}"#;
        let parsed: AggregatorConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(parsed.flush_batching, FlushBatching::Partition));
    }

    #[test]
    fn test_aggregator_merge_metadata() {
        let mut config = test_config();
        config.bucket_interval = 10;

        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut aggregator: Aggregator = Aggregator::new(config);

        let bucket1 = some_bucket(Some(UnixTimestamp::from_secs(999994711)));
        let bucket2 = some_bucket(Some(UnixTimestamp::from_secs(999994711)));

        // We create a bucket with 3 merges and monotonically increasing timestamps.
        let mut bucket3 = some_bucket(Some(UnixTimestamp::from_secs(999994711)));
        bucket3
            .metadata
            .merge(BucketMetadata::new(UnixTimestamp::from_secs(999997811)));
        bucket3
            .metadata
            .merge(BucketMetadata::new(UnixTimestamp::from_secs(999999811)));

        aggregator
            .merge(project_key, bucket1.clone(), None)
            .unwrap();
        aggregator
            .merge(project_key, bucket2.clone(), None)
            .unwrap();
        aggregator
            .merge(project_key, bucket3.clone(), None)
            .unwrap();

        let buckets_metadata: Vec<_> = aggregator
            .buckets
            .iter()
            .map(|(_, v)| &v.metadata)
            .collect();
        insta::assert_debug_snapshot!(buckets_metadata, @r###"
        [
            BucketMetadata {
                merges: 5,
                received_at: Some(
                    UnixTimestamp(999994711),
                ),
                extracted_from_indexed: false,
            },
        ]
        "###);
    }

    #[test]
    fn test_get_flush_time_with_backdated_bucket() {
        let mut config = test_config();
        config.bucket_interval = 3600;
        config.initial_delay = 1300;
        config.flush_partitions = Some(10);
        config.flush_batching = FlushBatching::Partition;

        let reference_time = Instant::now();

        let now_s =
            (UnixTimestamp::now().as_secs() / config.bucket_interval) * config.bucket_interval;

        // First bucket has a timestamp two hours ago.
        let timestamp = UnixTimestamp::from_secs(now_s - 7200);
        let bucket_key_1 = BucketKey {
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            timestamp,
            metric_name: "c:transactions/foo".into(),
            tags: BTreeMap::new(),
            extracted_from_indexed: false,
        };

        // Second bucket has a timestamp in this hour.
        let timestamp = UnixTimestamp::from_secs(now_s);
        let bucket_key_2 = BucketKey {
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            timestamp,
            metric_name: "c:transactions/foo".into(),
            tags: BTreeMap::new(),
            extracted_from_indexed: false,
        };

        let flush_time_1 = get_flush_time(&config, reference_time, &bucket_key_1);
        let flush_time_2 = get_flush_time(&config, reference_time, &bucket_key_2);

        assert_eq!(flush_time_1, flush_time_2);
    }
}
