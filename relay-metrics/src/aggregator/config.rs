use std::time::Duration;

use relay_common::time::UnixTimestamp;
use serde::{Deserialize, Serialize};

/// Configuration value for [`AggregatorConfig::flush_batching`].
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FlushBatching {
    /// Shifts the flush time by an offset based on the project key.
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

/// Parameters used by the [`crate::aggregator::Aggregator`].
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
    /// Similar measuring technique to [`Self::max_total_bucket_bytes`], but instead of a
    /// global/process-wide limit, it is enforced per project key.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_project_key_bucket_bytes: Option<usize>,

    /// Maximum amount of bytes used for metrics aggregation.
    ///
    /// When aggregating metrics, Relay keeps track of how many bytes a metric takes in memory.
    /// This is only an approximation and does not take into account things such as pre-allocation
    /// in hashmaps.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_total_bucket_bytes: Option<usize>,

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
    pub fn bucket_interval(&self) -> Duration {
        Duration::from_secs(self.bucket_interval)
    }

    /// Returns the initial flush delay after the end of a bucket's original time window.
    pub fn initial_delay(&self) -> Duration {
        Duration::from_secs(self.initial_delay)
    }

    /// Determines the target bucket for an incoming bucket timestamp and bucket width.
    ///
    /// We select the output bucket which overlaps with the center of the incoming bucket.
    /// Fails if timestamp is too old or too far into the future.
    pub fn get_bucket_timestamp(
        &self,
        timestamp: UnixTimestamp,
        bucket_width: u64,
    ) -> UnixTimestamp {
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
            max_total_bucket_bytes: None,
            flush_batching: FlushBatching::default(),
            flush_partitions: None,
        }
    }
}
