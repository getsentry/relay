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
    pub bucket_interval: u32,

    /// The aggregator size.
    ///
    /// This determines how many individual bucket intervals, are stored in the aggregator.
    /// Increasing this number will add additional delay for backdated and future buckets.
    ///
    /// Defaults to: 1.
    pub aggregator_size: u32,

    /// The initial delay in seconds to wait before flushing a bucket.
    ///
    /// Defaults to `30` seconds. Before sending an aggregated bucket, this is the time Relay waits
    /// for buckets that are being reported in real time.
    pub initial_delay: u32,

    /// The age in seconds of the oldest allowed bucket timestamp.
    ///
    /// Defaults to 5 days.
    pub max_secs_in_past: u64,

    /// The time in seconds that a timestamp may be in the future.
    ///
    /// Defaults to 1 minute.
    pub max_secs_in_future: u64,

    /// Maximum amount of bytes used for metrics aggregation per project key.
    ///
    /// Similar measuring technique to [`Self::max_total_bucket_bytes`], but instead of a
    /// global/process-wide limit, it is enforced per project key.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_project_key_bucket_bytes: Option<u64>,

    /// Maximum amount of bytes used for metrics aggregation.
    ///
    /// When aggregating metrics, Relay keeps track of how many bytes a metric takes in memory.
    /// This is only an approximation and does not take into account things such as pre-allocation
    /// in hashmaps.
    ///
    /// Defaults to `None`, i.e. no limit.
    pub max_total_bucket_bytes: Option<u64>,

    /// The number of logical partitions that can receive flushed buckets.
    pub flush_partitions: Option<u32>,

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

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            bucket_interval: 10,
            aggregator_size: 1,
            initial_delay: 30,
            max_secs_in_past: 5 * 24 * 60 * 60, // 5 days
            max_secs_in_future: 60,             // 1 minute
            max_project_key_bucket_bytes: None,
            max_total_bucket_bytes: None,
            flush_batching: FlushBatching::default(),
            flush_partitions: None,
        }
    }
}
