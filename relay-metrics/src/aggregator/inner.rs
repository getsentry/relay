use core::fmt;
use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::time::Duration;

use ahash::RandomState;
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use relay_base_schema::metrics::MetricName;
use relay_base_schema::project::ProjectKey;
use relay_common::time::UnixTimestamp;

use crate::aggregator::stats;
use crate::aggregator::{AggregateMetricsError, FlushBatching};
use crate::utils::ByNamespace;
use crate::{BucketMetadata, BucketValue, DistributionType, SetType};

#[derive(Default)]
pub struct Partition {
    pub partition_key: u32,
    pub buckets: HashMap<BucketKey, BucketData>,
    pub stats: PartitionStats,
}

impl fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(test)]
        let buckets = &self.buckets.iter().collect::<BTreeMap<_, _>>();
        #[cfg(not(test))]
        let buckets = &self.buckets;

        f.debug_struct("Partition")
            .field("partition_key", &self.partition_key)
            .field("stats", &self.stats)
            .field("buckets", buckets)
            .finish()
    }
}

#[derive(Default, Debug)]
pub struct PartitionStats {
    /// Amount of unique buckets in the partition.
    #[expect(unused, reason = "used for snapshot tests")]
    pub count: u64,
    /// Amount of unique buckets in the partition by namespace.
    pub count_by_namespace: ByNamespace<u64>,
    /// Amount of times a bucket was merged in the partition.
    #[expect(unused, reason = "used for snapshot tests")]
    pub merges: u64,
    /// Amount of times a bucket was merged in the partition by namespace.
    pub merges_by_namespace: ByNamespace<u64>,
    /// Cost of buckets in the partition.
    #[expect(unused, reason = "used for snapshot tests")]
    pub cost: u64,
    /// Cost of buckets in the partition by namespace.
    pub cost_by_namespace: ByNamespace<u64>,
}

impl From<&stats::Slot> for PartitionStats {
    fn from(value: &stats::Slot) -> Self {
        Self {
            count: value.count,
            count_by_namespace: value.count_by_namespace,
            merges: value.merges,
            merges_by_namespace: value.merges_by_namespace,
            cost: value.cost,
            cost_by_namespace: value.cost_by_namespace,
        }
    }
}

#[derive(Default, Debug)]
pub struct Stats {
    /// Total amount of buckets in the aggregator.
    #[expect(unused, reason = "used for snapshot tests")]
    pub count: u64,
    /// Total amount of buckets in the aggregator by namespace.
    pub count_by_namespace: ByNamespace<u64>,
    /// Total bucket cost in the aggregator.
    #[expect(unused, reason = "used for snapshot tests")]
    pub cost: u64,
    /// Total bucket cost in the aggregator by namespace.
    pub cost_by_namespace: ByNamespace<u64>,
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BucketKey {
    pub project_key: ProjectKey,
    pub timestamp: UnixTimestamp,
    pub metric_name: MetricName,
    pub tags: BTreeMap<String, String>,
    pub extracted_from_indexed: bool,
}

impl BucketKey {
    /// Estimates the number of bytes needed to encode the bucket key.
    ///
    /// Note that this does not necessarily match the exact memory footprint of the key,
    /// because data structures have a memory overhead.
    pub fn cost(&self) -> usize {
        std::mem::size_of::<Self>() + self.metric_name.len() + crate::utils::tags_cost(&self.tags)
    }
}

impl fmt::Debug for BucketKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.timestamp, self.project_key, self.metric_name
        )
    }
}

pub struct BucketData {
    pub value: BucketValue,
    pub metadata: BucketMetadata,
}

impl fmt::Debug for BucketData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl BucketData {
    /// Merges another bucket's data into this one.
    ///
    /// Returns the value cost increase on success.
    fn merge(&mut self, other: Self) -> Result<usize, AggregateMetricsError> {
        let cost_before = self.value.cost();

        self.value
            .merge(other.value)
            .map_err(|_| AggregateMetricsError::InvalidTypes)?;
        self.metadata.merge(other.metadata);

        Ok(self.value.cost().saturating_sub(cost_before))
    }
}

/// Config used to create a [`Inner`] instance.
#[derive(Debug)]
pub struct Config {
    /// Initial position/time of the aggregator.
    ///
    /// Except in testing, this should always be [`UnixTimestamp::now`].
    pub start: UnixTimestamp,
    /// Size of each individual bucket, inputs are truncated to this value.
    pub bucket_interval: u32,
    /// The amount of time slots to keep track off in the aggregator.
    ///
    /// The size of a time slot is defined by [`Self::bucket_interval`].
    pub num_time_slots: u32,
    /// The amount of partitions per time slot.
    pub num_partitions: u32,
    /// Delay how long a bucket should remain in the aggregator before being flushed.
    ///
    /// Ideally the delay is a multiple of [`Self::bucket_interval`].
    pub delay: u32,
    /// Maximum amount of bytes the aggregator can grow to.
    pub max_total_bucket_bytes: Option<u64>,
    /// Maximum amount of bytes the aggregator allows per project key.
    pub max_project_key_bucket_bytes: Option<u64>,
    /// The age in seconds of the oldest allowed bucket timestamp.
    pub max_secs_in_past: Option<u64>,
    /// The time in seconds that a timestamp may be in the future.
    pub max_secs_in_future: Option<u64>,
}

/// A metrics aggregator.
///
/// The aggregator is unaware of current time and needs to be driven by periodic flushes using
/// [`Self::flush_next`]. Each flush advances the internal clock by the configured
/// [`Config::bucket_interval`].
///
/// The internal time is set on construction to [`Config::start`].
///
/// Use [`Self::next_flush`] to determine the time when to call [`Self::flush_next`].
pub struct Inner {
    /// Ring-buffer of aggregation slots.
    ///
    /// This is treated as a ring-buffer of a two dimensional array. The first dimension is a
    /// "time slot" and the second dimension is the assigned partition.
    ///
    /// The total length of the ring-buffer is then determined by the amount of time slots times
    /// the amount of partitions. In other words, every time slot has [`Self::num_partitions`]
    /// partitions.
    ///
    /// Layout:
    /// Time slots: [            ][            ][            ]
    /// Partitions:  [    ][    ]  [    ][    ]  [    ][    ]
    ///
    /// An item is assigned by first determining its time slot and then assigning it to a partition
    /// based on selected [`Self::partition_by`] strategy.
    ///
    /// The first item in the buffer is tracked by [`Self::head`] which is at any time the
    /// current partition since the beginning "zero". The beginning in the aggregator refers to the
    /// beginning of the epoch. The [`Self::head`] is calculated with `f(x) = x / bucket_interval * num_partitions`.
    ///
    /// Flushing a partition advances the [`Self::head`] by a single value `+1`. Meaning
    /// effectively time is advanced by `bucket_interval / num_partitions`.
    slots: VecDeque<Slot>,
    /// The amount of partitions per time slot.
    num_partitions: u32,

    /// Position of the first element in [`Self::slots`].
    head: u64,

    /// Size of each individual bucket, inputs are truncated modulo to this value.
    bucket_interval: u32,
    /// Amount of slots which is added to a bucket as a delay.
    ///
    /// This is a fixed delay which is added to to the time returned by [`Self::next_flush`].
    delay: u32,

    /// Total stats of the aggregator.
    stats: stats::Total,
    /// Configured limits based on aggregator stats.
    limits: stats::Limits,

    /// The maximum amount of slots (size of a `bucket_interval`) the timestamp is allowed to be
    /// in the past or future.
    slot_range: RelativeRange,

    /// Determines how partitions are assigned based on the input bucket.
    partition_by: FlushBatching,
    /// Hasher used to calculate partitions.
    hasher: ahash::RandomState,
}

impl Inner {
    pub fn new(config: Config) -> Self {
        let bucket_interval = config.bucket_interval.max(1);
        // Extend the configured time slots with the delay (in slots), to make sure there is
        // enough space to satisfy the delay.
        //
        // We cannot just reserve space for enough partitions to satisfy the delay, because we have
        // no control over which partition a bucket is assigned to, so we have to prepare for the
        // 'worst' case, and that is full extra time slots.
        let num_time_slots = config.num_time_slots.max(1) + config.delay.div_ceil(bucket_interval);
        let num_partitions = config.num_partitions.max(1);

        let mut slots = Vec::new();
        for _ in 0..num_time_slots {
            for partition_key in 0..num_partitions {
                slots.push(Slot {
                    partition_key,
                    ..Default::default()
                });
            }
        }

        let slot_diff = RelativeRange {
            max_in_past: config
                .max_secs_in_past
                .map_or(u64::MAX, |v| v.div_ceil(u64::from(bucket_interval))),
            max_in_future: config
                .max_secs_in_future
                .map_or(u64::MAX, |v| v.div_ceil(u64::from(bucket_interval))),
        };

        let total_slots = slots.len();
        Self {
            slots: VecDeque::from(slots),
            num_partitions,
            head: config.start.as_secs() / u64::from(bucket_interval) * u64::from(num_partitions),
            bucket_interval,
            delay: config.delay,
            stats: stats::Total::default(),
            limits: stats::Limits {
                max_total: config.max_total_bucket_bytes.unwrap_or(u64::MAX),
                // Break down the maximum project cost to a maximum cost per partition.
                max_partition_project: config
                    .max_project_key_bucket_bytes
                    .map(|c| c.div_ceil(total_slots as u64))
                    .unwrap_or(u64::MAX),
            },
            slot_range: slot_diff,
            partition_by: FlushBatching::Partition,
            hasher: build_hasher(),
        }
    }

    /// Returns the configured bucket interval.
    pub fn bucket_interval(&self) -> u32 {
        self.bucket_interval
    }

    /// Returns total aggregator stats.
    pub fn stats(&self) -> Stats {
        Stats {
            count: self.stats.count,
            count_by_namespace: self.stats.count_by_namespace,
            cost: self.stats.count,
            cost_by_namespace: self.stats.cost_by_namespace,
        }
    }

    /// Returns `true` if the aggregator contains any metric buckets.
    pub fn is_empty(&self) -> bool {
        self.stats.count == 0
    }

    /// Returns the time as a duration since epoch when the next flush is supposed to happen.
    pub fn next_flush(&self) -> Duration {
        // `head + 1` to get the end time of the slot not the start.
        (Duration::from_secs(self.head + 1) / self.num_partitions * self.bucket_interval)
            + Duration::from_secs(u64::from(self.delay))
    }

    /// Merges a metric bucket.
    ///
    /// Returns `true` if the bucket already existed in the aggregator
    /// and `false` if the bucket did not exist yet.
    pub fn merge(
        &mut self,
        mut key: BucketKey,
        value: BucketData,
    ) -> Result<(), AggregateMetricsError> {
        let project_key = key.project_key;
        let namespace = key.metric_name.namespace();

        let time_slot = key.timestamp.as_secs() / u64::from(self.bucket_interval);
        // Make sure the timestamp is normalized to the correct interval as well.
        key.timestamp = UnixTimestamp::from_secs(time_slot * u64::from(self.bucket_interval));

        let now_slot = self.head / u64::from(self.num_partitions);
        if !self.slot_range.contains(now_slot, time_slot) {
            return Err(AggregateMetricsError::InvalidTimestamp(key.timestamp));
        }

        let assigned_partition = match self.partition_by {
            FlushBatching::None => 0,
            FlushBatching::Project => self.hasher.hash_one(key.project_key),
            FlushBatching::Bucket => self.hasher.hash_one(&key),
            FlushBatching::Partition => {
                self.hasher
                    .hash_one((key.project_key, &key.metric_name, &key.tags))
            }
        } % u64::from(self.num_partitions);

        let slot = time_slot * u64::from(self.num_partitions) + assigned_partition;

        let slots_len = self.slots.len() as u64;
        let index = (slot + slots_len).wrapping_sub(self.head % slots_len) % slots_len;

        let slot = self
            .slots
            .get_mut(index as usize)
            .expect("index should always be a valid partition");

        debug_assert_eq!(
            u64::from(slot.partition_key),
            assigned_partition,
            "assigned partition does not match selected partition"
        );

        let key_cost = key.cost() as u64;
        match slot.buckets.entry(key) {
            Entry::Occupied(occupied_entry) => {
                let estimated_cost = match &value.value {
                    // Counters and Gauges aggregate without additional costs.
                    BucketValue::Counter(_) | BucketValue::Gauge(_) => 0,
                    // Distributions are an accurate estimation, all values will be added.
                    BucketValue::Distribution(d) => d.len() * mem::size_of::<DistributionType>(),
                    // Sets are an upper bound.
                    BucketValue::Set(s) => s.len() * mem::size_of::<SetType>(),
                };

                // Reserve for the upper bound of the value.
                let reservation = slot.stats.reserve(
                    &mut self.stats,
                    project_key,
                    namespace,
                    estimated_cost as u64,
                    &self.limits,
                )?;

                let actual_cost = occupied_entry.into_mut().merge(value)?;

                // Track the actual cost increase, not just the reservation.
                reservation.consume_with(actual_cost as u64);
                slot.stats.incr_merges(namespace);
            }
            Entry::Vacant(vacant_entry) => {
                let reservation = slot.stats.reserve(
                    &mut self.stats,
                    project_key,
                    namespace,
                    key_cost + value.value.cost() as u64,
                    &self.limits,
                )?;

                vacant_entry.insert(value);

                reservation.consume();
                slot.stats.incr_count(&mut self.stats, namespace);
            }
        };

        debug_assert_eq!(slot.stats.count, slot.buckets.len() as u64);

        Ok(())
    }

    /// FLushes the next partition and advances time.
    pub fn flush_next(&mut self) -> Partition {
        let mut slot @ Slot { partition_key, .. } = self
            .slots
            .pop_front()
            .expect("there should always be at least one partition");

        let stats = PartitionStats::from(&slot.stats);

        // Remove the partition cost from the total cost and reset the partition cost.
        self.stats.remove_slot(&slot.stats);
        slot.stats.reset();

        // Create a new and empty slot with a similar capacity as the original one,
        // but still shrinking a little bit so that over time we free up memory again
        // without re-allocating too many times.
        //
        // Also re-use the original hasher, no need to create a new/expensive one.
        self.slots.push_back(Slot {
            buckets: HashMap::with_capacity_and_hasher(
                slot.buckets.len(),
                slot.buckets.hasher().clone(),
            ),
            ..slot
        });

        // Increment to the next partition.
        //
        // Effectively advance time by `bucket_interval / num_partitions`.
        self.head += 1;

        Partition {
            partition_key,
            buckets: slot.buckets,
            stats,
        }
    }

    /// Consumes the aggregator and returns an iterator over all contained partitions.
    pub fn into_partitions(self) -> impl Iterator<Item = Partition> {
        self.slots.into_iter().map(|slot| Partition {
            partition_key: slot.partition_key,
            buckets: slot.buckets,
            stats: PartitionStats::from(&slot.stats),
        })
    }
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        list.entry(&self.stats);
        for (i, v) in self.slots.iter().enumerate() {
            let head_partitions = self.head % u64::from(self.num_partitions);
            let time_offset = (head_partitions + i as u64) / u64::from(self.num_partitions);
            let head_time = self.head / u64::from(self.num_partitions);
            let time = (head_time + time_offset) * u64::from(self.bucket_interval);
            match v.is_empty() {
                // Make the output shorter with a string until `entry_with` is stable.
                true => list.entry(&format!("({time}, {v:?})")),
                false => list.entry(&(time, v)),
            };
        }
        list.finish()
    }
}

#[derive(Default)]
struct Slot {
    pub partition_key: u32,
    pub stats: stats::Slot,
    pub buckets: HashMap<BucketKey, BucketData>,
}

impl Slot {
    fn is_empty(&self) -> bool {
        self.stats == Default::default() && self.buckets.is_empty()
    }
}

impl fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            write!(f, "Slot({})", self.partition_key)
        } else {
            #[cfg(test)]
            let buckets = &self.buckets.iter().collect::<BTreeMap<_, _>>();
            #[cfg(not(test))]
            let buckets = &self.buckets;

            f.debug_struct("Slot")
                .field("partition_key", &self.partition_key)
                .field("stats", &self.stats)
                .field("buckets", buckets)
                .finish()
        }
    }
}

struct RelativeRange {
    max_in_past: u64,
    max_in_future: u64,
}

impl RelativeRange {
    fn contains(&self, now: u64, target: u64) -> bool {
        if target < now {
            // Timestamp/target in the past.
            let diff = now - target;
            diff <= self.max_in_past
        } else {
            // Timestamp/target in the future.
            let diff = target - now;
            diff <= self.max_in_future
        }
    }
}

fn build_hasher() -> RandomState {
    // A fixed, consistent seed across all instances of Relay.
    const K0: u64 = 0x06459b7d5da84ed8;
    const K1: u64 = 0x3321ce2636c567cc;
    const K2: u64 = 0x56c94d7107c49765;
    const K3: u64 = 0x685bf5f9abbea5ab;

    ahash::RandomState::with_seeds(K0, K1, K2, K3)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket_key(ts: u64, name: &str) -> BucketKey {
        BucketKey {
            project_key: ProjectKey::parse("00000000000000000000000000000000").unwrap(),
            timestamp: UnixTimestamp::from_secs(ts),
            metric_name: name.into(),
            tags: Default::default(),
            extracted_from_indexed: false,
        }
    }

    fn counter(value: f64) -> BucketData {
        BucketData {
            value: BucketValue::counter(value.try_into().unwrap()),
            metadata: Default::default(),
        }
    }

    #[test]
    fn test_merge_flush() -> Result<(), AggregateMetricsError> {
        let mut buckets = Inner::new(Config {
            bucket_interval: 10,
            num_time_slots: 6,
            num_partitions: 2,
            delay: 0,
            max_secs_in_past: None,
            max_secs_in_future: None,
            max_total_bucket_bytes: None,
            max_project_key_bucket_bytes: None,
            start: UnixTimestamp::from_secs(70),
        });

        // Within the time range.
        buckets.merge(bucket_key(70, "a"), counter(1.0))?;
        buckets.merge(bucket_key(80, "b"), counter(1.0))?;
        buckets.merge(bucket_key(80, "b"), counter(2.0))?;
        // Too early.
        buckets.merge(bucket_key(32, "c"), counter(1.0))?;
        buckets.merge(bucket_key(42, "d"), counter(1.0))?;
        // Too late.
        buckets.merge(bucket_key(171, "e"), counter(1.0))?;
        buckets.merge(bucket_key(181, "f"), counter(1.0))?;
        buckets.merge(bucket_key(191, "a"), counter(1.0))?;

        insta::assert_debug_snapshot!(buckets);

        let partition = buckets.flush_next();
        insta::assert_debug_snapshot!(partition, @r###"
        Partition {
            partition_key: 0,
            stats: PartitionStats {
                count: 2,
                count_by_namespace: (unsupported:2),
                merges: 0,
                merges_by_namespace: (0),
                cost: 274,
                cost_by_namespace: (unsupported:274),
            },
            buckets: {
                70-00000000000000000000000000000000-a: Counter(
                    1.0,
                ),
                190-00000000000000000000000000000000-a: Counter(
                    1.0,
                ),
            },
        }
        "###);

        // This was just flushed and now is supposed to be at the end.
        buckets.merge(bucket_key(70, "a"), counter(1.0))?;

        insta::assert_debug_snapshot!(buckets);

        let partition = buckets.flush_next();
        insta::assert_debug_snapshot!(partition, @r###"
        Partition {
            partition_key: 1,
            stats: PartitionStats {
                count: 0,
                count_by_namespace: (0),
                merges: 0,
                merges_by_namespace: (0),
                cost: 0,
                cost_by_namespace: (0),
            },
            buckets: {},
        }
        "###);

        insta::assert_debug_snapshot!(buckets);

        insta::assert_debug_snapshot!(buckets.stats(), @r###"
        Stats {
            count: 6,
            count_by_namespace: (unsupported:6),
            cost: 6,
            cost_by_namespace: (unsupported:822),
        }
        "###);

        Ok(())
    }

    #[test]
    fn test_merge_flush_cost_limits() -> Result<(), AggregateMetricsError> {
        const ONE_BUCKET_COST: u64 = 137;

        let mut buckets = Inner::new(Config {
            bucket_interval: 10,
            num_time_slots: 3,
            num_partitions: 1,
            delay: 0,
            max_secs_in_past: None,
            max_secs_in_future: None,
            max_total_bucket_bytes: Some(ONE_BUCKET_COST * 2),
            // Enough for one bucket per partition.
            max_project_key_bucket_bytes: Some(ONE_BUCKET_COST * 3),
            start: UnixTimestamp::from_secs(70),
        });

        buckets.merge(bucket_key(70, "a"), counter(1.0))?;
        // Adding a new bucket exceeds the cost.
        assert_eq!(
            buckets
                .merge(bucket_key(70, "b"), counter(1.0))
                .unwrap_err(),
            AggregateMetricsError::ProjectLimitExceeded
        );
        // Merging a counter bucket is fine.
        buckets.merge(bucket_key(70, "a"), counter(2.0))?;

        // There is still room in the total budget and for a different project.
        let other_project = BucketKey {
            project_key: ProjectKey::parse("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            ..bucket_key(70, "a")
        };
        buckets.merge(other_project, counter(3.0))?;

        // Add a bucket to a different partition, but the total limit is exceeded.
        assert_eq!(
            buckets
                .merge(bucket_key(80, "c"), counter(1.0))
                .unwrap_err(),
            AggregateMetricsError::TotalLimitExceeded
        );
        // Flush some data and make space.
        insta::assert_debug_snapshot!(buckets.flush_next(), @r###"
        Partition {
            partition_key: 0,
            stats: PartitionStats {
                count: 2,
                count_by_namespace: (unsupported:2),
                merges: 1,
                merges_by_namespace: (unsupported:1),
                cost: 274,
                cost_by_namespace: (unsupported:274),
            },
            buckets: {
                70-00000000000000000000000000000000-a: Counter(
                    3.0,
                ),
                70-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-a: Counter(
                    3.0,
                ),
            },
        }
        "###);
        buckets.merge(bucket_key(80, "c"), counter(1.0))?;

        insta::assert_debug_snapshot!(buckets, @r###"
        [
            Total {
                count: 1,
                count_by_namespace: (unsupported:1),
                cost: 137,
                cost_by_namespace: (unsupported:137),
            },
            (
                80,
                Slot {
                    partition_key: 0,
                    stats: Slot {
                        count: 1,
                        count_by_namespace: (unsupported:1),
                        merges: 0,
                        merges_by_namespace: (0),
                        cost: 137,
                        cost_by_namespace: (unsupported:137),
                        cost_by_project: {
                            ProjectKey("00000000000000000000000000000000"): 137,
                        },
                    },
                    buckets: {
                        80-00000000000000000000000000000000-c: Counter(
                            1.0,
                        ),
                    },
                },
            ),
            "(90, Slot(0))",
            "(100, Slot(0))",
        ]
        "###);

        insta::assert_debug_snapshot!(buckets.stats(), @r###"
        Stats {
            count: 1,
            count_by_namespace: (unsupported:1),
            cost: 1,
            cost_by_namespace: (unsupported:137),
        }
        "###);

        Ok(())
    }

    #[test]
    fn test_merge_flush_with_delay() {
        let mut buckets = Inner::new(Config {
            // 20 seconds.
            bucket_interval: 20,
            // Slots for 1 minute.
            num_time_slots: 3,
            // A partition for every 10 seconds.
            num_partitions: 2,
            // 20 second delay -> 1 extra time slot.
            delay: 20,
            max_total_bucket_bytes: None,
            max_project_key_bucket_bytes: None,
            max_secs_in_past: None,
            max_secs_in_future: None,
            // Truncated to 60 seconds.
            start: UnixTimestamp::from_secs(63),
        });

        // Add a bucket now -> should be flushed 30 seconds in the future.
        buckets.merge(bucket_key(60, "a"), counter(1.0)).unwrap();
        // Add a bucket 1 minute into the future, this one should still have a delay.
        buckets.merge(bucket_key(120, "b"), counter(2.0)).unwrap();

        // First flush is in 20 seconds + 10 seconds for the end of the partition.
        assert_eq!(buckets.next_flush(), Duration::from_secs(90));
        insta::assert_debug_snapshot!(buckets.flush_next(), @r###"
        Partition {
            partition_key: 0,
            stats: PartitionStats {
                count: 1,
                count_by_namespace: (unsupported:1),
                merges: 0,
                merges_by_namespace: (0),
                cost: 137,
                cost_by_namespace: (unsupported:137),
            },
            buckets: {
                60-00000000000000000000000000000000-a: Counter(
                    1.0,
                ),
            },
        }
        "###);
        assert!(buckets.flush_next().buckets.is_empty());

        // We're now at second 100s.
        assert_eq!(buckets.next_flush(), Duration::from_secs(110));
        assert!(buckets.flush_next().buckets.is_empty());
        assert!(buckets.flush_next().buckets.is_empty());

        // We're now at second 120s.
        assert_eq!(buckets.next_flush(), Duration::from_secs(130));
        assert!(buckets.flush_next().buckets.is_empty());
        assert!(buckets.flush_next().buckets.is_empty());

        // We're now at second 140s -> our second bucket is ready (120s + 20s delay).
        assert_eq!(buckets.next_flush(), Duration::from_secs(150));
        insta::assert_debug_snapshot!(buckets.flush_next(), @r###"
        Partition {
            partition_key: 0,
            stats: PartitionStats {
                count: 1,
                count_by_namespace: (unsupported:1),
                merges: 0,
                merges_by_namespace: (0),
                cost: 137,
                cost_by_namespace: (unsupported:137),
            },
            buckets: {
                120-00000000000000000000000000000000-b: Counter(
                    2.0,
                ),
            },
        }
        "###);
        assert!(buckets.flush_next().buckets.is_empty());

        // We're now at 160s.
        assert_eq!(buckets.next_flush(), Duration::from_secs(170));
    }

    #[test]
    fn test_next_flush() {
        let mut buckets = Inner::new(Config {
            bucket_interval: 10,
            num_time_slots: 6,
            num_partitions: 2,
            delay: 0,
            max_secs_in_past: None,
            max_secs_in_future: None,
            max_total_bucket_bytes: None,
            max_project_key_bucket_bytes: None,
            start: UnixTimestamp::from_secs(70),
        });

        assert_eq!(buckets.next_flush(), Duration::from_secs(75));
        assert_eq!(buckets.flush_next().partition_key, 0);
        assert_eq!(buckets.next_flush(), Duration::from_secs(80));
        assert_eq!(buckets.flush_next().partition_key, 1);
        assert_eq!(buckets.next_flush(), Duration::from_secs(85));
        assert_eq!(buckets.flush_next().partition_key, 0);
        assert_eq!(buckets.next_flush(), Duration::from_secs(90));
        assert_eq!(buckets.next_flush(), Duration::from_secs(90));
    }

    #[test]
    fn test_next_flush_with_delay() {
        let mut buckets = Inner::new(Config {
            bucket_interval: 10,
            num_time_slots: 6,
            num_partitions: 2,
            delay: 3,
            max_secs_in_past: None,
            max_secs_in_future: None,
            max_total_bucket_bytes: None,
            max_project_key_bucket_bytes: None,
            start: UnixTimestamp::from_secs(70),
        });

        assert_eq!(buckets.next_flush(), Duration::from_secs(78));
        assert_eq!(buckets.flush_next().partition_key, 0);
        assert_eq!(buckets.next_flush(), Duration::from_secs(83));
        assert_eq!(buckets.flush_next().partition_key, 1);
        assert_eq!(buckets.next_flush(), Duration::from_secs(88));
        assert_eq!(buckets.flush_next().partition_key, 0);
        assert_eq!(buckets.next_flush(), Duration::from_secs(93));
        assert_eq!(buckets.next_flush(), Duration::from_secs(93));
    }

    #[test]
    fn test_merge_flush_time_limits() -> Result<(), AggregateMetricsError> {
        let mut buckets = Inner::new(Config {
            bucket_interval: 10,
            num_time_slots: 6,
            num_partitions: 2,
            delay: 0,
            max_secs_in_past: Some(33), // -> Upgraded to 4 slots (40 seconds).
            max_secs_in_future: Some(22), // -> Upgraded to 3 slots (30 seconds).
            max_total_bucket_bytes: None,
            max_project_key_bucket_bytes: None,
            start: UnixTimestamp::from_secs(70),
        });

        buckets.merge(bucket_key(70, "a"), counter(1.0))?;

        // 4 slots in the past.
        buckets.merge(bucket_key(60, "a"), counter(1.0))?;
        buckets.merge(bucket_key(50, "a"), counter(1.0))?;
        buckets.merge(bucket_key(40, "a"), counter(1.0))?;
        buckets.merge(bucket_key(30, "a"), counter(1.0))?;
        assert_eq!(
            buckets
                .merge(bucket_key(29, "a"), counter(1.0))
                .unwrap_err(),
            AggregateMetricsError::InvalidTimestamp(UnixTimestamp::from_secs(20))
        );

        // 3 slots in the future.
        buckets.merge(bucket_key(80, "a"), counter(1.0))?;
        buckets.merge(bucket_key(90, "a"), counter(1.0))?;
        buckets.merge(bucket_key(109, "a"), counter(1.0))?;
        assert_eq!(
            buckets
                .merge(bucket_key(110, "a"), counter(1.0))
                .unwrap_err(),
            AggregateMetricsError::InvalidTimestamp(UnixTimestamp::from_secs(110))
        );

        Ok(())
    }
}
