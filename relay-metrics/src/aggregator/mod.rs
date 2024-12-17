//! Core functionality of metrics aggregation.
//!
//! A new implementation of the [`crate::aggregator::Aggregator`], with slightly
//! different semantics and more optimized aggregation.
//!
//! This module is supposed to replace the [`crate::aggregator`] module,
//! if proven successful.

use std::time::{Duration, SystemTime};

use hashbrown::HashMap;
use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::project::ProjectKey;
use relay_common::time::UnixTimestamp;

use crate::statsd::{MetricCounters, MetricGauges};
use crate::Bucket;

mod config;
mod inner;
mod stats;

pub use self::config::*;
use self::inner::{BucketData, BucketKey};

/// Default amount of partitions per second when there are no partitions configured.
const DEFAULT_PARTITIONS_PER_SECOND: u32 = 64;

/// Any error that may occur during aggregation.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum AggregateMetricsError {
    /// Internal error: Attempted to merge two metric buckets of different types.
    #[error("found incompatible metric types")]
    InvalidTypes,
    /// A metric bucket is too large for the global bytes limit.
    #[error("total metrics limit exceeded")]
    TotalLimitExceeded,
    /// A metric bucket is too large for the per-project bytes limit.
    #[error("project metrics limit exceeded")]
    ProjectLimitExceeded,
    /// The timestamp is outside the maximum allowed time range.
    #[error("the timestamp '{0}' is outside the maximum allowed time range")]
    InvalidTimestamp(UnixTimestamp),
}

/// An aggregator for metric [`Bucket`]'s.
#[derive(Debug)]
pub struct Aggregator {
    name: String,
    inner: inner::Inner,
}

impl Aggregator {
    /// Creates a new named [`Self`].
    pub fn named(name: String, config: &AggregatorConfig) -> Self {
        let num_partitions = match config.flush_batching {
            FlushBatching::Project => config.flush_partitions,
            FlushBatching::Bucket => config.flush_partitions,
            FlushBatching::Partition => config.flush_partitions,
            FlushBatching::None => Some(0),
        }
        .unwrap_or(DEFAULT_PARTITIONS_PER_SECOND * config.bucket_interval.max(1));

        Self {
            name,
            inner: inner::Inner::new(inner::Config {
                start: UnixTimestamp::now(),
                bucket_interval: config.bucket_interval,
                num_time_slots: config.aggregator_size,
                num_partitions,
                delay: config.initial_delay,
                max_total_bucket_bytes: config.max_total_bucket_bytes,
                max_project_key_bucket_bytes: config.max_project_key_bucket_bytes,
                max_secs_in_past: Some(config.max_secs_in_past),
                max_secs_in_future: Some(config.max_secs_in_future),
            }),
        }
    }

    /// Returns the name of the aggregator.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if the aggregator contains any metric buckets.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Merge a bucket into this aggregator.
    pub fn merge(
        &mut self,
        project_key: ProjectKey,
        bucket: Bucket,
    ) -> Result<(), AggregateMetricsError> {
        let key = BucketKey {
            project_key,
            timestamp: bucket.timestamp,
            metric_name: bucket.name,
            tags: bucket.tags,
            extracted_from_indexed: bucket.metadata.extracted_from_indexed,
        };

        let value = BucketData {
            value: bucket.value,
            metadata: bucket.metadata,
        };

        self.inner.merge(key, value)
    }

    /// Attempts to flush the next batch from the aggregator.
    ///
    /// If it is too early to flush the next batch, the error contains the timestamp when the flush should be retried.
    /// After a successful flush, retry immediately until an error is returned with the next flush
    /// time, this makes sure time is eventually synchronized.
    pub fn try_flush_next(&mut self, now: SystemTime) -> Result<Partition, Duration> {
        let next_flush = SystemTime::UNIX_EPOCH + self.inner.next_flush();

        if let Err(err) = now.duration_since(next_flush) {
            // The flush time is in the future, return the amount of time to wait before the next flush.
            return Err(err.duration());
        }

        let partition = self.inner.flush_next();

        emit_stats(&self.name, self.inner.stats());
        emit_flush_partition_stats(&self.name, partition.stats);

        Ok(Partition {
            partition_key: partition.partition_key,
            buckets: partition.buckets,
            bucket_interval: self.inner.bucket_interval(),
        })
    }

    /// Returns when the next partition is ready to be flushed using [`Self::try_flush_next`].
    pub fn next_flush(&mut self, now: SystemTime) -> Duration {
        let next_flush = SystemTime::UNIX_EPOCH + self.inner.next_flush();

        match now.duration_since(next_flush) {
            Ok(_) => Duration::ZERO,
            Err(err) => err.duration(),
        }
    }

    /// Consumes the aggregator and returns all contained partitions.
    pub fn into_partitions(self) -> impl Iterator<Item = Partition> {
        let bucket_interval = self.inner.bucket_interval();

        emit_stats(&self.name, self.inner.stats());

        self.inner.into_partitions().map(move |p| Partition {
            partition_key: p.partition_key,
            buckets: p.buckets,
            bucket_interval,
        })
    }
}

/// A flushed partition from [`Aggregator::try_flush_next`].
///
/// The partition contains the partition key and all flushed buckets.
pub struct Partition {
    /// The partition key.
    pub partition_key: u32,
    buckets: HashMap<BucketKey, BucketData>,
    bucket_interval: u32,
}

impl IntoIterator for Partition {
    type Item = (ProjectKey, Bucket);
    type IntoIter = PartitionIter;

    fn into_iter(self) -> Self::IntoIter {
        PartitionIter {
            inner: self.buckets.into_iter(),
            bucket_interval: self.bucket_interval,
        }
    }
}

/// Iterator yielded from [`Partition::into_iter`].
pub struct PartitionIter {
    inner: hashbrown::hash_map::IntoIter<BucketKey, BucketData>,
    bucket_interval: u32,
}

impl Iterator for PartitionIter {
    type Item = (ProjectKey, Bucket);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.inner.next()?;

        Some((
            key.project_key,
            Bucket {
                timestamp: key.timestamp,
                width: u64::from(self.bucket_interval),
                name: key.metric_name,
                tags: key.tags,
                value: data.value,
                metadata: data.metadata,
            },
        ))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl std::iter::ExactSizeIterator for PartitionIter {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl std::iter::FusedIterator for PartitionIter {}

fn emit_stats(name: &str, stats: inner::Stats) {
    for namespace in MetricNamespace::all() {
        relay_statsd::metric!(
            gauge(MetricGauges::Buckets) = *stats.count_by_namespace.get(namespace),
            namespace = namespace.as_str(),
            aggregator = name
        );
        relay_statsd::metric!(
            gauge(MetricGauges::BucketsCost) = *stats.cost_by_namespace.get(namespace),
            namespace = namespace.as_str(),
            aggregator = name
        );
    }
}

fn emit_flush_partition_stats(name: &str, stats: inner::PartitionStats) {
    relay_statsd::metric!(counter(MetricCounters::FlushCount) += 1, aggregator = name);

    for namespace in MetricNamespace::all() {
        relay_statsd::metric!(
            counter(MetricCounters::MergeHit) += *stats.count_by_namespace.get(namespace),
            namespace = namespace.as_str(),
            aggregator = name,
        );
        relay_statsd::metric!(
            counter(MetricCounters::MergeMiss) += *stats.merges_by_namespace.get(namespace),
            namespace = namespace.as_str(),
            aggregator = name,
        );
        relay_statsd::metric!(
            counter(MetricCounters::FlushCost) += *stats.cost_by_namespace.get(namespace),
            namespace = namespace.as_str(),
            aggregator = name,
        );
    }
}
