use relay_base_schema::metrics::MetricName;
use relay_event_schema::protocol::{MetricSummary, MetricsSummary};
use relay_metrics::{
    Bucket, BucketValue, CounterType, DistributionValue, FiniteF64, GaugeValue, SetValue,
};
use relay_protocol::Annotated;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};

/// Key of a bucket used to keep track of aggregates for the [`MetricsSummary`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct MetricsSummaryBucketKey {
    /// Name of the metric.
    metric_name: MetricName,
    /// Tags of the bucket.
    tags: BTreeMap<String, String>,
}

/// Value of a bucket used to keep track of aggregates for the [`MetricsSummary`].
#[derive(Debug)]
struct MetricsSummaryBucketValue {
    /// The minimum value reported in the bucket.
    pub min: FiniteF64,
    /// The maximum value reported in the bucket.
    pub max: FiniteF64,
    /// The sum of all values reported in the bucket.
    pub sum: FiniteF64,
    /// The number of times this bucket was updated with a new value.
    pub count: u64,
}

impl MetricsSummaryBucketValue {
    /// Builds a [`MetricsSummaryBucketValue`] from a [`BucketValue`] by aggregating the data into
    /// a gauge like summary.
    fn from_bucket_value(value: &BucketValue) -> MetricsSummaryBucketValue {
        match value {
            BucketValue::Counter(counter) => Self::from_counter(counter),
            BucketValue::Distribution(distribution) => Self::from_distribution(distribution),
            BucketValue::Set(set) => Self::from_set(set),
            BucketValue::Gauge(gauge) => Self::from_gauge(gauge),
        }
    }

    /// Builds a [`MetricsSummaryBucketValue`] from a [`CounterType`].
    fn from_counter(counter: &CounterType) -> MetricsSummaryBucketValue {
        MetricsSummaryBucketValue {
            min: *counter,
            max: *counter,
            sum: *counter,
            count: 1,
        }
    }

    /// Builds a [`MetricsSummaryBucketValue`] from a [`DistributionValue`].
    fn from_distribution(distribution: &DistributionValue) -> MetricsSummaryBucketValue {
        let mut min = FiniteF64::MAX;
        let mut max = FiniteF64::MIN;
        let mut sum = FiniteF64::new(0.0).unwrap();

        for value in distribution {
            min = std::cmp::min(min, *value);
            max = std::cmp::max(max, *value);
            sum = sum.saturating_add(*value);
        }

        MetricsSummaryBucketValue {
            min,
            max,
            sum,
            count: distribution.len() as u64,
        }
    }

    /// Builds a [`MetricsSummaryBucketValue`] from a [`SetValue`].
    fn from_set(set: &SetValue) -> MetricsSummaryBucketValue {
        // For sets, we limit to counting the number of occurrences.
        MetricsSummaryBucketValue {
            min: FiniteF64::new(0.0).unwrap(),
            max: FiniteF64::new(0.0).unwrap(),
            sum: FiniteF64::new(0.0).unwrap(),
            count: set.len() as u64,
        }
    }

    /// Builds a [`MetricsSummaryBucketValue`] from a [`GaugeValue`].
    fn from_gauge(gauge: &GaugeValue) -> MetricsSummaryBucketValue {
        MetricsSummaryBucketValue {
            min: gauge.min,
            max: gauge.max,
            sum: gauge.sum,
            count: gauge.count,
        }
    }

    /// Merges two [`MetricsSummaryBucketValue`]s together by mutating `self` and consuming
    /// `other`.
    fn merge(&mut self, other: MetricsSummaryBucketValue) {
        self.min = std::cmp::min(self.min, other.min);
        self.max = std::cmp::max(self.max, other.max);
        self.sum = self.sum.saturating_add(other.sum);
        self.count += other.count;
    }
}

/// Aggregator that tracks all the buckets containing the summaries for each
/// [`MetricsSummaryBucketKey`].
///
/// The need for an aggregator arises from the fact that we want to compute metrics summaries
/// generically on any slice of [`Bucket`]s meaning that we need to handle cases in which
/// the same metrics as identified by the [`MetricsSummaryBucketKey`] have to be merged.
struct MetricsSummaryAggregator {
    buckets: HashMap<MetricsSummaryBucketKey, MetricsSummaryBucketValue>,
}

impl MetricsSummaryAggregator {
    pub fn new() -> MetricsSummaryAggregator {
        MetricsSummaryAggregator {
            buckets: HashMap::new(),
        }
    }

    /// Merges into the [`MetricsSummaryAggregator`] a slice of [`Bucket`]s.
    pub fn from_buckets(buckets: &[Bucket]) -> MetricsSummaryAggregator {
        let mut aggregator = MetricsSummaryAggregator::new();

        for bucket in buckets {
            aggregator.add(bucket);
        }

        aggregator
    }

    /// Adds a [`Bucket`] into the [`MetricsSummaryAggregator`].
    pub fn add(&mut self, bucket: &Bucket) {
        let key = MetricsSummaryBucketKey {
            metric_name: bucket.name.clone(),
            tags: bucket.tags.clone(),
        };

        let value = MetricsSummaryBucketValue::from_bucket_value(&bucket.value);

        match self.buckets.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge(value);
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
    }

    /// Builds the [`MetricsSummary`] from the [`MetricsSummaryAggregator`].
    ///
    /// Note that this method consumes the aggregator itself, since the purpose of the aggregator
    /// is to be built and destroyed once the summary is ready to be computed.
    pub fn build_metrics_summary(self) -> MetricsSummary {
        let mut metrics_summary = BTreeMap::new();

        for (key, value) in self.buckets {
            let tags = key
                .tags
                .into_iter()
                .map(|(tag_key, tag_value)| (tag_key, Annotated::new(tag_value)))
                .collect();

            let metric_summary = MetricSummary {
                min: Annotated::new(value.min.to_f64()),
                max: Annotated::new(value.max.to_f64()),
                sum: Annotated::new(value.sum.to_f64()),
                count: Annotated::new(value.count),
                tags: Annotated::new(tags),
            };

            let entry = metrics_summary
                .entry(key.metric_name.to_string())
                .or_insert(Annotated::new(vec![]));

            if let Some(x) = entry.value_mut() {
                x.push(Annotated::new(metric_summary));
            }
        }

        MetricsSummary(metrics_summary)
    }
}

/// Computes the [`MetricsSummary`] from a slice of [`Bucket`]s.
pub fn compute(buckets: &[Bucket]) -> MetricsSummary {
    let aggregator = MetricsSummaryAggregator::from_buckets(buckets);
    aggregator.build_metrics_summary()
}
