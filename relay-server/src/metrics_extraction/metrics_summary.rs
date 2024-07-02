use relay_base_schema::metrics::{MetricName, MetricNamespace};
use relay_event_schema::protocol::{MetricSummary, MetricsSummary};
use relay_metrics::{
    Bucket, BucketValue, CounterType, DistributionValue, FiniteF64, GaugeValue, SetValue,
};
use relay_protocol::Annotated;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

/// Key of a bucket used to keep track of aggregates for the [`MetricsSummary`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    min: Option<FiniteF64>,
    /// The maximum value reported in the bucket.
    max: Option<FiniteF64>,
    /// The sum of all values reported in the bucket.
    sum: Option<FiniteF64>,
    /// The number of times this bucket was updated with a new value.
    count: u64,
}

impl MetricsSummaryBucketValue {
    /// Builds a [`MetricsSummaryBucketValue`] from a [`MetricSummary`] by aggregating the data into
    /// a gauge like summary.
    fn from_metric_summary(summary: &MetricSummary) -> MetricsSummaryBucketValue {
        MetricsSummaryBucketValue {
            min: summary
                .min
                .value()
                .and_then(|min| FiniteF64::try_from(*min).ok()),
            max: summary
                .max
                .value()
                .and_then(|max| FiniteF64::try_from(*max).ok()),
            sum: summary
                .sum
                .value()
                .and_then(|sum| FiniteF64::try_from(*sum).ok()),
            count: summary.count.value().cloned().unwrap_or(0),
        }
    }

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
            min: Some(*counter),
            max: Some(*counter),
            sum: Some(*counter),
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
            min: Some(min),
            max: Some(max),
            sum: Some(sum),
            count: distribution.len() as u64,
        }
    }

    /// Builds a [`MetricsSummaryBucketValue`] from a [`SetValue`].
    fn from_set(set: &SetValue) -> MetricsSummaryBucketValue {
        // For sets, we limit to counting the number of occurrences.
        MetricsSummaryBucketValue {
            min: None,
            max: None,
            sum: None,
            count: set.len() as u64,
        }
    }

    /// Builds a [`MetricsSummaryBucketValue`] from a [`GaugeValue`].
    fn from_gauge(gauge: &GaugeValue) -> MetricsSummaryBucketValue {
        MetricsSummaryBucketValue {
            min: Some(gauge.min),
            max: Some(gauge.max),
            sum: Some(gauge.sum),
            count: gauge.count,
        }
    }

    /// Merges two [`MetricsSummaryBucketValue`]s together by mutating `self` and consuming
    /// `other`.
    fn merge(&mut self, other: MetricsSummaryBucketValue) {
        self.min = std::cmp::min(self.min, other.min);
        self.max = std::cmp::max(self.max, other.max);
        self.sum = match (self.sum, other.sum) {
            (Some(sum), Some(other_sum)) => Some(sum.saturating_add(other_sum)),
            (None, Some(other_sum)) => Some(other_sum),
            (Some(sum), None) => Some(sum),
            _ => None,
        };
        self.count += other.count;
    }
}

/// metrics_summary_spec that tracks all the buckets containing the summaries for each
/// [`MetricsSummaryBucketKey`].
///
/// The need for an metrics_summary_spec arises from the fact that we want to compute metrics summaries
/// generically on any slice of [`Bucket`]s meaning that we need to handle cases in which
/// the same metrics as identified by the [`MetricsSummaryBucketKey`] have to be merged.
pub struct MetricsSummarySpec {
    buckets: BTreeMap<MetricsSummaryBucketKey, MetricsSummaryBucketValue>,
}

impl MetricsSummarySpec {
    fn new() -> MetricsSummarySpec {
        MetricsSummarySpec {
            buckets: BTreeMap::new(),
        }
    }

    /// Merges into the [`MetricsSummarySpec`] a slice of [`Bucket`]s.
    fn from_buckets<'a>(buckets: impl Iterator<Item = &'a Bucket>) -> MetricsSummarySpec {
        let mut metrics_summary_spec = MetricsSummarySpec::new();

        for bucket in buckets {
            metrics_summary_spec.merge_bucket(bucket);
        }

        metrics_summary_spec
    }

    /// Merges a [`Bucket`] into the [`MetricsSummarySpec`].
    fn merge_bucket(&mut self, bucket: &Bucket) {
        let key = MetricsSummaryBucketKey {
            metric_name: bucket.name.clone(),
            tags: bucket.tags.clone(),
        };

        let value = MetricsSummaryBucketValue::from_bucket_value(&bucket.value);

        self.merge(key, value);
    }

    /// Merges a [`MetricsSummary`] into the [`MetricsSummarySpec`].
    fn merge_metrics_summary(&mut self, metrics_summary: &MetricsSummary) {
        for (metric, summary_per_tag) in metrics_summary.0.iter() {
            let Some(summary_per_tag) = summary_per_tag.value() else {
                continue;
            };

            for summary in summary_per_tag {
                let Some(summary) = summary.value() else {
                    continue;
                };

                let mut tags = BTreeMap::new();
                if let Some(inner_tags) = summary.tags.value() {
                    for (tag_key, tag_value) in inner_tags.iter() {
                        let Some(tag_value) = tag_value.value() else {
                            continue;
                        };

                        tags.insert(tag_key.clone(), tag_value.clone());
                    }
                }

                let key = MetricsSummaryBucketKey {
                    metric_name: MetricName::from(metric.clone()),
                    tags,
                };

                let value = MetricsSummaryBucketValue::from_metric_summary(summary);

                self.merge(key, value);
            }
        }
    }

    /// Merges a [`MetricsSummaryBucketKey`] and [`MetricsSummaryBucketValue`] in the spec.
    fn merge(&mut self, key: MetricsSummaryBucketKey, value: MetricsSummaryBucketValue) {
        match self.buckets.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge(value);
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
    }

    /// Applies the [`MetricsSummarySpec`] on a receiving [`Annotated<MetricsSummary>`].
    ///
    /// In case the [`MetricsSummarySpec`] is empty, no mutation will take place.
    pub fn apply_on(mut self, receiver: &mut Annotated<MetricsSummary>) {
        if self.buckets.is_empty() {
            return;
        }

        if let Some(receiver) = receiver.value() {
            self.merge_metrics_summary(receiver);
        }

        *receiver = self.into()
    }
}

impl From<MetricsSummarySpec> for Annotated<MetricsSummary> {
    /// Builds the [`MetricsSummary`] from the [`MetricsSummarySpec`].
    ///
    /// Note that this method consumes the metrics_summary_spec itself, since the purpose of the metrics_summary_spec
    /// is to be built and destroyed once the summary is ready to be computed.
    fn from(spec: MetricsSummarySpec) -> Self {
        let mut metrics_summary = BTreeMap::new();

        for (key, value) in spec.buckets {
            let tags = key
                .tags
                .into_iter()
                .map(|(tag_key, tag_value)| (tag_key, Annotated::new(tag_value)))
                .collect();

            let metric_summary = MetricSummary {
                min: value
                    .min
                    .map_or(Annotated::empty(), |m| Annotated::new(m.to_f64())),
                max: value
                    .max
                    .map_or(Annotated::empty(), |m| Annotated::new(m.to_f64())),
                sum: value
                    .sum
                    .map_or(Annotated::empty(), |m| Annotated::new(m.to_f64())),
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

        Annotated::new(MetricsSummary(metrics_summary))
    }
}

/// Computes the [`MetricsSummary`] from a slice of [`Bucket`]s.
pub fn compute(buckets: &[Bucket]) -> MetricsSummarySpec {
    // For now, we only want metrics summaries to be extracted for custom metrics.
    let filtered_buckets = buckets
        .iter()
        .filter(|b| matches!(b.name.namespace(), MetricNamespace::Custom));

    MetricsSummarySpec::from_buckets(filtered_buckets)
}

#[cfg(test)]
mod tests {
    use crate::metrics_extraction::metrics_summary::MetricsSummarySpec;
    use relay_common::time::UnixTimestamp;
    use relay_event_schema::protocol::MetricsSummary;
    use relay_metrics::Bucket;
    use relay_protocol::Annotated;

    fn build_buckets(slice: &[u8]) -> Vec<Bucket> {
        Bucket::parse_all(slice, UnixTimestamp::now())
            .map(|b| b.unwrap())
            .collect()
    }

    #[test]
    fn test_with_counter_buckets() {
        let buckets =
            build_buckets(b"my_counter:3|c|#platform:ios\nmy_counter:2|c|#platform:android");

        let metrics_summary_spec = MetricsSummarySpec::from_buckets(buckets.iter());
        let metrics_summary: Annotated<MetricsSummary> = metrics_summary_spec.into();

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "c:custom/my_counter@none": [
                    MetricSummary {
                        min: 2.0,
                        max: 2.0,
                        sum: 2.0,
                        count: 1,
                        tags: {
                            "platform": "android",
                        },
                    },
                    MetricSummary {
                        min: 3.0,
                        max: 3.0,
                        sum: 3.0,
                        count: 1,
                        tags: {
                            "platform": "ios",
                        },
                    },
                ],
            },
        )
        "###);
    }

    #[test]
    fn test_with_distribution_buckets() {
        let buckets =
            build_buckets(b"my_dist:3.0:5.0|d|#platform:ios\nmy_dist:2.0:4.0|d|#platform:android");

        let metrics_summary_spec = MetricsSummarySpec::from_buckets(buckets.iter());
        let metrics_summary: Annotated<MetricsSummary> = metrics_summary_spec.into();

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "d:custom/my_dist@none": [
                    MetricSummary {
                        min: 2.0,
                        max: 4.0,
                        sum: 6.0,
                        count: 2,
                        tags: {
                            "platform": "android",
                        },
                    },
                    MetricSummary {
                        min: 3.0,
                        max: 5.0,
                        sum: 8.0,
                        count: 2,
                        tags: {
                            "platform": "ios",
                        },
                    },
                ],
            },
        )
        "###);
    }

    #[test]
    fn test_with_set_buckets() {
        let buckets =
            build_buckets(b"my_set:3.0:5.0|s|#platform:ios\nmy_set:2.0:4.0|s|#platform:android");

        let metrics_summary_spec = MetricsSummarySpec::from_buckets(buckets.iter());
        let metrics_summary: Annotated<MetricsSummary> = metrics_summary_spec.into();

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "s:custom/my_set@none": [
                    MetricSummary {
                        min: ~,
                        max: ~,
                        sum: ~,
                        count: 2,
                        tags: {
                            "platform": "android",
                        },
                    },
                    MetricSummary {
                        min: ~,
                        max: ~,
                        sum: ~,
                        count: 2,
                        tags: {
                            "platform": "ios",
                        },
                    },
                ],
            },
        )
        "###);
    }

    #[test]
    fn test_with_gauge_buckets() {
        let buckets =
            build_buckets(b"my_gauge:3.0|g|#platform:ios\nmy_gauge:2.0|g|#platform:android");

        let metrics_summary_spec = MetricsSummarySpec::from_buckets(buckets.iter());
        let metrics_summary: Annotated<MetricsSummary> = metrics_summary_spec.into();

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "g:custom/my_gauge@none": [
                    MetricSummary {
                        min: 2.0,
                        max: 2.0,
                        sum: 2.0,
                        count: 1,
                        tags: {
                            "platform": "android",
                        },
                    },
                    MetricSummary {
                        min: 3.0,
                        max: 3.0,
                        sum: 3.0,
                        count: 1,
                        tags: {
                            "platform": "ios",
                        },
                    },
                ],
            },
        )
        "###);
    }

    #[test]
    fn test_merge_buckets() {
        let mut buckets =
            build_buckets(b"my_counter:3|c|#platform:ios\nmy_counter:2|c|#platform:ios");
        buckets.extend(build_buckets(
            b"my_dist:3.0:5.0|d|#platform:ios\nmy_dist:2.0:4.0|d|#platform:ios",
        ));
        buckets.extend(build_buckets(
            b"my_set:3.0:5.0|s|#platform:ios\nmy_set:2.0:4.0|s|#platform:ios",
        ));
        buckets.extend(build_buckets(
            b"my_gauge:3.0|g|#platform:ios\nmy_gauge:2.0|g|#platform:ios",
        ));

        let metrics_summary_spec = MetricsSummarySpec::from_buckets(buckets.iter());
        let metrics_summary: Annotated<MetricsSummary> = metrics_summary_spec.into();

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "c:custom/my_counter@none": [
                    MetricSummary {
                        min: 2.0,
                        max: 3.0,
                        sum: 5.0,
                        count: 2,
                        tags: {
                            "platform": "ios",
                        },
                    },
                ],
                "d:custom/my_dist@none": [
                    MetricSummary {
                        min: 2.0,
                        max: 5.0,
                        sum: 14.0,
                        count: 4,
                        tags: {
                            "platform": "ios",
                        },
                    },
                ],
                "g:custom/my_gauge@none": [
                    MetricSummary {
                        min: 2.0,
                        max: 3.0,
                        sum: 5.0,
                        count: 2,
                        tags: {
                            "platform": "ios",
                        },
                    },
                ],
                "s:custom/my_set@none": [
                    MetricSummary {
                        min: ~,
                        max: ~,
                        sum: ~,
                        count: 4,
                        tags: {
                            "platform": "ios",
                        },
                    },
                ],
            },
        )
        "###);
    }
}
