use crate::metrics_extraction::generic;
use crate::metrics_extraction::generic::Extractable;
use relay_base_schema::metrics::{MetricName, MetricNamespace};
use relay_dynamic_config::CombinedMetricExtractionConfig;
use relay_event_schema::protocol as event;
use relay_metrics::{
    Bucket, BucketValue, CounterType, DistributionValue, FiniteF64, GaugeValue, SetValue,
};
use relay_protocol::Annotated;
use std::collections::BTreeMap;
use std::ops::AddAssign;

/// Maps two [`Option<T>`] values using a provided function.
fn map_multiple<T: Ord>(a: Option<T>, b: Option<T>, f: fn(T, T) -> T) -> Option<T> {
    match (a, b) {
        (Some(x), Some(y)) => Some(f(x, y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

/// Key of a bucket used to keep track of aggregates for the [`MetricsSummary`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MetricsSummaryBucketKey {
    /// Name of the metric.
    metric_name: MetricName,
    /// Tags of the bucket.
    tags: BTreeMap<String, String>,
}

/// Value of a bucket used to keep track of aggregates for the [`MetricsSummary`].
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
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

impl AddAssign for MetricsSummaryBucketValue {
    fn add_assign(&mut self, rhs: Self) {
        *self = MetricsSummaryBucketValue {
            min: map_multiple(self.min, rhs.min, std::cmp::min),
            max: map_multiple(self.max, rhs.max, std::cmp::max),
            sum: map_multiple(self.sum, rhs.sum, |l, r| l.saturating_add(r)),
            count: self.count + rhs.count,
        }
    }
}

impl<'a> From<&'a BucketValue> for MetricsSummaryBucketValue {
    fn from(value: &'a BucketValue) -> Self {
        match value {
            BucketValue::Counter(counter) => counter.into(),
            BucketValue::Distribution(distribution) => distribution.into(),
            BucketValue::Set(set) => set.into(),
            BucketValue::Gauge(gauge) => gauge.into(),
        }
    }
}

impl<'a> From<&'a CounterType> for MetricsSummaryBucketValue {
    fn from(counter: &'a CounterType) -> Self {
        MetricsSummaryBucketValue {
            min: Some(*counter),
            max: Some(*counter),
            sum: Some(*counter),
            count: 1,
        }
    }
}

impl<'a> From<&'a DistributionValue> for MetricsSummaryBucketValue {
    fn from(distribution: &'a DistributionValue) -> Self {
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
}

impl<'a> From<&'a SetValue> for MetricsSummaryBucketValue {
    fn from(set: &'a SetValue) -> Self {
        // For sets, we limit to counting the number of occurrences.
        MetricsSummaryBucketValue {
            min: None,
            max: None,
            sum: None,
            count: set.len() as u64,
        }
    }
}

impl<'a> From<&'a GaugeValue> for MetricsSummaryBucketValue {
    fn from(gauge: &'a GaugeValue) -> Self {
        MetricsSummaryBucketValue {
            min: Some(gauge.min),
            max: Some(gauge.max),
            sum: Some(gauge.sum),
            count: gauge.count,
        }
    }
}

/// [`MetricsSummary`] that tracks all the buckets containing the summaries for each
/// [`MetricsSummaryBucketKey`].
///
/// The need for a [`MetricsSummary`] arises from the fact that we want to compute metrics summaries
/// generically on any slice of [`Bucket`]s meaning that we need to handle cases in which
/// the same metrics as identified by the [`MetricsSummaryBucketKey`] have to be merged.
///
/// The [`MetricsSummary`] is a different in-memory representation of a metric summary from the
/// [`event::MetricsSummary`].
#[derive(Debug, Default)]
pub struct MetricsSummary {
    buckets: BTreeMap<MetricsSummaryBucketKey, MetricsSummaryBucketValue>,
}

impl MetricsSummary {
    fn new() -> MetricsSummary {
        MetricsSummary {
            buckets: BTreeMap::new(),
        }
    }

    /// Merges into the [`MetricsSummary`] a slice of [`Bucket`]s.
    fn from_buckets<'a>(buckets: impl Iterator<Item = &'a Bucket>) -> MetricsSummary {
        let mut metrics_summary_spec = MetricsSummary::new();

        for bucket in buckets {
            metrics_summary_spec.merge_bucket(bucket);
        }

        metrics_summary_spec
    }

    /// Merges a [`Bucket`] into the [`MetricsSummary`].
    fn merge_bucket(&mut self, bucket: &Bucket) {
        let key = MetricsSummaryBucketKey {
            metric_name: bucket.name.clone(),
            tags: bucket.tags.clone(),
        };

        let value = (&bucket.value).into();
        self.buckets
            .entry(key)
            .and_modify(|e| *e += value)
            .or_insert(value);
    }

    /// Applies the [`MetricsSummary`] on a receiving [`Annotated<MetricsSummary>`].
    pub fn apply_on(self, receiver: &mut Annotated<event::MetricsSummary>) {
        if self.buckets.is_empty() {
            return;
        }

        let event::MetricsSummary(metrics_summary_mapping) =
            &mut receiver.get_or_insert_with(|| event::MetricsSummary(BTreeMap::new()));

        for (key, value) in self.buckets {
            let min = value.min.map_or(Annotated::empty(), |m| m.to_f64().into());
            let max = value.max.map_or(Annotated::empty(), |m| m.to_f64().into());
            let sum = value.sum.map_or(Annotated::empty(), |m| m.to_f64().into());
            let metric_summary = event::MetricSummary {
                min,
                max,
                sum,
                count: Annotated::new(value.count),
                tags: Annotated::new(
                    key.tags
                        .into_iter()
                        .map(|(tag_key, tag_value)| (tag_key, Annotated::new(tag_value)))
                        .collect(),
                ),
            };

            let Some(metric_summary_tags) = metric_summary.tags.value() else {
                // This code should never be reached.
                continue;
            };

            let existing_summary = metrics_summary_mapping
                .get_mut(key.metric_name.as_ref())
                .and_then(|v| v.value_mut().as_mut());

            match existing_summary {
                Some(existing_summary) => {
                    let mut found_summary = None;
                    for existing_summary in existing_summary.iter_mut() {
                        let Some(existing_summary) = existing_summary.value_mut() else {
                            continue;
                        };

                        match existing_summary.tags.value() {
                            Some(existing_summary_tags)
                                if *metric_summary_tags == *existing_summary_tags =>
                            {
                                found_summary = Some(existing_summary);
                                break;
                            }
                            None if metric_summary_tags.is_empty() => {
                                found_summary = Some(existing_summary);
                                break;
                            }
                            _ => {}
                        }
                    }

                    match found_summary {
                        Some(found_summary) => {
                            found_summary.merge(metric_summary);
                        }
                        None => {
                            existing_summary.push(Annotated::new(metric_summary));
                        }
                    }
                }
                None => {
                    metrics_summary_mapping.insert(
                        key.metric_name.to_string(),
                        Annotated::new(vec![Annotated::new(metric_summary)]),
                    );
                }
            }
        }
    }
}

/// Computes the [`MetricsSummary`] from a slice of [`Bucket`]s that belong to
/// [`MetricNamespace::Custom`].
fn compute(buckets: &[Bucket]) -> MetricsSummary {
    // For now, we only want metrics summaries to be extracted for custom metrics.
    let filtered_buckets = buckets
        .iter()
        .filter(|b| matches!(b.name.namespace(), MetricNamespace::Custom));

    MetricsSummary::from_buckets(filtered_buckets)
}

/// Extract metrics and summarizes them.
pub fn extract_and_summarize_metrics<T>(
    instance: &T,
    config: CombinedMetricExtractionConfig<'_>,
) -> (Vec<Bucket>, MetricsSummary)
where
    T: Extractable,
{
    let metrics = generic::extract_metrics(instance, config);
    let metrics_summaries = compute(&metrics);

    (metrics, metrics_summaries)
}

#[cfg(test)]
mod tests {
    use crate::metrics_extraction::metrics_summary::{MetricsSummary, MetricsSummaryBucketValue};
    use relay_common::time::UnixTimestamp;
    use relay_event_schema::protocol as event;
    use relay_metrics::{Bucket, FiniteF64};
    use relay_protocol::Annotated;
    use std::collections::BTreeMap;

    fn build_buckets(slice: &[u8]) -> Vec<Bucket> {
        Bucket::parse_all(slice, UnixTimestamp::now())
            .map(|b| b.unwrap())
            .collect()
    }

    #[test]
    fn test_with_counter_buckets() {
        let buckets =
            build_buckets(b"my_counter:3|c|#platform:ios\nmy_counter:2|c|#platform:android");

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(BTreeMap::new()));
        metrics_summary_spec.apply_on(&mut metrics_summary);

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

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(BTreeMap::new()));
        metrics_summary_spec.apply_on(&mut metrics_summary);

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

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(BTreeMap::new()));
        metrics_summary_spec.apply_on(&mut metrics_summary);

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

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(BTreeMap::new()));
        metrics_summary_spec.apply_on(&mut metrics_summary);

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

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(BTreeMap::new()));
        metrics_summary_spec.apply_on(&mut metrics_summary);

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

    #[test]
    fn test_apply_on_with_different_metric() {
        let buckets = build_buckets(b"my_counter:3|c|#platform:ios");
        let mut tags_map = BTreeMap::new();
        tags_map.insert("region".to_owned(), Annotated::new("us".to_owned()));
        let mut summary_map = BTreeMap::new();
        summary_map.insert(
            "c:custom/my_other_counter@none".to_owned(),
            Annotated::new(vec![Annotated::new(event::MetricSummary {
                min: Annotated::new(5.0),
                max: Annotated::new(10.0),
                sum: Annotated::new(15.0),
                count: Annotated::new(2),
                tags: Annotated::new(tags_map),
            })]),
        );

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(summary_map));
        metrics_summary_spec.apply_on(&mut metrics_summary);

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "c:custom/my_counter@none": [
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
                "c:custom/my_other_counter@none": [
                    MetricSummary {
                        min: 5.0,
                        max: 10.0,
                        sum: 15.0,
                        count: 2,
                        tags: {
                            "region": "us",
                        },
                    },
                ],
            },
        )
        "###);
    }

    #[test]
    fn test_apply_on_with_same_metric_and_different_tags() {
        let buckets = build_buckets(b"my_counter:3|c|#platform:ios");
        let mut tags_map = BTreeMap::new();
        tags_map.insert("region".to_owned(), Annotated::new("us".to_owned()));
        let mut summary_map = BTreeMap::new();
        summary_map.insert(
            "c:custom/my_counter@none".to_owned(),
            Annotated::new(vec![Annotated::new(event::MetricSummary {
                min: Annotated::new(5.0),
                max: Annotated::new(10.0),
                sum: Annotated::new(15.0),
                count: Annotated::new(2),
                tags: Annotated::new(tags_map),
            })]),
        );

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(summary_map));
        metrics_summary_spec.apply_on(&mut metrics_summary);

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "c:custom/my_counter@none": [
                    MetricSummary {
                        min: 5.0,
                        max: 10.0,
                        sum: 15.0,
                        count: 2,
                        tags: {
                            "region": "us",
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
    fn test_apply_on_with_same_metric_and_same_tags() {
        let buckets = build_buckets(b"my_counter:3|c|#platform:ios");
        let mut tags_map = BTreeMap::new();
        tags_map.insert("platform".to_owned(), Annotated::new("ios".to_owned()));
        let mut summary_map = BTreeMap::new();
        summary_map.insert(
            "c:custom/my_counter@none".to_owned(),
            Annotated::new(vec![Annotated::new(event::MetricSummary {
                min: Annotated::new(5.0),
                max: Annotated::new(10.0),
                sum: Annotated::new(15.0),
                count: Annotated::new(2),
                tags: Annotated::new(tags_map),
            })]),
        );

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(summary_map));
        metrics_summary_spec.apply_on(&mut metrics_summary);

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "c:custom/my_counter@none": [
                    MetricSummary {
                        min: 3.0,
                        max: 10.0,
                        sum: 18.0,
                        count: 3,
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
    fn test_apply_on_with_same_metric_and_same_empty_tags() {
        let buckets = build_buckets(b"my_counter:3|c");
        let mut summary_map = BTreeMap::new();
        summary_map.insert(
            "c:custom/my_counter@none".to_owned(),
            Annotated::new(vec![Annotated::new(event::MetricSummary {
                min: Annotated::new(5.0),
                max: Annotated::new(10.0),
                sum: Annotated::new(15.0),
                count: Annotated::new(2),
                tags: Annotated::new(BTreeMap::new()),
            })]),
        );

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(summary_map));
        metrics_summary_spec.apply_on(&mut metrics_summary);

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "c:custom/my_counter@none": [
                    MetricSummary {
                        min: 3.0,
                        max: 10.0,
                        sum: 18.0,
                        count: 3,
                        tags: {},
                    },
                ],
            },
        )
        "###);
    }

    #[test]
    fn test_apply_on_with_same_metric_and_same_empty_tags_and_none_values() {
        let buckets = build_buckets(b"my_counter:3|c");
        let mut summary_map = BTreeMap::new();
        summary_map.insert(
            "c:custom/my_counter@none".to_owned(),
            Annotated::new(vec![Annotated::new(event::MetricSummary {
                min: Annotated::empty(),
                max: Annotated::new(10.0),
                sum: Annotated::empty(),
                count: Annotated::new(2),
                tags: Annotated::new(BTreeMap::new()),
            })]),
        );

        let metrics_summary_spec = MetricsSummary::from_buckets(buckets.iter());
        let mut metrics_summary = Annotated::new(event::MetricsSummary(summary_map));
        metrics_summary_spec.apply_on(&mut metrics_summary);

        insta::assert_debug_snapshot!(metrics_summary.value().unwrap(), @r###"
        MetricsSummary(
            {
                "c:custom/my_counter@none": [
                    MetricSummary {
                        min: 3.0,
                        max: 10.0,
                        sum: 3.0,
                        count: 3,
                        tags: {},
                    },
                ],
            },
        )
        "###);
    }

    #[test]
    fn test_bucket_value_merge() {
        let mut value_1 = MetricsSummaryBucketValue {
            min: Some(FiniteF64::MIN),
            max: None,
            sum: None,
            count: 0,
        };

        let value_2 = MetricsSummaryBucketValue {
            min: None,
            max: Some(FiniteF64::MAX),
            sum: Some(FiniteF64::from(10)),
            count: 10,
        };

        value_1 += value_2;

        assert_eq!(
            value_1,
            MetricsSummaryBucketValue {
                min: Some(FiniteF64::MIN),
                max: Some(FiniteF64::MAX),
                sum: Some(FiniteF64::from(10)),
                count: 10
            }
        );
    }
}
