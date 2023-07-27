use std::collections::BTreeMap;

use relay_common::{DurationUnit, MetricUnit, UnixTimestamp};
use relay_general::store::span::tag_extraction::SpanTagKey;
use relay_metrics::{Metric, MetricNamespace, MetricValue};

use crate::metrics_extraction::IntoMetric;

#[derive(Clone, Debug)]
pub(crate) enum SpanMetric {
    ExclusiveTime {
        value: f64,
        tags: BTreeMap<SpanTagKey, String>,
    },
    ExclusiveTimeLight {
        value: f64,
        tags: BTreeMap<SpanTagKey, String>,
    },
}

impl IntoMetric for SpanMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric {
        let namespace = MetricNamespace::Spans;

        match self {
            SpanMetric::ExclusiveTime { value, tags } => Metric::new_mri(
                namespace,
                "exclusive_time",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(value),
                timestamp,
                span_tag_mapping_to_string_mapping(tags),
            ),
            SpanMetric::ExclusiveTimeLight { value, tags } => Metric::new_mri(
                namespace,
                "exclusive_time_light",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(value),
                timestamp,
                span_tag_mapping_to_string_mapping(tags),
            ),
        }
    }
}

/// Returns the same mapping of tags, with the tag keys as strings.
///
/// Rust doesn't allow to add implementations on foreign methods (BTreeMap), so
/// this method is a workaround to parse [`SpanTagKey`]s to strings to strings.
fn span_tag_mapping_to_string_mapping(
    tags: BTreeMap<SpanTagKey, String>,
) -> BTreeMap<String, String> {
    tags.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use relay_common::UnixTimestamp;

    use crate::metrics_extraction::spans::types::{SpanMetric, SpanTagKey};
    use crate::metrics_extraction::IntoMetric;

    #[test]
    fn test_span_metric_conversion() {
        let timestamp =
            UnixTimestamp::from_datetime(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap())
                .unwrap();

        let tags = BTreeMap::from([(SpanTagKey::Release, "1.2.3".to_owned())]);
        let metric = SpanMetric::ExclusiveTime {
            value: 1000.0,
            tags,
        };
        let converted = metric.into_metric(timestamp);

        insta::assert_debug_snapshot!(converted, @r###"
        Metric {
            name: "d:spans/exclusive_time@millisecond",
            value: Distribution(
                1000.0,
            ),
            timestamp: UnixTimestamp(946684800),
            tags: {
                "release": "1.2.3",
            },
        }
        "###);
    }
}
