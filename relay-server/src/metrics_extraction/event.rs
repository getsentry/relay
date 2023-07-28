use std::collections::BTreeMap;

use relay_common::{DataCategory, UnixTimestamp};
use relay_dynamic_config::{MetricExtractionConfig, TagSource, TagSpec};
use relay_general::protocol::{Event, Span, Timestamp};
use relay_metrics::{Metric, MetricResourceIdentifier, MetricType, MetricValue};
use relay_sampling::FieldValueProvider;

/// Extract metrics from an [`Event`].
///
/// The event must have a valid timestamp; if the timestamp is missing or invalid, no metrics are
/// extracted. Timestamp and clock drift correction should occur before metrics extraction to ensure
/// valid timestamps.
pub fn extract_event_metrics(event: &Event, config: &MetricExtractionConfig) -> Vec<Metric> {
    extract_metrics_from(event, config)
}

/// Item from which metrics can be extracted.
pub trait Extractable {
    /// Data category for the metric spec to match on.
    fn category(&self) -> DataCategory;

    /// The timestamp to associate with the extracted metrics.
    fn timestamp(&self) -> Option<Timestamp>;
}

impl Extractable for Event {
    fn category(&self) -> DataCategory {
        // Obtain the event's data category, but treat default events as error events for the purpose of
        // metric tagging.
        match DataCategory::from(self.ty.value().copied().unwrap_or_default()) {
            DataCategory::Default => DataCategory::Error,
            category => category,
        }
    }

    fn timestamp(&self) -> Option<Timestamp> {
        self.timestamp.value().copied()
    }
}

impl Extractable for Span {
    fn category(&self) -> DataCategory {
        DataCategory::Span
    }

    fn timestamp(&self) -> Option<Timestamp> {
        self.timestamp.value().copied()
    }
}

pub fn extract_metrics_from<T>(data: &T, config: &MetricExtractionConfig) -> Vec<Metric>
where
    T: Extractable + FieldValueProvider,
{
    let mut metrics = Vec::new();

    let event_ts = data.timestamp();
    let Some(timestamp) = event_ts.and_then(|d| UnixTimestamp::from_datetime(d.0)) else {
        relay_log::error!(timestamp = ?event_ts, "invalid event timestamp for metric extraction");
        return metrics
    };

    for metric_spec in &config.metrics {
        if metric_spec.category != data.category() {
            continue;
        }

        if let Some(ref condition) = &metric_spec.condition {
            if !condition.matches(data) {
                continue;
            }
        }

        // Parse the MRI so that we can obtain the type, but subsequently re-serialize it into the
        // generated metric to ensure the MRI is normalized.
        let Ok(mri) = MetricResourceIdentifier::parse(&metric_spec.mri) else {
            relay_log::error!(mri=metric_spec.mri, "invalid MRI for metric extraction");
            continue;
        };

        let Some(value) = read_metric_value(data, metric_spec.field.as_deref(), mri.ty) else {
            continue;
        };

        // Combine global tag mapping with metric's own tags.
        // Global tags are overwritten by metric-specific tags.
        let tags = config
            .tags
            .iter()
            .filter_map(|t| {
                // TODO: support wildcards as the docs suggest.
                t.metrics
                    .contains(&metric_spec.mri)
                    .then_some(t.tags.iter())
            })
            .flatten()
            .chain(metric_spec.tags.iter());

        metrics.push(Metric {
            name: mri.to_string(),
            value,
            timestamp,
            tags: extract_event_tags(data, tags),
        });
    }

    metrics
}

fn extract_event_tags<'a>(
    event: &impl FieldValueProvider,
    tags: impl Iterator<Item = &'a TagSpec>,
) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();

    for tag_spec in tags {
        if let Some(ref condition) = tag_spec.condition {
            if !condition.matches(event) {
                continue;
            }
        }

        let value_opt = match tag_spec.source() {
            TagSource::Literal(value) => Some(value.to_owned()),
            TagSource::Field(field) => event.get_value(field).as_str().map(str::to_owned),
            TagSource::Unknown => None,
        };

        if let Some(value) = value_opt {
            // Explicitly do not override existing tags on a metric. First condition wins.
            if !map.contains_key(&tag_spec.key) {
                map.insert(tag_spec.key.clone(), value);
            }
        }
    }

    map
}

fn read_metric_value(
    event: &impl FieldValueProvider,
    field: Option<&str>,
    ty: MetricType,
) -> Option<MetricValue> {
    Some(match ty {
        MetricType::Counter => MetricValue::Counter(match field {
            Some(field) => event.get_value(field).as_f64()?,
            None => 1.0,
        }),
        MetricType::Distribution => MetricValue::Distribution(event.get_value(field?).as_f64()?),
        MetricType::Set => MetricValue::set_from_str(event.get_value(field?).as_str()?),
        MetricType::Gauge => MetricValue::Gauge(event.get_value(field?).as_f64()?),
    })
}

#[cfg(test)]
mod tests {
    use relay_general::types::FromValue;
    use serde_json::json;

    use super::*;

    #[test]
    fn extract_counter() {
        let event_json = json!({
            "type": "transaction",
            "timestamp": 1597976302.0,
        });
        let event = Event::from_value(event_json.into());

        let config_json = json!({
            "version": 1,
            "metrics": [
                {
                    "category": "transaction",
                    "mri": "c:transactions/counter@none",
                }
            ]
        });
        let config = serde_json::from_value(config_json).unwrap();

        let metrics = extract_event_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "c:transactions/counter@none",
                value: Counter(
                    1.0,
                ),
                timestamp: UnixTimestamp(1597976302),
                tags: {},
            },
        ]
        "###);
    }

    #[test]
    fn extract_distribution() {
        let event_json = json!({
            "type": "transaction",
            "start_timestamp": 1597976300.0,
            "timestamp": 1597976302.0,
        });
        let event = Event::from_value(event_json.into());

        let config_json = json!({
            "version": 1,
            "metrics": [
                {
                    "category": "transaction",
                    "mri": "d:transactions/duration@none",
                    "field": "event.duration",
                }
            ]
        });
        let config = serde_json::from_value(config_json).unwrap();

        let metrics = extract_event_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "d:transactions/duration@none",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1597976302),
                tags: {},
            },
        ]
        "###);
    }

    #[test]
    fn extract_set() {
        let event_json = json!({
            "type": "transaction",
            "timestamp": 1597976302.0,
            "user": {
                "id": "4711",
            },
        });
        let event = Event::from_value(event_json.into());

        let config_json = json!({
            "version": 1,
            "metrics": [
                {
                    "category": "transaction",
                    "mri": "s:transactions/users@none",
                    "field": "event.user.id",
                }
            ]
        });
        let config = serde_json::from_value(config_json).unwrap();

        let metrics = extract_event_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "s:transactions/users@none",
                value: Set(
                    943162418,
                ),
                timestamp: UnixTimestamp(1597976302),
                tags: {},
            },
        ]
        "###);
    }

    #[test]
    fn extract_tag_conditions() {
        let event_json = json!({
            "type": "transaction",
            "start_timestamp": 1597976300.0,
            "timestamp": 1597976302.0,
            "release": "myapp@1.0.0",
        });
        let event = Event::from_value(event_json.into());

        let config_json = json!({
            "version": 1,
            "metrics": [
                {
                    "category": "transaction",
                    "mri": "c:transactions/counter@none",
                    "tags": [
                        {"key": "id", "value": "4711"},
                        {"key": "release", "field": "event.release"},
                        {
                            "key": "fast",
                            "value": "yes",
                            "condition": {"op": "lt", "name": "event.duration", "value": 2000},
                        },
                        {
                            "key": "fast",
                            "value": "no",
                            "condition": {"op": "gte", "name": "event.duration", "value": 2000},
                        },
                    ]
                }
            ]
        });
        let config = serde_json::from_value(config_json).unwrap();

        let metrics = extract_event_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "c:transactions/counter@none",
                value: Counter(
                    1.0,
                ),
                timestamp: UnixTimestamp(1597976302),
                tags: {
                    "fast": "no",
                    "id": "4711",
                    "release": "myapp@1.0.0",
                },
            },
        ]
        "###);
    }
}
