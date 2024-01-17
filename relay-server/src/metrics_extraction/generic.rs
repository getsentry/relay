use std::collections::BTreeMap;

use relay_common::time::UnixTimestamp;
use relay_dynamic_config::{MetricExtractionConfig, TagMapping, TagSource, TagSpec};
use relay_metrics::{Bucket, BucketValue, FiniteF64, MetricResourceIdentifier, MetricType};
use relay_protocol::{Getter, Val};
use relay_quotas::DataCategory;

/// Item from which metrics can be extracted.
pub trait Extractable: Getter {
    /// Data category for the metric spec to match on.
    fn category(&self) -> DataCategory;

    /// The timestamp to associate with the extracted metrics.
    fn timestamp(&self) -> Option<UnixTimestamp>;
}

/// Extract metrics from any type that implements both [`Extractable`] and [`Getter`].
///
/// The instance must have a valid timestamp; if the timestamp is missing or invalid, no metrics are
/// extracted. Timestamp and clock drift correction should occur before metrics extraction to ensure
/// valid timestamps.
pub fn extract_metrics<T>(instance: &T, config: &MetricExtractionConfig) -> Vec<Bucket>
where
    T: Extractable,
{
    let mut metrics = Vec::new();

    let Some(timestamp) = instance.timestamp() else {
        relay_log::error!("invalid event timestamp for metric extraction");
        return metrics;
    };

    for metric_spec in &config.metrics {
        if metric_spec.category != instance.category() {
            continue;
        }

        if let Some(ref condition) = &metric_spec.condition {
            if !condition.matches(instance) {
                continue;
            }
        }

        // Parse the MRI so that we can obtain the type, but subsequently re-serialize it into the
        // generated metric to ensure the MRI is normalized.
        let Ok(mri) = MetricResourceIdentifier::parse(&metric_spec.mri) else {
            relay_log::error!(mri = metric_spec.mri, "invalid MRI for metric extraction");
            continue;
        };

        let Some(value) = read_metric_value(instance, metric_spec.field.as_deref(), mri.ty) else {
            continue;
        };

        metrics.push(Bucket {
            name: mri.to_string(),
            width: 0,
            value,
            timestamp,
            tags: extract_tags(instance, &metric_spec.tags),
        });
    }

    // TODO: Inline this again once transaction metric extraction has been moved to generic metrics.
    tmp_apply_tags(&mut metrics, instance, &config.tags);

    metrics
}

pub fn tmp_apply_tags<T>(metrics: &mut [Bucket], instance: &T, mappings: &[TagMapping])
where
    T: Getter,
{
    for mapping in mappings {
        let mut lazy_tags = None;

        for metric in &mut *metrics {
            if mapping.matches(&metric.name) {
                let tags = lazy_tags.get_or_insert_with(|| extract_tags(instance, &mapping.tags));

                for (key, val) in tags {
                    if !metric.tags.contains_key(key) {
                        metric.tags.insert(key.clone(), val.clone());
                    }
                }
            }
        }
    }
}

fn extract_tags<T>(instance: &T, tags: &[TagSpec]) -> BTreeMap<String, String>
where
    T: Getter,
{
    let mut map = BTreeMap::new();

    for tag_spec in tags {
        if let Some(ref condition) = tag_spec.condition {
            if !condition.matches(instance) {
                continue;
            }
        }

        let value_opt = match tag_spec.source() {
            TagSource::Literal(value) => Some(value.to_owned()),
            TagSource::Field(field) => match instance.get_value(field) {
                Some(Val::String(s)) => Some(s.to_owned()),
                _ => None,
            },
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
    instance: &impl Getter,
    field: Option<&str>,
    ty: MetricType,
) -> Option<BucketValue> {
    Some(match ty {
        MetricType::Counter => BucketValue::counter(match field {
            Some(field) => FiniteF64::new(instance.get_value(field)?.as_f64()?)?,
            None => 1.into(),
        }),
        MetricType::Distribution => {
            BucketValue::distribution(FiniteF64::new(instance.get_value(field?)?.as_f64()?)?)
        }
        MetricType::Set => BucketValue::set_from_str(instance.get_value(field?)?.as_str()?),
        MetricType::Gauge => {
            BucketValue::gauge(FiniteF64::new(instance.get_value(field?)?.as_f64()?)?)
        }
    })
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::Event;
    use relay_protocol::FromValue;
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

        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1597976302),
                width: 0,
                name: "c:transactions/counter@none",
                value: Counter(
                    1.0,
                ),
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

        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1597976302),
                width: 0,
                name: "d:transactions/duration@none",
                value: Distribution(
                    [
                        2000.0,
                    ],
                ),
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

        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1597976302),
                width: 0,
                name: "s:transactions/users@none",
                value: Set(
                    {
                        943162418,
                    },
                ),
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

        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1597976302),
                width: 0,
                name: "c:transactions/counter@none",
                value: Counter(
                    1.0,
                ),
                tags: {
                    "fast": "no",
                    "id": "4711",
                    "release": "myapp@1.0.0",
                },
            },
        ]
        "###);
    }

    #[test]
    fn extract_tag_precedence() {
        let event_json = json!({
            "type": "transaction",
            "start_timestamp": 1597976300.0,
            "timestamp": 1597976302.0,
            "release": "myapp@1.0.0",
        });
        let event = Event::from_value(event_json.into());

        // NOTE: The first condition should match and therefore the second tag should be skipped.

        let config_json = json!({
            "version": 1,
            "metrics": [
                {
                    "category": "transaction",
                    "mri": "c:transactions/counter@none",
                    "tags": [
                        {
                            "key": "fast",
                            "value": "yes",
                            "condition": {"op": "lte", "name": "event.duration", "value": 2000},
                        },
                        {
                            "key": "fast",
                            "value": "no",
                        },
                    ]
                }
            ]
        });
        let config = serde_json::from_value(config_json).unwrap();

        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1597976302),
                width: 0,
                name: "c:transactions/counter@none",
                value: Counter(
                    1.0,
                ),
                tags: {
                    "fast": "yes",
                },
            },
        ]
        "###);
    }

    #[test]
    fn extract_tag_precedence_multiple_rules() {
        let event_json = json!({
            "type": "transaction",
            "start_timestamp": 1597976300.0,
            "timestamp": 1597976302.0,
            "release": "myapp@1.0.0",
        });
        let event = Event::from_value(event_json.into());

        // NOTE: The first tagging condition should match and the second one should be skipped.

        let config_json = json!({
            "version": 1,
            "metrics": [{
                "category": "transaction",
                "mri": "c:transactions/counter@none",
            }],
            "tags": [
                {
                    "metrics": ["c:transactions/counter@none"],
                    "tags": [{
                        "key": "fast",
                        "value": "yes",
                        "condition": {"op": "lte", "name": "event.duration", "value": 2000},
                    }],
                },
                {
                    "metrics": ["c:transactions/counter@none"],
                    "tags": [{
                        "key": "fast",
                        "value": "no",
                    }]
                },
            ]
        });
        let config = serde_json::from_value(config_json).unwrap();

        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1597976302),
                width: 0,
                name: "c:transactions/counter@none",
                value: Counter(
                    1.0,
                ),
                tags: {
                    "fast": "yes",
                },
            },
        ]
        "###);
    }
}
