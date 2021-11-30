use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[cfg(feature = "processing")]
use {
    relay_common::UnixTimestamp,
    relay_general::protocol::{AsPair, Event, EventType},
    relay_metrics::{Metric, MetricUnit, MetricValue},
    std::collections::BTreeMap,
};

/// Configuration in relation to extracting metrics from transaction events.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TransactionMetricsConfig {
    extract_metrics: BTreeSet<String>,
    extract_custom_tags: BTreeSet<String>,
}

#[cfg(feature = "processing")]
pub fn extract_transaction_metrics(
    config: &TransactionMetricsConfig,
    event: &Event,
    target: &mut Vec<Metric>,
) {
    if event.ty.value() != Some(&EventType::Transaction) {
        return;
    }

    if config.extract_metrics.is_empty() {
        return;
    }

    let mut push_metric = move |metric: Metric| {
        if config.extract_metrics.contains(&metric.name) {
            target.push(metric);
        }
    };

    // Every metric push should go through push_metric, so let's shadow the identifier
    #[allow(unused_variables)]
    let target = ();

    let timestamp = match event
        .timestamp
        .value()
        .and_then(|ts| UnixTimestamp::from_datetime(ts.into_inner()))
    {
        Some(ts) => ts,
        None => return,
    };

    let mut tags = BTreeMap::new();
    if let Some(release) = event.release.as_str() {
        tags.insert("release".to_owned(), release.to_owned());
    }
    if let Some(environment) = event.environment.as_str() {
        tags.insert("environment".to_owned(), environment.to_owned());
    }
    if let Some(transaction) = event.transaction.as_str() {
        tags.insert("transaction".to_owned(), transaction.to_owned());
    }

    if !config.extract_custom_tags.is_empty() {
        // XXX(slow): event tags are a flat array
        if let Some(event_tags) = event.tags.value() {
            for tag_entry in &**event_tags {
                if let Some(entry) = tag_entry.value() {
                    let (key, value) = entry.as_pair();
                    if let (Some(key), Some(value)) = (key.as_str(), value.as_str()) {
                        if config.extract_custom_tags.contains(key) {
                            tags.insert(key.to_owned(), value.to_owned());
                        }
                    }
                }
            }
        }
    }

    if let Some(measurements) = event.measurements.value() {
        for (name, annotated) in measurements.iter() {
            let measurement = match annotated.value().and_then(|m| m.value.value()) {
                Some(measurement) => *measurement,
                None => continue,
            };

            let name = format!("measurements.{}", name);
            let mut tags = tags.clone();
            if let Some(rating) = get_measurement_rating(&name, measurement) {
                tags.insert("measurement_rating".to_owned(), rating);
            }

            push_metric(Metric {
                name,
                unit: MetricUnit::None,
                value: MetricValue::Distribution(measurement),
                timestamp,
                tags,
            });
        }
    }

    if let Some(breakdowns) = event.breakdowns.value() {
        for (breakdown, annotated) in breakdowns.iter() {
            let measurements = match annotated.value() {
                Some(measurements) => measurements,
                None => continue,
            };

            for (name, annotated) in measurements.iter() {
                let measurement = match annotated.value().and_then(|m| m.value.value()) {
                    Some(measurement) => *measurement,
                    None => continue,
                };

                push_metric(Metric {
                    name: format!("breakdown.{}.{}", breakdown, name),
                    unit: MetricUnit::None,
                    value: MetricValue::Distribution(measurement),
                    timestamp,
                    tags: tags.clone(),
                });
            }
        }
    }
}

#[cfg(feature = "processing")]
fn get_measurement_rating(name: &str, value: f64) -> Option<String> {
    let rate_range = |meh_ceiling: f64, poor_ceiling: f64| {
        debug_assert!(meh_ceiling < poor_ceiling);
        if value < meh_ceiling {
            Some("good".to_owned())
        } else if value < poor_ceiling {
            Some("meh".to_owned())
        } else {
            Some("poor".to_owned())
        }
    };

    match name {
        "measurements.lcp" => rate_range(2500.0, 4000.0),
        "measurements.fcp" => rate_range(1000.0, 3000.0),
        "measurements.fid" => rate_range(100.0, 300.0),
        "measurements.cls" => rate_range(0.1, 0.25),
        _ => None,
    }
}

#[cfg(test)]
#[cfg(feature = "processing")]
mod tests {
    use super::*;
    use relay_general::types::Annotated;

    #[test]
    fn test_extract_transaction_metrics() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "release": "1.2.3",
            "environment": "fake_environment",
            "transaction": "mytransaction",
            "tags": {
                "fOO": "bar",
                "bogus": "absolutely"
            },
            "measurements": {
                "foo": {"value": 420.69},
                "lcp": {"value": 3000.0}
            },
            "breakdowns": {
                "breakdown1": {
                    "bar": {"value": 123.4}
                },
                "breakdown2": {
                    "baz": {"value": 123.4},
                    "zap": {"value": 666},
                    "zippityzoppity": {"value": 666}
                }
            }
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(
            &TransactionMetricsConfig::default(),
            event.value().unwrap(),
            &mut metrics,
        );
        assert_eq!(metrics, &[]);

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "measurements.foo",
                "measurements.lcp",
                "breakdown.breakdown1.bar",
                "breakdown.breakdown2.baz",
                "breakdown.breakdown2.zap"
            ],
            "extractCustomTags": ["fOO"]
        }
        "#,
        )
        .unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(&config, event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 5);

        assert_eq!(metrics[0].name, "measurements.foo");
        assert_eq!(metrics[1].name, "measurements.lcp");
        assert_eq!(metrics[2].name, "breakdown.breakdown1.bar");
        assert_eq!(metrics[3].name, "breakdown.breakdown2.baz");
        assert_eq!(metrics[4].name, "breakdown.breakdown2.zap");

        assert_eq!(metrics[1].tags["measurement_rating"], "meh");

        for metric in metrics {
            assert!(matches!(metric.value, MetricValue::Distribution(_)));
            assert_eq!(metric.tags["release"], "1.2.3");
            assert_eq!(metric.tags["environment"], "fake_environment");
            assert_eq!(metric.tags["transaction"], "mytransaction");
            assert_eq!(metric.tags["fOO"], "bar");
            assert!(!metric.tags.contains_key("bogus"));
        }
    }
}
