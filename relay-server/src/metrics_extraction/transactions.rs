use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[cfg(feature = "processing")]
use {
    relay_common::UnixTimestamp,
    relay_general::protocol::{AsPair, Event, EventType},
    relay_general::store::{get_breakdown_measurements, normalize_dist, BreakdownsConfig},
    relay_metrics::{Metric, MetricUnit, MetricValue},
    std::collections::BTreeMap,
    std::fmt::Write,
};

/// Configuration in relation to extracting metrics from transaction events.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TransactionMetricsConfig {
    extract_metrics: BTreeSet<String>,
    extract_custom_tags: BTreeSet<String>,
}

#[cfg(feature = "processing")]
const METRIC_NAME_PREFIX: &str = "sentry.transactions";

/// Generate a transaction-related metric name
#[cfg(feature = "processing")]
fn metric_name(parts: &[&str]) -> String {
    let mut name = METRIC_NAME_PREFIX.to_owned();
    for part in parts {
        // Unwrapping here should be fine:
        // https://github.com/rust-lang/rust/blob/1.57.0/library/alloc/src/string.rs#L2721-L2724
        write!(name, ".{}", part).unwrap();
    }
    name
}

#[cfg(feature = "processing")]
fn extract_transaction_status(transaction: &Event) -> Option<String> {
    use relay_general::{
        protocol::{Context, ContextInner},
        types::Annotated,
    };

    let contexts = transaction.contexts.value()?;
    let trace_context = match contexts.get("trace").map(Annotated::value) {
        Some(Some(ContextInner(Context::Trace(trace_context)))) => trace_context,
        _ => return None,
    };
    let span_status = trace_context.status.value()?;
    Some(span_status.to_string())
}

#[cfg(feature = "processing")]
fn extract_dist(transaction: &Event) -> Option<String> {
    let mut dist = transaction.dist.0.clone();
    normalize_dist(&mut dist);
    dist
}

#[cfg(feature = "processing")]
pub fn extract_transaction_metrics(
    config: &TransactionMetricsConfig,
    breakdowns_config: Option<&BreakdownsConfig>,
    event: &Event,
    target: &mut Vec<Metric>,
) -> bool {
    use relay_metrics::DurationPrecision;

    use crate::metrics_extraction::utils::with_tag;

    if event.ty.value() != Some(&EventType::Transaction) {
        return false;
    }

    if config.extract_metrics.is_empty() {
        relay_log::trace!("dropping all transaction metrics because of empty allow-list");
        return false;
    }

    let mut push_metric = move |metric: Metric| {
        if config.extract_metrics.contains(&metric.name) {
            target.push(metric);
        } else {
            relay_log::trace!("dropping metric {} because of allow-list", metric.name);
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
        None => return false,
    };

    let mut tags = BTreeMap::new();
    if let Some(release) = event.release.as_str() {
        tags.insert("release".to_owned(), release.to_owned());
    }
    if let Some(dist) = extract_dist(event) {
        tags.insert("dist".to_owned(), dist);
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

    // Measurements
    if let Some(measurements) = event.measurements.value() {
        for (measurement_name, annotated) in measurements.iter() {
            let measurement = match annotated.value().and_then(|m| m.value.value()) {
                Some(measurement) => *measurement,
                None => continue,
            };

            let name = metric_name(&["measurements", measurement_name]);
            let mut tags = tags.clone();
            if let Some(rating) = get_measurement_rating(measurement_name, measurement) {
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

    // Breakdowns
    if let Some(breakdowns_config) = breakdowns_config {
        for (breakdown_name, measurements) in get_breakdown_measurements(event, breakdowns_config) {
            for (measurement_name, annotated) in measurements.iter() {
                let measurement = match annotated.value().and_then(|m| m.value.value()) {
                    Some(measurement) => *measurement,
                    None => continue,
                };

                push_metric(Metric {
                    name: metric_name(&["breakdowns", breakdown_name, measurement_name]),
                    unit: MetricUnit::None,
                    value: MetricValue::Distribution(measurement),
                    timestamp,
                    tags: tags.clone(),
                });
            }
        }
    }

    // Duration
    let start = event.start_timestamp.value();
    let end = event.timestamp.value();
    let duration_millis = match (start, end) {
        (Some(start), Some(end)) => {
            let start = start.timestamp_millis();
            let end = end.timestamp_millis();
            end.saturating_sub(start)
        }
        _ => 0,
    };

    // We always push the duration even if it's 0, because we use count(transaction.duration)
    // to get the total number of transactions.
    // This may need to be changed if it turns out that this skews the duration metric.
    push_metric(Metric {
        name: metric_name(&["transaction.duration"]),
        unit: MetricUnit::Duration(DurationPrecision::MilliSecond),
        value: MetricValue::Distribution(duration_millis as f64),
        timestamp,
        tags: match extract_transaction_status(event) {
            Some(status) => with_tag(&tags, "transaction.status", status),
            None => tags.clone(),
        },
    });

    true
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
        "lcp" => rate_range(2500.0, 4000.0),
        "fcp" => rate_range(1000.0, 3000.0),
        "fid" => rate_range(100.0, 300.0),
        "cls" => rate_range(0.1, 0.25),
        _ => None,
    }
}

#[cfg(test)]
#[cfg(feature = "processing")]
mod tests {
    use super::*;
    use relay_general::types::Annotated;
    use relay_metrics::DurationPrecision;

    #[test]
    fn test_extract_transaction_metrics() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "release": "1.2.3",
            "dist": "foo ",
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
            None,
            event.value().unwrap(),
            &mut metrics,
        );
        assert_eq!(metrics, &[]);

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "sentry.transactions.measurements.foo",
                "sentry.transactions.measurements.lcp",
                "sentry.transactions.breakdowns.breakdown1.bar",
                "sentry.transactions.breakdowns.breakdown2.baz",
                "sentry.transactions.breakdowns.breakdown2.zap",
                "sentry.transactions.transaction.duration"
            ],
            "extractCustomTags": ["fOO"]
        }
        "#,
        )
        .unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 6);

        assert_eq!(metrics[0].name, "sentry.transactions.measurements.foo");
        assert_eq!(metrics[1].name, "sentry.transactions.measurements.lcp");
        assert_eq!(
            metrics[2].name,
            "sentry.transactions.breakdowns.breakdown1.bar"
        );
        assert_eq!(
            metrics[3].name,
            "sentry.transactions.breakdowns.breakdown2.baz"
        );
        assert_eq!(
            metrics[4].name,
            "sentry.transactions.breakdowns.breakdown2.zap"
        );

        let duration_metric = &metrics[5];
        assert_eq!(
            duration_metric.name,
            "sentry.transactions.transaction.duration"
        );
        if let MetricValue::Distribution(value) = duration_metric.value {
            assert_eq!(value, 0.0);
        } else {
            panic!(); // Duration must be set
        }

        assert_eq!(metrics[1].tags["measurement_rating"], "meh");

        for metric in metrics {
            assert!(matches!(metric.value, MetricValue::Distribution(_)));
            assert_eq!(metric.tags["release"], "1.2.3");
            assert_eq!(metric.tags["dist"], "foo");
            assert_eq!(metric.tags["environment"], "fake_environment");
            assert_eq!(metric.tags["transaction"], "mytransaction");
            assert_eq!(metric.tags["fOO"], "bar");
            assert!(!metric.tags.contains_key("bogus"));
        }
    }

    #[test]
    fn test_transaction_duration() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "release": "1.2.3",
            "environment": "fake_environment",
            "transaction": "mytransaction",
            "contexts": {
                "trace": {
                    "status": "ok"
                }
            }
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "sentry.transactions.transaction.duration"
            ]
        }
        "#,
        )
        .unwrap();
        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 1);

        let duration_metric = &metrics[0];
        assert_eq!(
            duration_metric.name,
            "sentry.transactions.transaction.duration"
        );
        assert_eq!(
            duration_metric.unit,
            MetricUnit::Duration(DurationPrecision::MilliSecond)
        );
        if let MetricValue::Distribution(value) = duration_metric.value {
            assert_eq!(value, 59000.0); // millis
        } else {
            panic!(); // Duration must be set
        }

        assert_eq!(duration_metric.tags.len(), 4);
        assert_eq!(duration_metric.tags["release"], "1.2.3");
        assert_eq!(duration_metric.tags["transaction.status"], "ok");
        assert_eq!(duration_metric.tags["environment"], "fake_environment");
        assert_eq!(duration_metric.tags["transaction"], "mytransaction");
    }
}
