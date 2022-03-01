use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "processing")]
use {
    relay_common::UnixTimestamp,
    relay_general::protocol::{AsPair, Event, EventType},
    relay_general::store::{get_breakdown_measurements, normalize_dist, BreakdownsConfig},
    relay_metrics::{Metric, MetricUnit, MetricValue},
    std::fmt,
    std::fmt::Write,
};

/// The metric on which the user satisfaction threshold is applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SatisfactionMetric {
    Duration,
    Lcp,
}

/// Configuration for a single threshold.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SatisfactionThreshold {
    metric: SatisfactionMetric,
    threshold: f64,
}

/// Configuration for applying the user satisfaction threshold.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SatisfactionConfig {
    /// The project-wide threshold to apply.
    project_threshold: SatisfactionThreshold,
    /// Transaction-specific overrides of the project-wide threshold.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    transaction_thresholds: BTreeMap<String, SatisfactionThreshold>,
}

/// Configuration for extracting metrics from transaction payloads.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TransactionMetricsConfig {
    extract_metrics: BTreeSet<String>,
    extract_custom_tags: BTreeSet<String>,
    satisfaction_thresholds: Option<SatisfactionConfig>,
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

/// Satisfaction value used for Apdex and User Misery
/// https://docs.sentry.io/product/performance/metrics/#apdex
#[cfg(feature = "processing")]
enum UserSatisfaction {
    Satisfied,
    Tolerated,
    Frustrated,
}

#[cfg(feature = "processing")]
impl fmt::Display for UserSatisfaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserSatisfaction::Satisfied => write!(f, "satisfied"),
            UserSatisfaction::Tolerated => write!(f, "tolerated"),
            UserSatisfaction::Frustrated => write!(f, "frustrated"),
        }
    }
}

#[cfg(feature = "processing")]
impl UserSatisfaction {
    /// The frustration threshold is always four times the threshold
    /// (see https://docs.sentry.io/product/performance/metrics/#apdex)
    const FRUSTRATION_FACTOR: f64 = 4.0;

    fn from_value(value: f64, threshold: f64) -> Self {
        if value < threshold {
            Self::Satisfied
        } else if value < Self::FRUSTRATION_FACTOR * threshold {
            Self::Tolerated
        } else {
            Self::Frustrated
        }
    }
}

/// Get duration from timestamp and start_timestamp.
#[cfg(feature = "processing")]
fn get_duration_millis(transaction: &Event) -> Option<f64> {
    let start = transaction.start_timestamp.value();
    let end = transaction.timestamp.value();
    match (start, end) {
        (Some(start), Some(end)) => {
            let start = start.timestamp_millis();
            let end = end.timestamp_millis();
            Some(end.saturating_sub(start) as f64)
        }
        _ => None,
    }
}

/// Get the value for a measurement, e.g. lcp -> event.measurements.lcp
#[cfg(feature = "processing")]
fn get_measurement(transaction: &Event, name: &str) -> Option<f64> {
    if let Some(measurements) = transaction.measurements.value() {
        for (measurement_name, annotated) in measurements.iter() {
            if measurement_name == name {
                if let Some(value) = annotated.value().and_then(|m| m.value.value()) {
                    return Some(*value);
                }
            }
        }
    }
    None
}

/// Extract the the satisfaction value depending on the actual measurement/duration value
/// and the configured threshold.
#[cfg(feature = "processing")]
fn extract_user_satisfaction(
    config: &Option<SatisfactionConfig>,
    transaction: &Event,
) -> Option<UserSatisfaction> {
    if let Some(config) = config {
        let threshold = transaction
            .transaction
            .value()
            .and_then(|name| config.transaction_thresholds.get(name))
            .unwrap_or(&config.project_threshold);
        if let Some(value) = match threshold.metric {
            SatisfactionMetric::Duration => get_duration_millis(transaction),
            SatisfactionMetric::Lcp => get_measurement(transaction, "lcp"),
        } {
            return Some(UserSatisfaction::from_value(value, threshold.threshold));
        }
    }
    None
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

    let user_satisfaction = extract_user_satisfaction(&config.satisfaction_thresholds, event);
    let tags_with_satisfaction = match user_satisfaction {
        Some(satisfaction) => with_tag(&tags, "satisfaction", satisfaction),
        None => tags.clone(),
    };

    // Duration
    let duration_millis = get_duration_millis(event).unwrap_or(0.0);

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
            None => tags_with_satisfaction.clone(),
        },
    });

    // User
    if let Some(user) = event.user.value() {
        if let Some(user_id) = user.id.as_str() {
            // TODO: If we have a transaction duration of 0 or no transaction at all, does the user count as satisfied?
            push_metric(Metric {
                name: metric_name(&["user"]),
                unit: MetricUnit::None,
                value: MetricValue::set_from_str(user_id),
                timestamp,
                // A single user might end up in multiple satisfaction buckets when they have
                // some satisfying transactions and some frustrating transactions.
                // This is OK as long as we do not add these numbers *after* aggregation:
                //     <WRONG>total_users = uniqIf(user, satisfied) + uniqIf(user, tolerated) + uniqIf(user, frustrated)</WRONG>
                //     <RIGHT>total_users = uniq(user)</RIGHT>
                tags: tags_with_satisfaction,
            });
        }
    }

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

    use relay_general::store::BreakdownsConfig;
    use relay_general::types::Annotated;
    use relay_metrics::DurationPrecision;

    #[test]
    fn test_extract_transaction_metrics() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "release": "1.2.3",
            "dist": "foo ",
            "environment": "fake_environment",
            "transaction": "mytransaction",
            "user": {
                "id": "user123"
            },
            "tags": {
                "fOO": "bar",
                "bogus": "absolutely"
            },
            "measurements": {
                "foo": {"value": 420.69},
                "lcp": {"value": 3000.0}
            },
            "spans": [
                {
                    "description": "<OrganizationContext>",
                    "op": "react.mount",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976393.4619668,
                    "timestamp": 1597976393.4718769,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                }
            ]
        }
        "#;

        let breakdowns_config: BreakdownsConfig = serde_json::from_str(
            r#"
            {
                "span_ops": {
                    "type": "spanOperations",
                    "matches": ["react.mount"]
                }
            }
        "#,
        )
        .unwrap();

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(
            &TransactionMetricsConfig::default(),
            Some(&breakdowns_config),
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
                "sentry.transactions.breakdowns.span_ops.ops.react.mount",
                "sentry.transactions.transaction.duration",
                "sentry.transactions.user"
            ],
            "extractCustomTags": ["fOO"]
        }
        "#,
        )
        .unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(
            &config,
            Some(&breakdowns_config),
            event.value().unwrap(),
            &mut metrics,
        );

        assert_eq!(metrics.len(), 5, "{:?}", metrics);

        assert_eq!(metrics[0].name, "sentry.transactions.measurements.foo");
        assert_eq!(metrics[1].name, "sentry.transactions.measurements.lcp");
        assert_eq!(
            metrics[2].name,
            "sentry.transactions.breakdowns.span_ops.ops.react.mount"
        );

        let duration_metric = &metrics[3];
        assert_eq!(
            duration_metric.name,
            "sentry.transactions.transaction.duration"
        );
        if let MetricValue::Distribution(value) = duration_metric.value {
            assert_eq!(value, 59000.0);
        } else {
            panic!(); // Duration must be set
        }

        let user_metric = &metrics[4];
        assert_eq!(user_metric.name, "sentry.transactions.user");
        assert!(matches!(user_metric.value, MetricValue::Set(_)));

        assert_eq!(metrics[1].tags["measurement_rating"], "meh");

        for metric in &metrics[0..4] {
            assert!(matches!(metric.value, MetricValue::Distribution(_)));
        }

        for metric in metrics {
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

    #[test]
    fn test_user_satisfaction() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:01+0100",
            "user": {
                "id": "user123"
            }
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "sentry.transactions.transaction.duration",
                "sentry.transactions.user"
            ],
            "satisfactionThresholds": {
                "projectThreshold": {
                    "metric": "duration",
                    "threshold": 300
                }
            }
        }
        "#,
        )
        .unwrap();
        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, event.value().unwrap(), &mut metrics);
        assert_eq!(metrics.len(), 2);

        for metric in metrics {
            assert_eq!(metric.tags.len(), 2);
            assert_eq!(metric.tags["satisfaction"], "tolerated");
        }
    }

    #[test]
    fn test_user_satisfaction_override() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:02+0100",
            "measurements": {
                "lcp": {"value": 41}
            }
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "sentry.transactions.transaction.duration"
            ],
            "satisfactionThresholds": {
                "projectThreshold": {
                    "metric": "duration",
                    "threshold": 300
                },
                "transactionThresholds": {
                    "foo": {
                        "metric": "lcp",
                        "threshold": 42
                    }
                }
            }
        }
        "#,
        )
        .unwrap();
        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, event.value().unwrap(), &mut metrics);
        assert_eq!(metrics.len(), 1);

        for metric in metrics {
            assert_eq!(metric.tags.len(), 2);
            assert_eq!(metric.tags["satisfaction"], "satisfied");
        }
    }
}
