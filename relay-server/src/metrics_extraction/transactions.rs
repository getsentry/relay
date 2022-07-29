use crate::metrics_extraction::conditional_tagging::run_conditional_tagging;
use crate::metrics_extraction::utils;
use crate::metrics_extraction::TaggingRule;
use crate::statsd::RelayCounters;
use relay_common::{SpanStatus, UnixTimestamp};
use relay_general::protocol::TraceContext;
use relay_general::protocol::{AsPair, Timestamp, TransactionSource};
use relay_general::protocol::{Context, ContextInner};
use relay_general::protocol::{Event, EventType};
use relay_general::store;
use relay_general::types::Annotated;
use relay_metrics::{DurationUnit, Metric, MetricNamespace, MetricUnit, MetricValue};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// The metric on which the user satisfaction threshold is applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SatisfactionMetric {
    Duration,
    Lcp,
    #[serde(other)]
    Unknown,
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

/// Configuration for extracting custom measurements from transaction payloads.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CustomMeasurementConfig {
    /// The maximum number of custom measurements to extract. Defaults to zero.
    limit: usize,
}

/// Maximum supported version of metrics extraction from transactions.
///
/// The version is an integer scalar, incremented by one on each new version.
const EXTRACT_MAX_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum AcceptTransactionNames {
    /// For some SDKs, accept all transaction names, while for others, apply strict rules.
    ClientBased,

    /// Only accept transaction names with a low-cardinality source.
    /// Any value other than "clientBased" will be interpreted as "strict".
    #[serde(other)]
    Strict,
}

impl Default for AcceptTransactionNames {
    fn default() -> Self {
        Self::Strict
    }
}

/// Configuration for extracting metrics from transaction payloads.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TransactionMetricsConfig {
    /// The required version to extract transaction metrics.
    version: u16,
    extract_metrics: BTreeSet<String>,
    extract_custom_tags: BTreeSet<String>,
    satisfaction_thresholds: Option<SatisfactionConfig>,
    custom_measurements: CustomMeasurementConfig,
    accept_transaction_names: AcceptTransactionNames,
}

impl TransactionMetricsConfig {
    pub fn is_enabled(&self) -> bool {
        self.version > 0 && self.version <= EXTRACT_MAX_VERSION
    }
}

const METRIC_NAMESPACE: MetricNamespace = MetricNamespace::Transactions;

fn get_trace_context(event: &Event) -> Option<&TraceContext> {
    let contexts = event.contexts.value()?;
    let trace = contexts.get("trace").map(Annotated::value);
    if let Some(Some(ContextInner(Context::Trace(trace_context)))) = trace {
        return Some(trace_context.as_ref());
    }

    None
}

/// Extract transaction status, defaulting to [`SpanStatus::Unknown`].
/// Must be consistent with `process_trace_context` in [`relay_general::store`].
fn extract_transaction_status(trace_context: &TraceContext) -> SpanStatus {
    *trace_context.status.value().unwrap_or(&SpanStatus::Unknown)
}

fn extract_transaction_op(trace_context: &TraceContext) -> Option<String> {
    let op = trace_context.op.value()?;
    Some(op.to_string())
}

fn extract_dist(transaction: &Event) -> Option<String> {
    let mut dist = transaction.dist.0.clone();
    store::normalize_dist(&mut dist);
    dist
}

/// Extract HTTP method
/// See <https://github.com/getsentry/snuba/blob/2e038c13a50735d58cc9397a29155ab5422a62e5/snuba/datasets/errors_processor.py#L64-L67>.
fn extract_http_method(transaction: &Event) -> Option<String> {
    let request = transaction.request.value()?;
    let method = request.method.value()?;
    Some(method.to_owned())
}

/// Satisfaction value used for Apdex and User Misery
/// <https://docs.sentry.io/product/performance/metrics/#apdex>
enum UserSatisfaction {
    Satisfied,
    Tolerated,
    Frustrated,
}

impl fmt::Display for UserSatisfaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserSatisfaction::Satisfied => write!(f, "satisfied"),
            UserSatisfaction::Tolerated => write!(f, "tolerated"),
            UserSatisfaction::Frustrated => write!(f, "frustrated"),
        }
    }
}

impl UserSatisfaction {
    /// The frustration threshold is always four times the threshold
    /// (see <https://docs.sentry.io/product/performance/metrics/#apdex>)
    const FRUSTRATION_FACTOR: f64 = 4.0;

    fn from_value(value: f64, threshold: f64) -> Self {
        if value <= threshold {
            Self::Satisfied
        } else if value <= Self::FRUSTRATION_FACTOR * threshold {
            Self::Tolerated
        } else {
            Self::Frustrated
        }
    }
}

/// Extract the the satisfaction value depending on the actual measurement/duration value
/// and the configured threshold.
fn extract_user_satisfaction(
    config: &Option<SatisfactionConfig>,
    transaction: &Event,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
) -> Option<UserSatisfaction> {
    if let Some(config) = config {
        let threshold = transaction
            .transaction
            .value()
            .and_then(|name| config.transaction_thresholds.get(name))
            .unwrap_or(&config.project_threshold);
        if let Some(value) = match threshold.metric {
            SatisfactionMetric::Duration => Some(relay_common::chrono_to_positive_millis(
                end_timestamp - start_timestamp,
            )),
            SatisfactionMetric::Lcp => store::get_measurement(transaction, "lcp"),
            SatisfactionMetric::Unknown => None,
        } {
            return Some(UserSatisfaction::from_value(value, threshold.threshold));
        }
    }
    None
}

fn is_low_cardinality(source: &TransactionSource, treat_unknown_as_low_cardinality: bool) -> bool {
    match source {
        // For now, we hope that custom transaction names set by users are low-cardinality.
        TransactionSource::Custom => true,

        // "url" are raw URLs, potentially containing identifiers.
        TransactionSource::Url => false,

        // These four are names of software components, which we assume to be low-cardinality.
        TransactionSource::Route => true,
        TransactionSource::View => true,
        TransactionSource::Component => true,
        TransactionSource::Task => true,

        // "unknown" is the value for old SDKs that do not send a transaction source yet.
        // Caller decides how to treat this.
        TransactionSource::Unknown => treat_unknown_as_low_cardinality,

        // Any other value would be an SDK bug, assume high-cardinality and drop.
        TransactionSource::Other(source) => {
            relay_log::error!("Invalid transaction source: '{}'", source);
            false
        }
    }
}

/// Decide whether we want to keep the transaction name.
/// High-cardinality sources are excluded to protect our metrics infrastructure.
/// Note that this will produce a discrepancy between metrics and raw transaction data.
fn get_transaction_name(
    event: &Event,
    accept_transaction_names: &AcceptTransactionNames,
) -> Option<String> {
    let original_transaction_name = match event.transaction.value() {
        Some(name) => name,
        None => {
            return None;
        }
    };

    // In client-based mode, handling of "unknown" sources depends on the SDK name.
    // In strict mode, treat "unknown" as high cardinality.
    let treat_unknown_as_low_cardinality = matches!(
        accept_transaction_names,
        AcceptTransactionNames::ClientBased
    ) && !store::is_high_cardinality_sdk(event);

    let source = event.get_transaction_source();
    let use_original_name = is_low_cardinality(source, treat_unknown_as_low_cardinality);

    let name_used;
    let name = if use_original_name {
        name_used = "original";
        Some(original_transaction_name.clone())
    } else {
        // Pick a sentinel based on the transaction source:
        match source {
            TransactionSource::Unknown | TransactionSource::Other(_) => {
                name_used = "none";
                None
            }
            _ => {
                name_used = "placeholder";
                Some("<< unparameterized >>".to_owned())
            }
        }
    };

    relay_statsd::metric!(
        counter(RelayCounters::MetricsTransactionNameExtracted) += 1,
        strategy = match &accept_transaction_names {
            AcceptTransactionNames::ClientBased => "client-based",
            AcceptTransactionNames::Strict => "strict",
        },
        source = &source.to_string(),
        sdk_name = event
            .client_sdk
            .value()
            .and_then(|c| c.name.value())
            .map(|s| s.as_str())
            .unwrap_or_default(),
        name_used = name_used,
    );

    name
}

/// These are the tags that are added to all extracted metrics.
fn extract_universal_tags(
    event: &Event,
    config: &TransactionMetricsConfig,
) -> BTreeMap<String, String> {
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
    if let Some(transaction_name) = get_transaction_name(event, &config.accept_transaction_names) {
        tags.insert("transaction".to_owned(), transaction_name);
    }

    // The platform tag should not increase dimensionality in most cases, because most
    // transactions are specific to one platform
    let platform = match event.platform.as_str() {
        Some(platform) if store::is_valid_platform(platform) => platform,
        _ => "other",
    };
    tags.insert("platform".to_owned(), platform.to_owned());

    if let Some(trace_context) = get_trace_context(event) {
        let status = extract_transaction_status(trace_context);
        tags.insert("transaction.status".to_owned(), status.to_string());

        if let Some(op) = extract_transaction_op(trace_context) {
            tags.insert("transaction.op".to_owned(), op);
        }
    }

    if let Some(http_method) = extract_http_method(event) {
        tags.insert("http.method".to_owned(), http_method);
    }

    let custom_tags = &config.extract_custom_tags;
    if !custom_tags.is_empty() {
        // XXX(slow): event tags are a flat array
        if let Some(event_tags) = event.tags.value() {
            for tag_entry in &**event_tags {
                if let Some(entry) = tag_entry.value() {
                    let (key, value) = entry.as_pair();
                    if let (Some(key), Some(value)) = (key.as_str(), value.as_str()) {
                        if custom_tags.contains(key) {
                            tags.insert(key.to_owned(), value.to_owned());
                        }
                    }
                }
            }
        }
    }

    tags
}

/// Returns the unit of the provided metric.
///
/// For known measurements, this returns `Some(MetricUnit)`, which can also include
/// `Some(MetricUnit::None)`. For unknown measurement names, this returns `None`.
fn get_metric_measurement_unit(metric: &str) -> Option<MetricUnit> {
    match metric {
        // Web
        "fcp" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "lcp" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "fid" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "fp" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "ttfb" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "ttfb.requesttime" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "cls" => Some(MetricUnit::None),

        // Mobile
        "app_start_cold" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "app_start_warm" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "frames_total" => Some(MetricUnit::None),
        "frames_slow" => Some(MetricUnit::None),
        "frames_frozen" => Some(MetricUnit::None),

        // React-Native
        "stall_count" => Some(MetricUnit::None),
        "stall_total_time" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "stall_longest_time" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),

        // Default
        _ => None,
    }
}

pub fn extract_transaction_metrics(
    config: &TransactionMetricsConfig,
    breakdowns_config: Option<&store::BreakdownsConfig>,
    conditional_tagging_config: &[TaggingRule],
    event: &Event,
    target: &mut Vec<Metric>,
) -> bool {
    if config.extract_metrics.is_empty() {
        relay_log::trace!("dropping all transaction metrics because of empty allow-list");
        return false;
    }

    let before_len = target.len();

    let mut custom_measurement_budget = config.custom_measurements.limit;
    let push_metric = |metric: Metric| {
        if config.extract_metrics.contains(&metric.name) {
            // Anything in config.extract_metrics is considered a known / builtin metric,
            // as opposed to custom measurements.
            target.push(metric);
        } else if custom_measurement_budget > 0
            && metric.name.starts_with("d:transactions/measurements.")
        {
            // We allow a fixed amount of custom measurements in addition to
            // the known / builtin metrics.
            target.push(metric);
            custom_measurement_budget -= 1;
        } else {
            relay_log::trace!("dropping metric {} because of allow-list", metric.name);
        }
    };

    extract_transaction_metrics_inner(config, breakdowns_config, event, push_metric);

    let added_slice = &mut target[before_len..];
    run_conditional_tagging(event, conditional_tagging_config, added_slice);
    !added_slice.is_empty()
}

fn extract_transaction_metrics_inner(
    config: &TransactionMetricsConfig,
    breakdowns_config: Option<&store::BreakdownsConfig>,
    event: &Event,
    mut push_metric: impl FnMut(Metric),
) {
    if event.ty.value() != Some(&EventType::Transaction) {
        return;
    }

    let (start_timestamp, end_timestamp) = match store::validate_timestamps(event) {
        Ok(pair) => pair,
        Err(_) => {
            return; // invalid transaction
        }
    };

    let unix_timestamp = match UnixTimestamp::from_datetime(end_timestamp.into_inner()) {
        Some(ts) => ts,
        None => return,
    };

    let tags = extract_universal_tags(event, config);

    // Measurements
    if let Some(measurements) = event.measurements.value() {
        for (name, annotated) in measurements.iter() {
            let measurement = match annotated.value() {
                Some(m) => m,
                None => continue,
            };

            let value = match measurement.value.value() {
                Some(value) => *value,
                None => continue,
            };

            let mut tags_for_measurement = tags.clone();
            if let Some(rating) = get_measurement_rating(name, value) {
                tags_for_measurement.insert("measurement_rating".to_owned(), rating);
            }

            let stated_unit = measurement.unit.value().copied();
            let default_unit = get_metric_measurement_unit(name);
            if let (Some(default), Some(stated)) = (default_unit, stated_unit) {
                if default != stated {
                    relay_log::error!("unit mismatch on measurements.{}: {}", name, stated);
                }
            }

            push_metric(Metric::new_mri(
                METRIC_NAMESPACE,
                format!("measurements.{}", name),
                stated_unit.or(default_unit).unwrap_or_default(),
                MetricValue::Distribution(value),
                unix_timestamp,
                tags_for_measurement,
            ));
        }
    }

    // Breakdowns
    if let Some(breakdowns_config) = breakdowns_config {
        for (breakdown, measurements) in store::get_breakdown_measurements(event, breakdowns_config)
        {
            for (measurement_name, annotated) in measurements.iter() {
                let measurement = match annotated.value() {
                    Some(m) => m,
                    None => continue,
                };

                let value = match measurement.value.value() {
                    Some(value) => *value,
                    None => continue,
                };

                let unit = measurement.unit.value();

                push_metric(Metric::new_mri(
                    METRIC_NAMESPACE,
                    format!("breakdowns.{}.{}", breakdown, measurement_name),
                    unit.copied().unwrap_or(MetricUnit::None),
                    MetricValue::Distribution(value),
                    unix_timestamp,
                    tags.clone(),
                ));
            }
        }
    }

    let user_satisfaction = extract_user_satisfaction(
        &config.satisfaction_thresholds,
        event,
        start_timestamp,
        end_timestamp,
    );
    let tags_with_satisfaction = match user_satisfaction {
        Some(satisfaction) => utils::with_tag(&tags, "satisfaction", satisfaction),
        None => tags,
    };

    // Duration
    let duration_millis = relay_common::chrono_to_positive_millis(end_timestamp - start_timestamp);

    push_metric(Metric::new_mri(
        METRIC_NAMESPACE,
        "duration",
        MetricUnit::Duration(DurationUnit::MilliSecond),
        MetricValue::Distribution(duration_millis),
        unix_timestamp,
        tags_with_satisfaction.clone(),
    ));

    // User
    if let Some(user) = event.user.value() {
        if let Some(user_id) = user.id.as_str() {
            push_metric(Metric::new_mri(
                METRIC_NAMESPACE,
                "user",
                MetricUnit::None,
                MetricValue::set_from_str(user_id),
                unix_timestamp,
                // A single user might end up in multiple satisfaction buckets when they have
                // some satisfying transactions and some frustrating transactions.
                // This is OK as long as we do not add these numbers *after* aggregation:
                //     <WRONG>total_users = uniqIf(user, satisfied) + uniqIf(user, tolerated) + uniqIf(user, frustrated)</WRONG>
                //     <RIGHT>total_users = uniq(user)</RIGHT>
                tags_with_satisfaction,
            ));
        }
    }
}

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

    use crate::metrics_extraction::TaggingRule;
    use insta::assert_debug_snapshot;
    use relay_general::store::BreakdownsConfig;
    use relay_general::types::Annotated;
    use relay_metrics::DurationUnit;

    #[test]
    fn test_extract_transaction_metrics() {
        let json = r#"
        {
            "type": "transaction",
            "platform": "javascript",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "release": "1.2.3",
            "dist": "foo ",
            "environment": "fake_environment",
            "transaction": "mytransaction",
            "transaction_info": {"source": "custom"},
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
            "contexts": {
                "trace": {
                    "op": "myop",
                    "status": "ok"
                }
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
            ],
            "request": {
                "method": "POST"
            }
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
            &[],
            event.value().unwrap(),
            &mut metrics,
        );
        assert_eq!(metrics, &[]);

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "d:transactions/measurements.foo@none",
                "d:transactions/measurements.lcp@millisecond",
                "d:transactions/breakdowns.span_ops.ops.react.mount@millisecond",
                "d:transactions/duration@millisecond",
                "s:transactions/user@none"
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
            &[],
            event.value().unwrap(),
            &mut metrics,
        );

        assert_eq!(metrics.len(), 5, "{:?}", metrics);

        assert_eq!(metrics[0].name, "d:transactions/measurements.foo@none");
        assert_eq!(
            metrics[1].name,
            "d:transactions/measurements.lcp@millisecond"
        );
        assert_eq!(
            metrics[2].name,
            "d:transactions/breakdowns.span_ops.ops.react.mount@millisecond"
        );

        let duration_metric = &metrics[3];
        assert_eq!(duration_metric.name, "d:transactions/duration@millisecond");
        if let MetricValue::Distribution(value) = duration_metric.value {
            assert_eq!(value, 59000.0);
        } else {
            panic!(); // Duration must be set
        }

        let user_metric = &metrics[4];
        assert_eq!(user_metric.name, "s:transactions/user@none");
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
            assert_eq!(metric.tags["http.method"], "POST");
            assert_eq!(metric.tags["transaction.status"], "ok");
            assert_eq!(metric.tags["transaction.op"], "myop");
            assert_eq!(metric.tags["platform"], "javascript");
            assert!(!metric.tags.contains_key("bogus"));
        }
    }

    #[test]
    fn test_metric_measurement_units() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "measurements": {
                "fcp": {"value": 1.1},
                "stall_count": {"value": 3.3},
                "foo": {"value": 8.8}
            }
        }
        "#;

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "d:transactions/measurements.fcp@millisecond",
                "d:transactions/measurements.stall_count@none",
                "d:transactions/measurements.foo@none"
            ]
        }
        "#,
        )
        .unwrap();

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 3, "{:?}", metrics);

        assert_eq!(
            metrics[0].name, "d:transactions/measurements.fcp@millisecond",
            "{:?}",
            metrics[0]
        );

        assert_eq!(
            metrics[1].name, "d:transactions/measurements.foo@none",
            "{:?}",
            metrics[1]
        );

        assert_eq!(
            metrics[2].name, "d:transactions/measurements.stall_count@none",
            "{:?}",
            metrics[2]
        );
    }

    #[test]
    fn test_metric_measurement_unit_overrides() {
        let json = r#"{
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "measurements": {
                "fcp": {"value": 1.1, "unit": "second"},
                "lcp": {"value": 2.2, "unit": "none"}
            }
        }"#;

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"{
                "extractMetrics": [
                    "d:transactions/measurements.fcp@second",
                    "d:transactions/measurements.lcp@none"
                ]
            }"#,
        )
        .unwrap();

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 2);

        assert_eq!(metrics[0].name, "d:transactions/measurements.fcp@second");

        // None is an override, too.
        assert_eq!(metrics[1].name, "d:transactions/measurements.lcp@none");
    }

    #[test]
    fn test_transaction_duration() {
        let json = r#"
        {
            "type": "transaction",
            "platform": "bogus",
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
                "d:transactions/duration@millisecond"
            ]
        }
        "#,
        )
        .unwrap();
        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 1);

        let duration_metric = &metrics[0];
        assert_eq!(duration_metric.name, "d:transactions/duration@millisecond");
        if let MetricValue::Distribution(value) = duration_metric.value {
            assert_eq!(value, 59000.0); // millis
        } else {
            panic!(); // Duration must be set
        }

        assert_eq!(duration_metric.tags.len(), 4);
        assert_eq!(duration_metric.tags["release"], "1.2.3");
        assert_eq!(duration_metric.tags["transaction.status"], "ok");
        assert_eq!(duration_metric.tags["environment"], "fake_environment");
        assert_eq!(duration_metric.tags["platform"], "other");
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
            },
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
                "d:transactions/duration@millisecond",
                "s:transactions/user@none"
            ],
            "satisfactionThresholds": {
                "projectThreshold": {
                    "metric": "duration",
                    "threshold": 300
                },
                "extra_key": "should_be_ignored"
            }
        }
        "#,
        )
        .unwrap();
        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);
        assert_eq!(metrics.len(), 2);

        let duration_metric = &metrics[0];
        assert_eq!(duration_metric.tags.len(), 3);
        assert_eq!(duration_metric.tags["satisfaction"], "tolerated");
        assert_eq!(duration_metric.tags["transaction.status"], "ok");

        let user_metric = &metrics[1];
        assert_eq!(user_metric.tags.len(), 3);
        assert_eq!(user_metric.tags["satisfaction"], "tolerated");
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
                "d:transactions/duration@millisecond"
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
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);
        assert_eq!(metrics.len(), 1);

        for metric in metrics {
            assert_eq!(metric.tags.len(), 2);
            assert_eq!(metric.tags["satisfaction"], "satisfied");
        }
    }

    #[test]
    fn test_user_satisfaction_catch_new_metric() {
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
                "d:transactions/duration@millisecond"
            ],
            "satisfactionThresholds": {
                "projectThreshold": {
                    "metric": "unknown_metric",
                    "threshold": 300
                }
            }
        }
        "#,
        )
        .unwrap();
        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);
        assert_eq!(metrics.len(), 1);

        for metric in metrics {
            assert_eq!(metric.tags.len(), 1);
            assert!(!metric.tags.contains_key("satisfaction"));
        }
    }

    #[test]
    fn test_custom_measurements() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:02+0100",
            "measurements": {
                "a_custom1": {"value": 41},
                "fcp": {"value": 0.123},
                "g_custom2": {"value": 42, "unit": "second"},
                "h_custom3": {"value": 43}
            }
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "d:transactions/measurements.fcp@millisecond"
            ],
            "customMeasurements": {
                "limit": 2
            }
        }
        "#,
        )
        .unwrap();
        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        assert_debug_snapshot!(metrics, @r###"
            [
                Metric {
                    name: "d:transactions/measurements.a_custom1@none",
                    value: Distribution(
                        41.0,
                    ),
                    timestamp: UnixTimestamp(1619420402),
                    tags: {
                        "platform": "other",
                    },
                },
                Metric {
                    name: "d:transactions/measurements.fcp@millisecond",
                    value: Distribution(
                        0.123,
                    ),
                    timestamp: UnixTimestamp(1619420402),
                    tags: {
                        "measurement_rating": "good",
                        "platform": "other",
                    },
                },
                Metric {
                    name: "d:transactions/measurements.g_custom2@second",
                    value: Distribution(
                        42.0,
                    ),
                    timestamp: UnixTimestamp(1619420402),
                    tags: {
                        "platform": "other",
                    },
                },
            ]
        "###);
    }

    #[test]
    fn test_conditional_tagging() {
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
                "d:transactions/duration@millisecond"
            ]
        }
        "#,
        )
        .unwrap();

        let tagging_config: Vec<TaggingRule> = serde_json::from_str(
            r#"
        [
            {
                "condition": {"op": "gte", "name": "event.duration", "value": 9001},
                "targetMetrics": ["d:transactions/duration@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "frustrated"
            },
            {
                "condition": {"op": "gte", "name": "event.duration", "value": 666},
                "targetMetrics": ["d:transactions/duration@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "tolerated"
            },
            {
                "condition": {"op": "and", "inner": []},
                "targetMetrics": ["d:transactions/duration@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "satisfied"
            }
        ]
        "#,
        )
        .unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(
            &config,
            None,
            &tagging_config,
            event.value().unwrap(),
            &mut metrics,
        );
        assert_eq!(
            metrics,
            &[Metric::new_mri(
                METRIC_NAMESPACE,
                "duration",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(2000.0),
                UnixTimestamp::from_secs(1619420402),
                {
                    let mut tags = BTreeMap::new();
                    tags.insert("satisfaction".to_owned(), "tolerated".to_owned());
                    tags.insert("platform".to_owned(), "other".to_owned());
                    tags
                }
            )]
        );
    }

    #[test]
    fn test_conditional_tagging_lcp() {
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
                "d:transactions/measurements.lcp@millisecond"
            ]
        }
        "#,
        )
        .unwrap();

        let tagging_config: Vec<TaggingRule> = serde_json::from_str(
            r#"
        [
            {
                "condition": {"op": "gte", "name": "event.measurements.lcp.value", "value": 41},
                "targetMetrics": ["d:transactions/measurements.lcp@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "frustrated"
            },
            {
                "condition": {"op": "gte", "name": "event.measurements.lcp.value", "value": 20},
                "targetMetrics": ["d:transactions/measurements.lcp@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "tolerated"
            },
            {
                "condition": {"op": "and", "inner": []},
                "targetMetrics": ["d:transactions/measurements.lcp@millisecond"],
                "targetTag": "satisfaction",
                "tagValue": "satisfied"
            }
        ]
        "#,
        )
        .unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(
            &config,
            None,
            &tagging_config,
            event.value().unwrap(),
            &mut metrics,
        );
        assert_eq!(
            metrics,
            &[Metric::new_mri(
                METRIC_NAMESPACE,
                "measurements.lcp",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(41.0),
                UnixTimestamp::from_secs(1619420402),
                {
                    let mut tags = BTreeMap::new();
                    tags.insert("satisfaction".to_owned(), "frustrated".to_owned());
                    tags.insert("measurement_rating".to_owned(), "good".to_owned());
                    tags.insert("platform".to_owned(), "other".to_owned());
                    tags
                }
            )]
        );
    }

    #[test]
    fn test_unknown_transaction_status_no_trace_context() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100"
        }
        "#;

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "d:transactions/duration@millisecond"
            ]
        }
        "#,
        )
        .unwrap();

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 1, "{:?}", metrics);

        assert_eq!(metrics[0].name, "d:transactions/duration@millisecond");
        assert_eq!(
            metrics[0].tags,
            BTreeMap::from([("platform".to_string(), "other".to_string())])
        );
    }

    #[test]
    fn test_unknown_transaction_status() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}}
        }
        "#;

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "d:transactions/duration@millisecond"
            ]
        }
        "#,
        )
        .unwrap();

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 1, "{:?}", metrics);

        assert_eq!(metrics[0].name, "d:transactions/duration@millisecond");
        assert_eq!(
            metrics[0].tags,
            BTreeMap::from([
                ("transaction.status".to_string(), "unknown".to_string()),
                ("platform".to_string(), "other".to_string())
            ])
        );
    }

    /// Helper function to check if the transaction name is set correctly
    fn extract_transaction_name(json: &str, strategy: AcceptTransactionNames) -> Option<String> {
        let mut config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "d:transactions/duration@millisecond"
            ]
        }
        "#,
        )
        .unwrap();

        let event = Annotated::<Event>::from_json(json).unwrap();
        config.accept_transaction_names = strategy;

        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        assert_eq!(metrics.len(), 1);
        metrics[0].tags.get("transaction").cloned()
    }

    #[test]
    fn test_js_unknown_strict() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.browser"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::Strict);
        assert!(name.is_none());
    }

    #[test]
    fn test_js_unknown_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.browser"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert!(name.is_none());
    }

    #[test]
    fn test_js_url_strict() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.browser"},
            "transaction_info": {"source": "url"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::Strict);
        assert_eq!(name, Some("<< unparameterized >>".to_owned()));
    }

    #[test]
    fn test_js_url_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.browser"},
            "transaction_info": {"source": "url"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert_eq!(name, Some("<< unparameterized >>".to_owned()));
    }

    #[test]
    fn test_python_404_strict() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.python", "integrations":["django"]},
            "tags": {"http.status": "404"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::Strict);
        assert!(name.is_none());
    }

    #[test]
    fn test_python_404_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.python", "integrations":["django"]},
            "tags": {"http.status_code": "404"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert!(name.is_none());
    }

    #[test]
    fn test_python_200_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.python", "integrations":["django"]},
            "tags": {"http.status_code": "200"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert_eq!(name, Some("foo".to_owned()));
    }

    #[test]
    fn test_express_options_strict() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.node", "integrations":["Express"]},
            "tags": {"http.method": "OPTIONS"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::Strict);
        assert!(name.is_none());
    }

    #[test]
    fn test_express_options_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.node", "integrations":["Express"]},
            "tags": {"http.method": "OPTIONS"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert!(name.is_none());
    }

    #[test]
    fn test_express_get_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.node", "integrations":["Express"]},
            "tags": {"http.method": "GET"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert_eq!(name, Some("foo".to_owned()));
    }

    #[test]
    fn test_other_client_unknown_strict() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "some_client"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::Strict);
        assert!(name.is_none());
    }

    #[test]
    fn test_other_client_unknown_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "some_client"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert_eq!(name, Some("foo".to_owned()));
    }

    #[test]
    fn test_other_client_url_strict() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "some_client"},
            "transaction_info": {"source": "url"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::Strict);
        assert_eq!(name, Some("<< unparameterized >>".to_owned()));
    }

    #[test]
    fn test_other_client_url_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "some_client"},
            "transaction_info": {"source": "url"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert_eq!(name, Some("<< unparameterized >>".to_owned()));
    }

    #[test]
    fn test_any_client_route_strict() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "some_client"},
            "transaction_info": {"source": "route"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::Strict);
        assert_eq!(name, Some("foo".to_owned()));
    }

    #[test]
    fn test_any_client_route_client_based() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "some_client"},
            "transaction_info": {"source": "route"}
        }
        "#;

        let name = extract_transaction_name(json, AcceptTransactionNames::ClientBased);
        assert_eq!(name, Some("foo".to_owned()));
    }

    #[test]
    fn test_parse_transaction_name_strategy() {
        for (config, expected_strategy) in [
            (r#"{}"#, AcceptTransactionNames::Strict),
            (
                r#"{"acceptTransactionNames": "unknown-strategy"}"#,
                AcceptTransactionNames::Strict,
            ),
            (
                r#"{"acceptTransactionNames": "strict"}"#,
                AcceptTransactionNames::Strict,
            ),
            (
                r#"{"acceptTransactionNames": "clientBased"}"#,
                AcceptTransactionNames::ClientBased,
            ),
        ] {
            let config: TransactionMetricsConfig = serde_json::from_str(config).unwrap();
            assert_eq!(config.accept_transaction_names, expected_strategy);
        }
    }
}
