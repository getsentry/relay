use crate::metrics_extraction::conditional_tagging::run_conditional_tagging;
use crate::metrics_extraction::utils;
use crate::metrics_extraction::TaggingRule;
use crate::statsd::RelayCounters;
use relay_common::FractionUnit;
use relay_common::{SpanStatus, UnixTimestamp};
use relay_general::protocol::{
    AsPair, Context, ContextInner, Event, EventType, Timestamp, TraceContext, TransactionSource,
    User,
};
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
    extract_custom_tags: BTreeSet<String>,
    satisfaction_thresholds: Option<SatisfactionConfig>,
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
    let trace = contexts.get("trace").and_then(Annotated::value);
    if let Some(ContextInner(Context::Trace(trace_context))) = trace {
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
    if op == "default" {
        // This was likely set by normalization, so let's treat it as None
        // See https://github.com/getsentry/relay/blob/bb2ac4ee82c25faa07a6d078f93d22d799cfc5d1/relay-general/src/store/transactions.rs#L96

        // Note that this is the opposite behavior of what we do for transaction.status, where
        // we coalesce None to "unknown".
        return None;
    }
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
        "frames_slow_rate" => Some(MetricUnit::Fraction(FractionUnit::Ratio)),
        "frames_frozen" => Some(MetricUnit::None),
        "frames_frozen_rate" => Some(MetricUnit::Fraction(FractionUnit::Ratio)),

        // React-Native
        "stall_count" => Some(MetricUnit::None),
        "stall_total_time" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "stall_longest_time" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "stall_percentage" => Some(MetricUnit::Fraction(FractionUnit::Ratio)),

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
    let before_len = target.len();

    extract_transaction_metrics_inner(config, breakdowns_config, event, target);

    let added_slice = &mut target[before_len..];
    run_conditional_tagging(event, conditional_tagging_config, added_slice);
    !added_slice.is_empty()
}

fn extract_transaction_metrics_inner(
    config: &TransactionMetricsConfig,
    breakdowns_config: Option<&store::BreakdownsConfig>,
    event: &Event,
    metrics: &mut Vec<Metric>, // output parameter
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
    let duration_millis = relay_common::chrono_to_positive_millis(end_timestamp - start_timestamp);

    let unix_timestamp = match UnixTimestamp::from_datetime(end_timestamp.into_inner()) {
        Some(ts) => ts,
        None => return,
    };

    let tags = extract_universal_tags(event, config);

    // Measurements
    if let Some(measurements) = event.measurements.value() {
        let mut measurements = measurements.clone();
        store::compute_measurements(duration_millis, &mut measurements);
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

            metrics.push(Metric::new_mri(
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

                metrics.push(Metric::new_mri(
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
    metrics.push(Metric::new_mri(
        METRIC_NAMESPACE,
        "duration",
        MetricUnit::Duration(DurationUnit::MilliSecond),
        MetricValue::Distribution(duration_millis),
        unix_timestamp,
        tags_with_satisfaction.clone(),
    ));

    // User
    if let Some(user) = event.user.value() {
        if let Some(value) = get_eventuser_tag(user) {
            metrics.push(Metric::new_mri(
                METRIC_NAMESPACE,
                "user",
                MetricUnit::None,
                MetricValue::set_from_str(&value),
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

/// Compute the transaction event's "user" tag as close as possible to how users are determined in
/// the transactions dataset in Snuba. This should produce the exact same user counts as the `user`
/// column in Discover for Transactions, barring:
///
/// * imprecision caused by HLL sketching in Snuba, which we don't have in events
/// * hash collisions in [`MetricValue::set_from_display`], which we don't have in events
/// * MD5-collisions caused by `EventUser.hash_from_tag`, which we don't have in metrics
///
///   MD5 is used to efficiently look up the current event user for an event, and if there is a
///   collision it seems that this code will fetch an event user with potentially different values
///   for everything that is in `defaults`:
///   <https://github.com/getsentry/sentry/blob/f621cd76da3a39836f34802ba9b35133bdfbe38b/src/sentry/event_manager.py#L1058-L1060>
///
/// The performance product runs a discover query such as `count_unique(user)`, which maps to two
/// things:
///
/// * `user` metric for the metrics dataset
/// * the "promoted tag" column `user` in the transactions clickhouse table
///
/// A promoted tag is a tag that snuba pulls out into its own column. In this case it pulls out the
/// `sentry:user` tag from the event payload:
/// <https://github.com/getsentry/snuba/blob/430763e67e30957c89126e62127e34051eb52fd6/snuba/datasets/transactions_processor.py#L151>
///
/// Sentry's processing pipeline defers to `sentry.models.EventUser` to produce the `sentry:user` tag
/// here: <https://github.com/getsentry/sentry/blob/f621cd76da3a39836f34802ba9b35133bdfbe38b/src/sentry/event_manager.py#L790-L794>
///
/// `sentry.models.eventuser.KEYWORD_MAP` determines which attributes are looked up in which order, here:
/// <https://github.com/getsentry/sentry/blob/f621cd76da3a39836f34802ba9b35133bdfbe38b/src/sentry/models/eventuser.py#L18>
/// If its order is changed, this function needs to be changed.
fn get_eventuser_tag(user: &User) -> Option<String> {
    if let Some(id) = user.id.as_str() {
        return Some(format!("id:{id}"));
    }

    if let Some(username) = user.username.as_str() {
        return Some(format!("username:{username}"));
    }

    if let Some(email) = user.email.as_str() {
        return Some(format!("email:{email}"));
    }

    if let Some(ip_address) = user.ip_address.as_str() {
        return Some(format!("ip:{ip_address}"));
    }

    None
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

    use std::iter::FromIterator;

    use super::*;

    use relay_general::protocol::User;
    use relay_general::store::{
        light_normalize_event, BreakdownsConfig, LightNormalizationConfig, MeasurementsConfig,
    };
    use relay_general::types::Annotated;
    use relay_metrics::DurationUnit;

    use crate::metrics_extraction::TaggingRule;

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
            "version": 1,
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

        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "d:transactions/measurements.foo@none",
                value: Distribution(
                    420.69,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "http.method": "POST",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                    "transaction.status": "ok",
                },
            },
            Metric {
                name: "d:transactions/measurements.lcp@millisecond",
                value: Distribution(
                    3000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "http.method": "POST",
                    "measurement_rating": "meh",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                    "transaction.status": "ok",
                },
            },
            Metric {
                name: "d:transactions/breakdowns.span_ops.ops.react.mount@millisecond",
                value: Distribution(
                    9.910106,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "http.method": "POST",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                    "transaction.status": "ok",
                },
            },
            Metric {
                name: "d:transactions/breakdowns.span_ops.total.time@millisecond",
                value: Distribution(
                    9.910106,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "http.method": "POST",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                    "transaction.status": "ok",
                },
            },
            Metric {
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "http.method": "POST",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                    "transaction.status": "ok",
                },
            },
            Metric {
                name: "s:transactions/user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "http.method": "POST",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                    "transaction.status": "ok",
                },
            },
        ]
        "###)
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

        insta::assert_debug_snapshot!(&metrics[..3], @r###"
        [
            Metric {
                name: "d:transactions/measurements.fcp@millisecond",
                value: Distribution(
                    1.1,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/measurements.foo@none",
                value: Distribution(
                    8.8,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/measurements.stall_count@none",
                value: Distribution(
                    3.3,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "platform": "other",
                },
            },
        ]
        "###);
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
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "d:transactions/measurements.lcp@millisecond",
                value: Distribution(
                    41.0,
                ),
                timestamp: UnixTimestamp(1619420402),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420402),
                tags: {
                    "platform": "other",
                    "satisfaction": "satisfied",
                },
            },
        ]
        "###);
        // assert_eq!(metrics.len(), 1);

        // for metric in metrics {
        //     assert_eq!(metric.tags.len(), 2);
        //     assert_eq!(metric.tags["satisfaction"], "satisfied");
        // }
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

        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "d:transactions/measurements.lcp@millisecond",
                value: Distribution(
                    41.0,
                ),
                timestamp: UnixTimestamp(1619420402),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    2000.0,
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

        let mut event = Annotated::from_json(json).unwrap();

        let _res = light_normalize_event(
            &mut event,
            &LightNormalizationConfig {
                measurements_config: Some(&MeasurementsConfig {
                    known_measurements: BTreeSet::from_iter(["fcp".to_owned()]),
                    max_custom_measurements: 2,
                }),
                ..Default::default()
            },
        );

        let config = TransactionMetricsConfig::default();
        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        insta::assert_debug_snapshot!(metrics, @r###"
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
            Metric {
                name: "d:transactions/measurements.h_custom3@none",
                value: Distribution(
                    43.0,
                ),
                timestamp: UnixTimestamp(1619420402),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    2000.0,
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
        metrics.retain(|m| m.name.contains("lcp"));
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
            "request": {"method": "OPTIONS"}
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
            "request": {"method": "OPTIONS"}
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
            "request": {"method": "GET"}
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

    #[test]
    fn test_computed_metrics() {
        let json = r#"{
            "type": "transaction",
            "timestamp": 1619420520,
            "start_timestamp": 1619420400,
            "measurements": {
                "frames_frozen": {
                    "value": 2
                },
                "frames_slow": {
                    "value": 1
                },
                "frames_total": {
                    "value": 4
                },
                "stall_total_time": {
                    "value": 4
                }
            }
        }"#;

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "extractMetrics": [
                "d:transactions/measurements.frames_frozen_rate@ratio",
                "d:transactions/measurements.frames_slow_rate@ratio",
                "d:transactions/measurements.stall_percentage@ratio"
            ]
        }
        "#,
        )
        .unwrap();

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        extract_transaction_metrics(&config, None, &[], event.value().unwrap(), &mut metrics);

        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "d:transactions/measurements.frames_frozen@none",
                value: Distribution(
                    2.0,
                ),
                timestamp: UnixTimestamp(1619420520),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/measurements.frames_frozen_rate@ratio",
                value: Distribution(
                    0.5,
                ),
                timestamp: UnixTimestamp(1619420520),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/measurements.frames_slow@none",
                value: Distribution(
                    1.0,
                ),
                timestamp: UnixTimestamp(1619420520),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/measurements.frames_slow_rate@ratio",
                value: Distribution(
                    0.25,
                ),
                timestamp: UnixTimestamp(1619420520),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/measurements.frames_total@none",
                value: Distribution(
                    4.0,
                ),
                timestamp: UnixTimestamp(1619420520),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/measurements.stall_percentage@ratio",
                value: Distribution(
                    3.3333333333333335e-5,
                ),
                timestamp: UnixTimestamp(1619420520),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/measurements.stall_total_time@millisecond",
                value: Distribution(
                    4.0,
                ),
                timestamp: UnixTimestamp(1619420520),
                tags: {
                    "platform": "other",
                },
            },
            Metric {
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    120000.0,
                ),
                timestamp: UnixTimestamp(1619420520),
                tags: {
                    "platform": "other",
                },
            },
        ]
        "###);
    }

    #[test]
    fn test_get_eventuser_tag() {
        // Note: If this order changes,
        // https://github.com/getsentry/sentry/blob/f621cd76da3a39836f34802ba9b35133bdfbe38b/src/sentry/models/eventuser.py#L18
        // has to be changed. Though it is probably not a good idea!
        let user = User {
            id: Annotated::new("ident".to_owned().into()),
            username: Annotated::new("username".to_owned()),
            email: Annotated::new("email".to_owned()),
            ip_address: Annotated::new("127.0.0.1".parse().unwrap()),
            ..User::default()
        };

        assert_eq!(get_eventuser_tag(&user).unwrap(), "id:ident");

        let user = User {
            username: Annotated::new("username".to_owned()),
            email: Annotated::new("email".to_owned()),
            ip_address: Annotated::new("127.0.0.1".parse().unwrap()),
            ..User::default()
        };

        assert_eq!(get_eventuser_tag(&user).unwrap(), "username:username");

        let user = User {
            email: Annotated::new("email".to_owned()),
            ip_address: Annotated::new("127.0.0.1".parse().unwrap()),
            ..User::default()
        };

        assert_eq!(get_eventuser_tag(&user).unwrap(), "email:email");

        let user = User {
            ip_address: Annotated::new("127.0.0.1".parse().unwrap()),
            ..User::default()
        };

        assert_eq!(get_eventuser_tag(&user).unwrap(), "ip:127.0.0.1");

        let user = User::default();

        assert!(get_eventuser_tag(&user).is_none());
    }
}
