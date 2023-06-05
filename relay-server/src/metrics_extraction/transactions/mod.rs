use md5;
use std::collections::BTreeMap;

use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use relay_common::{DurationUnit, EventType, MetricUnit, SpanStatus, UnixTimestamp};
use relay_dynamic_config::{AcceptTransactionNames, TaggingRule, TransactionMetricsConfig};
use relay_filter::csp::SchemeDomainPort;
use relay_general::protocol::{
    AsPair, Context, ContextInner, Event, Span, TraceContext, TransactionSource, User,
};
use relay_general::store;
use relay_general::types::{Annotated, Value};
use relay_metrics::{AggregatorConfig, Metric, MetricNamespace, MetricValue};

use crate::metrics_extraction::conditional_tagging::run_conditional_tagging;
use crate::metrics_extraction::transactions::types::{
    CommonTag, CommonTags, ExtractMetricsError, TransactionCPRTags, TransactionMeasurementTags,
    TransactionMetric,
};
use crate::metrics_extraction::IntoMetric;
use crate::statsd::RelayCounters;
use crate::utils::SamplingResult;

pub mod types;

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

/// Extract HTTP method
/// See <https://github.com/getsentry/snuba/blob/2e038c13a50735d58cc9397a29155ab5422a62e5/snuba/datasets/errors_processor.py#L64-L67>.
fn extract_http_method(transaction: &Event) -> Option<String> {
    let request = transaction.request.value()?;
    let method = request.method.value()?;
    Some(method.clone())
}

/// Extract the browser name from the [`Context::Browser`] context.
fn extract_browser_name(event: &Event) -> Option<String> {
    let contexts = event.contexts.value()?;
    let browser = contexts.get("browser").and_then(Annotated::value);
    if let Some(ContextInner(Context::Browser(browser_context))) = browser {
        return browser_context.name.value().cloned();
    }

    None
}

/// Extract the OS name from the [`Context::Os`] context.
fn extract_os_name(event: &Event) -> Option<String> {
    let contexts = event.contexts.value()?;
    let os = contexts.get("os").and_then(Annotated::value);
    if let Some(ContextInner(Context::Os(os_context))) = os {
        return os_context.name.value().cloned();
    }

    None
}

/// Extract the GEO country code from the [`User`] context.
fn extract_geo_country_code(event: &Event) -> Option<String> {
    if let Some(user) = event.user.value() {
        if let Some(geo) = user.geo.value() {
            return geo.country_code.value().cloned();
        }
    }

    None
}

/// Extract the HTTP status code from the span data.
fn http_status_code_from_span(span: &Span) -> Option<String> {
    // For SDKs which put the HTTP status code into the span data.
    if let Some(status_code) = span
        .data
        .value()
        .and_then(|v| v.get("status_code"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
    {
        return Some(status_code);
    }

    // For SDKs which put the HTTP status code into the span tags.
    if let Some(status_code) = span
        .tags
        .value()
        .and_then(|tags| tags.get("http.status_code"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_owned())
    {
        return Some(status_code);
    }

    None
}

/// Extracts the HTTP status code.
pub(crate) fn extract_http_status_code(event: &Event) -> Option<String> {
    if let Some(spans) = event.spans.value() {
        for span in spans {
            if let Some(span_value) = span.value() {
                if let Some(status_code) = http_status_code_from_span(span_value) {
                    return Some(status_code);
                }
            }
        }
    }

    // For SDKs which put the HTTP status code into the breadcrumbs data.
    if let Some(breadcrumbs) = event.breadcrumbs.value() {
        if let Some(values) = breadcrumbs.values.value() {
            for breadcrumb in values {
                // We need only the `http` type.
                if let Some(crumb) = breadcrumb
                    .value()
                    .filter(|bc| bc.ty.as_str() == Some("http"))
                {
                    // Try to get the status code om the map.
                    if let Some(status_code) = crumb.data.value().and_then(|v| v.get("status_code"))
                    {
                        return status_code.value().and_then(|v| v.as_str()).map(Into::into);
                    }
                }
            }
        }
    }

    // For SDKs which put the HTTP status code in the `Response` context.
    if let Some(contexts) = event.contexts.value() {
        let response = contexts.get("response").and_then(Annotated::value);
        if let Some(ContextInner(Context::Response(response_context))) = response {
            let status_code = response_context
                .status_code
                .value()
                .map(|code| code.to_string());
            return status_code;
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
        TransactionSource::Route
        | TransactionSource::View
        | TransactionSource::Component
        | TransactionSource::Task => true,

        // We know now that the rules to remove high cardinality were applied, so we assume
        // low-cardinality now.
        TransactionSource::Sanitized => true,

        // "unknown" is the value for old SDKs that do not send a transaction source yet.
        // Caller decides how to treat this.
        TransactionSource::Unknown => treat_unknown_as_low_cardinality,

        // Any other value would be an SDK bug or users manually configuring the
        // source, assume high-cardinality and drop.
        TransactionSource::Other(_) => false,
    }
}

/// Decide whether we want to keep the transaction name.
/// High-cardinality sources are excluded to protect our metrics infrastructure.
/// Note that this will produce a discrepancy between metrics and raw transaction data.
fn get_transaction_name(
    event: &Event,
    accept_transaction_names: AcceptTransactionNames,
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
            .map(std::string::String::as_str)
            .unwrap_or_default(),
        name_used = name_used,
    );

    name
}

/// These are the tags that are added to all extracted metrics.
fn extract_universal_tags(event: &Event, config: &TransactionMetricsConfig) -> CommonTags {
    let mut tags = BTreeMap::new();
    if let Some(release) = event.release.as_str() {
        tags.insert(CommonTag::Release, release.to_string());
    }
    if let Some(dist) = event.dist.value() {
        tags.insert(CommonTag::Dist, dist.to_string());
    }
    if let Some(environment) = event.environment.as_str() {
        tags.insert(CommonTag::Environment, environment.to_string());
    }
    if let Some(transaction_name) = get_transaction_name(event, config.accept_transaction_names) {
        tags.insert(CommonTag::Transaction, transaction_name);
    }

    // The platform tag should not increase dimensionality in most cases, because most
    // transactions are specific to one platform.
    // NOTE: we might want to reconsider light normalization a little and include the
    // `store::is_valid_platform` into light normalization.
    let platform = match event.platform.as_str() {
        Some(platform) if store::is_valid_platform(platform) => platform,
        _ => "other",
    };

    tags.insert(CommonTag::Platform, platform.to_string());

    if let Some(trace_context) = get_trace_context(event) {
        let status = extract_transaction_status(trace_context);

        tags.insert(CommonTag::TransactionStatus, status.to_string());

        if let Some(op) = extract_transaction_op(trace_context) {
            tags.insert(CommonTag::TransactionOp, op);
        }
    }

    if let Some(http_method) = extract_http_method(event) {
        tags.insert(CommonTag::HttpMethod, http_method);
    }

    if let Some(browser_name) = extract_browser_name(event) {
        tags.insert(CommonTag::BrowserName, browser_name);
    }

    if let Some(os_name) = extract_os_name(event) {
        tags.insert(CommonTag::OsName, os_name);
    }

    if let Some(geo_country_code) = extract_geo_country_code(event) {
        tags.insert(CommonTag::GeoCountryCode, geo_country_code);
    }

    if let Some(status_code) = extract_http_status_code(event) {
        tags.insert(CommonTag::HttpStatusCode, status_code);
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
                            tags.insert(CommonTag::Custom(key.to_string()), value.to_string());
                        }
                    }
                }
            }
        }
    }

    CommonTags(tags)
}

#[allow(clippy::too_many_arguments)] // TODO: Provide a more sensible API for this.
pub fn extract_transaction_metrics(
    aggregator_config: &AggregatorConfig,
    config: &TransactionMetricsConfig,
    conditional_tagging_config: &[TaggingRule],
    extract_spans_metrics: bool,
    event: &mut Event,
    transaction_from_dsc: Option<&str>,
    sampling_result: &SamplingResult,
    project_metrics: &mut Vec<Metric>,  // output parameter
    sampling_metrics: &mut Vec<Metric>, // output parameter
) -> Result<bool, ExtractMetricsError> {
    let before_len = project_metrics.len();

    extract_transaction_metrics_inner(
        aggregator_config,
        config,
        event,
        transaction_from_dsc,
        sampling_result,
        project_metrics,
        sampling_metrics,
    )?;

    if extract_spans_metrics {
        extract_span_metrics(aggregator_config, event, project_metrics)?;
    }

    let added_slice = &mut project_metrics[before_len..];
    run_conditional_tagging(event, conditional_tagging_config, added_slice);
    Ok(!added_slice.is_empty())
}

fn extract_transaction_metrics_inner(
    aggregator_config: &AggregatorConfig,
    config: &TransactionMetricsConfig,
    event: &Event,
    transaction_from_dsc: Option<&str>,
    sampling_result: &SamplingResult,
    metrics: &mut Vec<Metric>,          // output parameter
    sampling_metrics: &mut Vec<Metric>, // output parameter
) -> Result<(), ExtractMetricsError> {
    if event.ty.value() != Some(&EventType::Transaction) {
        return Ok(());
    }

    let (Some(&start), Some(&end)) = (event.start_timestamp.value(), event.timestamp.value()) else {
        relay_log::debug!("failed to extract the start and the end timestamps from the event");
        return Err(ExtractMetricsError::MissingTimestamp);
    };

    let Some(timestamp) = UnixTimestamp::from_datetime(end.into_inner()) else {
        relay_log::debug!("event timestamp is not a valid unix timestamp");
        return Err(ExtractMetricsError::InvalidTimestamp);
    };

    // Validate the transaction event against the metrics timestamp limits. If the metric is too
    // old or too new, we cannot extract the metric and also need to drop the transaction event
    // for consistency between metrics and events.
    if !aggregator_config.timestamp_range().contains(&timestamp) {
        relay_log::debug!("event timestamp is out of the valid range for metrics");
        return Err(ExtractMetricsError::InvalidTimestamp);
    }

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

            let measurement_tags = TransactionMeasurementTags {
                measurement_rating: get_measurement_rating(name, value),
                universal_tags: tags.clone(),
            };

            metrics.push(
                TransactionMetric::Measurement {
                    name: name.to_string(),
                    value,
                    unit: measurement.unit.value().copied().unwrap_or_default(),
                    tags: measurement_tags,
                }
                .into_metric(timestamp),
            );
        }
    }

    // Breakdowns
    if let Some(breakdowns) = event.breakdowns.value() {
        for (breakdown, measurements) in breakdowns.iter() {
            if let Some(measurements) = measurements.value() {
                for (measurement_name, annotated) in measurements.iter() {
                    if measurement_name == "total.time" {
                        // The only reason we do not emit total.time as a metric is that is was not
                        // on the allowlist in sentry before, and nobody seems to be missing it.
                        continue;
                    }

                    let measurement = match annotated.value() {
                        Some(m) => m,
                        None => continue,
                    };

                    let value = match measurement.value.value() {
                        Some(value) => *value,
                        None => continue,
                    };
                    metrics.push(
                        TransactionMetric::Breakdown {
                            name: format!("{breakdown}.{measurement_name}"),
                            value,
                            tags: tags.clone(),
                        }
                        .into_metric(timestamp),
                    );
                }
            }
        }
    }

    // Duration
    metrics.push(
        TransactionMetric::Duration {
            unit: DurationUnit::MilliSecond,
            value: relay_common::chrono_to_positive_millis(end - start),
            tags: tags.clone(),
        }
        .into_metric(timestamp),
    );

    let root_counter_tags = {
        let mut universal_tags = CommonTags(BTreeMap::default());
        if let Some(transaction_from_dsc) = transaction_from_dsc {
            universal_tags
                .0
                .insert(CommonTag::Transaction, transaction_from_dsc.to_string());
        }
        TransactionCPRTags {
            decision: match sampling_result {
                SamplingResult::Keep => "keep".to_owned(),
                SamplingResult::Drop(_) => "drop".to_owned(),
            },
            universal_tags,
        }
    };
    // Count the transaction towards the root
    sampling_metrics.push(
        TransactionMetric::CountPerRootProject {
            value: 1.0,
            tags: root_counter_tags,
        }
        .into_metric(timestamp),
    );

    // User
    if let Some(user) = event.user.value() {
        if let Some(value) = get_eventuser_tag(user) {
            metrics.push(TransactionMetric::User { value, tags }.into_metric(timestamp));
        }
    }

    Ok(())
}

/// Extracts metrics from the spans of the given transaction, and sets common
/// tags for all the metrics and spans. If a span already contains a tag
/// extracted for a metric, the tag value is overwritten.
fn extract_span_metrics(
    aggregator_config: &AggregatorConfig,
    event: &mut Event,
    metrics: &mut Vec<Metric>, // output parameter
) -> Result<(), ExtractMetricsError> {
    // TODO(iker): measure the performance of this whole method

    if event.ty.value() != Some(&EventType::Transaction) {
        return Ok(());
    }
    let (Some(&start), Some(&end)) = (event.start_timestamp.value(), event.timestamp.value()) else {
        relay_log::debug!("failed to extract the start and the end timestamps from the event");
        return Err(ExtractMetricsError::MissingTimestamp);
    };

    let Some(timestamp) = UnixTimestamp::from_datetime(end.into_inner()) else {
        relay_log::debug!("event timestamp is not a valid unix timestamp");
        return Err(ExtractMetricsError::InvalidTimestamp);
    };

    // Validate the transaction event against the metrics timestamp limits. If the metric is too
    // old or too new, we cannot extract the metric and also need to drop the transaction event
    // for consistency between metrics and events.
    if !aggregator_config.timestamp_range().contains(&timestamp) {
        relay_log::debug!("event timestamp is out of the valid range for metrics");
        return Err(ExtractMetricsError::InvalidTimestamp);
    }

    // Collect the shared tags for all the metrics and spans on this transaction.
    let mut shared_tags = BTreeMap::new();

    if let Some(environment) = event.environment.as_str() {
        shared_tags.insert("environment".to_owned(), environment.to_owned());
    }

    if let Some(transaction_name) = event.transaction.value() {
        shared_tags.insert("transaction".to_owned(), transaction_name.to_owned());

        if let Some(transaction_method) = http_method_from_transaction_name(transaction_name) {
            shared_tags.insert(
                "transaction.method".to_owned(),
                transaction_method.to_owned(),
            );
        }
    }

    if let Some(trace_context) = get_trace_context(event) {
        if let Some(op) = extract_transaction_op(trace_context) {
            shared_tags.insert("transaction.op".to_owned(), op);
        }
    }

    let Some(spans) = event.spans.value_mut() else { return Ok(())};

    for annotated_span in spans {
        if let Some(span) = annotated_span.value_mut() {
            let mut span_tags = shared_tags.clone();

            if let Some(scrubbed_description) = span
                .data
                .value()
                .and_then(|data| data.get("description.scrubbed"))
                .and_then(|value| value.as_str())
            {
                span_tags.insert(
                    "span.description".to_owned(),
                    scrubbed_description.to_owned(),
                );

                let mut span_group = format!("{:?}", md5::compute(scrubbed_description));
                span_group.truncate(16);
                span_tags.insert("span.group".to_owned(), span_group);
            }

            if let Some(span_op) = span.op.value() {
                span_tags.insert("span.op".to_owned(), span_op.to_owned());

                let span_module = if span_op.starts_with("http") {
                    Some("http")
                } else if span_op.starts_with("db") {
                    Some("db")
                } else if span_op.starts_with("cache") {
                    Some("cache")
                } else {
                    None
                };

                if let Some(module) = span_module {
                    span_tags.insert("span.module".to_owned(), module.to_owned());
                }

                // TODO(iker): we're relying on the existance of `http.method`
                // or `db.operation`. This is not guaranteed, and we'll need to
                // parse the span description in that case.
                let action = match span_module {
                    Some("http") => span
                        .data
                        .value()
                        // TODO(iker): some SDKs extract this as method
                        .and_then(|v| v.get("http.method"))
                        .and_then(|method| method.as_str()),
                    Some("db") => {
                        let action_from_data = span
                            .data
                            .value()
                            .and_then(|v| v.get("db.operation"))
                            .and_then(|db_op| db_op.as_str());
                        action_from_data.or_else(|| {
                            span.description
                                .value()
                                .and_then(|d| sql_action_from_query(d))
                        })
                    }
                    _ => None,
                };

                if let Some(act) = action {
                    span_tags.insert("span.action".to_owned(), act.to_owned());
                }

                let domain = if span_op == "http.client" {
                    span.description
                        .value()
                        .and_then(|url| domain_from_http_url(url))
                } else if span_op.starts_with("db") {
                    span.description
                        .value()
                        .and_then(|query| sql_table_from_query(query))
                        .map(|s| s.to_owned())
                } else {
                    None
                };

                if let Some(dom) = domain {
                    span_tags.insert("span.domain".to_owned(), dom.to_owned());
                }
            }

            let system = span
                .data
                .value()
                .and_then(|v| v.get("db.system"))
                .and_then(|system| system.as_str());
            if let Some(sys) = system {
                span_tags.insert("span.system".to_owned(), sys.to_owned());
            }

            if let Some(span_status) = span.status.value() {
                span_tags.insert("span.status".to_owned(), span_status.to_string());
            }

            if let Some(status_code) = http_status_code_from_span(span) {
                span_tags.insert("span.status_code".to_owned(), status_code);
            }

            // Even if we emit metrics, we want this info to be duplicated in every span.
            span.data.get_or_insert_with(BTreeMap::new).extend(
                span_tags
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k, Annotated::new(Value::String(v)))),
            );

            if let Some(user) = event.user.value() {
                if let Some(value) = get_eventuser_tag(user) {
                    metrics.push(Metric::new_mri(
                        MetricNamespace::Transactions,
                        "span.user",
                        MetricUnit::None,
                        MetricValue::set_from_str(&value),
                        timestamp,
                        span_tags.clone(),
                    ));
                }
            }

            if let Some(exclusive_time) = span.exclusive_time.value() {
                // NOTE(iker): this exclusive time doesn't consider all cases,
                // such as sub-transactions. We accept these limitations for
                // now.
                metrics.push(Metric::new_mri(
                    MetricNamespace::Transactions,
                    "span.exclusive_time",
                    MetricUnit::Duration(DurationUnit::MilliSecond),
                    MetricValue::Distribution(*exclusive_time),
                    timestamp,
                    span_tags.clone(),
                ));
            }

            // The `duration` of a span. This metric also serves as the
            // counter metric `throughput`.
            metrics.push(Metric::new_mri(
                MetricNamespace::Transactions,
                "span.duration",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(relay_common::chrono_to_positive_millis(end - start)),
                timestamp,
                span_tags.clone(),
            ));
        }
    }

    Ok(())
}

/// Regex with a capture group to extract the database action from a query.
///
/// Currently, we're only interested in either `SELECT` or `INSERT` statements.
static SQL_ACTION_EXTRACTOR_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)(?P<action>(SELECT|INSERT))"#).unwrap());

fn sql_action_from_query(query: &str) -> Option<&str> {
    extract_captured_substring(query, &SQL_ACTION_EXTRACTOR_REGEX)
}

/// Regex with a capture group to extract the table from a database query,
/// based on `FROM` and `INTO` keywords.
static SQL_TABLE_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(from|into)(\s|"|'|\()+(?P<table>(\w+(\.\w+)*))(\s|"|'|\))+"#).unwrap()
});

/// Returns the table in the SQL query, if any.
///
/// If multiple tables exist, only the first one is returned.
fn sql_table_from_query(query: &str) -> Option<&str> {
    extract_captured_substring(query, &SQL_TABLE_EXTRACTOR_REGEX)
}

/// Regex with a capture group to extract the HTTP method from a string.
static HTTP_METHOD_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)^(?P<method>(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH))\s"#)
        .unwrap()
});

fn http_method_from_transaction_name(name: &str) -> Option<&str> {
    extract_captured_substring(name, &HTTP_METHOD_EXTRACTOR_REGEX)
}

/// Returns the captured substring in `string` with the capture group in `pattern`.
///
/// It assumes there's only one capture group in `pattern`, and only returns the first one.
fn extract_captured_substring<'a>(string: &'a str, pattern: &'a Lazy<Regex>) -> Option<&'a str> {
    let capture_names: Vec<_> = pattern.capture_names().flatten().collect();

    for captures in pattern.captures_iter(string) {
        for name in &capture_names {
            if let Some(capture) = captures.name(name) {
                return Some(&string[capture.start()..capture.end()]);
            }
        }
    }

    None
}

fn domain_from_http_url(url: &str) -> Option<String> {
    match url.split_once(' ') {
        Some((_method, url)) => {
            let domain_port = SchemeDomainPort::from(url);
            match (domain_port.domain, domain_port.port) {
                (Some(domain), port) => normalize_domain(&domain, port.as_ref()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn normalize_domain(domain: &str, port: Option<&String>) -> Option<String> {
    let mut tokens = domain.rsplitn(3, '.');
    let tld = tokens.next();
    let domain = tokens.next();
    let prefix = tokens.next().map(|_| "*");

    let mut replaced = prefix
        .iter()
        .chain(domain.iter())
        .chain(tld.iter())
        .join(".");

    if let Some(port) = port {
        replaced = format!("{replaced}:{port}");
    }
    Some(replaced)
}

/// Compute the transaction event's "user" tag as close as possible to how users are determined in
/// the transactions dataset in Snuba. This should produce the exact same user counts as the `user`
/// column in Discover for Transactions, barring:
///
/// * imprecision caused by HLL sketching in Snuba, which we don't have in events
/// * hash collisions in [`relay_metrics::MetricValue::set_from_display`], which we don't have in events
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
        Some(if value < meh_ceiling {
            "good".to_owned()
        } else if value < poor_ceiling {
            "meh".to_owned()
        } else {
            "poor".to_owned()
        })
    };

    match name {
        "lcp" => rate_range(2500.0, 4000.0),
        "fcp" => rate_range(1000.0, 3000.0),
        "fid" => rate_range(100.0, 300.0),
        "inp" => rate_range(200.0, 500.0),
        "cls" => rate_range(0.1, 0.25),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use insta;
    use relay_common::MetricUnit;
    use relay_dynamic_config::TaggingRule;
    use relay_general::protocol::{Contexts, Timestamp, User};
    use relay_general::store::{
        self, BreakdownsConfig, LightNormalizationConfig, MeasurementsConfig,
    };
    use relay_general::types::Annotated;
    use relay_metrics::{DurationUnit, MetricNamespace, MetricValue};

    use super::*;

    /// Returns an aggregator config that permits every timestamp.
    fn aggregator_config() -> AggregatorConfig {
        AggregatorConfig {
            max_secs_in_past: u64::MAX,
            max_secs_in_future: u64::MAX,
            ..Default::default()
        }
    }

    #[test]
    fn test_extract_transaction_metrics() {
        let json = r#"
        {
            "type": "transaction",
            "platform": "javascript",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "server_name": "myhost",
            "release": "1.2.3",
            "dist": "foo ",
            "environment": "fake_environment",
            "transaction": "GET /api/:version/users/",
            "transaction_info": {"source": "custom"},
            "user": {
                "id": "user123",
                "geo": {
                    "country_code": "US"
                }
            },
            "tags": {
                "fOO": "bar",
                "bogus": "absolutely"
            },
            "measurements": {
                "foo": {"value": 420.69},
                "lcp": {"value": 3000.0, "unit": "millisecond"}
            },
            "contexts": {
                "trace": {
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "span_id": "bd429c44b67a3eb4",
                    "op": "myop",
                    "status": "ok"
                },
                "browser": {
                    "name": "Chrome"
                },
                "os": {
                    "name": "Windows"
                }
            },
            "spans": [
                {
                    "description": "<OrganizationContext>",
                    "op": "react.mount",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "description": "POST http://sth.subdomain.domain.tld:targetport/api/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST",
                        "status_code": "200"
                    }
                },
                {
                    "description": "POST http://sth.subdomain.domain.tld:targetport/api/hi",
                    "op": "http.client",
                    "tags": {
                        "http.status_code": "200"
                    },
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST"
                    }
                },
                {
                    "description": "POST http://sth.subdomain.domain.tld:targetport/api/hi",
                    "op": "http.client",
                    "tags": {
                        "http.status_code": "200"
                    },
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST",
                        "status_code": "200"
                    }
                },
                {
                    "description": "POST http://targetdomain.tld:targetport/api/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST",
                        "status_code": "200"
                    }
                },
                {
                    "description": "POST http://targetdomain:targetport/api/id/0987654321",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST",
                        "status_code": "200"
                    }
                },
                {
                    "description": "SELECT column FROM table WHERE id IN (1, 2, 3)",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SELECT column FROM table WHERE id IN (1, 2, 3)",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "INSERT INTO table (col) VALUES (val)",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "INSERT"
                    }
                },
                {
                    "description": "INSERT INTO from_date (col) VALUES (val)",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "INSERT"
                    }
                },
                {
                    "description": "INSERT INTO table (col) VALUES (val)",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "SELECT\n*\nFROM\ntable\nWHERE\nid\nIN\n(val)",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SELECT 'table'.'col' FROM 'table' WHERE 'table'.'col' = %s",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SAVEPOINT save_this_one",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase"
                    }
                },
                {
                    "description": "GET cache:user:{123}",
                    "op": "cache.get_item",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "cache.hit": false
                    }
                },
                {
                    "description": "http://domain/static/myscript-v1.9.23.js",
                    "op": "resource.script",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
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

        let mut event = Annotated::from_json(json).unwrap();

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"
        {
            "version": 1,
            "extractCustomTags": ["fOO"]
        }
        "#,
        )
        .unwrap();

        let aggregator_config = aggregator_config();

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        let res = store::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                breakdowns_config: Some(&breakdowns_config),
                scrub_span_descriptions: true,
                light_normalize_spans: true,
                ..Default::default()
            },
        );
        assert!(res.is_ok());

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            true,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

        insta::assert_debug_snapshot!(event.value().unwrap().spans);

        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "d:transactions/measurements.foo@none",
                value: Distribution(
                    420.69,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "browser.name": "Chrome",
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "geo.country_code": "US",
                    "http.method": "POST",
                    "http.status_code": "200",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "GET /api/:version/users/",
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
                    "browser.name": "Chrome",
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "geo.country_code": "US",
                    "http.method": "POST",
                    "http.status_code": "200",
                    "measurement_rating": "meh",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "GET /api/:version/users/",
                    "transaction.op": "myop",
                    "transaction.status": "ok",
                },
            },
            Metric {
                name: "d:transactions/breakdowns.span_ops.ops.react.mount@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "browser.name": "Chrome",
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "geo.country_code": "US",
                    "http.method": "POST",
                    "http.status_code": "200",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "GET /api/:version/users/",
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
                    "browser.name": "Chrome",
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "geo.country_code": "US",
                    "http.method": "POST",
                    "http.status_code": "200",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "GET /api/:version/users/",
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
                    "browser.name": "Chrome",
                    "dist": "foo",
                    "environment": "fake_environment",
                    "fOO": "bar",
                    "geo.country_code": "US",
                    "http.method": "POST",
                    "http.status_code": "200",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "GET /api/:version/users/",
                    "transaction.op": "myop",
                    "transaction.status": "ok",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.op": "react.mount",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.op": "react.mount",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.op": "react.mount",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "*.domain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "mytransaction",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "targetdomain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "targetdomain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.domain": "targetdomain.tld:targetport",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.description": "POST http://targetdomain:targetport/api/id/*",
                    "span.domain": "targetdomain:targetport",
                    "span.group": "ca77233e5cdb864b",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.description": "POST http://targetdomain:targetport/api/id/*",
                    "span.domain": "targetdomain:targetport",
                    "span.group": "ca77233e5cdb864b",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "POST",
                    "span.description": "POST http://targetdomain:targetport/api/id/*",
                    "span.domain": "targetdomain:targetport",
                    "span.group": "ca77233e5cdb864b",
                    "span.module": "http",
                    "span.op": "http.client",
                    "span.status": "ok",
                    "span.status_code": "200",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT column FROM table WHERE id IN (%s)",
                    "span.domain": "table",
                    "span.group": "a31d8fd4438bc382",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT column FROM table WHERE id IN (%s)",
                    "span.domain": "table",
                    "span.group": "a31d8fd4438bc382",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT column FROM table WHERE id IN (%s)",
                    "span.domain": "table",
                    "span.group": "a31d8fd4438bc382",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT column FROM table WHERE id IN (%s)",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT column FROM table WHERE id IN (%s)",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT column FROM table WHERE id IN (%s)",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "from_date",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "from_date",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "from_date",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "INSERT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db.sql.query",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.domain": "table",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT %s.%s FROM %s WHERE %s.%s = %s",
                    "span.domain": "table",
                    "span.group": "c55478a060a56db3",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT %s.%s FROM %s WHERE %s.%s = %s",
                    "span.domain": "table",
                    "span.group": "c55478a060a56db3",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.action": "SELECT",
                    "span.description": "SELECT %s.%s FROM %s WHERE %s.%s = %s",
                    "span.domain": "table",
                    "span.group": "c55478a060a56db3",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "SAVEPOINT %s",
                    "span.group": "3f955cbde39e04b9",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "SAVEPOINT %s",
                    "span.group": "3f955cbde39e04b9",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "SAVEPOINT %s",
                    "span.group": "3f955cbde39e04b9",
                    "span.module": "db",
                    "span.op": "db",
                    "span.status": "ok",
                    "span.system": "MyDatabase",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "GET cache:user:*",
                    "span.group": "325fa5feb926f121",
                    "span.module": "cache",
                    "span.op": "cache.get_item",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "GET cache:user:*",
                    "span.group": "325fa5feb926f121",
                    "span.module": "cache",
                    "span.op": "cache.get_item",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "GET cache:user:*",
                    "span.group": "325fa5feb926f121",
                    "span.module": "cache",
                    "span.op": "cache.get_item",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "s:transactions/span.user@none",
                value: Set(
                    933084975,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "http://domain/static/myscript-*.js",
                    "span.group": "022f81fdf31228bf",
                    "span.op": "resource.script",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.exclusive_time@millisecond",
                value: Distribution(
                    2000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "http://domain/static/myscript-*.js",
                    "span.group": "022f81fdf31228bf",
                    "span.op": "resource.script",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
            Metric {
                name: "d:transactions/span.duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "environment": "fake_environment",
                    "span.description": "http://domain/static/myscript-*.js",
                    "span.group": "022f81fdf31228bf",
                    "span.op": "resource.script",
                    "span.status": "ok",
                    "transaction": "GET /api/:version/users/",
                    "transaction.method": "GET",
                    "transaction.op": "myop",
                },
            },
        ]
        "###);
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
            },
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053"
                }
            }
        }
        "#;

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut event = Annotated::from_json(json).unwrap();

        // Normalize first, to make sure the units are correct:
        let res = store::light_normalize_event(&mut event, LightNormalizationConfig::default());
        assert!(res.is_ok(), "{res:?}");

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

        insta::assert_debug_snapshot!(metrics, @r###"
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
                    "transaction.status": "unknown",
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
                    "transaction.status": "unknown",
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
                    "transaction.status": "unknown",
                },
            },
            Metric {
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "platform": "other",
                    "transaction.status": "unknown",
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
            },
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053"
                }
            }
        }"#;

        let config: TransactionMetricsConfig = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut event = Annotated::from_json(json).unwrap();

        // Normalize first, to make sure the units are correct:
        let res = store::light_normalize_event(&mut event, LightNormalizationConfig::default());
        assert!(res.is_ok(), "{res:?}");

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "d:transactions/measurements.fcp@second",
                value: Distribution(
                    1.1,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                    "transaction.status": "unknown",
                },
            },
            Metric {
                name: "d:transactions/measurements.lcp@none",
                value: Distribution(
                    2.2,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                    "transaction.status": "unknown",
                },
            },
            Metric {
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    59000.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "platform": "other",
                    "transaction.status": "unknown",
                },
            },
        ]
        "###);
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

        let mut event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

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
    fn test_custom_measurements() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "timestamp": "2021-04-26T08:00:02+0100",
            "measurements": {
                "a_custom1": {"value": 41},
                "fcp": {"value": 0.123, "unit": "millisecond"},
                "g_custom2": {"value": 42, "unit": "second"},
                "h_custom3": {"value": 43}
            },
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053"
                }}
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();

        // Normalize first, to make sure the units are correct:
        let measurements_config: MeasurementsConfig = serde_json::from_value(serde_json::json!(
            {
                "builtinMeasurements": [{"name": "fcp", "unit": "millisecond"}],
                "maxCustomMeasurements": 2
            }
        ))
        .unwrap();
        let res = store::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                measurements_config: Some(&measurements_config),
                ..Default::default()
            },
        );
        assert!(res.is_ok(), "{res:?}");

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

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
                        "transaction.status": "unknown",
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
                        "transaction.status": "unknown",
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
                        "transaction.status": "unknown",
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
                        "transaction.status": "unknown",
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
                "lcp": {"value": 41, "unit": "millisecond"}
            }
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

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
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &tagging_config,
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

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
                    "satisfaction": "tolerated",
                },
            },
        ]
        "###);
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
                "lcp": {"value": 41, "unit": "millisecond"}
            }
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

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
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &tagging_config,
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();
        metrics.retain(|m| m.name.contains("lcp"));

        assert_eq!(
            metrics,
            &[Metric::new_mri(
                MetricNamespace::Transactions,
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

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

        assert_eq!(metrics.len(), 1, "{metrics:?}");

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

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

        assert_eq!(metrics.len(), 1, "{metrics:?}");

        assert_eq!(metrics[0].name, "d:transactions/duration@millisecond");
        assert_eq!(
            metrics[0].tags,
            BTreeMap::from([
                ("transaction.status".to_string(), "unknown".to_string()),
                ("platform".to_string(), "other".to_string())
            ])
        );
    }

    #[test]
    fn test_expired_timestamp() {
        let config = TransactionMetricsConfig::default();
        let aggregator_config = AggregatorConfig {
            max_secs_in_past: 3600,
            ..Default::default()
        };

        let timestamp = Timestamp(chrono::Utc::now() - chrono::Duration::seconds(7200));

        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(timestamp),
            start_timestamp: Annotated::new(timestamp),
            contexts: Annotated::new({
                let mut contexts = Contexts::new();
                contexts.add(Context::Trace(Box::default()));
                contexts
            }),
            ..Default::default()
        });

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        let result = extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        );

        assert_eq!(result, Err(ExtractMetricsError::InvalidTimestamp));
        assert_eq!(metrics, &[]);
    }

    /// Helper function to check if the transaction name is set correctly
    fn extract_transaction_name(json: &str, strategy: AcceptTransactionNames) -> Option<String> {
        let mut config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut event = Annotated::<Event>::from_json(json).unwrap();
        config.accept_transaction_names = strategy;

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

        assert_eq!(metrics.len(), 1);
        metrics[0].tags.get("transaction").cloned()
    }

    #[test]
    fn test_root_counter_keep() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "transaction": "ignored",
            "contexts": {
                "trace": {
                    "status": "ok"
                }
            }
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("root_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

        insta::assert_debug_snapshot!(sampling_metrics, @r###"
        [
            Metric {
                name: "c:transactions/count_per_root_project@none",
                value: Counter(
                    1.0,
                ),
                timestamp: UnixTimestamp(1619420400),
                tags: {
                    "decision": "keep",
                    "transaction": "root_transaction",
                },
            },
        ]
        "###);
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
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053"
                }
            },
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
                    "value": 4,
                    "unit": "millisecond"
                }
            }
        }"#;

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut event = Annotated::from_json(json).unwrap();
        // Normalize first, to make sure that the metrics were computed:
        let _ = store::light_normalize_event(&mut event, LightNormalizationConfig::default());

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            false,
            event.value_mut().as_mut().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

        let metrics_names: Vec<_> = metrics.into_iter().map(|m| m.name).collect();
        insta::assert_debug_snapshot!(metrics_names, @r###"
        [
            "d:transactions/measurements.frames_frozen@none",
            "d:transactions/measurements.frames_frozen_rate@ratio",
            "d:transactions/measurements.frames_slow@none",
            "d:transactions/measurements.frames_slow_rate@ratio",
            "d:transactions/measurements.frames_total@none",
            "d:transactions/measurements.stall_percentage@ratio",
            "d:transactions/measurements.stall_total_time@millisecond",
            "d:transactions/duration@millisecond",
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
