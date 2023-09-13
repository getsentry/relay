use std::collections::BTreeMap;

use relay_common::{DurationUnit, EventType, SpanStatus, UnixTimestamp};
use relay_dynamic_config::{AcceptTransactionNames, TaggingRule, TransactionMetricsConfig};
use relay_general::protocol::{
    AsPair, Context, ContextInner, Event, TraceContext, TransactionSource, User,
};
use relay_general::store;
use relay_general::types::Annotated;
use relay_metrics::{AggregatorConfig, Metric};

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

/// These are the tags that are added to extracted low cardinality metrics.
fn extract_coarse_transaction_tags(event: &Event, config: &TransactionMetricsConfig) -> CommonTags {
    let mut tags = BTreeMap::new();
    if let Some(transaction_name) = get_transaction_name(event, config.accept_transaction_names) {
        tags.insert(CommonTags::Transaction, transaction_name);
    }

    CommonTags(tags)
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
    event: &Event,
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
    let coarse_tags = extract_coarse_transaction_tags(event, config);

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

    // Lower cardinality duration
    metrics.push(
        TransactionMetric::Duration {
            unit: DurationUnit::MilliSecond,
            value: relay_common::chrono_to_positive_millis(end - start),
            tags: coarse_tags.clone(),
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
                "lcp": {"value": 3000.0, "unit": "millisecond"}
            },
            "contexts": {
                "trace": {
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "span_id": "bd429c44b67a3eb4",
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
            event.value().unwrap(),
            Some("test_transaction"),
            &SamplingResult::Keep,
            &mut metrics,
            &mut sampling_metrics,
        )
        .unwrap();

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
            event.value().unwrap(),
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
            event.value().unwrap(),
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

        let event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            event.value().unwrap(),
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
            event.value().unwrap(),
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

        let event = Annotated::from_json(json).unwrap();

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
            event.value().unwrap(),
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

        let event = Annotated::from_json(json).unwrap();

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
            event.value().unwrap(),
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

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            event.value().unwrap(),
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

        let event = Annotated::from_json(json).unwrap();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            event.value().unwrap(),
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

        let event = Annotated::new(Event {
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
            event.value().unwrap(),
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

        let event = Annotated::<Event>::from_json(json).unwrap();
        config.accept_transaction_names = strategy;

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            event.value().unwrap(),
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

        let event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();

        let mut metrics = vec![];
        let mut sampling_metrics = vec![];
        extract_transaction_metrics(
            &aggregator_config,
            &config,
            &[],
            event.value().unwrap(),
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
            event.value().unwrap(),
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
