use std::collections::BTreeMap;

use relay_common::{DurationUnit, EventType, SpanStatus, UnixTimestamp};
use relay_dynamic_config::TransactionMetricsConfig;
use relay_general::protocol::{
    AsPair, BrowserContext, Event, OsContext, TraceContext, TransactionSource,
};
use relay_general::store;
use relay_metrics::{AggregatorConfig, Metric};

use crate::metrics_extraction::transactions::types::{
    CommonTag, CommonTags, ExtractMetricsError, TransactionCPRTags, TransactionDurationTags,
    TransactionMeasurementTags, TransactionMetric,
};
use crate::metrics_extraction::IntoMetric;
use crate::statsd::RelayCounters;
use crate::utils::{transaction_source_tag, SamplingResult};
use relay_general::store::utils::{
    extract_http_status_code, extract_transaction_op, get_eventuser_tag,
};

pub mod types;

/// Extract transaction status, defaulting to [`SpanStatus::Unknown`].
/// Must be consistent with `process_trace_context` in [`relay_general::store`].
fn extract_transaction_status(trace_context: &TraceContext) -> SpanStatus {
    *trace_context.status.value().unwrap_or(&SpanStatus::Unknown)
}

/// Extract HTTP method
/// See <https://github.com/getsentry/snuba/blob/2e038c13a50735d58cc9397a29155ab5422a62e5/snuba/datasets/errors_processor.py#L64-L67>.
fn extract_http_method(transaction: &Event) -> Option<String> {
    let request = transaction.request.value()?;
    let method = request.method.value()?;
    Some(method.clone())
}

/// Extract the browser name from the [`BrowserContext`] context.
fn extract_browser_name(event: &Event) -> Option<String> {
    let browser = event.context::<BrowserContext>()?;
    browser.name.value().cloned()
}

/// Extract the OS name from the [`OsContext`] context.
fn extract_os_name(event: &Event) -> Option<String> {
    let os = event.context::<OsContext>()?;
    os.name.value().cloned()
}

/// Extract the GEO country code from the [`relay_general::protocol::User`] context.
fn extract_geo_country_code(event: &Event) -> Option<String> {
    let user = event.user.value()?;
    let geo = user.geo.value()?;
    geo.country_code.value().cloned()
}

fn is_low_cardinality(source: Option<&TransactionSource>) -> bool {
    // `None` is used to mark a legacy SDK that does not send the transaction name,
    // and we assume sends high-cardinality data. See `is_high_cardinality_transaction`.
    let Some(source) = source else { return false };

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

        // Explicit `Unknown` is used to mark a legacy SDK that does not send the transaction name,
        // but we assume sends low-cardinality data. See `is_high_cardinality_transaction`.
        TransactionSource::Unknown => true,

        // Any other value would be an SDK bug or users manually configuring the
        // source, assume high-cardinality and drop.
        TransactionSource::Other(_) => false,
    }
}

/// Decide whether we want to keep the transaction name.
/// High-cardinality sources are excluded to protect our metrics infrastructure.
/// Note that this will produce a discrepancy between metrics and raw transaction data.
fn get_transaction_name(event: &Event) -> Option<String> {
    let original_transaction_name = match event.transaction.value() {
        Some(name) => name,
        None => {
            return None;
        }
    };

    let source = event
        .transaction_info
        .value()
        .and_then(|info| info.source.value());

    let use_original_name = is_low_cardinality(source);

    let name_used;
    let name = if use_original_name {
        name_used = "original";
        Some(original_transaction_name.clone())
    } else {
        // Pick a sentinel based on the transaction source:
        match source {
            None | Some(TransactionSource::Other(_)) => {
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
        source = transaction_source_tag(event),
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
    if let Some(transaction_name) = get_transaction_name(event) {
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

    if let Some(trace_context) = event.context::<TraceContext>() {
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

/// TODO(ja): Doc
#[derive(Debug, Default)]
pub struct ExtractedMetrics {
    /// Metrics associated with the project that the transaction belongs to.
    pub project_metrics: Vec<Metric>,

    /// Metrics associated with the sampling project (a.k.a. root or head project)
    /// which started the trace. `ProcessEnvelopeState::sampling_project_state`.
    pub sampling_metrics: Vec<Metric>,
}

impl ExtractedMetrics {
    /// Extends the set of metrics with the supplied newly extracted metrics.
    pub fn extend(&mut self, other: Self) {
        self.project_metrics.extend(other.project_metrics);
        self.sampling_metrics.extend(other.sampling_metrics);
    }
}

/// A utility that extracts metrics from transactions.
pub struct TransactionExtractor<'a> {
    pub aggregator_config: &'a AggregatorConfig,
    pub config: &'a TransactionMetricsConfig,
    pub transaction_from_dsc: Option<&'a str>,
    pub sampling_result: &'a SamplingResult,
    pub has_profile: bool,
}

impl TransactionExtractor<'_> {
    pub fn extract(&self, event: &Event) -> Result<ExtractedMetrics, ExtractMetricsError> {
        let mut metrics = ExtractedMetrics::default();

        if event.ty.value() != Some(&EventType::Transaction) {
            return Ok(metrics);
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
        if !self
            .aggregator_config
            .timestamp_range()
            .contains(&timestamp)
        {
            relay_log::debug!("event timestamp is out of the valid range for metrics");
            return Err(ExtractMetricsError::InvalidTimestamp);
        }

        let tags = extract_universal_tags(event, self.config);

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

                metrics.project_metrics.push(
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
                        metrics.project_metrics.push(
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
        metrics.project_metrics.push(
            TransactionMetric::Duration {
                unit: DurationUnit::MilliSecond,
                value: relay_common::chrono_to_positive_millis(end - start),
                tags: TransactionDurationTags {
                    has_profile: self.has_profile,
                    universal_tags: tags.clone(),
                },
            }
            .into_metric(timestamp),
        );

        let root_counter_tags = {
            let mut universal_tags = CommonTags(BTreeMap::default());
            if let Some(transaction_from_dsc) = self.transaction_from_dsc {
                universal_tags
                    .0
                    .insert(CommonTag::Transaction, transaction_from_dsc.to_string());
            }
            TransactionCPRTags {
                decision: match self.sampling_result {
                    SamplingResult::Keep => "keep".to_owned(),
                    SamplingResult::Drop(_) => "drop".to_owned(),
                },
                universal_tags,
            }
        };
        // Count the transaction towards the root
        metrics.sampling_metrics.push(
            TransactionMetric::CountPerRootProject {
                value: 1.0,
                tags: root_counter_tags,
            }
            .into_metric(timestamp),
        );

        // User
        if let Some(user) = event.user.value() {
            if let Some(value) = get_eventuser_tag(user) {
                metrics
                    .project_metrics
                    .push(TransactionMetric::User { value, tags }.into_metric(timestamp));
            }
        }

        Ok(metrics)
    }
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
    use relay_dynamic_config::AcceptTransactionNames;
    use relay_general::protocol::{Contexts, Timestamp, User};
    use relay_general::store::{
        self, set_default_transaction_source, BreakdownsConfig, LightNormalizationConfig,
        MeasurementsConfig,
    };
    use relay_general::types::Annotated;
    use relay_metrics::MetricValue;

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
            "release": "1.2.3",
            "dist": "foo ",
            "environment": "fake_environment",
            "transaction": "gEt /api/:version/users/",
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
                    "op": "mYOp",
                    "status": "ok"
                },
                "browser": {
                    "name": "Chrome"
                },
                "os": {
                    "name": "Windows"
                }
            },
            "request": {
                "method": "post"
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

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        let res = store::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                breakdowns_config: Some(&breakdowns_config),
                enrich_spans: false,
                light_normalize_spans: true,
                ..Default::default()
            },
        );
        assert!(res.is_ok());

        let aggregator_config = aggregator_config();
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(event.value().unwrap().spans, @r#"
        [
            Span {
                timestamp: Timestamp(
                    2020-08-21T02:18:22Z,
                ),
                start_timestamp: Timestamp(
                    2020-08-21T02:18:20Z,
                ),
                exclusive_time: 2000.0,
                description: "<OrganizationContext>",
                op: "react.mount",
                span_id: SpanId(
                    "bd429c44b67a3eb4",
                ),
                parent_span_id: SpanId(
                    "8f5a2b8768cafb4e",
                ),
                trace_id: TraceId(
                    "ff62a8b040f340bda5d830223def1d81",
                ),
                status: ~,
                tags: ~,
                origin: ~,
                data: ~,
                other: {},
            },
        ]
        "#);

        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
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
                    "http.method": "post",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "gEt /api/:version/users/",
                    "transaction.op": "mYOp",
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
                    "http.method": "post",
                    "measurement_rating": "meh",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "gEt /api/:version/users/",
                    "transaction.op": "mYOp",
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
                    "http.method": "post",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "gEt /api/:version/users/",
                    "transaction.op": "mYOp",
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
                    "http.method": "post",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "gEt /api/:version/users/",
                    "transaction.op": "mYOp",
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
                    "http.method": "post",
                    "os.name": "Windows",
                    "platform": "javascript",
                    "release": "1.2.3",
                    "transaction": "gEt /api/:version/users/",
                    "transaction.op": "mYOp",
                    "transaction.status": "ok",
                },
            },
        ]
        "#);
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
        assert_eq!(res, Ok(()));

        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
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
                    "transaction": "<unlabeled transaction>",
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
                    "transaction": "<unlabeled transaction>",
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
                    "transaction": "<unlabeled transaction>",
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
                    "transaction": "<unlabeled transaction>",
                    "transaction.status": "unknown",
                },
            },
        ]
        "#);
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

        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
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
                    "transaction": "<unlabeled transaction>",
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
                    "transaction": "<unlabeled transaction>",
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
                    "transaction": "<unlabeled transaction>",
                    "transaction.status": "unknown",
                },
            },
        ]
        "#);
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
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        assert_eq!(extracted.project_metrics.len(), 1);

        let duration_metric = &extracted.project_metrics[0];
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
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
        [
            Metric {
                name: "d:transactions/measurements.a_custom1@none",
                value: Distribution(
                    41.0,
                ),
                timestamp: UnixTimestamp(1619420402),
                tags: {
                    "platform": "other",
                    "transaction": "foo",
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
                    "transaction": "foo",
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
                    "transaction": "foo",
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
                    "transaction": "foo",
                    "transaction.status": "unknown",
                },
            },
        ]
        "#);
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

        let event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();

        assert_eq!(extracted.project_metrics.len(), 1);
        assert_eq!(
            extracted.project_metrics[0].name,
            "d:transactions/duration@millisecond"
        );
        assert_eq!(
            extracted.project_metrics[0].tags,
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

        let event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();

        assert_eq!(extracted.project_metrics.len(), 1);
        assert_eq!(
            extracted.project_metrics[0].name,
            "d:transactions/duration@millisecond"
        );
        assert_eq!(
            extracted.project_metrics[0].tags,
            BTreeMap::from([
                ("transaction.status".to_string(), "unknown".to_string()),
                ("platform".to_string(), "other".to_string())
            ])
        );
    }

    #[test]
    fn test_span_tags() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
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
                        "http.response.status_code": "200"
                    }
                }
            ]
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();

        assert_eq!(extracted.project_metrics.len(), 1);
        assert_eq!(
            extracted.project_metrics[0].name,
            "d:transactions/duration@millisecond"
        );
        assert_eq!(
            extracted.project_metrics[0].tags,
            BTreeMap::from([
                ("transaction.status".to_string(), "unknown".to_string()),
                ("platform".to_string(), "other".to_string()),
                ("http.status_code".to_string(), "200".to_string())
            ])
        );
    }

    #[test]
    fn test_expired_timestamp() {
        let timestamp = Timestamp(chrono::Utc::now() - chrono::Duration::seconds(7200));

        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(timestamp),
            start_timestamp: Annotated::new(timestamp),
            contexts: Annotated::new({
                let mut contexts = Contexts::new();
                contexts.add(TraceContext::default());
                contexts
            }),
            ..Default::default()
        });

        let config = TransactionMetricsConfig::default();
        let aggregator_config = AggregatorConfig {
            max_secs_in_past: 3600,
            ..Default::default()
        };
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let err = extractor.extract(event.value().unwrap()).unwrap_err();
        assert_eq!(err, ExtractMetricsError::InvalidTimestamp);
    }

    /// Helper function to check if the transaction name is set correctly
    fn extract_transaction_name(json: &str) -> Option<String> {
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        // Logic from `set_default_transaction_source` was previously duplicated in
        // `extract_transaction_metrics`. Add it here such that tests can remain.
        set_default_transaction_source(event.value_mut().as_mut().unwrap());

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();

        assert_eq!(extracted.project_metrics.len(), 1);
        extracted.project_metrics[0]
            .tags
            .get("transaction")
            .cloned()
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
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("root_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.sampling_metrics, @r#"
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
        "#);
    }

    #[test]
    fn test_legacy_js_looks_like_url() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo/",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.browser"}
        }
        "#;

        let name = extract_transaction_name(json);
        assert!(name.is_none());
    }

    #[test]
    fn test_legacy_js_does_not_look_like_url() {
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

        let name = extract_transaction_name(json);
        assert_eq!(name.as_deref(), Some("foo"));
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

        let name = extract_transaction_name(json);
        assert_eq!(name, Some("<< unparameterized >>".to_owned()));
    }

    #[test]
    fn test_python_404() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo/",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.python", "integrations":["django"]},
            "tags": {"http.status_code": "404"}
        }
        "#;

        let name = extract_transaction_name(json);
        assert!(name.is_none());
    }

    #[test]
    fn test_python_200() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo/",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.python", "integrations":["django"]},
            "tags": {"http.status_code": "200"}
        }
        "#;

        let name = extract_transaction_name(json);
        assert_eq!(name, Some("foo/".to_owned()));
    }

    #[test]
    fn test_express_options() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo/",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.node", "integrations":["Express"]},
            "request": {"method": "OPTIONS"}
        }
        "#;

        let name = extract_transaction_name(json);
        assert!(name.is_none());
    }

    #[test]
    fn test_express() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo/",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "sentry.javascript.node", "integrations":["Express"]},
            "request": {"method": "GET"}
        }
        "#;

        let name = extract_transaction_name(json);
        assert_eq!(name, Some("foo/".to_owned()));
    }

    #[test]
    fn test_other_client_unknown() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo/",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {"trace": {}},
            "sdk": {"name": "some_client"}
        }
        "#;

        let name = extract_transaction_name(json);
        assert_eq!(name.as_deref(), Some("foo/"));
    }

    #[test]
    fn test_other_client_url() {
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

        let name = extract_transaction_name(json);
        assert_eq!(name, Some("<< unparameterized >>".to_owned()));
    }

    #[test]
    fn test_any_client_route() {
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

        let name = extract_transaction_name(json);
        assert_eq!(name, Some("foo".to_owned()));
    }

    #[test]
    fn test_parse_transaction_name_strategy() {
        for (config_str, expected_strategy) in [
            (r#"{}"#, AcceptTransactionNames::ClientBased),
            (
                r#"{"acceptTransactionNames": "unknown-strategy"}"#,
                AcceptTransactionNames::ClientBased,
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
            let config: TransactionMetricsConfig = serde_json::from_str(config_str).unwrap();
            assert_eq!(config.deprecated1, expected_strategy, "{}", config_str);
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

        let mut event = Annotated::from_json(json).unwrap();
        // Normalize first, to make sure that the metrics were computed:
        let _ = store::light_normalize_event(&mut event, LightNormalizationConfig::default());

        let config = TransactionMetricsConfig::default();
        let aggregator_config = aggregator_config();
        let extractor = TransactionExtractor {
            aggregator_config: &aggregator_config,
            config: &config,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Keep,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();

        let metrics_names: Vec<_> = extracted
            .project_metrics
            .into_iter()
            .map(|m| m.name)
            .collect();

        insta::assert_debug_snapshot!(metrics_names, @r#"
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
        "#);
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
