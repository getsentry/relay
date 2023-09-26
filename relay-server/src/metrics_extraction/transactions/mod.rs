use std::collections::BTreeMap;

use relay_base_schema::events::EventType;
use relay_common::time::UnixTimestamp;
use relay_dynamic_config::{TagMapping, TransactionMetricsConfig};
use relay_event_normalization::utils as normalize_utils;
use relay_event_schema::protocol::{
    AsPair, BrowserContext, Event, OsContext, TraceContext, TransactionSource,
};
use relay_metrics::{Bucket, DurationUnit};

use crate::metrics_extraction::generic;
use crate::metrics_extraction::transactions::types::{
    CommonTag, CommonTags, ExtractMetricsError, TransactionCPRTags, TransactionDurationTags,
    TransactionMeasurementTags, TransactionMetric,
};
use crate::metrics_extraction::IntoMetric;
use crate::statsd::RelayCounters;
use crate::utils::{self, SamplingResult};

pub mod types;

/// Placeholder for transaction names in metrics that is used when SDKs are likely sending high
/// cardinality data such as raw URLs.
const PLACEHOLDER_UNPARAMETERIZED: &str = "<< unparameterized >>";

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

/// Extract the GEO country code from the [`relay_event_schema::protocol::User`] context.
fn extract_geo_country_code(event: &Event) -> Option<String> {
    let user = event.user.value()?;
    let geo = user.geo.value()?;
    geo.country_code.value().cloned()
}

fn is_low_cardinality(source: &TransactionSource) -> bool {
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
    let original = event.transaction.value()?;

    let source = event
        .transaction_info
        .value()
        .and_then(|info| info.source.value());

    match source {
        Some(source) if is_low_cardinality(source) => Some(original.clone()),
        Some(TransactionSource::Other(_)) | None => None,
        Some(_) => Some(PLACEHOLDER_UNPARAMETERIZED.to_owned()),
    }
}

fn track_transaction_name_stats(event: &Event) {
    let name_used = match get_transaction_name(event).as_deref() {
        Some(self::PLACEHOLDER_UNPARAMETERIZED) => "placeholder",
        Some(_) => "original",
        None => "none",
    };

    relay_statsd::metric!(
        counter(RelayCounters::MetricsTransactionNameExtracted) += 1,
        source = utils::transaction_source_tag(event),
        sdk_name = event
            .client_sdk
            .value()
            .and_then(|sdk| sdk.name.as_str())
            .unwrap_or_default(),
        name_used = name_used,
    );
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
    // `relay_event_normalization::is_valid_platform` into light normalization.
    let platform = match event.platform.as_str() {
        Some(platform) if relay_event_normalization::is_valid_platform(platform) => platform,
        _ => "other",
    };

    tags.insert(CommonTag::Platform, platform.to_string());

    if let Some(trace_context) = event.context::<TraceContext>() {
        // We assume that the trace context status is automatically set to unknown inside of the
        // light event normalization step.
        if let Some(status) = trace_context.status.value() {
            tags.insert(CommonTag::TransactionStatus, status.to_string());
        }

        if let Some(op) = normalize_utils::extract_transaction_op(trace_context) {
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

    if let Some(status_code) = normalize_utils::extract_http_status_code(event) {
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

/// Metrics extracted from an envelope.
///
/// Metric extraction derives pre-computed metrics (time series data) from payload items in
/// envelopes. Depending on their semantics, these metrics can be ingested into the same project as
/// the envelope or a different project.
#[derive(Debug, Default)]
pub struct ExtractedMetrics {
    /// Metrics associated with the project of the envelope.
    pub project_metrics: Vec<Bucket>,

    /// Metrics associated with the project of the trace parent.
    pub sampling_metrics: Vec<Bucket>,
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
    pub config: &'a TransactionMetricsConfig,
    pub generic_tags: &'a [TagMapping],
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

        let (Some(&start), Some(&end)) = (event.start_timestamp.value(), event.timestamp.value())
        else {
            relay_log::debug!("failed to extract the start and the end timestamps from the event");
            return Err(ExtractMetricsError::MissingTimestamp);
        };

        let Some(timestamp) = UnixTimestamp::from_datetime(end.into_inner()) else {
            relay_log::debug!("event timestamp is not a valid unix timestamp");
            return Err(ExtractMetricsError::InvalidTimestamp);
        };

        track_transaction_name_stats(event);
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
                value: relay_common::time::chrono_to_positive_millis(end - start),
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
                decision: if self.sampling_result.should_keep() {
                    "keep".to_owned()
                } else {
                    "drop".to_owned()
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
            if let Some(value) = normalize_utils::get_eventuser_tag(user) {
                metrics
                    .project_metrics
                    .push(TransactionMetric::User { value, tags }.into_metric(timestamp));
            }
        }

        // Apply shared tags from generic metric extraction. Transaction metrics will adopt generic
        // metric extraction, after which this is done automatically.
        generic::tmp_apply_tags(&mut metrics.project_metrics, event, self.generic_tags);
        generic::tmp_apply_tags(&mut metrics.sampling_metrics, event, self.generic_tags);

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
    use relay_event_normalization::{
        set_default_transaction_source, BreakdownsConfig, DynamicMeasurementsConfig,
        LightNormalizationConfig, MeasurementsConfig,
    };
    use relay_event_schema::protocol::User;
    use relay_metrics::BucketValue;
    use relay_protocol::Annotated;

    use super::*;

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

        let mut event = Annotated::from_json(json).unwrap();

        let breakdowns_config: BreakdownsConfig = serde_json::from_str(
            r#"{
                "span_ops": {
                    "type": "spanOperations",
                    "matches": ["react.mount"]
                }
            }"#,
        )
        .unwrap();

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        relay_event_normalization::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                breakdowns_config: Some(&breakdowns_config),
                enrich_spans: false,
                light_normalize_spans: true,
                ..Default::default()
            },
        )
        .unwrap();

        let config: TransactionMetricsConfig = serde_json::from_str(
            r#"{
                "version": 1,
                "extractCustomTags": ["fOO"]
            }"#,
        )
        .unwrap();

        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
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
                segment_id: ~,
                is_segment: ~,
                status: ~,
                tags: ~,
                origin: ~,
                data: ~,
                other: {},
            },
        ]
        "#);

        insta::assert_debug_snapshot!(extracted.project_metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/measurements.foo@none",
                value: Distribution(
                    [
                        420.69,
                    ],
                ),
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
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/measurements.lcp@millisecond",
                value: Distribution(
                    [
                        3000.0,
                    ],
                ),
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
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/breakdowns.span_ops.ops.react.mount@millisecond",
                value: Distribution(
                    [
                        2000.0,
                    ],
                ),
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
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    [
                        59000.0,
                    ],
                ),
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
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "s:transactions/user@none",
                value: Set(
                    {
                        933084975,
                    },
                ),
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

        // Normalize first, to make sure the units are correct:
        let mut event = Annotated::from_json(json).unwrap();
        relay_event_normalization::light_normalize_event(
            &mut event,
            LightNormalizationConfig::default(),
        )
        .unwrap();

        let config = TransactionMetricsConfig::default();
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/measurements.fcp@millisecond",
                value: Distribution(
                    [
                        1.1,
                    ],
                ),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                    "transaction": "<unlabeled transaction>",
                    "transaction.status": "unknown",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/measurements.foo@none",
                value: Distribution(
                    [
                        8.8,
                    ],
                ),
                tags: {
                    "platform": "other",
                    "transaction": "<unlabeled transaction>",
                    "transaction.status": "unknown",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/measurements.stall_count@none",
                value: Distribution(
                    [
                        3.3,
                    ],
                ),
                tags: {
                    "platform": "other",
                    "transaction": "<unlabeled transaction>",
                    "transaction.status": "unknown",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    [
                        59000.0,
                    ],
                ),
                tags: {
                    "platform": "other",
                    "transaction": "<unlabeled transaction>",
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

        // Normalize first, to make sure the units are correct:
        let mut event = Annotated::from_json(json).unwrap();
        relay_event_normalization::light_normalize_event(
            &mut event,
            LightNormalizationConfig::default(),
        )
        .unwrap();

        let config: TransactionMetricsConfig = TransactionMetricsConfig::default();
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/measurements.fcp@second",
                value: Distribution(
                    [
                        1.1,
                    ],
                ),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                    "transaction": "<unlabeled transaction>",
                    "transaction.status": "unknown",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/measurements.lcp@none",
                value: Distribution(
                    [
                        2.2,
                    ],
                ),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                    "transaction": "<unlabeled transaction>",
                    "transaction.status": "unknown",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    [
                        59000.0,
                    ],
                ),
                tags: {
                    "platform": "other",
                    "transaction": "<unlabeled transaction>",
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
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        assert_eq!(extracted.project_metrics.len(), 1);

        let duration_metric = &extracted.project_metrics[0];
        assert_eq!(duration_metric.name, "d:transactions/duration@millisecond");
        assert_eq!(duration_metric.value, BucketValue::distribution(59000.0));

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

        let config = DynamicMeasurementsConfig::new(Some(&measurements_config), None);

        relay_event_normalization::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                measurements: Some(config),
                ..Default::default()
            },
        )
        .unwrap();

        let config = TransactionMetricsConfig::default();
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420402),
                width: 0,
                name: "d:transactions/measurements.a_custom1@none",
                value: Distribution(
                    [
                        41.0,
                    ],
                ),
                tags: {
                    "platform": "other",
                    "transaction": "foo",
                    "transaction.status": "unknown",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420402),
                width: 0,
                name: "d:transactions/measurements.fcp@millisecond",
                value: Distribution(
                    [
                        0.123,
                    ],
                ),
                tags: {
                    "measurement_rating": "good",
                    "platform": "other",
                    "transaction": "foo",
                    "transaction.status": "unknown",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420402),
                width: 0,
                name: "d:transactions/measurements.g_custom2@second",
                value: Distribution(
                    [
                        42.0,
                    ],
                ),
                tags: {
                    "platform": "other",
                    "transaction": "foo",
                    "transaction.status": "unknown",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420402),
                width: 0,
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    [
                        2000.0,
                    ],
                ),
                tags: {
                    "platform": "other",
                    "transaction": "foo",
                    "transaction.status": "unknown",
                },
            },
        ]
        "###);
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
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
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
            "contexts": {
                "trace": {
                    "status": "ok"
                }
            }
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
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
                ("transaction.status".to_string(), "ok".to_string()),
                ("platform".to_string(), "other".to_string())
            ])
        );
    }

    #[test]
    fn test_span_tags() {
        // Status is normalized upstream in the light normalization step.
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "status": "ok"
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
                        "http.response.status_code": "200"
                    }
                }
            ]
        }
        "#;

        let event = Annotated::from_json(json).unwrap();

        let config = TransactionMetricsConfig::default();
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
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
                ("transaction.status".to_string(), "ok".to_string()),
                ("platform".to_string(), "other".to_string()),
                ("http.status_code".to_string(), "200".to_string())
            ])
        );
    }

    /// Helper function to check if the transaction name is set correctly
    fn extract_transaction_name(json: &str) -> Option<String> {
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        // Logic from `set_default_transaction_source` was previously duplicated in
        // `extract_transaction_metrics`. Add it here such that tests can remain.
        set_default_transaction_source(event.value_mut().as_mut().unwrap());

        let config = TransactionMetricsConfig::default();
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
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
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("root_transaction"),
            sampling_result: &SamplingResult::Pending,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.sampling_metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: "c:transactions/count_per_root_project@none",
                value: Counter(
                    1.0,
                ),
                tags: {
                    "decision": "keep",
                    "transaction": "root_transaction",
                },
            },
        ]
        "###);
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
        let _ = relay_event_normalization::light_normalize_event(
            &mut event,
            LightNormalizationConfig::default(),
        );

        let config = TransactionMetricsConfig::default();
        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &[],
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
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

        assert_eq!(
            normalize_utils::get_eventuser_tag(&user).unwrap(),
            "id:ident"
        );

        let user = User {
            username: Annotated::new("username".to_owned()),
            email: Annotated::new("email".to_owned()),
            ip_address: Annotated::new("127.0.0.1".parse().unwrap()),
            ..User::default()
        };

        assert_eq!(
            normalize_utils::get_eventuser_tag(&user).unwrap(),
            "username:username"
        );

        let user = User {
            email: Annotated::new("email".to_owned()),
            ip_address: Annotated::new("127.0.0.1".parse().unwrap()),
            ..User::default()
        };

        assert_eq!(
            normalize_utils::get_eventuser_tag(&user).unwrap(),
            "email:email"
        );

        let user = User {
            ip_address: Annotated::new("127.0.0.1".parse().unwrap()),
            ..User::default()
        };

        assert_eq!(
            normalize_utils::get_eventuser_tag(&user).unwrap(),
            "ip:127.0.0.1"
        );

        let user = User::default();

        assert!(normalize_utils::get_eventuser_tag(&user).is_none());
    }

    #[test]
    fn test_conditional_tagging() {
        let event = Annotated::from_json(
            r#"{
                "type": "transaction",
                "platform": "javascript",
                "transaction": "foo",
                "start_timestamp": "2021-04-26T08:00:00+0100",
                "timestamp": "2021-04-26T08:00:02+0100",
                "measurements": {
                    "lcp": {"value": 41, "unit": "millisecond"}
                }
            }"#,
        )
        .unwrap();

        let config = TransactionMetricsConfig::new();
        let generic_tags: Vec<TagMapping> = serde_json::from_str(
            r#"[
                {
                    "metrics": ["d:transactions/duration@millisecond"],
                    "tags": [
                        {
                            "condition": {"op": "gte", "name": "event.duration", "value": 9001},
                            "key": "satisfaction",
                            "value": "frustrated"
                        },
                        {
                            "condition": {"op": "gte", "name": "event.duration", "value": 666},
                            "key": "satisfaction",
                            "value": "tolerated"
                        },
                        {
                            "condition": {"op": "and", "inner": []},
                            "key": "satisfaction",
                            "value": "satisfied"
                        }
                    ]
                }
            ]"#,
        )
        .unwrap();

        let extractor = TransactionExtractor {
            config: &config,
            generic_tags: &generic_tags,
            transaction_from_dsc: Some("test_transaction"),
            sampling_result: &SamplingResult::Pending,
            has_profile: false,
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420402),
                width: 0,
                name: "d:transactions/measurements.lcp@millisecond",
                value: Distribution(
                    [
                        41.0,
                    ],
                ),
                tags: {
                    "measurement_rating": "good",
                    "platform": "javascript",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1619420402),
                width: 0,
                name: "d:transactions/duration@millisecond",
                value: Distribution(
                    [
                        2000.0,
                    ],
                ),
                tags: {
                    "platform": "javascript",
                    "satisfaction": "tolerated",
                },
            },
        ]
        "###);
    }
}
