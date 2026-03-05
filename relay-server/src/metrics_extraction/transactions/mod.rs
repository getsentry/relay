use std::collections::BTreeMap;

use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_dynamic_config::CombinedMetricExtractionConfig;
use relay_event_schema::protocol::Event;
use relay_metrics::Bucket;
use relay_sampling::evaluation::SamplingDecision;

use crate::metrics_extraction::IntoMetric;
use crate::metrics_extraction::generic;
use crate::metrics_extraction::transactions::types::{
    CommonTag, CommonTags, ExtractMetricsError, TransactionCPRTags, TransactionMetric,
};

pub mod types;

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

/// A utility that extracts metrics from transactions.
pub struct TransactionExtractor<'a> {
    pub generic_config: Option<CombinedMetricExtractionConfig<'a>>,
    pub transaction_from_dsc: Option<&'a str>,
    pub sampling_decision: SamplingDecision,
    pub target_project_id: ProjectId,
}

impl TransactionExtractor<'_> {
    pub fn extract(&self, event: &Event) -> Result<ExtractedMetrics, ExtractMetricsError> {
        let mut metrics = ExtractedMetrics::default();

        if event.ty.value() != Some(&EventType::Transaction) {
            return Ok(metrics);
        }

        let Some(&end) = event.timestamp.value() else {
            relay_log::debug!("failed to extract the end timestamp from the event");
            return Err(ExtractMetricsError::MissingTimestamp);
        };

        let Some(timestamp) = UnixTimestamp::from_datetime(end.into_inner()) else {
            relay_log::debug!("event timestamp is not a valid unix timestamp");
            return Err(ExtractMetricsError::InvalidTimestamp);
        };

        // Internal usage counter
        metrics
            .project_metrics
            .push(TransactionMetric::Usage.into_metric(timestamp));

        let root_counter_tags = {
            let mut universal_tags = CommonTags(BTreeMap::default());
            if let Some(transaction_from_dsc) = self.transaction_from_dsc {
                universal_tags
                    .0
                    .insert(CommonTag::Transaction, transaction_from_dsc.to_owned());
            }

            TransactionCPRTags {
                decision: self.sampling_decision.to_string(),
                target_project_id: self.target_project_id,
                universal_tags,
            }
        };
        // Count the transaction towards the root
        metrics.sampling_metrics.push(
            TransactionMetric::CountPerRootProject {
                tags: root_counter_tags,
            }
            .into_metric(timestamp),
        );

        // Apply shared tags from generic metric extraction. Transaction metrics will adopt generic
        // metric extraction, after which this is done automatically.
        if let Some(generic_config) = self.generic_config {
            generic::tmp_apply_tags(&mut metrics.project_metrics, event, generic_config.tags());
            generic::tmp_apply_tags(&mut metrics.sampling_metrics, event, generic_config.tags());
        }

        Ok(metrics)
    }
}

#[cfg(test)]
mod tests {
    use relay_dynamic_config::{
        CombinedMetricExtractionConfig, MetricExtractionConfig, TagMapping,
        TransactionMetricsConfig,
    };
    use relay_event_normalization::{
        CombinedMeasurementsConfig, MeasurementsConfig, NormalizationConfig,
        PerformanceScoreConfig, PerformanceScoreProfile, PerformanceScoreWeightedComponent,
        normalize_event,
    };
    use relay_protocol::{Annotated, RuleCondition};

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
                "bogus": "absolutely",
                "device.class": "1"
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
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "8f5a2b8768cafb4e",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                }
             ]
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();

        let breakdowns_config: relay_event_normalization::BreakdownsConfig =
            serde_json::from_str(
                r#"{
                "span_ops": {
                    "type": "spanOperations",
                    "matches": ["react.mount"]
                }
            }"#,
            )
            .unwrap();

        relay_event_normalization::validate_event(
            &mut event,
            &relay_event_normalization::EventValidationConfig::default(),
        )
        .unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                breakdowns_config: Some(&breakdowns_config),
                enrich_spans: false,
                performance_score: Some(&PerformanceScoreConfig {
                    profiles: vec![PerformanceScoreProfile {
                        name: Some("".into()),
                        score_components: vec![PerformanceScoreWeightedComponent {
                            measurement: "lcp".into(),
                            weight: 0.5.try_into().unwrap(),
                            p10: 2.0.try_into().unwrap(),
                            p50: 3.0.try_into().unwrap(),
                            optional: false,
                        }],
                        condition: Some(RuleCondition::all()),
                        version: Some("alpha".into()),
                    }],
                }),
                ..Default::default()
            },
        );

        let extractor = TransactionExtractor {
            generic_config: None,
            transaction_from_dsc: Some("test_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(4711),
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(event.value().unwrap().spans, @r###"
        [
            Span {
                timestamp: Timestamp(
                    2020-08-21T02:18:22Z,
                ),
                start_timestamp: Timestamp(
                    2020-08-21T02:18:20Z,
                ),
                exclusive_time: 2000.0,
                op: "react.mount",
                span_id: SpanId("8f5a2b8768cafb4e"),
                parent_span_id: SpanId("bd429c44b67a3eb4"),
                trace_id: TraceId("ff62a8b040f340bda5d830223def1d81"),
                segment_id: ~,
                is_segment: ~,
                is_remote: ~,
                status: ~,
                description: "<OrganizationContext>",
                tags: ~,
                origin: ~,
                profile_id: ~,
                data: ~,
                links: ~,
                sentry_tags: ~,
                received: ~,
                measurements: ~,
                platform: ~,
                was_transaction: ~,
                kind: ~,
                performance_issues_spans: ~,
                other: {},
            },
        ]
        "###);

        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: MetricName(
                    "c:transactions/usage@none",
                ),
                value: Counter(
                    1.0,
                ),
                tags: {},
                metadata: BucketMetadata {
                    merges: 1,
                    received_at: Some(
                        UnixTimestamp(0),
                    ),
                    extracted_from_indexed: false,
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

        // Normalize first, to make sure the units are correct:
        let mut event = Annotated::from_json(json).unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());

        let extractor = TransactionExtractor {
            generic_config: None,
            transaction_from_dsc: Some("test_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(4711),
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: MetricName(
                    "c:transactions/usage@none",
                ),
                value: Counter(
                    1.0,
                ),
                tags: {},
                metadata: BucketMetadata {
                    merges: 1,
                    received_at: Some(
                        UnixTimestamp(0),
                    ),
                    extracted_from_indexed: false,
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

        // Normalize first, to make sure the units are correct:
        let mut event = Annotated::from_json(json).unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());

        let extractor = TransactionExtractor {
            generic_config: None,
            transaction_from_dsc: Some("test_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(4711),
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: MetricName(
                    "c:transactions/usage@none",
                ),
                value: Counter(
                    1.0,
                ),
                tags: {},
                metadata: BucketMetadata {
                    merges: 1,
                    received_at: Some(
                        UnixTimestamp(0),
                    ),
                    extracted_from_indexed: false,
                },
            },
        ]
        "#);
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

        let config = CombinedMeasurementsConfig::new(Some(&measurements_config), None);

        normalize_event(
            &mut event,
            &NormalizationConfig {
                measurements: Some(config),
                ..Default::default()
            },
        );

        let extractor = TransactionExtractor {
            generic_config: None,
            transaction_from_dsc: Some("test_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(4711),
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420402),
                width: 0,
                name: MetricName(
                    "c:transactions/usage@none",
                ),
                value: Counter(
                    1.0,
                ),
                tags: {},
                metadata: BucketMetadata {
                    merges: 1,
                    received_at: Some(
                        UnixTimestamp(0),
                    ),
                    extracted_from_indexed: false,
                },
            },
        ]
        "#);
    }

    #[test]
    fn test_span_tags() {
        // Status is normalized upstream in the normalization step.
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

        let extractor = TransactionExtractor {
            generic_config: None,
            transaction_from_dsc: Some("test_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(4711),
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: MetricName(
                    "c:transactions/usage@none",
                ),
                value: Counter(
                    1.0,
                ),
                tags: {},
                metadata: BucketMetadata {
                    merges: 1,
                    received_at: Some(
                        UnixTimestamp(0),
                    ),
                    extracted_from_indexed: false,
                },
            },
        ]
        "#);
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

        let extractor = TransactionExtractor {
            generic_config: None,
            transaction_from_dsc: Some("root_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(4711),
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.sampling_metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420400),
                width: 0,
                name: MetricName(
                    "c:transactions/count_per_root_project@none",
                ),
                value: Counter(
                    1.0,
                ),
                tags: {
                    "decision": "keep",
                    "target_project_id": "4711",
                    "transaction": "root_transaction",
                },
                metadata: BucketMetadata {
                    merges: 1,
                    received_at: Some(
                        UnixTimestamp(0),
                    ),
                    extracted_from_indexed: false,
                },
            },
        ]
        "###);
    }

    #[test]
    fn test_parse_transaction_name_strategy() {
        use relay_dynamic_config::AcceptTransactionNames;

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
            assert_eq!(config.deprecated1, expected_strategy, "{config_str}");
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
        normalize_event(&mut event, &NormalizationConfig::default());

        let extractor = TransactionExtractor {
            generic_config: None,
            transaction_from_dsc: Some("test_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(4711),
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();

        let metrics_names: Vec<_> = extracted
            .project_metrics
            .into_iter()
            .map(|m| m.name)
            .collect();

        insta::assert_debug_snapshot!(metrics_names, @r#"
        [
            MetricName(
                "c:transactions/usage@none",
            ),
        ]
        "#);
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
        let generic_config = MetricExtractionConfig {
            version: 1,
            tags: generic_tags,
            ..Default::default()
        };
        let combined_config = CombinedMetricExtractionConfig::from(&generic_config);

        let extractor = TransactionExtractor {
            generic_config: Some(combined_config),
            transaction_from_dsc: Some("test_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(4711),
        };

        let extracted = extractor.extract(event.value().unwrap()).unwrap();
        insta::assert_debug_snapshot!(extracted.project_metrics, @r#"
        [
            Bucket {
                timestamp: UnixTimestamp(1619420402),
                width: 0,
                name: MetricName(
                    "c:transactions/usage@none",
                ),
                value: Counter(
                    1.0,
                ),
                tags: {},
                metadata: BucketMetadata {
                    merges: 1,
                    received_at: Some(
                        UnixTimestamp(0),
                    ),
                    extracted_from_indexed: false,
                },
            },
        ]
        "#);
    }
}
