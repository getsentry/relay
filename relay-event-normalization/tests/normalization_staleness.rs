//! Failing ordering and fallback regressions, and proposed contracts from the dependency audit.
//!
//! These tests intentionally fail on the committed normalizer. The cases in
//! `proposed_contracts` describe candidate normalization and fallback behavior;
//! their intended product behavior is not established merely by the observed discrepancies.

use relay_base_schema::metrics::{FractionUnit, MetricUnit};
use relay_event_normalization::{
    BuiltinMeasurementKey, CombinedMeasurementsConfig, GeoIpLookup, MeasurementsConfig,
    NormalizationConfig, PerformanceScoreConfig, RawUserAgentInfo, normalize_event, replay,
};
use relay_event_schema::protocol::{
    BrowserContext, DeviceClass, DeviceContext, Event, OsContext, Replay,
};
use relay_protocol::{Annotated, SerializableAnnotated, get_value};
use serde_json::{Value, json};

const CHROME_USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

fn normalize(payload: Value, config: &NormalizationConfig<'_>) -> Event {
    let mut event = Annotated::<Event>::from_json(&payload.to_string()).unwrap();
    normalize_event(&mut event, config);
    event.into_value().unwrap()
}

#[test]
fn rejected_frame_count_does_not_produce_a_rate() {
    let measurements = MeasurementsConfig {
        builtin_measurements: vec![
            BuiltinMeasurementKey::new("frames_total", MetricUnit::None),
            BuiltinMeasurementKey::new("frames_slow", MetricUnit::None),
            BuiltinMeasurementKey::new(
                "frames_slow_rate",
                MetricUnit::Fraction(FractionUnit::Ratio),
            ),
        ],
        max_custom_measurements: 0,
    };
    let config = NormalizationConfig {
        measurements: Some(CombinedMeasurementsConfig::new(Some(&measurements), None)),
        ..Default::default()
    };

    // A count is dimensionless. A count carrying a duration unit is rejected.
    for (unit, valid) in [("none", true), ("millisecond", false)] {
        let event = normalize(
            json!({
                "type": "transaction",
                "measurements": {
                    "frames_total": {"value": 100, "unit": unit},
                    "frames_slow": {"value": 10},
                },
            }),
            &config,
        );
        let measurements = event.measurements.value().unwrap();
        assert_eq!(measurements.contains_key("frames_total"), valid);
        assert_eq!(
            measurements.contains_key("frames_slow_rate"),
            valid,
            "A rejected denominator must not leave a derived rate; unit={unit}",
        );
    }
}

#[test]
fn sdk_from_client_header_is_used_to_scrub_transaction_names() {
    let event = normalize(
        json!({"type": "transaction", "transaction": "/users/12345"}),
        &NormalizationConfig {
            client: Some("sentry.javascript.browser/9.0.0".into()),
            ..Default::default()
        },
    );

    assert_eq!(event.sdk_name(), "sentry.javascript.browser");
    assert_eq!(
        event.transaction.as_str(),
        Some("/users/*"),
        "The accepted SDK identifies this legacy transaction name as a raw URL",
    );
}

#[test]
fn partial_sdk_uses_name_from_client_header() {
    let config = NormalizationConfig {
        client: Some("sentry.javascript.browser/9.0.0".into()),
        ..Default::default()
    };
    let control = normalize(json!({}), &config);
    assert_eq!(control.sdk_name(), "sentry.javascript.browser");

    let event = normalize(
        json!({"sdk": {"version": "9.0.0", "integrations": ["BrowserTracing"]}}),
        &config,
    );
    let sdk = event.client_sdk.value().unwrap();
    assert_eq!(sdk.version.as_str(), Some("9.0.0"));
    assert!(sdk.has_integration("BrowserTracing"));
    assert_eq!(
        event.sdk_name(),
        control.sdk_name(),
        "A partial SDK object must not block the matching client header's name fallback",
    );
}

#[test]
fn partial_sdk_uses_version_from_client_header_for_app_start_fixup() {
    let config = NormalizationConfig {
        client: Some("sentry.javascript.react-native/4.4.0".into()),
        ..Default::default()
    };
    let payload = |sdk| {
        json!({
            "type": "transaction",
            "sdk": sdk,
            "spans": [{
                "op": "app_start_cold",
                "span_id": "bbbbbbbbbbbbbbbb",
                "start_timestamp": 1_700_000_000.0,
                "timestamp": 1_700_000_001.0,
            }],
        })
    };
    let control = normalize(
        payload(json!({"name": "sentry.javascript.react-native", "version": "4.4.0"})),
        &config,
    );
    let control_op = control.spans.value().unwrap()[0]
        .value()
        .unwrap()
        .op
        .as_str();
    assert_eq!(control_op, Some("app.start.cold"));

    let event = normalize(
        payload(json!({"name": "sentry.javascript.react-native", "integrations": ["test"]})),
        &config,
    );
    let sdk = event.client_sdk.value().unwrap();
    assert_eq!(sdk.name.as_str(), Some("sentry.javascript.react-native"));
    assert!(sdk.has_integration("test"));
    assert_eq!(
        sdk.version.as_str(),
        Some("4.4.0"),
        "A partial SDK object must not block the matching client header's version fallback",
    );
    assert_eq!(
        event.spans.value().unwrap()[0].value().unwrap().op.as_str(),
        control_op,
        "The recovered version must be available for SDK-specific app-start correction",
    );
}

#[test]
fn replay_uses_submission_user_agent_without_payload_headers() {
    let user_agent = RawUserAgentInfo {
        user_agent: Some(CHROME_USER_AGENT),
        ..Default::default()
    };
    let normalize = |payload: Value| {
        let mut replay = Annotated::<Replay>::from_json(&payload.to_string()).unwrap();
        replay::normalize(&mut replay, None, &user_agent, &GeoIpLookup::empty());
        replay.into_value().unwrap()
    };

    // Empty payload headers already allow the submission request's UA fallback.
    let control = normalize(json!({"platform": "javascript", "request": {"headers": {}}}));
    let control_browser = control.contexts.value().unwrap().get::<BrowserContext>();
    assert_eq!(control_browser.unwrap().name.as_str(), Some("Chrome"));
    assert!(control_browser.unwrap().version.value().is_some());

    for payload in [
        json!({"platform": "javascript"}),
        json!({"platform": "javascript", "request": {}}),
    ] {
        let replay = normalize(payload);
        assert_eq!(
            replay
                .contexts
                .value()
                .and_then(|c| c.get::<BrowserContext>()),
            control_browser,
            "Absent payload headers must not prevent the submission User-Agent fallback",
        );
    }
}

#[test]
fn environment_from_legacy_tag_is_validated() {
    let config = NormalizationConfig::default();
    let direct = normalize(json!({"environment": "bad/environment"}), &config);
    assert!(direct.environment.value().is_none());

    let migrated = normalize(json!({"tags": {"environment": "bad/environment"}}), &config);
    assert!(
        migrated.environment.value().is_none(),
        "Moving an environment from legacy tags must not bypass validation",
    );
}

#[test]
fn environment_from_legacy_tag_rejects_whitespace_only_values() {
    // L038: tag parsing trims this to an empty string before migration.
    let config = NormalizationConfig::default();
    let direct = normalize(json!({"environment": "   "}), &config);
    assert_eq!(direct.environment.as_str(), None);

    let migrated = normalize(json!({"tags": {"environment": "   "}}), &config);
    assert_eq!(migrated.environment.as_str(), direct.environment.as_str());
}

fn transaction(patch: Value) -> Value {
    let mut payload = json!({
        "type": "transaction",
        "start_timestamp": 1_700_000_000.0,
        "timestamp": 1_700_000_005.0,
        "measurements": {"lcp": {"value": 1200}},
    });
    payload
        .as_object_mut()
        .unwrap()
        .extend(patch.as_object().unwrap().clone());
    payload
}

fn error_with_errno(patch: Value) -> Value {
    let mut payload = json!({
        "exception": {"values": [{
            "type": "OSError",
            "mechanism": {"type": "generic", "meta": {"errno": {"number": 2}}},
        }]},
    });
    payload
        .as_object_mut()
        .unwrap()
        .extend(patch.as_object().unwrap().clone());
    payload
}

fn assert_errno_enriched(event: &Event) {
    assert_eq!(
        event.context::<OsContext>().unwrap().name.as_str(),
        Some("Android")
    );
    let exceptions = &event.exceptions;
    assert_eq!(
        get_value!(exceptions.values[0].mechanism.meta.errno.name).map(String::as_str),
        Some("ENOENT"),
        "The OS is known after normalization, but errno enrichment already ran",
    );
}

#[test]
fn errno_name_uses_os_parsed_from_description() {
    let event = normalize(
        error_with_errno(json!({"contexts": {"os": {
            "raw_description": "Android OS 11 / API-30 (RP1A.201005.001)",
        }}})),
        &NormalizationConfig::default(),
    );
    assert_errno_enriched(&event);
}

#[test]
fn errno_name_uses_os_inferred_from_client_hints() {
    let event = normalize(
        error_with_errno(json!({
            "platform": "javascript",
            "request": {"headers": {"Sec-CH-UA-Platform": "\"Android\""}},
        })),
        &NormalizationConfig {
            normalize_user_agent: Some(true),
            ..Default::default()
        },
    );
    assert_errno_enriched(&event);
}

#[test]
fn signal_name_uses_os_parsed_from_description() {
    // L057: signal names are OS-dependent too (7 is SIGBUS on Linux).
    let config = NormalizationConfig::default();
    let payload = |os| {
        json!({
            "contexts": {"os": os},
            "exception": {"values": [{"type": "SignalError", "mechanism": {
                "type": "generic", "meta": {"signal": {"number": 7}},
            }}]},
        })
    };
    let direct = normalize(payload(json!({"name": "Android"})), &config);
    let exceptions = &direct.exceptions;
    assert_eq!(
        get_value!(exceptions.values[0].mechanism.meta.signal.name).map(String::as_str),
        Some("SIGBUS"),
    );

    let inferred = normalize(
        payload(json!({
            "raw_description": "Android OS 11 / API-30 (RP1A.201005.001)",
        })),
        &config,
    );
    assert_eq!(
        inferred.context::<OsContext>().unwrap().name.as_str(),
        Some("Android")
    );
    let exceptions = &inferred.exceptions;
    assert_eq!(
        get_value!(exceptions.values[0].mechanism.meta.signal.name).map(String::as_str),
        Some("SIGBUS"),
    );
}

#[test]
fn signal_code_name_uses_os_parsed_from_description() {
    // L057: Darwin additionally decodes signal.code using the OS hint.
    let config = NormalizationConfig::default();
    let payload = |os| {
        json!({
            "contexts": {"os": os},
            "exception": {"values": [{"type": "SignalError", "mechanism": {
                "type": "generic", "meta": {"signal": {"number": 11, "code": 1}},
            }}]},
        })
    };
    let direct = normalize(payload(json!({"name": "macOS"})), &config);
    let exceptions = &direct.exceptions;
    assert_eq!(
        get_value!(exceptions.values[0].mechanism.meta.signal.code_name).map(String::as_str),
        Some("SEGV_MAPERR"),
    );

    let inferred = normalize(
        payload(json!({
            "raw_description": "Mac OS X 10.14.2 (18C54)",
        })),
        &config,
    );
    assert_eq!(
        inferred.context::<OsContext>().unwrap().name.as_str(),
        Some("macOS")
    );
    let exceptions = &inferred.exceptions;
    assert_eq!(
        get_value!(exceptions.values[0].mechanism.meta.signal.code_name).map(String::as_str),
        Some("SEGV_MAPERR"),
    );
}

#[test]
fn device_class_uses_device_inferred_from_user_agent() {
    let config = NormalizationConfig {
        normalize_user_agent: Some(true),
        ..Default::default()
    };
    let explicit = normalize(
        json!({"platform": "javascript", "contexts": {"device": {
            "family": "iPhone", "model": "iPhone",
        }}}),
        &config,
    );
    assert_eq!(explicit.tag_value("device.class"), Some("3"));

    let inferred = normalize(
        json!({"platform": "javascript", "request": {"headers": {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) - (-)",
        }}}),
        &config,
    );
    let device = inferred.context::<DeviceContext>().unwrap();
    assert_eq!(device.family.as_str(), Some("iPhone"));
    assert_eq!(device.model.as_str(), Some("iPhone"));
    assert_eq!(
        DeviceClass::from_contexts(inferred.contexts.value().unwrap()),
        Some(DeviceClass::HIGH),
    );
    assert_eq!(
        inferred.tag_value("device.class"),
        explicit.tag_value("device.class"),
        "The final inferred device can be classified, but classification already ran",
    );
}

#[test]
fn breakdowns_use_normalized_app_start_ops() {
    let breakdowns = serde_json::from_value(json!({
        "span_ops": {"type": "spanOperations", "matches": ["app.start"]},
    }))
    .unwrap();
    let config = NormalizationConfig {
        breakdowns_config: Some(&breakdowns),
        ..Default::default()
    };
    // The canonical spelling is a passing control for the legacy spelling.
    for op in ["app.start.cold", "app_start_cold"] {
        let event = normalize(
            transaction(json!({
                "sdk": {"name": "sentry.javascript.react-native", "version": "4.4.0"},
                "spans": [{
                    "span_id": "bbbbbbbbbbbbbbbb", "op": op,
                    "start_timestamp": 1_700_000_000.0, "timestamp": 1_700_000_001.0,
                }],
            })),
            &config,
        );
        let span = event.spans.value().unwrap()[0].value().unwrap();
        assert_eq!(span.op.as_str(), Some("app.start.cold"));
        assert_eq!(
            event
                .breakdown("span_ops", "ops.app.start")
                .map(|m| m.to_f64()),
            Some(1000.0),
            "The span op is normalized, but its breakdown used the old spelling; input={op}",
        );
    }
}

#[test]
fn lcp_tag_backfill_rejects_empty_values() {
    let direct = normalize(
        json!({"tags": {"lcp.element": ""}}),
        &NormalizationConfig::default(),
    );
    assert_eq!(direct.tag_value("lcp.element"), None);
    let backfilled = normalize(
        json!({"contexts": {"trace": {"data": {
            "browser.web_vital.lcp.element": "",
        }}}}),
        &NormalizationConfig::default(),
    );
    assert_eq!(
        backfilled.tag_value("lcp.element"),
        None,
        "LCP tags inserted after tag validation must not retain an empty value",
    );
}

mod proposed_contracts {
    use super::*;
    use relay_base_schema::project::ProjectId;
    use relay_event_normalization::{
        BreakdownsConfig, CombinedMeasurementsConfig, MeasurementsConfig, PerformanceScoreConfig,
        SpanOpDefaults,
    };
    use relay_protocol::Getter;
    use relay_sampling::DynamicSamplingContext;

    #[test]
    fn django_transaction_name_uses_response_status_when_status_tag_is_missing() {
        // Proposes using the transaction's own response status for the existing legacy
        // Django heuristic. Explicit sources and existing status tags retain precedence.
        let check = |patch: Value, expected_name: &str, expected_source: &str| {
            let mut payload = transaction(json!({
                "transaction": "/users/12345",
                "sdk": {
                    "name": "sentry.python", "version": "1.0.0", "integrations": ["django"],
                },
            }));
            payload
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            let event = normalize(payload, &NormalizationConfig::default());
            assert_eq!(
                event.transaction.as_str(),
                Some(expected_name),
                "input={patch}"
            );
            assert_eq!(
                event
                    .transaction_info
                    .value()
                    .and_then(|info| info.source.value())
                    .map(|source| source.as_str()),
                Some(expected_source),
                "input={patch}",
            );
        };

        // Passing controls for the existing heuristic and status-tag precedence.
        check(
            json!({"tags": {"http.status_code": "404"}}),
            "/users/*",
            "sanitized",
        );
        check(
            json!({"tags": {"http.status_code": "200"}}),
            "/users/12345",
            "unknown",
        );
        check(
            json!({"contexts": {"response": {"status_code": 200}}}),
            "/users/12345",
            "unknown",
        );
        for (tag_status, response_status, name, source) in [
            ("200", 404, "/users/12345", "unknown"),
            ("404", 200, "/users/*", "sanitized"),
        ] {
            check(
                json!({
                    "tags": {"http.status_code": tag_status},
                    "contexts": {"response": {"status_code": response_status}},
                }),
                name,
                source,
            );
        }

        for source in ["route", "custom"] {
            check(
                json!({
                    "transaction_info": {"source": source},
                    "tags": {"http.status_code": "404"},
                    "contexts": {"response": {"status_code": 404}},
                }),
                "/users/12345",
                source,
            );
        }

        // An outgoing request's 404 must not classify the transaction as a Django 404.
        check(
            json!({"spans": [{
                "span_id": "bbbbbbbbbbbbbbbb", "op": "http.client",
                "start_timestamp": 1_700_000_000.0, "timestamp": 1_700_000_001.0,
                "data": {"http.response.status_code": 404},
            }]}),
            "/users/12345",
            "unknown",
        );
        check(
            json!({
                "sdk": {"name": "sentry.python", "version": "1.0.0", "integrations": ["flask"]},
                "contexts": {"response": {"status_code": 404}},
            }),
            "/users/12345",
            "unknown",
        );

        // Equivalent to the tagged 404 control, but currently the response status is ignored.
        check(
            json!({"contexts": {"response": {"status_code": 404}}}),
            "/users/*",
            "sanitized",
        );
    }

    #[test]
    fn partial_browser_context_uses_version_from_matching_user_agent() {
        // Proposes filling missing fields when the supplied browser name agrees with the UA.
        // An explicitly supplied version remains authoritative, even if the UA disagrees.
        let config = NormalizationConfig {
            normalize_user_agent: Some(true),
            ..Default::default()
        };
        let payload = |contexts| {
            json!({
                "platform": "javascript",
                "request": {"headers": {"User-Agent": CHROME_USER_AGENT}},
                "contexts": contexts,
            })
        };
        let control = normalize(payload(json!({})), &config);
        let control_browser = control.context::<BrowserContext>().unwrap();
        assert_eq!(control_browser.name.as_str(), Some("Chrome"));
        assert!(control_browser.version.value().is_some());

        let explicit = normalize(
            payload(json!({"browser": {"name": "Chrome", "version": "119"}})),
            &config,
        );
        assert_eq!(
            explicit
                .context::<BrowserContext>()
                .unwrap()
                .version
                .as_str(),
            Some("119")
        );

        let partial = normalize(payload(json!({"browser": {"name": "Chrome"}})), &config);
        let browser = partial.context::<BrowserContext>().unwrap();
        assert_eq!(browser.name.as_str(), Some("Chrome"));
        assert_eq!(
            browser.version.as_str(),
            control_browser.version.as_str(),
            "A partial context must not block recovery of the version from a matching User-Agent",
        );
    }

    // OS/runtime tests configure profiles that match values Relay's existing parsers
    // in src/normalize/contexts.rs produce. Scoring runs before those values exist,
    // so the profiles do not match, even though the final event satisfies their conditions.
    fn profile(field: &str, value: Value) -> PerformanceScoreConfig {
        serde_json::from_value(json!({"profiles": [{
            "condition": {
                "op": if value.is_number() { "gte" } else { "eq" },
                "name": field,
                "value": value,
            },
            "scoreComponents": [{
                "measurement": "lcp", "weight": 1.0, "p10": 1000, "p50": 2000,
            }],
        }]}))
        .unwrap()
    }

    #[test]
    fn app_start_backfill_respects_measurement_config() {
        // Should Relay-generated measurements bypass configured unit restrictions?
        for unit in ["millisecond", "none"] {
            let measurements: MeasurementsConfig = serde_json::from_value(json!({
                "builtinMeasurements": [
                    {"name": "app_start_cold", "unit": "millisecond"},
                    {"name": "app.vitals.start.value", "unit": unit},
                ],
                "maxCustomMeasurements": 0,
            }))
            .unwrap();
            let config = NormalizationConfig {
                measurements: Some(CombinedMeasurementsConfig::new(Some(&measurements), None)),
                ..Default::default()
            };
            let explicit = normalize(
                transaction(json!({"measurements": {
                    "app.vitals.start.value": {"value": 1000, "unit": "millisecond"},
                }})),
                &config,
            );
            let backfilled = normalize(
                transaction(json!({"measurements": {
                    "app_start_cold": {"value": 1000, "unit": "millisecond"},
                }})),
                &config,
            );
            assert_eq!(
                explicit.measurement("app.vitals.start.value").is_some(),
                unit == "millisecond"
            );
            assert_eq!(
                backfilled.measurement("app.vitals.start.value").is_some(),
                explicit.measurement("app.vitals.start.value").is_some(),
                "Backfill runs after measurement filtering; configured unit={unit}",
            );
        }
    }

    #[test]
    fn ai_telemetry_uses_defaulted_platform() {
        // Should this metric describe the normalized platform or the raw input?
        let mut final_platform = None;
        let captures = relay_statsd::with_capturing_test_client(|| {
            let event = normalize(
                error_with_errno(json!({"contexts": {"trace": {"op": "gen_ai.invoke_agent"}}})),
                &NormalizationConfig::default(),
            );
            final_platform = event.platform.into_value();
        });
        assert_eq!(final_platform.as_deref(), Some("other"));
        let metric = captures
            .iter()
            .find(|m| m.starts_with("gen_ai.cost_calculation.result:"))
            .unwrap();
        assert!(
            metric.contains("platform:other"),
            "Metric saw the raw platform: {metric}"
        );
    }

    #[test]
    fn score_output_respects_measurement_config() {
        // Like app-start backfill: are generated score fields exempt from configuration?
        let measurements: MeasurementsConfig = serde_json::from_value(json!({
            "builtinMeasurements": [
                {"name": "lcp", "unit": "millisecond"},
                {"name": "score.total", "unit": "millisecond"},
            ], "maxCustomMeasurements": 0,
        }))
        .unwrap();
        let profile = profile("event.sdk.name", json!("sentry.cocoa"));
        let config = NormalizationConfig {
            performance_score: Some(&profile),
            measurements: Some(CombinedMeasurementsConfig::new(Some(&measurements), None)),
            ..Default::default()
        };
        let explicit = normalize(
            transaction(json!({"measurements": {
                "score.total": {"value": 0.5, "unit": "ratio"},
            }})),
            &config,
        );
        assert!(explicit.measurement("score.total").is_none());
        let generated = normalize(
            transaction(json!({"sdk": {"name": "sentry.cocoa", "version": "9.0.0"}})),
            &config,
        );
        assert!(generated.measurement("lcp").is_some());
        assert!(
            generated.measurement("score.total").is_none(),
            "Score generation bypassed configured output units"
        );
    }

    macro_rules! score_case {
        ($name:ident, $patch:expr, $field:literal, $value:literal) => {
            #[test]
            fn $name() {
                let profile = profile($field, json!($value));
                let event = normalize(
                    transaction($patch),
                    &NormalizationConfig {
                        client: Some("sentry.cocoa/9.0.0".into()),
                        performance_score: Some(&profile),
                        ..Default::default()
                    },
                );
                assert_eq!(event.get_value($field).unwrap().as_str(), Some($value));
                assert!(
                    event.measurement("score.total").is_some(),
                    "Profile selection did not see the final value of {}",
                    $field,
                );
            }
        };
    }

    score_case!(
        score_uses_header_sdk,
        json!({}),
        "event.sdk.name",
        "sentry.cocoa"
    );
    // Should an explicitly configured "other" profile score the fallback platform?
    // This test assumes profile conditions see the normalized platform.
    score_case!(
        score_uses_validated_platform,
        json!({"platform": "invalid-platform"}),
        "event.platform",
        "other"
    );
    score_case!(
        score_uses_trimmed_java_logger,
        json!({"platform": "java", "logger": " hello "}),
        "event.logger",
        "hello"
    );
    score_case!(score_uses_default_level, json!({}), "event.level", "info");
    score_case!(score_uses_default_logger, json!({}), "event.logger", "");
    score_case!(
        score_uses_uppercase_request_method,
        json!({"request": {"method": "get"}}),
        "event.request.method",
        "GET"
    );
    score_case!(
        score_uses_normalized_request_url,
        json!({"request": {"url": "https://example.com/path?q=1"}}),
        "event.request.url",
        "https://example.com/path"
    );
    // Should a profile targeting "unknown" score transactions with a defaulted trace status?
    score_case!(
        score_uses_default_trace_status,
        json!({"contexts": {"trace": {}}}),
        "event.contexts.trace.status",
        "unknown"
    );
    score_case!(
        score_uses_parsed_os_name,
        json!({"contexts": {"os": {"raw_description": "Android OS 11 / API-30 (RP1A.201005.001)"}}}),
        "event.contexts.os.name",
        "Android"
    );
    score_case!(
        score_uses_computed_os_display_name,
        json!({"contexts": {"os": {"name": "Android", "version": "11"}}}),
        "event.contexts.os",
        "Android 11"
    );
    score_case!(
        score_uses_parsed_runtime_name,
        json!({"contexts": {"runtime": {"raw_description": ".NET 6.0.0"}}}),
        "event.contexts.runtime.name",
        ".NET"
    );
    score_case!(
        score_uses_computed_browser_display_name,
        json!({"contexts": {"browser": {"name": "Chrome", "version": "120"}}}),
        "event.contexts.browser",
        "Chrome 120"
    );
    score_case!(
        score_uses_device_marketing_name,
        json!({"contexts": {"device": {"model": "SM-G991B"}}}),
        "event.contexts.device.name",
        "Galaxy S21 5G"
    );
    score_case!(
        score_uses_extracted_lcp_tag,
        json!({"contexts": {"trace": {"data": {"browser.web_vital.lcp.element": "img.hero"}}}}),
        "event.tags.lcp.element",
        "img.hero"
    );

    #[test]
    fn score_uses_computed_breakdown() {
        let profile = profile("event.breakdowns.span_ops.ops.db", json!(1000));
        let breakdowns: BreakdownsConfig = serde_json::from_value(json!({
            "span_ops": {"type": "spanOperations", "matches": ["db"]},
        }))
        .unwrap();
        let event = normalize(
            transaction(json!({"spans": [{
                "op": "db", "span_id": "bbbbbbbbbbbbbbbb",
                "start_timestamp": 1_700_000_000.0, "timestamp": 1_700_000_001.0,
            }]})),
            &NormalizationConfig {
                performance_score: Some(&profile),
                breakdowns_config: Some(&breakdowns),
                ..Default::default()
            },
        );
        assert_eq!(
            event.breakdown("span_ops", "ops.db").unwrap().to_f64(),
            1000.0
        );
        assert!(event.measurement("score.total").is_some());
    }

    #[test]
    fn rejected_stall_time_does_not_leave_a_percentage() {
        // Computation explicitly permits unitless stall time for compatibility.
        // This proposes that configured unit rejection also rejects derivatives.
        // Keeping stall_percentage without stall_total_time can make stored percentages
        // disagree with queries that recompute them from the now-missing stall duration.
        let measurements: MeasurementsConfig = serde_json::from_value(json!({
            "builtinMeasurements": [
                {"name": "stall_total_time", "unit": "millisecond"},
                {"name": "stall_percentage", "unit": "ratio"},
            ],
            "maxCustomMeasurements": 0,
        }))
        .unwrap();
        let event = normalize(
            transaction(json!({"measurements": {
                "stall_total_time": {"value": 1000, "unit": "none"},
            }})),
            &NormalizationConfig {
                measurements: Some(CombinedMeasurementsConfig::new(Some(&measurements), None)),
                ..Default::default()
            },
        );
        assert!(event.measurement("stall_total_time").is_none());
        assert!(event.measurement("stall_percentage").is_none());
    }

    fn span_input() -> Value {
        transaction(json!({
            "transaction": "checkout",
            "contexts": {"trace": {
                "trace_id": "22222222222222222222222222222222",
                "span_id": "aaaaaaaaaaaaaaaa",
            }},
            "spans": [{
                "span_id": "bbbbbbbbbbbbbbbb", "parent_span_id": "aaaaaaaaaaaaaaaa",
                "start_timestamp": 1_700_000_000.0, "timestamp": 1_700_000_001.0,
            }],
        }))
    }

    fn op_rule(field: &str, value: Value) -> SpanOpDefaults {
        serde_json::from_value(json!({"rules": [{
            "condition": {
                "op": if value.is_number() { "gte" } else { "eq" },
                "name": field, "value": value,
            },
            "value": "db.audit",
        }]}))
        .unwrap()
    }

    #[test]
    fn op_rule_uses_backfilled_dsc_project_id() {
        let field = r"span.data.sentry\.dsc\.project_id";
        let rules = op_rule(field, json!("42"));
        let mut dsc: DynamicSamplingContext = serde_json::from_value(json!({
            "trace_id": "22222222222222222222222222222222",
            "public_key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        }))
        .unwrap();
        dsc.project_id = Some(ProjectId::new(42));
        let event = normalize(
            span_input(),
            &NormalizationConfig {
                dsc: Some(&dsc),
                span_op_defaults: rules.borrow(),
                ..Default::default()
            },
        );
        let span = event.spans.value().unwrap()[0].value().unwrap();
        assert_eq!(span.get_value(field).unwrap().as_str(), Some("42"));
        assert_eq!(span.op.as_str(), Some("db.audit"));
    }

    #[test]
    fn op_rule_uses_computed_exclusive_time() {
        let rules = op_rule("span.exclusive_time", json!(1));
        let event = normalize(
            span_input(),
            &NormalizationConfig {
                span_op_defaults: rules.borrow(),
                ..Default::default()
            },
        );
        let span = event.spans.value().unwrap()[0].value().unwrap();
        assert_eq!(span.exclusive_time.value().copied(), Some(1000.0));
        assert_eq!(span.op.as_str(), Some("db.audit"));
    }

    #[test]
    fn op_rule_uses_extracted_segment_name() {
        let field = r"span.data.sentry\.segment\.name";
        let rules = op_rule(field, json!("checkout"));
        let event = normalize(
            span_input(),
            &NormalizationConfig {
                span_op_defaults: rules.borrow(),
                enrich_spans: true,
                ..Default::default()
            },
        );
        let span = event.spans.value().unwrap()[0].value().unwrap();
        assert_eq!(span.get_value(field).unwrap().as_str(), Some("checkout"));
        assert_eq!(span.op.as_str(), Some("db.audit"));
    }
}

fn normalize_json(payload: Value, config: &NormalizationConfig<'_>) -> Value {
    let mut event = Annotated::<Event>::from_json(&payload.to_string()).unwrap();
    normalize_event(&mut event, config);
    serde_json::to_value(SerializableAnnotated(&event)).unwrap()
}

fn observed(case: &str, values: Value) {
    println!(
        "SCC_OBSERVATION {}",
        json!({"case": case, "values": values})
    );
}

fn transaction_with_span() -> Value {
    json!({
        "type": "transaction", "transaction": "HomeScreen",
        "transaction_info": {"source": "custom"},
        "start_timestamp": 1700000000.0, "timestamp": 1700000005.0,
        "contexts": {"trace": {"trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "span_id": "bbbbbbbbbbbbbbbb", "op": "ui.load"}},
        "spans": [{"span_id": "cccccccccccccccc", "parent_span_id": "bbbbbbbbbbbbbbbb", "op": "http.client", "start_timestamp": 1700000000.25, "timestamp": 1700000001.0}]
    })
}

fn span_config(trimming: bool) -> NormalizationConfig<'static> {
    NormalizationConfig {
        enable_trimming: trimming,
        normalize_spans: true,
        enrich_spans: true,
        ..Default::default()
    }
}

fn stacktrace() -> Value {
    json!({"frames": [{"filename": "https://example.com/app.js", "function": "main"}]})
}

#[test]
fn rejected_exception_keeps_independent_stacktrace() {
    let mut missing = Vec::new();
    for variable_bytes in [0, 600] {
        let mut trace = stacktrace();
        if variable_bytes > 0 {
            trace["frames"][0]["vars"] = json!({"value": "x".repeat(variable_bytes)});
        }
        let result = normalize_json(
            json!({"stacktrace": trace, "exception": {"values": [{}]}}),
            &span_config(false),
        );
        observed(
            "rejected_exception_stacktrace",
            json!({
                "variable_bytes": variable_bytes,
                "payload_frame": result.pointer("/stacktrace/frames/0/filename"),
                "exception": result.pointer("/exception/values/0"),
                "quarantined_frame": result.pointer("/_meta/exception/values/0//val/stacktrace/frames/0/filename")
            }),
        );
        assert_eq!(result.pointer("/exception/values/0"), Some(&Value::Null));
        if result.pointer("/stacktrace/frames/0/filename") != Some(&json!("/app.js")) {
            missing.push(variable_bytes);
        }
    }
    assert!(
        missing.is_empty(),
        "Rejecting the exception removed the independent trace: {missing:?}"
    );
}

#[test]
fn trimmed_user_identity_is_consistent() {
    let mut input = transaction_with_span();
    input["user"] = json!({"id": "u".repeat(200)});
    let result = normalize_json(input, &span_config(true));
    let id = result.pointer("/user/id").and_then(Value::as_str).unwrap();
    let sentry_user = result
        .pointer("/user/sentry_user")
        .and_then(Value::as_str)
        .unwrap();
    observed(
        "trimmed_user_identity",
        json!({"id_bytes":id.len(), "sentry_user_bytes":sentry_user.len(), "consistent":sentry_user==format!("id:{id}")}),
    );
    assert_eq!(
        id.len(),
        128,
        "The source field must actually have been trimmed"
    );
    assert_eq!(
        sentry_user,
        format!("id:{id}"),
        "sentry_user retains the pre-trimming identifier: source={} bytes, derived={} bytes",
        id.len(),
        sentry_user.len()
    );
}

#[test]
fn trimmed_transaction_segment_is_consistent() {
    let mut input = transaction_with_span();
    input["transaction"] = json!("HomeScreen".repeat(30));
    let result = normalize_json(input, &span_config(true));
    let transaction = result
        .pointer("/transaction")
        .and_then(Value::as_str)
        .unwrap();
    let segment = result
        .pointer("/spans/0/data/sentry.segment.name")
        .and_then(Value::as_str)
        .unwrap();
    observed(
        "trimmed_transaction_segment",
        json!({"transaction_bytes":transaction.len(), "segment_bytes":segment.len(), "consistent":segment==transaction}),
    );
    assert_eq!(
        transaction.len(),
        200,
        "The source field must actually have been trimmed"
    );
    assert_eq!(
        segment,
        transaction,
        "The child span retains the pre-trimming transaction: source={} bytes, derived={} bytes",
        transaction.len(),
        segment.len()
    );
}

#[test]
fn trimmed_transaction_app_start_screen_is_consistent() {
    let mut input = transaction_with_span();
    input["transaction"] = json!("HomeScreen".repeat(30));
    input["measurements"] = json!({"app_start_cold": {"value": 1000.0, "unit": "millisecond"}});
    let result = normalize_json(input, &span_config(true));
    let transaction = result
        .pointer("/transaction")
        .and_then(Value::as_str)
        .unwrap();
    let screen = result
        .pointer("/contexts/trace/data/app.vitals.start.screen")
        .and_then(Value::as_str)
        .unwrap();
    observed(
        "trimmed_transaction_app_start_screen",
        json!({"transaction_bytes":transaction.len(), "screen_bytes":screen.len(), "consistent":screen==transaction}),
    );
    assert_eq!(transaction.len(), 200);
    assert_eq!(
        screen,
        transaction,
        "The app-start screen retains the pre-trimming transaction: source={} bytes, derived={} bytes",
        transaction.len(),
        screen.len()
    );
}

fn tag<'a>(event: &'a Value, name: &str) -> Option<&'a str> {
    event.get("tags")?.as_array()?.iter().find_map(|pair| {
        (pair.get(0)?.as_str()? == name)
            .then(|| pair.get(1)?.as_str())
            .flatten()
    })
}

#[test]
fn generated_tags_obey_schema() {
    let mut violations = Vec::new();
    for trimming in [false, true] {
        for source in ["server_name", "site", "lcp.element", "lcp.id", "lcp.url"] {
            for value in ["before\nafter", "", "   "] {
                let direct =
                    normalize_json(json!({"tags": {source: value}}), &span_config(trimming));
                assert!(
                    tag(&direct, source).is_none(),
                    "The direct tag must be rejected"
                );
                let input = if source.starts_with("lcp.") {
                    let key = format!("browser.web_vital.{source}");
                    json!({"contexts": {"trace": {"data": {key: value}}}})
                } else {
                    json!({source: value})
                };
                let output = normalize_json(input, &span_config(trimming));
                let actual = tag(&output, source);
                observed(
                    "generated_tag_schema",
                    json!({"source":source,"input":value,"trimming":trimming,"direct":tag(&direct,source),"derived":actual}),
                );
                if actual.is_some_and(|s| s.contains('\n') || s.trim().is_empty()) {
                    violations.push((source, value, trimming));
                }
            }
            let valid = "valid-value";
            let input = if source.starts_with("lcp.") {
                let key = format!("browser.web_vital.{source}");
                json!({"contexts": {"trace": {"data": {key: valid}}}})
            } else {
                json!({source: valid})
            };
            let output = normalize_json(input, &span_config(trimming));
            assert_eq!(tag(&output, source), Some(valid));
        }
    }
    assert!(
        violations.is_empty(),
        "Generated tags bypass schema validation: {violations:?}"
    );
}

#[test]
fn span_copies_match_final_sources() {
    let mut violations = Vec::new();
    for trimming in [false, true] {
        let mut input = transaction_with_span();
        input["user"] = json!({
            "id":"i".repeat(200),"username":"n".repeat(200),
            "email":format!("{}@example.com","e".repeat(100)),
            "geo":{"city":"c".repeat(1200),"region":"r".repeat(1200),
                "subdivision":"s".repeat(1200),"country_code":"z".repeat(1200)}
        });
        input["tags"] = json!({"environment":"p".repeat(100)});
        input["sdk"] = json!({"name": "n".repeat(300), "version": "v".repeat(300)});
        input["contexts"]["trace"]["op"] = json!(format!("custom.{}", "r".repeat(200)));
        input["spans"][0]["op"] = json!(format!("custom.{}", "x".repeat(200)));
        let output = normalize_json(input, &span_config(trimming));
        let pairs = [
            ("user.id", "/user/id", "/spans/0/sentry_tags/user.id"),
            (
                "user.username",
                "/user/username",
                "/spans/0/sentry_tags/user.username",
            ),
            (
                "user.email",
                "/user/email",
                "/spans/0/sentry_tags/user.email",
            ),
            (
                "user.geo.city",
                "/user/geo/city",
                "/spans/0/sentry_tags/user.geo.city",
            ),
            (
                "user.geo.region",
                "/user/geo/region",
                "/spans/0/sentry_tags/user.geo.region",
            ),
            (
                "user.geo.subdivision",
                "/user/geo/subdivision",
                "/spans/0/sentry_tags/user.geo.subdivision",
            ),
            (
                "user.geo.country_code",
                "/user/geo/country_code",
                "/spans/0/sentry_tags/user.geo.country_code",
            ),
            (
                "environment",
                "/environment",
                "/spans/0/sentry_tags/environment",
            ),
            ("span.op", "/spans/0/op", "/spans/0/sentry_tags/op"),
            ("sdk.name", "/sdk/name", "/spans/0/sentry_tags/sdk.name"),
            (
                "sdk.version",
                "/sdk/version",
                "/spans/0/sentry_tags/sdk.version",
            ),
            (
                "trace.op",
                "/contexts/trace/op",
                "/spans/0/sentry_tags/transaction.op",
            ),
        ];
        for (field, source, derived) in pairs {
            let source = output.pointer(source).and_then(Value::as_str);
            let derived = output.pointer(derived).and_then(Value::as_str);
            assert!(
                source.is_some() && derived.is_some(),
                "Both sides must exist: {field}"
            );
            observed(
                "span_copy_after_trim",
                json!({"field":field,"trimming":trimming,"source_bytes":source.map(str::len),"derived_bytes":derived.map(str::len),"consistent":source==derived}),
            );
            if source != derived {
                violations.push((field, trimming));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "Span copies retain pre-trimming sources: {violations:?}"
    );
}

#[test]
fn span_device_class_uses_device_inferred_from_user_agent() {
    let mut explicit = transaction_with_span();
    explicit["platform"] = json!("javascript");
    explicit["contexts"]["device"] = json!({"family":"iPhone","model":"iPhone"});
    let mut inferred = transaction_with_span();
    inferred["platform"] = json!("javascript");
    inferred["request"] = json!({"headers": {
        "User-Agent":"Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) - (-)"
    }});
    let config = NormalizationConfig {
        normalize_user_agent: Some(true),
        ..span_config(false)
    };
    let control = normalize_json(explicit, &config);
    let output = normalize_json(inferred, &config);
    let path = "/spans/0/sentry_tags/device.class";
    assert_eq!(control.pointer(path), Some(&json!("3")));
    assert_eq!(
        output.pointer("/contexts/device/model"),
        Some(&json!("iPhone"))
    );
    observed(
        "span_device_class_after_ua",
        json!({
            "control":control.pointer(path),"derived":output.pointer(path),
            "device_model":output.pointer("/contexts/device/model"),
            "event_tag":tag(&output,"device.class")
        }),
    );
    assert_eq!(
        output.pointer(path),
        control.pointer(path),
        "The span must use the device class available after UA inference"
    );
}

// Rediscovered behavior: verification/normalization/proof_mutations.py already
// covers os-fallback-on-unknown-sdk; this adds a runtime desired-invariant test.
#[test]
fn unknown_system_sdk_uses_os_fallback() {
    let input = json!({
        "contexts":{"os":{"name":"Android"}},
        "exception":{"values":[{"type":"OSError","mechanism":{"type":"generic","meta":{
            "errno":{"number":2},"signal":{"number":7}
        }}}]}
    });
    let control = normalize_json(input.clone(), &span_config(false));
    let mut recognized = input.clone();
    recognized["debug_meta"] = json!({"sdk_info":{"sdk_name":"Android"}});
    let recognized = normalize_json(recognized, &span_config(false));
    let mut unknown = input;
    unknown["debug_meta"] = json!({"sdk_info":{"sdk_name":"unrecognized-sdk"}});
    let output = normalize_json(unknown, &span_config(false));
    assert_eq!(
        output.pointer("/debug_meta/sdk_info/sdk_name"),
        Some(&json!("unrecognized-sdk"))
    );
    let paths = [
        "/exception/values/0/mechanism/meta/errno/name",
        "/exception/values/0/mechanism/meta/signal/name",
    ];
    let mut consistent = true;
    for path in paths {
        assert!(control.pointer(path).is_some());
        assert_eq!(recognized.pointer(path), control.pointer(path));
        observed(
            "unknown_system_sdk_fallback",
            json!({
                "path":path,"without_system_sdk":control.pointer(path),
                "recognized_system_sdk":recognized.pointer(path),"unknown_system_sdk":output.pointer(path),
                "os_name":output.pointer("/contexts/os/name")
            }),
        );
        consistent &= output.pointer(path) == control.pointer(path);
    }
    assert!(
        consistent,
        "An unrecognized system SDK name must not block the usable OS context"
    );
}

#[test]
fn standalone_stacktrace_fallback_preserves_exception_trace() {
    let mut own_trace = stacktrace();
    own_trace["frames"][0]["function"] = json!("crash");
    let result = normalize_json(
        json!({"stacktrace": stacktrace(), "exception": {"values": [{"type": "Error", "stacktrace": own_trace}]}}),
        &NormalizationConfig::default(),
    );
    let function = result.pointer("/exception/values/0/stacktrace/frames/0/function");
    observed(
        "stacktrace_fallback_precedence",
        json!({"exception_function": function}),
    );
    assert_eq!(
        function,
        Some(&json!("crash")),
        "An existing exception trace must not be overwritten by the standalone fallback"
    );
}

#[test]
fn partial_contexts_use_matching_raw_description() {
    let mut mismatches = Vec::new();
    for (kind, name, version, description) in [
        ("os", "Android", "12", "Android 12"),
        ("runtime", ".NET", "8.0.0", ".NET 8.0.0"),
    ] {
        let mut input = json!({"contexts": {kind: {"raw_description": description}}});
        let control = normalize_json(input.clone(), &NormalizationConfig::default());
        assert_eq!(control["contexts"][kind]["name"], json!(name));
        assert_eq!(control["contexts"][kind]["version"], json!(version));
        for (provided, value, missing) in [("name", name, "version"), ("version", version, "name")]
        {
            input["contexts"][kind] = json!({"raw_description": description, provided: value});
            let result = normalize_json(input.clone(), &NormalizationConfig::default());
            observed(
                "partial_description_context",
                json!({"kind": kind, "provided": provided, "result": result["contexts"][kind]}),
            );
            if result["contexts"][kind][missing] != control["contexts"][kind][missing] {
                mismatches.push((kind, provided));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "Matching descriptions did not fill missing components: {mismatches:?}"
    );
}

#[test]
fn span_domain_uses_server_address_without_a_scrubbable_description() {
    let mut missing = Vec::new();
    for description in [json!("GET /relative"), Value::Null, json!("unparseable")] {
        let mut input = transaction_with_span();
        input["spans"][0]["description"] = description.clone();
        input["spans"][0]["data"] = json!({"server.address": "example.com", "url.scheme": "https"});
        let result = normalize_json(input, &span_config(false));
        let domain = result.pointer("/spans/0/sentry_tags/domain");
        observed(
            "span_domain_fallback",
            json!({"description": description, "domain": domain}),
        );
        if domain != Some(&json!("example.com")) {
            missing.push(description);
        }
    }
    assert!(
        missing.is_empty(),
        "Valid server.address fallback was blocked: {missing:?}"
    );
}

#[test]
fn resource_extension_is_derived_before_description_truncation() {
    let mut violations = Vec::new();
    for limit in [200, 16] {
        let mut input = transaction_with_span();
        input["spans"][0]["op"] = json!("resource.script");
        input["spans"][0]["description"] = json!("https://example.com/app.js");
        let config = NormalizationConfig {
            max_tag_value_length: limit,
            ..span_config(false)
        };
        let result = normalize_json(input, &config);
        let extension = result.pointer("/spans/0/sentry_tags/file_extension");
        observed(
            "resource_extension_before_truncation",
            json!({"limit": limit, "extension": extension}),
        );
        if extension != Some(&json!("js")) {
            violations.push(limit);
        }
    }
    assert!(
        violations.is_empty(),
        "Truncating the description changed its derived extension: {violations:?}"
    );
}

#[test]
fn score_components_read_the_same_input_snapshot() {
    let mut totals = Vec::new();
    for reverse in [false, true] {
        let mut components = vec![
            json!({"measurement": "lcp", "weight": 1, "p10": 1000, "p50": 2000}),
            json!({"measurement": "score.lcp", "weight": 1, "p10": 0.3, "p50": 0.7}),
        ];
        if reverse {
            components.reverse();
        }
        let profile: PerformanceScoreConfig = serde_json::from_value(json!({"profiles": [{
            "condition": {"op": "eq", "name": "event.platform", "value": "javascript"},
            "scoreComponents": components,
        }]}))
        .unwrap();
        let mut input = transaction_with_span();
        input["platform"] = json!("javascript");
        input["measurements"] =
            json!({"lcp": {"value": 1000}, "score.lcp": {"value": 0.5, "unit": "ratio"}});
        let result = normalize(
            input,
            &NormalizationConfig {
                performance_score: Some(&profile),
                ..Default::default()
            },
        );
        let total = result.measurement("score.total").unwrap().to_f64();
        observed(
            "score_component_input_collision",
            json!({"reverse": reverse, "total": total}),
        );
        totals.push(total);
    }
    assert!(
        (totals[0] - totals[1]).abs() < 1e-12,
        "Component order changed the score: {totals:?}"
    );
}

#[test]
fn unknown_ai_operation_name_uses_recognized_span_op() {
    let mut mismatches = Vec::new();
    for name in [Value::Null, json!("unrecognized")] {
        let mut input = transaction_with_span();
        for pointer in ["/contexts/trace", "/spans/0"] {
            let span = input.pointer_mut(pointer).unwrap();
            span["op"] = json!("gen_ai.execute_tool");
            span["data"] = json!({"gen_ai.operation.name": name});
        }
        let result = normalize_json(input, &span_config(false));
        for pointer in [
            "/contexts/trace/data/gen_ai.operation.type",
            "/spans/0/data/gen_ai.operation.type",
        ] {
            let operation = result.pointer(pointer);
            observed(
                "ai_operation_fallback",
                json!({"name": name, "path": pointer, "operation": operation}),
            );
            if operation != Some(&json!("tool")) {
                mismatches.push((name.clone(), pointer));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "Recognized op fallback was blocked: {mismatches:?}"
    );
}

#[test]
fn score_condition_does_not_survive_a_removed_or_rewritten_input() {
    let mut stale = Vec::new();
    for (patch, field, op, value) in [
        (
            json!({"request": {"url": "https://example.com/path"}}),
            "event.request.url",
            "eq",
            json!("https://example.com/path?q=1"),
        ),
        (
            json!({"request": {"url": "https://example.com/path?q=1"}}),
            "event.request.url",
            "eq",
            json!("https://example.com/path?q=1"),
        ),
        (
            json!({"request": {"headers": {"Cookie": "a=b"}}}),
            "event.request.headers.Cookie",
            "eq",
            json!("a=b"),
        ),
        (
            json!({"measurements": {"lcp": {"value": 1000}, "score.total": {"value": 0.5}}}),
            "event.measurements.score.total.value",
            "lt",
            json!(0.95),
        ),
        (
            json!({"measurements": {"lcp": {"value": 1000}, "score.total": {"value": 0.5}}}),
            "event.measurements.score.total.value",
            "lt",
            json!(0.75),
        ),
    ] {
        let profile: PerformanceScoreConfig = serde_json::from_value(json!({"profiles": [{
            "condition": {"op": op, "name": field, "value": value},
            "scoreComponents": [{"measurement": "lcp", "weight": 1, "p10": 1000, "p50": 2000}],
        }]}))
        .unwrap();
        let mut input = transaction_with_span();
        input["measurements"] = json!({"lcp": {"value": 1000}});
        for (key, value) in patch.as_object().unwrap() {
            input[key.as_str()] = value.clone();
        }
        let result = normalize(
            input,
            &NormalizationConfig {
                performance_score: Some(&profile),
                ..Default::default()
            },
        );
        let final_match = profile.profiles[0]
            .condition
            .as_ref()
            .unwrap()
            .matches(&result);
        let scored = result.measurement("score.total").is_some();
        let total = result
            .measurement("score.total")
            .map(|value| value.to_f64());
        if field == "event.measurements.score.total.value" {
            assert!(
                total.is_some_and(|value| value > 0.5),
                "The predicate input must actually be overwritten"
            );
        }
        observed(
            "score_condition_invalidated",
            json!({"field": field, "op": op, "value": value, "final_match": final_match, "scored": scored, "total": total}),
        );
        if final_match != scored {
            stale.push(field);
        }
    }
    assert!(
        stale.is_empty(),
        "Scoring retained a condition invalidated by later normalization: {stale:?}"
    );
}

#[test]
fn partial_user_geo_uses_missing_fields_from_geoip() {
    let lookup = GeoIpLookup::open(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/GeoIP2-Enterprise-Test.mmdb"),
    )
    .unwrap();
    let mut missing = Vec::new();
    for geo in [
        Value::Null,
        json!({}),
        json!({"country_code": "GB"}),
        json!({"city": "Explicit"}),
    ] {
        let expected = geo.get("city").cloned().unwrap_or(json!("Boxford"));
        let result = normalize_json(
            json!({"user": {"ip_address": "2.125.160.216", "geo": geo}}),
            &NormalizationConfig {
                geoip_lookup: Some(&lookup),
                ..Default::default()
            },
        );
        let city = result.pointer("/user/geo/city");
        observed(
            "partial_user_geo",
            json!({"geo": geo, "city": city, "expected": expected}),
        );
        if city != Some(&expected) {
            missing.push(geo);
        }
    }
    assert!(
        missing.is_empty(),
        "Partial geo blocked available GeoIP fields: {missing:?}"
    );
}

#[test]
fn user_data_migration_preserves_existing_entries() {
    let mut conflicts = Vec::new();
    for data in [json!({}), json!({"key": "canonical"})] {
        let expected = data.get("key").cloned().unwrap_or(json!("legacy"));
        let result = normalize_json(
            json!({"user": {"data": data, "key": "legacy"}}),
            &NormalizationConfig::default(),
        );
        observed(
            "user_data_migration_conflict",
            json!({"input": data, "result": result.pointer("/user/data/key")}),
        );
        if result.pointer("/user/data/key") != Some(&expected) {
            conflicts.push(data);
        }
    }
    assert!(
        conflicts.is_empty(),
        "Migrating legacy data overwrote existing entries: {conflicts:?}"
    );
}

#[test]
fn computed_context_display_does_not_starve_its_input_fields() {
    let mut starved = Vec::new();
    for kind in ["os", "runtime", "browser"] {
        let input =
            json!({"contexts": {kind: {"name": "n".repeat(3000), "version": "v".repeat(3000)}}});
        let control = normalize_json(
            input.clone(),
            &NormalizationConfig {
                is_renormalize: true,
                enable_trimming: true,
                ..Default::default()
            },
        );
        let result = normalize_json(
            input,
            &NormalizationConfig {
                enable_trimming: true,
                ..Default::default()
            },
        );
        for field in ["name", "version"] {
            let original = control["contexts"][kind][field].as_str().unwrap();
            let retained = result["contexts"][kind][field].as_str();
            assert_eq!(
                original.len(),
                3000,
                "The supplied components fit without the derived display field"
            );
            observed(
                "context_display_budget",
                json!({"kind": kind, "field": field, "original_bytes": original.len(), "retained_bytes": retained.map(str::len)}),
            );
            if retained != Some(original) {
                starved.push((kind, field));
            }
        }
    }
    assert!(
        starved.is_empty(),
        "A derived display string consumed its inputs' budget: {starved:?}"
    );
}

#[test]
fn app_start_rewrite_processes_every_matching_span() {
    let mut input = transaction_with_span();
    input["sdk"] = json!({"name": "sentry.javascript.react-native", "version": "4.4.0"});
    let mut second = input["spans"][0].clone();
    second["span_id"] = json!("dddddddddddddddd");
    input["spans"][0]["op"] = json!("app_start_cold");
    second["op"] = json!("app_start_warm");
    input["spans"].as_array_mut().unwrap().push(second);
    let result = normalize_json(input, &span_config(false));
    let ops: Vec<_> = result["spans"]
        .as_array()
        .unwrap()
        .iter()
        .map(|span| span["op"].clone())
        .collect();
    observed("app_start_multiple_spans", json!({"ops": ops}));
    assert_eq!(ops, vec![json!("app.start.cold"), json!("app.start.warm")]);
}

#[test]
fn score_inputs_require_usable_measurement_values() {
    let component = |measurement, optional| {
        json!({
            "measurement": measurement, "optional": optional, "weight": 1, "p10": 1000, "p50": 2000,
        })
    };
    let profile = |components| {
        json!({
            "condition": {"op": "eq", "name": "event.platform", "value": "javascript"},
            "scoreComponents": components,
        })
    };
    let mut incorrect = Vec::new();
    for optional in [false, true] {
        let profiles = if optional {
            vec![profile(vec![
                component("lcp", true),
                component("fcp", false),
            ])]
        } else {
            vec![
                profile(vec![component("lcp", false)]),
                profile(vec![component("fcp", false)]),
            ]
        };
        let scoring: PerformanceScoreConfig =
            serde_json::from_value(json!({"profiles": profiles})).unwrap();
        let config = NormalizationConfig {
            performance_score: Some(&scoring),
            ..Default::default()
        };
        let mut base = transaction_with_span();
        base["platform"] = json!("javascript");
        base["measurements"] = json!({"fcp": {"value": 1000}});
        let expected = normalize(base.clone(), &config)
            .measurement("score.total")
            .unwrap()
            .to_f64();
        for primary in [
            Value::Null,
            json!({"value": "invalid"}),
            json!({"value": 1000}),
        ] {
            let mut input = base.clone();
            input["measurements"]["lcp"] = primary.clone();
            let result = normalize(input, &config);
            let total = result
                .measurement("score.total")
                .map(|value| value.to_f64());
            observed(
                "score_profile_value_fallback",
                json!({"optional": optional, "primary": primary, "total": total, "expected": expected}),
            );
            if total.is_none_or(|total| (total - expected).abs() > 1e-12) {
                incorrect.push((optional, primary));
            }
        }
    }
    assert!(
        incorrect.is_empty(),
        "Unusable values blocked a profile or diluted optional-component weights: {incorrect:?}"
    );
}

#[test]
fn request_method_validation_is_independent_of_existing_errors() {
    let mut retained = Vec::new();
    for method in ["get", "g@t"] {
        for has_error in [false, true] {
            let mut input = json!({"request": {"method": method}});
            if has_error {
                input["_meta"] = json!({"request": {"method": {"": {"err": ["invalid_data"]}}}});
            }
            let result = normalize_json(input, &NormalizationConfig::default());
            let normalized = result.pointer("/request/method").and_then(Value::as_str);
            observed(
                "method_validation_with_metadata",
                json!({"method": method, "has_error": has_error, "normalized": normalized}),
            );
            if method == "get" {
                assert_eq!(normalized, Some("GET"));
            } else if normalized.is_some() {
                retained.push(has_error);
            }
        }
    }
    assert!(
        retained.is_empty(),
        "Existing diagnostics bypassed method validation: {retained:?}"
    );
}

#[test]
fn empty_user_identity_fields_allow_the_next_fallback() {
    let mut blocked = Vec::new();
    for (preferred, fallback, value, expected) in [
        ("id", "username", "alice", "username:alice"),
        (
            "username",
            "email",
            "alice@example.com",
            "email:alice@example.com",
        ),
        ("email", "ip_address", "127.0.0.1", "ip:127.0.0.1"),
    ] {
        for empty in [Value::Null, json!("")] {
            let result = normalize_json(
                json!({"user": {preferred: empty, fallback: value}}),
                &NormalizationConfig::default(),
            );
            let identity = result.pointer("/user/sentry_user").and_then(Value::as_str);
            observed(
                "empty_user_identity_fallback",
                json!({"preferred": preferred, "input": empty, "identity": identity, "expected": expected}),
            );
            if identity != Some(expected) {
                blocked.push((preferred, empty));
            }
        }
    }
    assert!(
        blocked.is_empty(),
        "Empty preferred identities blocked usable fallback fields: {blocked:?}"
    );
}

#[test]
fn child_app_start_type_uses_the_canonical_event_fallback() {
    let mut mismatches = Vec::new();
    for explicit in [false, true] {
        for legacy in [false, true] {
            let mut input = transaction_with_span();
            input["sdk"] = json!({"name": "sentry.cocoa", "version": "9.0.0"});
            if explicit {
                input["tags"] = json!({"app.vitals.start.type": "warm"});
            }
            if legacy {
                input["measurements"] =
                    json!({"app_start_cold": {"value": 1000, "unit": "millisecond"}});
            }
            let expected = if explicit {
                Some("warm")
            } else if legacy {
                Some("cold")
            } else {
                None
            };
            let result = normalize(input, &span_config(false));
            assert_eq!(result.tag_value("app.vitals.start.type"), expected);
            let spans = result.spans.value().unwrap();
            let span = spans[0].value().unwrap();
            let span_type = span
                .sentry_tags
                .value()
                .and_then(|tags| tags.app_start_type.as_str());
            observed(
                "canonical_app_start_type_fallback",
                json!({"explicit": explicit, "legacy": legacy, "event_type": expected, "span_type": span_type}),
            );
            if span_type != expected {
                mismatches.push((explicit, legacy));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "Child spans ignored the canonical event type: {mismatches:?}"
    );
}
