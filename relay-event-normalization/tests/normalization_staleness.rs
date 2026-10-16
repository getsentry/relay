//! Failing ordering and fallback regressions, and proposed contracts from the dependency audit.
//!
//! These tests intentionally fail on the committed normalizer. The cases in
//! `proposed_contracts` describe candidate normalization and fallback behavior;
//! their intended product behavior is not established merely by the observed discrepancies.

use relay_base_schema::metrics::{FractionUnit, MetricUnit};
use relay_event_normalization::{
    BuiltinMeasurementKey, CombinedMeasurementsConfig, GeoIpLookup, MeasurementsConfig,
    NormalizationConfig, RawUserAgentInfo, normalize_event, replay,
};
use relay_event_schema::protocol::{
    BrowserContext, DeviceClass, DeviceContext, Event, OsContext, Replay,
};
use relay_protocol::{Annotated, get_value};
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
