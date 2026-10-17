//! Passing controls for dependency candidates explained by field disjointness or ordering policy.
//! L-numbers refer to the pinned 119-pair normalization dependency review.

use relay_event_normalization::{NormalizationConfig, normalize_event};
use relay_event_schema::protocol::{DeviceContext, Event, OsContext};
use relay_protocol::{Annotated, SerializableAnnotated, get_value};
use serde_json::{Value, json};

fn normalize(payload: Value, config: &NormalizationConfig<'_>) -> Event {
    let mut event = Annotated::<Event>::from_json(&payload.to_string()).unwrap();
    normalize_event(&mut event, config);
    event.into_value().unwrap()
}

mod candidate_checks {
    use super::*;
    use relay_base_schema::project::ProjectId;
    use relay_event_normalization::{PerformanceScoreConfig, SpanOpDefaults};
    use relay_event_schema::protocol::{
        BrowserContext, EventType, ReplayContext, Span, TraceContext,
    };
    use relay_protocol::Getter;
    use relay_sampling::DynamicSamplingContext;

    fn dsc() -> DynamicSamplingContext {
        let mut dsc: DynamicSamplingContext = serde_json::from_value(json!({
            "trace_id": "22222222222222222222222222222222",
            "public_key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        }))
        .unwrap();
        dsc.project_id = Some(ProjectId::new(42));
        dsc
    }

    fn score() -> PerformanceScoreConfig {
        serde_json::from_value(json!({"profiles": [{
            "condition": {"op": "eq", "name": "event.platform", "value": "javascript"},
            "version": "audit", "scoreComponents": [{
                "measurement": "lcp", "weight": 1, "p10": 1000, "p50": 2000,
            }],
        }]}))
        .unwrap()
    }

    fn trace(event: &Event) -> &TraceContext {
        event.context::<TraceContext>().unwrap()
    }

    fn child(event: &Event) -> &Span {
        event.spans.value().unwrap()[0].value().unwrap()
    }

    fn mobile_transaction() -> Value {
        transaction(json!({
            "platform": "javascript", "transaction": "Checkout",
            "sdk": {"name": "sentry.javascript.react-native", "version": "4.4.0"},
            "measurements": {
                "lcp": {"value": 1200},
                "app_start_cold": {"value": 1000, "unit": "millisecond"},
            },
            "request": {"headers": {"Sec-CH-UA-Platform": "\"Android\""}},
            "contexts": {
                "device": {"family": "iPhone", "model": "iPhone"},
                "trace": {
                    "trace_id": "22222222222222222222222222222222",
                    "span_id": "aaaaaaaaaaaaaaaa", "op": "ui.load",
                    "data": {"browser.web_vital.lcp.element": "img.hero"},
                },
            },
            "spans": [{
                "span_id": "bbbbbbbbbbbbbbbb", "parent_span_id": "aaaaaaaaaaaaaaaa",
                "op": "app_start_cold", "start_timestamp": 1_700_000_000.0,
                "timestamp": 1_700_000_001.0,
            }],
        }))
    }

    #[test]
    fn late_contexts_preserve_error_device_os_and_lcp_derivations() {
        // L047-L048, L051, L055-L056, L103-L105, L107-L109, L111, L116.
        for replay in [false, true] {
            for force in [false, true] {
                let event = normalize(
                    error_with_errno(json!({
                        "platform": "javascript",
                        "contexts": {
                            "device": {"family": "iPhone", "model": "iPhone"},
                            "os": {"name": "Android"},
                            "trace": {"data": {"browser.web_vital.lcp.element": "img.hero"}},
                        },
                    })),
                    &NormalizationConfig {
                        force_trace_context: force,
                        replay_id: replay.then_some(uuid::Uuid::from_u128(1)),
                        client_sample_rate: Some(0.25),
                        ..Default::default()
                    },
                );
                assert_errno_enriched(&event);
                assert_eq!(event.tag_value("device.class"), Some("3"));
                assert_eq!(event.tag_value("lcp.element"), Some("img.hero"));
                assert_eq!(trace(&event).client_sample_rate.value(), Some(&0.25));
                assert_eq!(trace(&event).status.value().unwrap().as_str(), "unknown");
                assert_eq!(trace(&event).trace_id.value().is_some(), force);
                assert_eq!(event.context::<ReplayContext>().is_some(), replay);
            }
        }
    }

    #[test]
    fn inferred_ua_contexts_coexist_with_late_replay_trace_and_sampling() {
        // L062, L064, L067: actually infer missing contexts while later writers run.
        let event = normalize(
            json!({
                "platform": "javascript", "request": {"headers": {
                    "Sec-CH-UA-Platform": "\"Android\"", "Sec-CH-UA-Model": "\"SM-G991B\"",
                    "User-Agent": "Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
                }},
            }),
            &NormalizationConfig {
                normalize_user_agent: Some(true),
                force_trace_context: true,
                replay_id: Some(uuid::Uuid::from_u128(1)),
                client_sample_rate: Some(0.25),
                ..Default::default()
            },
        );
        assert_eq!(
            event.context::<OsContext>().unwrap().name.as_str(),
            Some("Android")
        );
        assert_eq!(
            event.context::<DeviceContext>().unwrap().model.as_str(),
            Some("SM-G991B")
        );
        assert_eq!(
            event.context::<BrowserContext>().unwrap().name.as_str(),
            Some("Chrome Mobile")
        );
        assert!(event.context::<ReplayContext>().is_some());
        assert!(trace(&event).trace_id.value().is_some());
        assert_eq!(trace(&event).client_sample_rate.value(), Some(&0.25));
    }

    #[test]
    fn context_parsing_preserves_ua_presence_policy() {
        // L049, L065: parsing fields in an existing context does not make it absent.
        let event = normalize(
            json!({
                "platform": "javascript", "request": {"headers": {
                    "Sec-CH-UA-Platform": "\"Windows\"", "Sec-CH-UA-Model": "\"Other model\"",
                }},
                "contexts": {
                    "os": {"raw_description": "Android OS 11 / API-30 (RP1A.201005.001)"},
                    "device": {"family": "Samsung", "model": "SM-G991B", "processor_frequency": 3000,
                        "processor_count": 8, "memory_size": 8589934592_u64},
                },
            }),
            &NormalizationConfig {
                normalize_user_agent: Some(true),
                ..Default::default()
            },
        );
        assert_eq!(
            event.context::<OsContext>().unwrap().name.as_str(),
            Some("Android")
        );
        let device = event.context::<DeviceContext>().unwrap();
        assert_eq!(device.model.as_str(), Some("SM-G991B"));
        assert_eq!(device.name.as_str(), Some("Galaxy S21 5G"));
        assert_eq!(event.tag_value("device.class"), Some("3"));
    }

    #[test]
    fn security_ua_headers_survive_request_cookie_normalization() {
        // L032: activate security header insertion as well as Cookie removal.
        let mut config = NormalizationConfig {
            normalize_user_agent: Some(true),
            ..Default::default()
        };
        config.user_agent.user_agent = Some("connection-agent");
        config.user_agent.client_hints.sec_ch_ua_platform = Some("\"Android\"");
        let event = normalize(
            json!({"csp": {}, "platform": "javascript", "request": {
                "headers": {"Cookie": "sid=abc", "User-Agent": "sdk-agent"},
            }}),
            &config,
        );
        assert_eq!(event.logger.as_str(), Some("csp"));
        assert_eq!(
            event.context::<OsContext>().unwrap().name.as_str(),
            Some("Android")
        );
        let request = event.request.value().unwrap();
        let headers = request.headers.value().unwrap();
        assert_eq!(headers.get_header("User-Agent"), Some("sdk-agent"));
        assert_eq!(
            headers.get_header("SEC-CH-UA-Platform"),
            Some("\"Android\"")
        );
        assert!(headers.get_header("Cookie").is_none());
        assert!(request.cookies.value().is_some());
    }

    #[test]
    fn app_start_score_and_ua_preserve_device_and_exception_inputs() {
        // L039-L040, L043-L044, L050, L054, L058-L059, L066, L071, L073-L074, L076-L078.
        let mut payload = mobile_transaction();
        payload["contexts"]["os"] = json!({"name": "Android"});
        payload["exception"] = error_with_errno(json!({}))["exception"].clone();
        let dsc = dsc();
        let score = score();
        let event = normalize(
            payload,
            &NormalizationConfig {
                normalize_user_agent: Some(true),
                performance_score: Some(&score),
                dsc: Some(&dsc),
                replay_id: Some(uuid::Uuid::from_u128(1)),
                client_sample_rate: Some(0.25),
                ..Default::default()
            },
        );
        assert_errno_enriched(&event);
        assert_eq!(event.tag_value("device.class"), Some("3"));
        assert_eq!(event.tag_value("app.vitals.start.type"), Some("cold"));
        assert_eq!(event.tag_value("lcp.element"), Some("img.hero"));
        assert_eq!(
            event
                .measurement("app.vitals.start.value")
                .unwrap()
                .to_f64(),
            1000.0
        );
        assert!(event.measurement("score.total").is_some());
        assert!(
            event
                .context::<relay_event_schema::protocol::PerformanceScoreContext>()
                .is_some()
        );
        let root = trace(&event);
        assert_eq!(
            root.data
                .value()
                .unwrap()
                .get_str("app.vitals.start.screen"),
            Some("Checkout")
        );
        assert_eq!(
            root.data.value().unwrap().get_str("sentry.dsc.project_id"),
            Some("42")
        );
        assert_eq!(root.status.value().unwrap().as_str(), "unknown");
        assert_eq!(root.client_sample_rate.value(), Some(&0.25));
        assert!(event.context::<ReplayContext>().is_some());
        assert_eq!(child(&event).op.as_str(), Some("app.start.cold"));
        assert!(
            child(&event)
                .data
                .value()
                .unwrap()
                .get_str("gen_ai.operation.type")
                .is_none()
        );
    }

    #[test]
    fn app_start_score_and_dsc_preserve_inferred_ua_contexts() {
        // L058-L059, L066: unlike the errno control, OS is genuinely absent initially.
        let dsc = dsc();
        let score = score();
        let event = normalize(
            mobile_transaction(),
            &NormalizationConfig {
                normalize_user_agent: Some(true),
                dsc: Some(&dsc),
                performance_score: Some(&score),
                ..Default::default()
            },
        );
        assert_eq!(
            event.context::<OsContext>().unwrap().name.as_str(),
            Some("Android")
        );
        assert_eq!(
            event.context::<DeviceContext>().unwrap().model.as_str(),
            Some("iPhone")
        );
        assert_eq!(event.tag_value("app.vitals.start.type"), Some("cold"));
        assert!(event.measurement("score.total").is_some());
        assert_eq!(
            trace(&event)
                .data
                .value()
                .unwrap()
                .get_str("sentry.dsc.project_id"),
            Some("42")
        );
        assert_eq!(child(&event).op.as_str(), Some("app.start.cold"));
    }

    #[test]
    fn late_ai_payload_writes_preserve_device_and_inferred_os() {
        // L045, L060: AI changes trace.data, not the device or OS context.
        let event = normalize(
            json!({"platform": "javascript", "contexts": {
            "device": {"family": "iPhone", "model": "iPhone"},
            "trace": {"op": "gen_ai.invoke_agent", "data": {"gen_ai.request.model": "model"}},
        }, "request": {"headers": {"Sec-CH-UA-Platform": "\"Android\""}}}),
            &NormalizationConfig {
                normalize_user_agent: Some(true),
                ..Default::default()
            },
        );
        assert_eq!(event.tag_value("device.class"), Some("3"));
        assert_eq!(
            event.context::<OsContext>().unwrap().name.as_str(),
            Some("Android")
        );
        assert_eq!(
            trace(&event)
                .data
                .value()
                .unwrap()
                .get_str("gen_ai.operation.type"),
            Some("agent")
        );
    }

    #[test]
    fn nontransaction_ai_trace_survives_late_context_writers() {
        // L087-L089, L092: exercise the root AI trace on an error event.
        let event = normalize(
            error_with_errno(json!({"contexts": {
                "os": {"name": "Android"},
                "trace": {"op": "gen_ai.invoke_agent", "data": {
                    "gen_ai.request.model": "model", "gen_ai.usage.input_tokens": 10,
                    "gen_ai.usage.output_tokens": 3,
                }},
            }})),
            &NormalizationConfig {
                force_trace_context: true,
                replay_id: Some(uuid::Uuid::from_u128(1)),
                client_sample_rate: Some(0.25),
                ..Default::default()
            },
        );
        assert_errno_enriched(&event);
        let root = trace(&event);
        let data = root.data.value().unwrap();
        assert_eq!(data.get_str("gen_ai.operation.type"), Some("agent"));
        assert_eq!(data.get_str("gen_ai.response.model"), Some("model"));
        assert_eq!(
            data.get_value("gen_ai.usage.total_tokens")
                .unwrap()
                .as_f64(),
            Some(13.0)
        );
        assert!(root.trace_id.value().is_some());
        assert_eq!(root.status.value().unwrap().as_str(), "unknown");
        assert_eq!(root.client_sample_rate.value(), Some(&0.25));
        assert!(event.context::<ReplayContext>().is_some());
    }

    #[test]
    fn late_span_metadata_preserves_ai_usage_and_timing() {
        // L090-L091: AI legacy-measurement mapping precedes DSC/exclusive time/tag extraction.
        let mut payload = mobile_transaction();
        payload["spans"][0]["op"] = json!("gen_ai.invoke_agent");
        payload["spans"][0]["data"] = json!({"gen_ai.request.model": "model"});
        payload["spans"][0]["measurements"] = json!({
            "ai_prompt_tokens_used": {"value": 10}, "ai_completion_tokens_used": {"value": 3},
        });
        let dsc = dsc();
        let event = normalize(
            payload,
            &NormalizationConfig {
                dsc: Some(&dsc),
                enrich_spans: true,
                ..Default::default()
            },
        );
        let span = child(&event);
        let data = span.data.value().unwrap();
        assert_eq!(data.get_str("gen_ai.operation.type"), Some("agent"));
        assert_eq!(data.get_str("gen_ai.response.model"), Some("model"));
        assert_eq!(
            data.get_value("gen_ai.usage.total_tokens")
                .unwrap()
                .as_f64(),
            Some(13.0)
        );
        assert_eq!(data.get_str("sentry.dsc.project_id"), Some("42"));
        assert_eq!(data.get_str("sentry.segment.name"), Some("Checkout"));
        assert_eq!(span.exclusive_time.value(), Some(&1000.0));
        assert!(span.sentry_tags.value().is_some());
    }

    #[test]
    fn breakdowns_use_child_intervals_even_on_nontransactions() {
        // L093-L098, L100-L101: activate real breakdowns and each later kind of metadata.
        // In particular L097 is field disjointness, not a transaction-only breakdown guard.
        let breakdowns = serde_json::from_value(json!({
            "ops": {"type": "spanOperations", "matches": ["db"]},
        }))
        .unwrap();
        for transaction_event in [false, true] {
            let mut payload = mobile_transaction();
            if !transaction_event {
                payload.as_object_mut().unwrap().remove("type");
                payload["contexts"]["trace"]
                    .as_object_mut()
                    .unwrap()
                    .remove("trace_id");
                payload["contexts"]["trace"]
                    .as_object_mut()
                    .unwrap()
                    .remove("span_id");
            }
            payload["platform"] = json!("invalid-platform");
            payload["request"] = json!({"method": "get", "headers": {"Cookie": "sid=abc"}});
            payload["contexts"]["os"] =
                json!({"raw_description": "Android OS 11 / API-30 (RP1A.201005.001)"});
            payload["spans"][0]["op"] = json!("db.sql");
            let event = normalize(
                payload,
                &NormalizationConfig {
                    breakdowns_config: Some(&breakdowns),
                    force_trace_context: true,
                    replay_id: Some(uuid::Uuid::from_u128(1)),
                    client_sample_rate: Some(0.25),
                    enrich_spans: true,
                    ..Default::default()
                },
            );
            assert_eq!(event.breakdown("ops", "ops.db").unwrap().to_f64(), 1000.0);
            assert_eq!(
                event.breakdown("ops", "total.time").unwrap().to_f64(),
                1000.0
            );
            assert_eq!(event.platform.as_str(), Some("other"));
            assert_eq!(event.tag_value("lcp.element"), Some("img.hero"));
            assert!(event.context::<ReplayContext>().is_some());
            assert_eq!(
                event.context::<OsContext>().unwrap().name.as_str(),
                Some("Android")
            );
            assert_eq!(event.request.value().unwrap().method.as_str(), Some("GET"));
            assert!(event.request.value().unwrap().cookies.value().is_some());
            assert_eq!(trace(&event).status.value().unwrap().as_str(), "unknown");
            assert_eq!(trace(&event).client_sample_rate.value(), Some(&0.25));
            assert!(child(&event).sentry_tags.value().is_some());
            assert!(trace(&event).trace_id.value().is_some());
            assert!(trace(&event).span_id.value().is_some());
        }
    }

    #[test]
    fn lcp_and_replay_survive_late_trace_and_span_writes() {
        // L046, L103-L111: exercise all LCP backfills, replay and actual DSC writes.
        let dsc = dsc();
        for transaction_event in [false, true] {
            let mut payload = mobile_transaction();
            if !transaction_event {
                payload.as_object_mut().unwrap().remove("type");
                payload["contexts"]["trace"]
                    .as_object_mut()
                    .unwrap()
                    .remove("trace_id");
                payload["contexts"]["trace"]
                    .as_object_mut()
                    .unwrap()
                    .remove("span_id");
            }
            payload["contexts"]["trace"]["data"] = json!({
                "browser.web_vital.lcp.element": "img.hero", "browser.web_vital.lcp.size": 123,
                "browser.web_vital.lcp.id": "hero", "browser.web_vital.lcp.url": "https://example.com/hero.png",
            });
            let event = normalize(
                payload,
                &NormalizationConfig {
                    dsc: Some(&dsc),
                    force_trace_context: true,
                    replay_id: Some(uuid::Uuid::from_u128(1)),
                    client_sample_rate: Some(0.25),
                    ..Default::default()
                },
            );
            for (key, expected) in [
                ("lcp.element", "img.hero"),
                ("lcp.size", "123"),
                ("lcp.id", "hero"),
                ("lcp.url", "https://example.com/hero.png"),
                ("device.class", "3"),
            ] {
                assert_eq!(event.tag_value(key), Some(expected), "{key}");
            }
            assert_eq!(
                event
                    .context::<ReplayContext>()
                    .unwrap()
                    .replay_id
                    .value()
                    .unwrap()
                    .to_string(),
                "00000000000000000000000000000001"
            );
            assert_eq!(
                trace(&event)
                    .data
                    .value()
                    .unwrap()
                    .get_str("sentry.dsc.project_id"),
                transaction_event.then_some("42")
            );
            assert_eq!(trace(&event).status.value().unwrap().as_str(), "unknown");
            assert_eq!(trace(&event).client_sample_rate.value(), Some(&0.25));
            assert!(trace(&event).trace_id.value().is_some());
            assert!(trace(&event).span_id.value().is_some());
        }
    }

    #[test]
    fn forced_trace_ids_preserve_event_id_and_survive_context_defaults() {
        // L102, L112, L114, L116: cover both missing and supplied trace/span IDs.
        for supplied in [false, true] {
            let mut payload = json!({"event_id": "11111111111111111111111111111111"});
            if supplied {
                payload["contexts"] = json!({"trace": {
                    "trace_id": "22222222222222222222222222222222", "span_id": "aaaaaaaaaaaaaaaa",
                }});
            }
            let event = normalize(
                payload,
                &NormalizationConfig {
                    force_trace_context: true,
                    client_sample_rate: Some(0.25),
                    ..Default::default()
                },
            );
            assert_eq!(
                event.id.value().unwrap().to_string(),
                "11111111111111111111111111111111"
            );
            assert_eq!(
                trace(&event).trace_id.value().unwrap().to_string(),
                if supplied {
                    "22222222222222222222222222222222"
                } else {
                    "11111111111111111111111111111111"
                }
            );
            assert!(trace(&event).span_id.value().is_some());
            if supplied {
                assert_eq!(
                    trace(&event).span_id.value().unwrap().to_string(),
                    "aaaaaaaaaaaaaaaa"
                );
            }
            assert_eq!(trace(&event).status.value().unwrap().as_str(), "unknown");
            assert_eq!(trace(&event).client_sample_rate.value(), Some(&0.25));
        }
    }

    #[test]
    fn late_enrichment_preserves_nested_span_exclusive_time_inputs() {
        // L115, L117-L119: nested spans make parent IDs/timing observable in the result.
        let mut payload = mobile_transaction();
        payload["spans"][0]["timestamp"] = json!(1_700_000_002.0);
        payload["spans"].as_array_mut().unwrap().push(json!({
            "span_id": "cccccccccccccccc", "parent_span_id": "bbbbbbbbbbbbbbbb", "op": "db",
            "start_timestamp": 1_700_000_000.5, "timestamp": 1_700_000_001.5,
        }));
        let dsc = dsc();
        let baseline = normalize(
            payload.clone(),
            &NormalizationConfig {
                dsc: Some(&dsc),
                ..Default::default()
            },
        );
        let event = normalize(
            payload,
            &NormalizationConfig {
                dsc: Some(&dsc),
                enrich_spans: true,
                client_sample_rate: Some(0.25),
                ..Default::default()
            },
        );
        for (before, after) in baseline
            .spans
            .value()
            .unwrap()
            .iter()
            .zip(event.spans.value().unwrap())
        {
            let (before, after) = (before.value().unwrap(), after.value().unwrap());
            assert_eq!(after.span_id, before.span_id);
            assert_eq!(after.parent_span_id, before.parent_span_id);
            assert_eq!(after.start_timestamp, before.start_timestamp);
            assert_eq!(after.timestamp, before.timestamp);
            assert_eq!(after.exclusive_time.value(), Some(&1000.0));
            assert_eq!(
                after.data.value().unwrap().get_str("sentry.dsc.project_id"),
                Some("42")
            );
            assert_eq!(
                after.data.value().unwrap().get_str("sentry.segment.name"),
                Some("Checkout")
            );
            assert!(after.sentry_tags.value().is_some());
        }
        assert_eq!(trace(&event).exclusive_time.value(), Some(&3000.0));
        assert_eq!(
            trace(&event)
                .data
                .value()
                .unwrap()
                .get_str("sentry.dsc.project_id"),
            Some("42")
        );
        assert_eq!(trace(&event).status.value().unwrap().as_str(), "unknown");
        assert_eq!(trace(&event).client_sample_rate.value(), Some(&0.25));
    }

    #[test]
    fn replay_does_not_expose_new_score_predicate_fields() {
        // L082: score is active, while replay has no supported Event Getter path.
        let score = score();
        let event = normalize(
            mobile_transaction(),
            &NormalizationConfig {
                performance_score: Some(&score),
                replay_id: Some(uuid::Uuid::from_u128(1)),
                ..Default::default()
            },
        );
        assert!(event.measurement("score.total").is_some());
        assert!(event.context::<ReplayContext>().is_some());
        assert!(event.get_value("event.contexts.replay.replay_id").is_none());
    }

    #[test]
    fn request_validation_cannot_create_an_options_classification_match() {
        // L022: rejected methods do not turn into OPTIONS; valid other methods stay other.
        for (method, normalized) in [(" options ", None), ("OPTION1", None), ("get", Some("GET"))] {
            let event = normalize(
                transaction(json!({
                    "transaction": "/users/12345", "request": {"method": method},
                    "sdk": {"name": "sentry.javascript.node", "version": "9.0.0", "integrations": ["Express"]},
                })),
                &NormalizationConfig::default(),
            );
            assert_eq!(event.transaction.as_str(), Some("/users/12345"));
            assert_eq!(event.request.value().unwrap().method.as_str(), normalized);
        }
    }

    #[test]
    fn nontransaction_type_inference_cannot_enable_transaction_only_passes() {
        // L006, L008, L023, L070, L072, L075, L084, L113.
        let dsc = dsc();
        let score = score();
        for patch in [
            json!({}),
            json!({"csp": {}}),
            error_with_errno(json!({})),
            json!({"type": "user_report_v2"}),
        ] {
            let mut payload = mobile_transaction();
            payload.as_object_mut().unwrap().remove("type");
            payload
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            payload["contexts"]["trace"]["data"]["http.method"] = json!("GET");
            payload.as_object_mut().unwrap().remove("transaction");
            payload["contexts"]["trace"]
                .as_object_mut()
                .unwrap()
                .remove("trace_id");
            payload["contexts"]["trace"]
                .as_object_mut()
                .unwrap()
                .remove("span_id");
            let event = normalize(
                payload,
                &NormalizationConfig {
                    force_trace_context: true,
                    dsc: Some(&dsc),
                    performance_score: Some(&score),
                    ..Default::default()
                },
            );
            assert_ne!(event.ty.value(), Some(&EventType::Transaction));
            assert!(event.transaction.value().is_none());
            assert!(event.transaction_info.value().is_none());
            assert!(
                trace(&event)
                    .data
                    .value()
                    .unwrap()
                    .get_str("http.request.method")
                    .is_none()
            );
            assert!(event.measurements.value().is_none());
            assert_eq!(event.tag_value("app.vitals.start.type"), None);
            assert!(
                trace(&event)
                    .data
                    .value()
                    .unwrap()
                    .get_str("app.vitals.start.screen")
                    .is_none()
            );
            assert!(
                trace(&event)
                    .data
                    .value()
                    .unwrap()
                    .get_str("sentry.dsc.project_id")
                    .is_none()
            );
            assert_eq!(child(&event).op.as_str(), Some("app_start_cold"));
            assert!(child(&event).exclusive_time.value().is_none());
            assert!(trace(&event).trace_id.value().is_some());
            assert!(trace(&event).span_id.value().is_some());
        }
    }

    #[test]
    fn transaction_only_passes_run_even_when_force_trace_is_enabled() {
        // The other side of L006, L008, L023, L070, L072, L075, L084, L113.
        let dsc = dsc();
        let score = score();
        let mut payload = mobile_transaction();
        payload["contexts"]["trace"]["data"]["http.method"] = json!("GET");
        payload["contexts"]["trace"]
            .as_object_mut()
            .unwrap()
            .remove("trace_id");
        let event = normalize(
            payload,
            &NormalizationConfig {
                force_trace_context: true,
                dsc: Some(&dsc),
                performance_score: Some(&score),
                ..Default::default()
            },
        );
        assert_eq!(event.ty.value(), Some(&EventType::Transaction));
        assert_eq!(
            trace(&event)
                .data
                .value()
                .unwrap()
                .get_str("http.request.method"),
            Some("GET")
        );
        assert!(event.measurement("score.total").is_some());
        assert_eq!(event.tag_value("app.vitals.start.type"), Some("cold"));
        assert_eq!(child(&event).op.as_str(), Some("app.start.cold"));
        assert_eq!(child(&event).exclusive_time.value(), Some(&1000.0));
        // Transaction validation, rather than force_trace_context, controls missing IDs.
        assert!(trace(&event).trace_id.value().is_none());
    }

    #[test]
    fn conventions_precede_current_attribute_producers() {
        // L001-L005, L007, L009-L012: canonical names survive all active later producers.
        let mut payload = mobile_transaction();
        payload["contexts"]["trace"]["data"]["http.method"] = json!("GET");
        payload["spans"][0].as_object_mut().unwrap().remove("op");
        payload["spans"][0]["data"] = json!({"http.method": "GET"});
        let rules: SpanOpDefaults = serde_json::from_value(json!({"rules": [{
            "condition": {"op": "eq", "name": "span.data.http\\.request\\.method", "value": "GET"},
            "value": "http.client",
        }]}))
        .unwrap();
        payload["spans"].as_array_mut().unwrap().push(json!({
            "span_id": "cccccccccccccccc", "parent_span_id": "aaaaaaaaaaaaaaaa",
            "op": "gen_ai.invoke_agent", "start_timestamp": 1_700_000_001.0,
            "timestamp": 1_700_000_002.0,
            "data": {"http.method": "POST", "gen_ai.request.model": "model"},
        }));
        let dsc = dsc();
        let score = score();
        let event = normalize(
            payload,
            &NormalizationConfig {
                span_op_defaults: rules.borrow(),
                normalize_user_agent: Some(true),
                dsc: Some(&dsc),
                performance_score: Some(&score),
                enrich_spans: true,
                replay_id: Some(uuid::Uuid::from_u128(1)),
                client_sample_rate: Some(0.25),
                ..Default::default()
            },
        );
        assert_eq!(
            trace(&event)
                .data
                .value()
                .unwrap()
                .get_str("http.request.method"),
            Some("GET")
        );
        assert_eq!(
            child(&event)
                .data
                .value()
                .unwrap()
                .get_str("http.request.method"),
            Some("GET")
        );
        assert_eq!(child(&event).op.as_str(), Some("http.client"));
        let ai_data = event.spans.value().unwrap()[1]
            .value()
            .unwrap()
            .data
            .value()
            .unwrap();
        assert_eq!(ai_data.get_str("http.request.method"), Some("POST"));
        assert_eq!(ai_data.get_str("gen_ai.operation.type"), Some("agent"));
        assert_eq!(
            child(&event)
                .data
                .value()
                .unwrap()
                .get_str("sentry.dsc.project_id"),
            Some("42")
        );
        assert_eq!(
            child(&event)
                .data
                .value()
                .unwrap()
                .get_str("sentry.segment.name"),
            Some("Checkout")
        );
        assert!(event.measurement("score.total").is_some());
        assert!(event.context::<OsContext>().is_some());
        assert!(event.context::<ReplayContext>().is_some());
        assert_eq!(event.tag_value("app.vitals.start.type"), Some("cold"));
        assert_eq!(event.level.value().unwrap().name(), "info");
        assert_eq!(trace(&event).client_sample_rate.value(), Some(&0.25));
    }

    #[test]
    fn transaction_status_tag_classification_survives_late_producers() {
        // L013-L018, L020-L021, L027: make classification depend on the first status tag.
        let score = score();
        for (first_status, expected) in [("404", "/users/*"), ("200", "/users/12345")] {
            let mut payload = mobile_transaction();
            payload["transaction"] = json!("/users/12345");
            payload["sdk"] =
                json!({"name": "sentry.python", "version": "9.0.0", "integrations": ["django"]});
            payload["tags"] = json!([
                ["http.status_code", first_status],
                ["http.status_code", "404"],
                ["environment", "production"],
                ["release", "obsolete"]
            ]);
            payload["csp"] = json!({});
            payload.as_object_mut().unwrap().remove("request");
            let mut config = NormalizationConfig {
                normalize_user_agent: Some(true),
                performance_score: Some(&score),
                replay_id: Some(uuid::Uuid::from_u128(1)),
                client_sample_rate: Some(0.25),
                ..Default::default()
            };
            config.user_agent.client_hints.sec_ch_ua_platform = Some("\"Android\"");
            let event = normalize(payload, &config);
            assert_eq!(event.transaction.as_str(), Some(expected));
            assert_eq!(event.tag_value("http.status_code"), Some(first_status));
            assert_eq!(event.environment.as_str(), Some("production"));
            assert_eq!(event.tag_value("release"), None);
            assert_eq!(event.tag_value("device.class"), Some("3"));
            assert_eq!(event.tag_value("app.vitals.start.type"), Some("cold"));
            assert_eq!(event.tag_value("lcp.element"), Some("img.hero"));
            assert_eq!(
                event.context::<OsContext>().unwrap().name.as_str(),
                Some("Android")
            );
            assert!(event.measurement("score.total").is_some());
            assert!(event.context::<ReplayContext>().is_some());
            assert_eq!(event.logger.as_str(), Some("csp"));
            assert_eq!(
                event
                    .request
                    .value()
                    .unwrap()
                    .headers
                    .value()
                    .unwrap()
                    .get_header("SEC-CH-UA-Platform"),
                Some("\"Android\"")
            );
            assert!(event.request.value().unwrap().method.value().is_none());
            assert_eq!(trace(&event).op.as_str(), Some("ui.load"));
            assert_eq!(trace(&event).status.value().unwrap().as_str(), "unknown");
            assert_eq!(trace(&event).client_sample_rate.value(), Some(&0.25));
        }
    }
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

mod ordering_controls {
    use super::*;
    use relay_event_schema::protocol::{
        IpAddr, PerformanceScoreContext, ReplayContext, TraceContext,
    };

    #[test]
    fn legacy_environment_is_already_trimmed_by_tag_parsing() {
        // L038: ordinary surrounding whitespace is handled by TagEntry::from_value.
        let config = NormalizationConfig::default();
        let direct = normalize(json!({"environment": " production "}), &config);
        assert_eq!(direct.environment.as_str(), Some("production"));
        let migrated = normalize(json!({"tags": {"environment": " production "}}), &config);
        assert_eq!(migrated.environment.as_str(), direct.environment.as_str());
    }

    fn input() -> Value {
        transaction(json!({
            "platform": "javascript", "environment": "production", "logger": "app",
            "transaction": "checkout", "sdk": {"name": "sentry.javascript.browser", "version": "9.0.0"},
            "user": {"id": "42", "ip_address": "203.0.113.1"},
            "tags": {"http.status_code": "404", "lcp.element": "img.hero"},
            "request": {"method": "GET", "url": "https://example.com/path"},
            "contexts": {
                "device": {"family": "iPhone", "model": "iPhone13,1"},
                "os": {"name": "Android", "version": "11"},
                "trace": {
                    "trace_id": "22222222222222222222222222222222", "span_id": "aaaaaaaaaaaaaaaa",
                    "op": "ui.load", "status": "ok", "data": {"browser.web_vital.lcp.element": "img.other"},
                },
            },
            "exception": {"values": [{"type": "OSError", "mechanism": {
                "type": "generic", "meta": {"errno": {"number": 2}},
            }}]},
            "spans": [{
                "span_id": "bbbbbbbbbbbbbbbb", "parent_span_id": "aaaaaaaaaaaaaaaa", "op": "db",
                "start_timestamp": 1_700_000_000.0, "timestamp": 1_700_000_001.0,
                "data": {"db.system": "sqlite"},
            }],
        }))
    }

    fn early_values(event: &Event) -> Value {
        let exceptions = &event.exceptions;
        json!({
            "sdk": event.sdk_name(), "platform": event.platform.as_str(),
            "logger": event.logger.as_str(), "environment": event.environment.as_str(),
            "transaction": event.transaction.as_str(),
            "os": event.context::<OsContext>().and_then(|c| c.name.as_str()),
            "device": event.context::<DeviceContext>().and_then(|c| c.model.as_str()),
            "device.class": event.tag_value("device.class"),
            "http.status_code": event.tag_value("http.status_code"),
            "lcp.element": event.tag_value("lcp.element"),
            "errno": get_value!(exceptions.values[0].mechanism.meta.errno.name).map(String::as_str),
            "ip": event.user.value().and_then(|u| u.ip_address.as_str()),
            "user": event.user.value().and_then(|u| u.sentry_user.as_str()),
            "method": event.request.value().and_then(|r| r.method.as_str()),
            "trace.op": event.context::<TraceContext>().and_then(|t| t.op.as_str()),
            "trace.status": event.context::<TraceContext>().and_then(|t| t.status.value()).map(|s| s.as_str()),
        })
    }

    fn check(config: &NormalizationConfig<'_>) -> Event {
        let baseline = normalize(
            input(),
            &NormalizationConfig {
                normalize_spans: false,
                ..Default::default()
            },
        );
        let enriched = normalize(input(), config);
        assert_eq!(early_values(&baseline), early_values(&enriched));
        enriched
    }

    #[test]
    fn replay_context_preserves_early_inputs() {
        let event = check(&NormalizationConfig {
            replay_id: Some(uuid::Uuid::from_u128(1)),
            ..Default::default()
        });
        assert!(event.context::<ReplayContext>().is_some());
    }

    #[test]
    fn client_sample_rate_preserves_early_inputs() {
        let event = check(&NormalizationConfig {
            client_sample_rate: Some(0.25),
            ..Default::default()
        });
        assert_eq!(
            event
                .context::<TraceContext>()
                .unwrap()
                .client_sample_rate
                .value()
                .copied(),
            Some(0.25)
        );
    }

    #[test]
    fn span_normalization_preserves_early_inputs() {
        let mut dsc: relay_sampling::DynamicSamplingContext = serde_json::from_value(json!({
            "trace_id": "22222222222222222222222222222222", "public_key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        })).unwrap();
        dsc.project_id = Some(relay_base_schema::project::ProjectId::new(42));
        let event = check(&NormalizationConfig {
            dsc: Some(&dsc),
            ..Default::default()
        });
        let span = event.spans.value().unwrap()[0].value().unwrap();
        assert_eq!(span.exclusive_time.value().copied(), Some(1000.0));
        assert_eq!(
            span.data.value().unwrap().get_str("sentry.dsc.project_id"),
            Some("42")
        );
    }

    #[test]
    fn span_enrichment_preserves_early_inputs() {
        let event = check(&NormalizationConfig {
            enrich_spans: true,
            ..Default::default()
        });
        let span = event.spans.value().unwrap()[0].value().unwrap();
        assert!(span.sentry_tags.value().is_some());
        assert_eq!(
            span.data.value().unwrap().get_str("sentry.segment.name"),
            Some("checkout")
        );
    }

    #[test]
    fn score_context_preserves_early_inputs() {
        let profile = serde_json::from_value(json!({"profiles": [{
            "condition": {"op": "eq", "name": "event.sdk.name", "value": "sentry.javascript.browser"},
            "version": "audit", "scoreComponents": [{"measurement": "lcp", "weight": 1, "p10": 1000, "p50": 2000}],
        }]})).unwrap();
        let event = check(&NormalizationConfig {
            performance_score: Some(&profile),
            ..Default::default()
        });
        assert!(event.measurement("score.total").is_some());
        assert_eq!(
            event
                .context::<PerformanceScoreContext>()
                .unwrap()
                .score_profile_version
                .as_str(),
            Some("audit")
        );
    }

    #[test]
    fn user_agent_preserves_existing_contexts() {
        let mut payload = input();
        payload["request"]["headers"] =
            json!({"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) - (-)"});
        let baseline = normalize(payload.clone(), &NormalizationConfig::default());
        let inferred = normalize(
            payload,
            &NormalizationConfig {
                normalize_user_agent: Some(true),
                ..Default::default()
            },
        );
        assert_eq!(early_values(&baseline), early_values(&inferred));
    }

    #[test]
    fn platform_fallback_preserves_ua_destination_and_ip_policy() {
        let ip = IpAddr::parse("203.0.113.1").unwrap();
        for platform in [None, Some("invalid-platform"), Some("other")] {
            for auto in [false, true] {
                let mut payload =
                    json!({"request": {"headers": {"Sec-CH-UA-Platform": "\"Android\""}}});
                if let Some(platform) = platform {
                    payload["platform"] = json!(platform);
                }
                if auto {
                    payload["user"] = json!({"ip_address": "{{auto}}"});
                }
                let event = normalize(
                    payload,
                    &NormalizationConfig {
                        normalize_user_agent: Some(true),
                        client_ip: Some(&ip),
                        client: Some("sentry.cocoa/9.0.0".into()),
                        ..Default::default()
                    },
                );
                assert_eq!(event.platform.as_str(), Some("other"));
                assert!(event.context::<OsContext>().is_none());
                assert!(event.contexts.value().unwrap().0.contains_key("client_os"));
                assert_eq!(
                    event.user.value().and_then(|u| u.ip_address.as_str()),
                    auto.then_some("203.0.113.1")
                );
            }
        }
    }

    #[test]
    fn transaction_classification_already_ignores_method_case() {
        for method in ["options", "OPTIONS", "OpTiOnS"] {
            let event = normalize(
                transaction(json!({
                    "transaction": "/users/12345", "request": {"method": method},
                    "sdk": {"name": "sentry.javascript.node", "version": "9.0.0", "integrations": ["Express"]},
                })),
                &NormalizationConfig::default(),
            );
            assert_eq!(event.transaction.as_str(), Some("/users/*"));
            assert_eq!(
                event.request.value().unwrap().method.as_str(),
                Some("OPTIONS")
            );
        }
    }

    #[test]
    fn transaction_classification_already_defaults_trace_status() {
        for trace in [json!({}), json!({"status": "unknown"})] {
            let event = normalize(
                transaction(json!({
                    "transaction": "/users/12345", "modules": {"rack": "3.0"},
                    "sdk": {"name": "sentry.ruby", "version": "9.0.0"}, "contexts": {"trace": trace},
                })),
                &NormalizationConfig::default(),
            );
            assert_eq!(event.transaction.as_str(), Some("/users/12345"));
            assert_eq!(
                event
                    .context::<TraceContext>()
                    .unwrap()
                    .status
                    .value()
                    .unwrap()
                    .as_str(),
                "unknown"
            );
        }
    }

    #[test]
    fn trace_creation_and_transaction_only_consumers_are_disjoint() {
        let profile = serde_json::from_value(json!({"profiles": [{
            "condition": {"op": "eq", "name": "event.contexts.trace.status", "value": "unknown"},
            "scoreComponents": [{"measurement": "lcp", "weight": 1, "p10": 1000, "p50": 2000}],
        }]}))
        .unwrap();
        let event = normalize(
            error_with_errno(json!({
                "contexts": {"os": {"name": "Android"}}, "measurements": {"lcp": {"value": 1200}},
            })),
            &NormalizationConfig {
                force_trace_context: true,
                performance_score: Some(&profile),
                ..Default::default()
            },
        );
        assert_errno_enriched(&event);
        assert!(
            event
                .context::<TraceContext>()
                .unwrap()
                .trace_id
                .value()
                .is_some()
        );
        assert!(event.measurements.value().is_none());
        assert!(event.measurement("score.total").is_none());
    }

    #[test]
    fn device_marketing_name_does_not_change_class_inputs() {
        let event = normalize(
            json!({"contexts": {"device": {
                "family": "Samsung", "model": "SM-G991B", "processor_frequency": 3000,
                "processor_count": 8, "memory_size": 8589934592_u64,
            }}}),
            &NormalizationConfig::default(),
        );
        assert_eq!(
            event.context::<DeviceContext>().unwrap().name.as_str(),
            Some("Galaxy S21 5G")
        );
        assert_eq!(event.tag_value("device.class"), Some("3"));
    }

    #[test]
    fn transferred_stacktrace_is_already_normalized() {
        let stacktrace =
            json!({"frames": [{"filename": "https://example.com/app.js", "function": "?"}]});
        let moved = normalize(
            json!({"stacktrace": stacktrace, "exception": {"values": [{"type": "Error"}]}}),
            &NormalizationConfig::default(),
        );
        let direct = normalize(
            json!({"exception": {"values": [{"type": "Error", "stacktrace": stacktrace}]}}),
            &NormalizationConfig::default(),
        );
        assert!(moved.stacktrace.value().is_none());
        let moved_exceptions = &moved.exceptions;
        let direct_exceptions = &direct.exceptions;
        let moved_frame = get_value!(moved_exceptions.values[0].stacktrace.frames[0]).unwrap();
        let direct_frame = get_value!(direct_exceptions.values[0].stacktrace.frames[0]).unwrap();
        assert_eq!(moved_frame, direct_frame);
        assert_eq!(
            moved_frame.filename.value().map(|p| p.0.as_str()),
            Some("/app.js")
        );
        assert!(moved_frame.function.value().is_none());
    }

    #[test]
    fn request_cookie_parsing_preserves_user_agent_headers() {
        let event = normalize(
            json!({"platform": "javascript", "request": {"headers": {
                "Cookie": "sid=abc", "Sec-CH-UA-Platform": "\"Android\"",
            }}}),
            &NormalizationConfig {
                normalize_user_agent: Some(true),
                ..Default::default()
            },
        );
        assert_eq!(
            event.context::<OsContext>().unwrap().name.as_str(),
            Some("Android")
        );
        let request = event.request.value().unwrap();
        assert!(request.cookies.value().is_some());
        assert!(
            request
                .headers
                .value()
                .unwrap()
                .get_header("Cookie")
                .is_none()
        );
        assert_eq!(
            request
                .headers
                .value()
                .unwrap()
                .get_header("Sec-CH-UA-Platform"),
            Some("\"Android\"")
        );
    }

    #[test]
    fn security_and_generic_defaults_preserve_specific_values() {
        let event = normalize(
            json!({"csp": {}, "user": {"id": "42"}}),
            &NormalizationConfig {
                client_ip: Some(&IpAddr::parse("203.0.113.1").unwrap()),
                geoip_lookup: Some(&relay_event_normalization::GeoIpLookup::empty()),
                ..Default::default()
            },
        );
        assert_eq!(event.logger.as_str(), Some("csp"));
        let user = event.user.value().unwrap();
        assert_eq!(user.ip_address.as_str(), Some("203.0.113.1"));
        assert_eq!(user.sentry_user.as_str(), Some("id:42"));
    }

    #[test]
    fn security_ip_survives_successful_geo_and_user_enrichment() {
        // L028-L031, L033-L034, L037: exercise a real lookup, not an empty DB.
        let lookup = relay_event_normalization::GeoIpLookup::open(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/GeoIP2-Enterprise-Test.mmdb",
        ))
        .unwrap();
        let expected_geo = lookup.lookup("2.125.160.216".parse().unwrap()).unwrap();
        assert!(expected_geo.country_code.value().is_some());
        let client_ip = IpAddr::parse("2.125.160.216").unwrap();
        let event = normalize(
            json!({"csp": {}, "user": {"id": "42"}}),
            &NormalizationConfig {
                client_ip: Some(&client_ip),
                geoip_lookup: Some(&lookup),
                ..Default::default()
            },
        );
        assert_eq!(event.logger.as_str(), Some("csp"));
        let user = event.user.value().unwrap();
        assert_eq!(user.ip_address.as_str(), Some("2.125.160.216"));
        assert_eq!(user.geo.value(), Some(&expected_geo));
        assert_eq!(user.sentry_user.as_str(), Some("id:42"));
    }

    #[test]
    fn remote_addr_survives_request_normalization_and_supplies_geo() {
        // L036: later URL/method/cookie normalization leaves REMOTE_ADDR alone.
        let lookup = relay_event_normalization::GeoIpLookup::open(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/GeoIP2-Enterprise-Test.mmdb",
        ))
        .unwrap();
        let expected_geo = lookup.lookup("2.125.160.216".parse().unwrap()).unwrap();
        let event = normalize(
            json!({"request": {
                "env": {"REMOTE_ADDR": "2.125.160.216"},
                "method": "get", "url": "https://example.com/path?q=1",
                "headers": {"Cookie": "session=abc"},
            }}),
            &NormalizationConfig {
                client_ip: Some(&IpAddr::parse("203.0.113.1").unwrap()),
                geoip_lookup: Some(&lookup),
                ..Default::default()
            },
        );
        let request = event.request.value().unwrap();
        assert_eq!(request.method.as_str(), Some("GET"));
        assert!(request.cookies.value().is_some());
        assert!(
            request
                .headers
                .value()
                .unwrap()
                .get_header("Cookie")
                .is_none()
        );
        let user = event.user.value().unwrap();
        assert_eq!(user.ip_address.as_str(), Some("2.125.160.216"));
        assert_eq!(user.geo.value(), Some(&expected_geo));
        assert_eq!(user.sentry_user.as_str(), Some("ip:2.125.160.216"));
    }

    #[test]
    fn app_start_backfill_preserves_unrelated_tags_and_trace_fields() {
        let mut payload = input();
        payload["measurements"] = json!({"app_start_cold": {"value": 1000, "unit": "millisecond"}});
        let event = normalize(payload, &NormalizationConfig::default());
        assert_eq!(event.tag_value("app.vitals.start.type"), Some("cold"));
        assert_eq!(event.tag_value("http.status_code"), Some("404"));
        assert_eq!(event.tag_value("lcp.element"), Some("img.hero"));
        assert_eq!(
            event.context::<TraceContext>().unwrap().op.as_str(),
            Some("ui.load")
        );
    }

    #[test]
    fn ai_payload_enrichment_is_independent_of_late_metadata() {
        let mut payload = input();
        payload["spans"][0]["op"] = json!("gen_ai.invoke_agent");
        payload["spans"][0]["data"] = json!({"gen_ai.request.model": "model"});
        let baseline = normalize(payload.clone(), &NormalizationConfig::default());
        let enriched = normalize(
            payload,
            &NormalizationConfig {
                enrich_spans: true,
                replay_id: Some(uuid::Uuid::from_u128(1)),
                client_sample_rate: Some(0.25),
                ..Default::default()
            },
        );
        for event in [&baseline, &enriched] {
            let data = event.spans.value().unwrap()[0]
                .value()
                .unwrap()
                .data
                .value()
                .unwrap();
            assert_eq!(data.get_str("gen_ai.operation.type"), Some("agent"));
            assert_eq!(data.get_str("gen_ai.response.model"), Some("model"));
        }
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

fn tag<'a>(event: &'a Value, name: &str) -> Option<&'a str> {
    event.get("tags")?.as_array()?.iter().find_map(|pair| {
        (pair.get(0)?.as_str()? == name)
            .then(|| pair.get(1)?.as_str())
            .flatten()
    })
}

#[test]
fn exclusive_time_after_span_trimming() {
    let mut input = transaction_with_span();
    let mut filler = input["spans"][0].clone();
    filler["span_id"] = json!("dddddddddddddddd");
    filler["parent_span_id"] = json!("eeeeeeeeeeeeeeee");
    filler["op"] = json!("custom");
    filler["description"] = json!("x".repeat(900_000));
    input["spans"].as_array_mut().unwrap().insert(0, filler);
    let untrimmed = normalize_json(input.clone(), &NormalizationConfig::default());
    let output = normalize_json(
        input,
        &NormalizationConfig {
            enable_trimming: true,
            ..Default::default()
        },
    );
    let mut retained = output.clone();
    retained["contexts"]["trace"]
        .as_object_mut()
        .unwrap()
        .remove("exclusive_time");
    let recomputed = normalize_json(retained, &NormalizationConfig::default());
    observed(
        "exclusive_time_after_span_trimming",
        json!({
            "remaining_spans":output["spans"].as_array().map(Vec::len),
            "exclusive_time":output.pointer("/contexts/trace/exclusive_time"),
            "exclusive_time_before_trimming":untrimmed.pointer("/contexts/trace/exclusive_time"),
            "exclusive_time_from_retained_spans":recomputed.pointer("/contexts/trace/exclusive_time"),
            "span_array_original_length":output.pointer("/_meta/spans//len")
        }),
    );
    assert!(
        output["spans"].as_array().unwrap().len() < 2,
        "The late trimming must actually remove the child"
    );
    assert_eq!(
        output.pointer("/contexts/trace/exclusive_time"),
        untrimmed.pointer("/contexts/trace/exclusive_time")
    );
    assert_eq!(
        recomputed.pointer("/contexts/trace/exclusive_time"),
        Some(&json!(5000.0))
    );
}

#[test]
fn chain_controls() {
    for trimming in [false, true] {
        let mut input = transaction_with_span();
        input["contexts"]["device"] = json!({"family":"iPhone","model":"iPhone"});
        input["contexts"]["os"] = json!({"name":"Android","version":"11"});
        input["request"] = json!({"method":"get","url":"https://example.test/a?q=1","env":{"REMOTE_ADDR":"1.2.3.4"}});
        input["user"] = json!({"id":"short","username":"name","email":"a@example.test","ip_address":"{{auto}}"});
        input["tags"] = json!({"environment":"production","unrelated":"kept"});
        input["server_name"] = json!("host");
        input["site"] = json!("site");
        input["exception"] = json!({"values":[{"type":"OSError","mechanism":{"type":"generic","meta":{"errno":{"number":2}}}}]});
        input["stacktrace"] = stacktrace();
        let mut cfg = span_config(trimming);
        cfg.client = Some("sentry.javascript.browser/9.0.0".into());
        cfg.remove_other = true;
        let output = normalize_json(input, &cfg);
        for (path, expected) in [
            ("/spans/0/sentry_tags/environment", "production"),
            ("/spans/0/sentry_tags/device.class", "3"),
            ("/spans/0/sentry_tags/trace.status", "unknown"),
            ("/spans/0/sentry_tags/transaction.method", "GET"),
            ("/spans/0/sentry_tags/sdk.name", "sentry.javascript.browser"),
            ("/spans/0/sentry_tags/sdk.version", "9.0.0"),
            ("/spans/0/sentry_tags/user.ip", "1.2.3.4"),
            ("/exception/values/0/mechanism/meta/errno/name", "ENOENT"),
            (
                "/exception/values/0/stacktrace/frames/0/filename",
                "/app.js",
            ),
        ] {
            assert_eq!(
                output.pointer(path).and_then(Value::as_str),
                Some(expected),
                "{path}"
            );
        }
        assert_eq!(
            output.pointer("/contexts/trace/exclusive_time"),
            Some(&json!(4250.0))
        );
        assert_eq!(tag(&output, "server_name"), Some("host"));
        assert_eq!(tag(&output, "site"), Some("site"));
        assert_eq!(tag(&output, "unrelated"), Some("kept"));
        assert!(output.pointer("/stacktrace").is_none());
        assert_eq!(
            output.pointer("/spans/0/op"),
            output.pointer("/spans/0/sentry_tags/op")
        );
        let mut rejected_release = transaction_with_span();
        rejected_release["release"] = json!("latest");
        let rejected_release = normalize_json(rejected_release, &cfg);
        assert!(
            rejected_release
                .pointer("/release")
                .and_then(Value::as_str)
                .is_none()
        );
        assert!(
            rejected_release
                .pointer("/spans/0/sentry_tags/release")
                .and_then(Value::as_str)
                .is_none()
        );
    }
    let output = normalize_json(
        json!({"event_id":"11111111111111111111111111111111"}),
        &NormalizationConfig {
            force_trace_context: true,
            ..Default::default()
        },
    );
    assert_eq!(
        output.pointer("/contexts/trace/trace_id"),
        Some(&json!("11111111111111111111111111111111"))
    );
    observed("chain_controls", json!({"passed":true}));
}

#[test]
fn derivation_controls() {
    // Rejection is the trigger: a valid exception retains the transferred trace.
    let moved = normalize_json(
        json!({"stacktrace": stacktrace(), "exception": {"values": [{"type": "Error"}]}}),
        &span_config(false),
    );
    assert_eq!(
        moved.pointer("/exception/values/0/stacktrace/frames/0/filename"),
        Some(&json!("/app.js"))
    );
    let standalone = normalize_json(json!({"stacktrace": stacktrace()}), &span_config(false));
    assert_eq!(
        standalone.pointer("/stacktrace/frames/0/filename"),
        Some(&json!("/app.js"))
    );

    // Trimming is the trigger: the same derived values agree without it.
    let mut input = transaction_with_span();
    input["transaction"] = json!("HomeScreen".repeat(30));
    input["user"] = json!({"id": "u".repeat(200)});
    input["measurements"] = json!({"app_start_cold": {"value": 1000.0, "unit": "millisecond"}});
    let output = normalize_json(input, &span_config(false));
    assert_eq!(
        output.pointer("/transaction"),
        output.pointer("/spans/0/data/sentry.segment.name")
    );
    assert_eq!(
        output.pointer("/transaction"),
        output.pointer("/contexts/trace/data/app.vitals.start.screen")
    );
    assert_eq!(
        output.pointer("/user/sentry_user"),
        Some(&json!(format!("id:{}", "u".repeat(200))))
    );

    // Short inputs remain consistent even when trimming is enabled.
    let mut input = transaction_with_span();
    input["user"] = json!({"id": "short"});
    let output = normalize_json(input, &span_config(true));
    assert_eq!(
        output.pointer("/user/sentry_user"),
        Some(&json!("id:short"))
    );
    assert_eq!(
        output.pointer("/transaction"),
        output.pointer("/spans/0/data/sentry.segment.name")
    );
    observed("controls", json!({"passed":true}));
}

#[test]
fn materialized_summary_retention() {
    let text = "x".repeat(10000);
    let body = json!({"text": text}).to_string();
    for (kind, input, source, summary) in [
        (
            "logentry",
            json!({"logentry": {"message": "{}", "params": ["x".repeat(3000)]}}),
            "/logentry/params/0",
            "/logentry/formatted",
        ),
        (
            "request",
            json!({"request": {"data": body}}),
            "/request/data/text",
            "/request/inferred_content_type",
        ),
        (
            "response",
            json!({"contexts": {"response": {"data": body}}}),
            "/contexts/response/data/text",
            "/contexts/response/inferred_content_type",
        ),
    ] {
        let control = normalize_json(input.clone(), &NormalizationConfig::default());
        let retained = normalize_json(
            input,
            &NormalizationConfig {
                enable_trimming: true,
                ..Default::default()
            },
        );
        let before = control
            .pointer(source)
            .and_then(Value::as_str)
            .unwrap()
            .len();
        let after = retained
            .pointer(source)
            .and_then(Value::as_str)
            .unwrap()
            .len();
        observed(
            "materialized_summary_retention",
            json!({"kind": kind, "source_bytes": before, "retained_bytes": after, "summary_preserved": control.pointer(summary) == retained.pointer(summary)}),
        );
        assert!(after < before);
        if kind == "response" {
            assert!(retained.pointer(summary).is_none_or(Value::is_null));
        } else {
            assert_eq!(control.pointer(summary), retained.pointer(summary));
        }
    }
}
