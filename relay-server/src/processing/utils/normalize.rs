use std::time::Duration;

use relay_dynamic_config::{RetentionConfig, RetentionsConfig};
use relay_event_normalization::eap;
use relay_event_normalization::eap::time::TimeEnforcement;

use crate::envelope::EnvelopeHeaders;
use crate::processing::Context;
use crate::services::processor::MINIMUM_CLOCK_DRIFT;

/// Creates a lazy enrichment cache for one container using the span tag limit.
pub fn user_agent(ctx: Context<'_>) -> eap::UserAgentNormalizer {
    eap::UserAgentNormalizer::new(
        ctx.config
            .aggregator_config_for(relay_metrics::MetricNamespace::Spans)
            .max_tag_value_length,
    )
}

/// Utility to create an [`eap::time::Config`].
pub fn time_config<F>(headers: &EnvelopeHeaders, f: F, ctx: Context<'_>) -> eap::time::Config
where
    F: FnOnce(&RetentionsConfig) -> Option<&RetentionConfig>,
{
    eap::time::Config {
        apply_sequence_shift: ctx.is_processing(),
        received_at: headers.meta().received_at(),
        sent_at: headers.sent_at(),
        max_in_past: Some(TimeEnforcement::Reject(retention_days_to_duration(
            ctx.to_forward().retention(f).standard,
        ))),
        max_in_future: ctx
            .config
            .max_secs_in_future()
            .try_into()
            .ok()
            .map(Duration::from_secs)
            .map(TimeEnforcement::Shift),
        minimum_clock_drift: MINIMUM_CLOCK_DRIFT,
    }
}

fn retention_days_to_duration(days: u16) -> Duration {
    const DAYS_TO_SECONDS: u64 = 24 * 60 * 60;
    Duration::from_secs(days as u64 * DAYS_TO_SECONDS)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::envelope::{ContentType, ItemType};
    use crate::managed::ManagedEnvelope;
    use crate::processing::logs::LogsProcessor;
    use crate::processing::spans::SpansProcessor;
    use crate::processing::trace_metrics::TraceMetricsProcessor;
    use crate::processing::{Forward, QuotaRateLimiter};

    #[tokio::test]
    async fn user_agent_uses_configured_limit_and_inference_setting() {
        let config = relay_config::Config::from_json_value(json!({
            "relay": {"mode": "proxy"},
            "aggregator": {"max_tag_value_length": 100},
            "secondary_aggregators": [{
                "name": "spans",
                "condition": {"op": "eq", "field": "namespace", "value": "spans"},
                "config": {"max_tag_value_length": 3}
            }]
        }))
        .unwrap()
        .current();
        let ctx = Context {
            config: &config,
            ..Context::for_test()
        };
        #[cfg(feature = "processing")]
        let limiter = QuotaRateLimiter::new(
            crate::services::projects::cache::ProjectCacheHandle::for_test(),
            None,
        );
        #[cfg(not(feature = "processing"))]
        let limiter = QuotaRateLimiter::new();
        let limiter = Arc::new(limiter);
        let logs = LogsProcessor::new(limiter.clone());
        let spans = SpansProcessor::new(
            limiter.clone(),
            relay_event_normalization::GeoIpLookup::empty(),
        );
        let metrics = TraceMetricsProcessor::new(limiter);
        let timestamp = chrono::Utc::now().timestamp() as f64;
        let ua = format!("Mozilla/5.0 Chrome/131.0.0.0 {}", "x".repeat(4096));
        let hints = format!(r#""{}";v="{}""#, "x".repeat(4096), "1".repeat(4096));
        let explicit = "Mozilla/5.0 Firefox/128.0";

        let empty = json!([null, null, null]);
        let firefox = json!(["Fir", "128", explicit]);
        let chrome = json!(["Chr", "131", "Moz"]);
        let hinted = json!(["xxx", "111", null]);
        let hinted_ua = json!(["xxx", "111", "Moz"]);
        let hinted_explicit = json!(["xxx", "111", explicit]);
        // Header inputs, expected explicit-UA item, expected transport-only item.
        let cases = [
            (None, None, &firefox, &empty),
            (Some(ua.as_str()), None, &firefox, &chrome),
            (None, Some(hints.as_str()), &hinted_explicit, &hinted),
            (
                Some(ua.as_str()),
                Some(hints.as_str()),
                &hinted_explicit,
                &hinted_ua,
            ),
            (None, Some("invalid hints"), &firefox, &empty),
            (Some(ua.as_str()), Some("invalid hints"), &firefox, &chrome),
        ];
        let item_attributes = [
            json!({"browser.name": {"type": "string", "value": "existing"}}),
            json!({"browser.version": {"type": "string", "value": "existing"}}),
            json!({"user_agent.original": {"type": "string", "value": explicit}}),
            json!({}),
            json!({}),
        ];

        for (ty, content_type, item) in [
            (
                ItemType::Log,
                ContentType::LogContainer,
                json!({
                    "timestamp": timestamp, "level": "info", "body": "test"
                }),
            ),
            (
                ItemType::Span,
                ContentType::SpanV2Container,
                json!({
                    "start_timestamp": timestamp, "end_timestamp": timestamp + 0.5,
                    "name": "test", "status": "ok"
                }),
            ),
            (
                ItemType::TraceMetric,
                ContentType::TraceMetricContainer,
                json!({
                    "timestamp": timestamp, "name": "test.metric", "type": "counter", "value": 1.0
                }),
            ),
        ] {
            let items: Vec<_> = item_attributes
                .iter()
                .enumerate()
                .map(|(index, attributes)| {
                    let mut item = item.clone();
                    item["trace_id"] = json!("0123456789abcdef0123456789abcdef");
                    if ty == ItemType::Span {
                        item["span_id"] = json!(format!("{:016x}", index + 1));
                    }
                    item["attributes"] = attributes.clone();
                    item
                })
                .collect();
            for (case, (ua, hints, explicit, transport)) in cases.iter().enumerate() {
                for infer in [false, true] {
                    let headers = json!({
                        "dsn": "https://a94ae32be2584e0bbd7a4cbb95971fee@sentry.io/42",
                        "user_agent": ua, "client_hints": {"sec_ch_ua": hints},
                        "trace": {
                            "trace_id": "0123456789abcdef0123456789abcdef",
                            "public_key": "a94ae32be2584e0bbd7a4cbb95971fee"
                        }
                    });
                    let payload = json!({
                        "version": 2, "items": items,
                        "ingest_settings": {"infer_user_agent": if infer {"auto"} else {"never"}}
                    });
                    let wire = format!(
                        "{}\n{}\n{}\n",
                        headers,
                        json!({
                            "type": ty, "content_type": content_type.as_str(), "item_count": items.len()
                        }),
                        payload
                    );
                    let output = match ty {
                        ItemType::Log => process_user_agent_container(&logs, &wire, ctx).await,
                        ItemType::Span => process_user_agent_container(&spans, &wire, ctx).await,
                        ItemType::TraceMetric => {
                            process_user_agent_container(&metrics, &wire, ctx).await
                        }
                        _ => unreachable!(),
                    };
                    let actual: Vec<_> = output["items"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|item| {
                            ["browser.name", "browser.version", "user_agent.original"]
                                .map(|key| item["attributes"][key]["value"].clone())
                        })
                        .collect();
                    // Skipped items must not prevent later items from using the shared cache.
                    let (explicit, transport) = if infer {
                        (*explicit, *transport)
                    } else {
                        (&firefox, &empty)
                    };
                    assert_eq!(
                        json!(actual),
                        json!([
                            ["existing", null, null],
                            [null, "existing", null],
                            explicit,
                            transport,
                            transport
                        ]),
                        "{ty}, case={case}, infer={infer}"
                    );
                }
            }
        }
    }

    async fn process_user_agent_container(
        processor: &impl crate::processing::Processor,
        wire: &str,
        ctx: Context<'_>,
    ) -> serde_json::Value {
        let (outcomes, _received) = relay_system::Addr::custom();
        let envelope = crate::Envelope::parse_bytes(wire.to_owned().into()).unwrap();
        let mut envelope = ManagedEnvelope::new(envelope, outcomes);
        let input = processor.prepare_envelope(&mut envelope).unwrap();
        assert!(envelope.envelope().is_empty());
        envelope.accept();
        let output = processor.process(input, ctx).await.unwrap();
        let envelope = output
            .main
            .unwrap()
            .serialize_envelope(ctx.to_forward())
            .unwrap();
        envelope.accept(|envelope| {
            serde_json::from_slice(&envelope.items().next().unwrap().payload()).unwrap()
        })
    }

    #[test]
    fn test_days_to_duration() {
        assert_eq!(retention_days_to_duration(1), Duration::from_secs(86400));
    }
}
