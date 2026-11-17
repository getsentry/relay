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
    use super::*;

    #[tokio::test]
    async fn user_agent_uses_configured_limit_and_inference_setting() {
        use std::sync::Arc;

        use crate::envelope::{ContentType, ItemType};
        use crate::processing::QuotaRateLimiter;
        use crate::processing::logs::LogsProcessor;
        use crate::processing::spans::SpansProcessor;
        use crate::processing::trace_metrics::TraceMetricsProcessor;
        use serde_json::json;

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

        for infer in [false, true] {
            for ua in [None, Some(ua.as_str())] {
                for hints in [None, Some(hints.as_str()), Some("invalid hints")] {
                    for (ty, content_type, mut item) in [
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
                                "span_id": "0000000000000001", "name": "test", "status": "ok"
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
                        item["trace_id"] = json!("0123456789abcdef0123456789abcdef");
                        // Skipped items precede an explicit UA and repeated transport enrichment.
                        let items: Vec<_> = [
                            json!({"browser.name": {"type": "string", "value": "existing"}}),
                            json!({"browser.version": {"type": "string", "value": "existing"}}),
                            json!({"user_agent.original": {"type": "string", "value": explicit}}),
                            json!({}),
                            json!({}),
                        ]
                        .into_iter()
                        .enumerate()
                        .map(|(index, attributes)| {
                            let mut item = item.clone();
                            if ty == ItemType::Span {
                                item["span_id"] = json!(format!("{:016x}", index + 1));
                            }
                            item["attributes"] = attributes;
                            item
                        })
                        .collect();
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
                                "type": ty, "content_type": content_type.as_str(), "item_count": 5
                            }),
                            payload
                        );
                        let output = match ty {
                            ItemType::Log => process_user_agent_container(&logs, &wire, ctx).await,
                            ItemType::Span => {
                                process_user_agent_container(&spans, &wire, ctx).await
                            }
                            ItemType::TraceMetric => {
                                process_user_agent_container(&metrics, &wire, ctx).await
                            }
                            _ => unreachable!(),
                        };
                        let items = output["items"].as_array().unwrap();
                        assert_eq!(items.len(), 5, "{ty}");
                        for (index, item) in items.iter().enumerate() {
                            let attributes = &item["attributes"];
                            let (name, version, original) = match index {
                                0 => (Some("existing"), None, None),
                                1 => (None, Some("existing"), None),
                                _ => {
                                    let (name, version) =
                                        if infer && hints.is_some_and(|h| h.starts_with('"')) {
                                            (Some("xxx"), Some("111"))
                                        } else if index == 2 {
                                            (Some("Fir"), Some("128"))
                                        } else if infer && ua.is_some() {
                                            (Some("Chr"), Some("131"))
                                        } else {
                                            (None, None)
                                        };
                                    (
                                        name,
                                        version,
                                        if index == 2 {
                                            Some(explicit)
                                        } else if infer && ua.is_some() {
                                            Some("Moz")
                                        } else {
                                            None
                                        },
                                    )
                                }
                            };
                            for (key, expected) in [
                                ("browser.name", name),
                                ("browser.version", version),
                                ("user_agent.original", original),
                            ] {
                                assert_eq!(
                                    attributes[key]["value"].as_str(),
                                    expected,
                                    "{ty}, item={index}, infer={infer}, ua={}, hints={hints:?}, key={key}",
                                    ua.is_some()
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    async fn process_user_agent_container(
        processor: &impl crate::processing::Processor,
        wire: &str,
        ctx: Context<'_>,
    ) -> serde_json::Value {
        use crate::managed::ManagedEnvelope;
        use crate::processing::Forward;

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
