use std::time::Duration;

use relay_dynamic_config::{RetentionConfig, RetentionsConfig};
use relay_event_normalization::eap;
use relay_event_normalization::eap::time::TimeEnforcement;

use crate::envelope::EnvelopeHeaders;
use crate::processing::Context;
use crate::services::processor::MINIMUM_CLOCK_DRIFT;

/// Creates a lazy enrichment cache for one container using the span tag limit.
pub fn user_agent<'a>(
    headers: &'a EnvelopeHeaders,
    infer: bool,
    ctx: Context<'_>,
) -> eap::UserAgentNormalizer<'a> {
    eap::UserAgentNormalizer::new(
        infer.then(|| eap::ClientUserAgentInfo {
            user_agent: headers.meta().user_agent(),
            hints: headers.meta().client_hints(),
        }),
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

    #[test]
    fn user_agent_uses_configured_limit_and_inference_setting() {
        use relay_conventions::attributes::{
            BROWSER__NAME, BROWSER__VERSION, USER_AGENT__ORIGINAL,
        };
        use relay_protocol::{Annotated, Value};
        use serde_json::json;

        let config = relay_config::Config::from_json_value(json!({
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
        let headers: EnvelopeHeaders = serde_json::from_value(json!({
            "dsn": "https://a94ae32be2584e0bbd7a4cbb95971fee@sentry.io/42",
            "user_agent": "Mozilla/5.0",
            "client_hints": {"sec_ch_ua": "\"Google Chrome\";v=\"123456\""}
        }))
        .unwrap();
        for infer in [false, true] {
            let normalizer = user_agent(&headers, infer, ctx);
            for _ in 0..3 {
                let mut attributes = Annotated::default();
                eap::normalize_user_agent(&mut attributes, &normalizer);
                let attributes = attributes.value().unwrap();
                for (key, expected) in [
                    (BROWSER__NAME, "Chr"),
                    (BROWSER__VERSION, "123"),
                    (USER_AGENT__ORIGINAL, "Moz"),
                ] {
                    assert_eq!(
                        attributes.get_value(key).and_then(Value::as_str),
                        infer.then_some(expected)
                    );
                }
            }
        }
    }

    #[test]
    fn test_days_to_duration() {
        assert_eq!(retention_days_to_duration(1), Duration::from_secs(86400));
    }
}
