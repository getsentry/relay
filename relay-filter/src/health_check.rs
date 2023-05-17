//! Implements event filtering based on whether the endpoint called is a healthcheck endpoint
//!
//! If this filter is enabled messages to healthcheck endpoints will be filtered out

use once_cell::sync::Lazy;
use regex::Regex;
use relay_general::protocol::Event;

use crate::{FilterConfig, FilterStatKey};

static HEALTH_CHECK_ENDPOINTS: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?ix)
        healthcheck|
        healthy|
        live|
        ready|
        heartbeat|
        /health$|
        /healthz$
        "#,
    )
    .expect("Invalid healthcheck filter Regex")
});

fn matches(event: &Event) -> bool {
    if let Some(transaction) = _event.transaction.value() {
        HEALTH_CHECK_ENDPOINTS.is_match(transaction)
    } else {
        false
    }
}

/// Filters events for calls to healthcheck endpoints
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }
    if matches(event) {
        return Err(FilterStatKey::HealthCheck);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_general::protocol::Event;
    use relay_general::types::Annotated;

    /// tests matching for various transactions
    #[test]
    fn test_matches() {
        let transaction_names = [
            "a/b/healthcheck/c",
            "a_HEALTHCHECK_b",
            "healthcheck",
            "a/healthy/b",
            "a_healthy_b",
            "HEALTHY",
            "a/live/b",
            "a_live_b",
            "live",
            "a/READY/b",
            "a_ready_b",
            "ready",
            "a/heartbeat/b",
            "a_heartbeat_b",
            "heartbeat",
            "/health",
            "a/HEALTH",
            "/healthz",
            "a/HEALTHZ",
        ];

        for name in transaction_names {
            let event = Event {
                transaction: Annotated::new(name.into()),
                ..Event::default()
            };
            assert!(matches(&event), "Did not match `{name}`")
        }
    }

    /// tests non matching transactions transactions
    #[test]
    fn test_does_not_match() {
        let transaction_names = [
            "bad",
            "/bad",
            "a/b/c",
            "health",
            "healthz",
            "/health/",
            "/healthz/",
            "/healthx",
            "/healthzx",
        ];

        for name in transaction_names {
            let event = Event {
                transaction: Annotated::new(name.into()),
                ..Event::default()
            };
            assert!(
                !matches(&event),
                "Did match `{name}` but it shouldn't have."
            )
        }
    }

    // test it doesn't match when the transaction name is missing
    #[test]
    fn test_does_not_match_missing_transaction() {
        let event = Event { ..Event::default() };
        assert!(
            !matches(&event),
            "Did match with empty transaction but it shouldn't have."
        )
    }

    #[test]
    fn test_filters_when_matching() {
        let event = Event {
            transaction: Annotated::new("/health".into()),
            ..Event::default()
        };
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_eq!(
            filter_result,
            Err(FilterStatKey::HealthCheck),
            "Event did not filter health check event"
        )
    }
    #[test]
    fn test_does_not_filter_when_disabled() {
        let event = Event {
            transaction: Annotated::new("/health".into()),
            ..Event::default()
        };
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: false });
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have been disabled"
        )
    }

    #[test]
    fn test_does_not_filter_when_not_matching() {
        let event = Event {
            transaction: Annotated::new("/a/b/c".into()),
            ..Event::default()
        };
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have not matched"
        )
    }
}
