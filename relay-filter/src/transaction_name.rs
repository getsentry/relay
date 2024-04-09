//! Implements event filtering based on whether the endpoint called is a healthcheck endpoint.
//!
//! If this filter is enabled transactions from healthcheck endpoints will be filtered out.

use relay_common::glob3::GlobPatterns;

use crate::{FilterStatKey, Filterable, IgnoreTransactionsFilterConfig};

fn matches(transaction: Option<&str>, patterns: &GlobPatterns) -> bool {
    transaction.map_or(false, |transaction| patterns.is_match(transaction))
}

/// Filters [Transaction](relay_event_schema::protocol::EventType::Transaction) events based on a list of provided transaction
/// name globs.
pub fn should_filter<F: Filterable>(
    item: &F,
    config: &IgnoreTransactionsFilterConfig,
) -> Result<(), FilterStatKey> {
    if config.is_empty() {
        return Ok(());
    }

    if matches(item.transaction(), &config.patterns) {
        return Err(FilterStatKey::FilteredTransactions);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{Event, EventType};
    use relay_protocol::Annotated;

    use super::*;

    fn _get_config() -> IgnoreTransactionsFilterConfig {
        let patterns_raw = [
            "*healthcheck*",
            "*healthy*",
            "live",
            "live[z/-]*",
            "*[/-]live",
            "*[/-]live[z/-]*",
            "ready",
            "ready[z/-]*",
            "*[/-]ready",
            "*[/-]ready[z/-]*",
            "*heartbeat*",
            "*/health",
            "*/healthz",
            "*/ping",
        ]
        .map(|val| val.to_string())
        .to_vec();

        IgnoreTransactionsFilterConfig {
            patterns: GlobPatterns::new(patterns_raw),
            is_enabled: true,
        }
    }

    /// tests matching for various transactions
    #[test]
    fn test_matches() {
        let config = _get_config();

        let transaction_names = [
            "a/b/healthcheck/c",
            "a_HEALTHCHECK_b",
            "healthcheck",
            "/health",
            "a/HEALTH",
            "/health",
            "a/HEALTH",
            "live",
            "livez",
            "live/123",
            "live-33",
            "x-live",
            "abc/live",
            "abc/live-23",
            "ready",
            "123/ready",
            "readyz",
            "ready/123",
            "abc/ready/1234",
            "abcheartbeat123",
            "123/health",
            "123/healthz",
            "123/ping",
        ];

        for name in transaction_names {
            let event = Event {
                transaction: Annotated::new(name.into()),
                ty: Annotated::new(EventType::Transaction),
                ..Event::default()
            };
            assert!(
                should_filter(&event, &config).is_err(),
                "Did not match `{name}`"
            )
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
            "service-delivery",
            "delivery",
            "notready",
            "already",
        ];
        let config = _get_config();

        for name in transaction_names {
            let event = Event {
                transaction: Annotated::new(name.into()),
                ty: Annotated::new(EventType::Transaction),
                ..Event::default()
            };
            assert!(
                should_filter(&event, &config).is_ok(),
                "Did match `{name}` but it shouldn't have."
            )
        }
    }

    // test it doesn't match when the transaction name is missing
    #[test]
    fn test_does_not_match_missing_transaction() {
        let event = Event {
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };
        let config = _get_config();
        assert!(
            should_filter(&event, &config).is_ok(),
            "Did match with empty transaction but it shouldn't have."
        )
    }

    #[test]
    fn test_filters_when_matching() {
        let event = Event {
            transaction: Annotated::new("/health".into()),
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };
        let config = _get_config();

        let filter_result = should_filter(&event, &config);
        assert_eq!(
            filter_result,
            Err(FilterStatKey::FilteredTransactions),
            "Event was not filtered "
        )
    }

    #[test]
    fn test_does_not_filter_when_disabled() {
        let event = Event {
            transaction: Annotated::new("/health".into()),
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };
        let filter_result = should_filter(
            &event,
            &IgnoreTransactionsFilterConfig {
                patterns: GlobPatterns::new(vec![]),
                is_enabled: true,
            },
        );
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have been disabled"
        )
    }
    #[test]
    // Tests that is_enabled flag disables the transaction name filter
    fn test_does_not_filter_when_disabled_with_flag() {
        let event = Event {
            transaction: Annotated::new("/health".into()),
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };
        let mut config = _get_config();
        config.is_enabled = false;
        let filter_result = should_filter(&event, &config);
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
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };
        let filter_result = should_filter(&event, &_get_config());
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have not matched"
        )
    }

    #[test]
    fn test_only_filters_transactions_not_anything_else() {
        let config = _get_config();

        for event_type in [EventType::Transaction, EventType::Error, EventType::Csp] {
            let expect_to_filter = event_type == EventType::Transaction;
            let event = Event {
                transaction: Annotated::new("/health".into()),
                ty: Annotated::new(event_type),
                ..Event::default()
            };
            let filter_result = should_filter(&event, &config);

            if expect_to_filter {
                assert_eq!(
                    filter_result,
                    Err(FilterStatKey::FilteredTransactions),
                    "Event was not filtered "
                );
            } else {
                assert_eq!(
                    filter_result,
                    Ok(()),
                    "Event filtered for event_type={} although filter should have not matched",
                    event_type
                )
            }
        }
    }
}
