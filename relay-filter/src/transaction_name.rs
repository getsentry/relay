//! Implements event filtering based on whether the endpoint called is a healthcheck endpoint.
//!
//! If this filter is enabled transactions from healthcheck endpoints will be filtered out.

use relay_common::glob3::GlobPatterns;
use relay_event_schema::protocol::{Event, EventType};

use crate::{FilterStatKey, IgnoreTransactionsFilterConfig};

fn matches(event: &Event, patterns: &GlobPatterns) -> bool {
    if event.ty.value() != Some(&EventType::Transaction) {
        return false;
    }

    event
        .transaction
        .value()
        .map_or(false, |transaction| patterns.is_match(transaction))
}

/// Filters [Transaction](EventType::Transaction) events based on a list of provided transaction
/// name globs.
pub fn should_filter(
    event: &Event,
    config: &IgnoreTransactionsFilterConfig,
) -> Result<(), FilterStatKey> {
    if config.is_empty() {
        return Ok(());
    }

    if matches(event, &config.patterns) {
        return Err(FilterStatKey::FilteredTransactions);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use super::*;

    fn _get_patterns() -> GlobPatterns {
        let patterns_raw = vec!["*healthcheck*".into(), "*/health".into()];
        GlobPatterns::new(patterns_raw)
    }

    /// tests matching for various transactions
    #[test]
    fn test_matches() {
        let patterns = _get_patterns();

        let transaction_names = [
            "a/b/healthcheck/c",
            "a_HEALTHCHECK_b",
            "healthcheck",
            "/health",
            "a/HEALTH",
            "/health",
            "a/HEALTH",
        ];

        for name in transaction_names {
            let event = Event {
                transaction: Annotated::new(name.into()),
                ty: Annotated::new(EventType::Transaction),
                ..Event::default()
            };
            assert!(matches(&event, &patterns), "Did not match `{name}`")
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
        let patterns = _get_patterns();

        for name in transaction_names {
            let event = Event {
                transaction: Annotated::new(name.into()),
                ty: Annotated::new(EventType::Transaction),
                ..Event::default()
            };
            assert!(
                !matches(&event, &patterns),
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
        let patterns = _get_patterns();
        assert!(
            !matches(&event, &patterns),
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
        let config = IgnoreTransactionsFilterConfig {
            patterns: _get_patterns(),
            is_enabled: true,
        };

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
        let filter_result = should_filter(
            &event,
            &IgnoreTransactionsFilterConfig {
                patterns: _get_patterns(),
                is_enabled: false,
            },
        );
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
        let filter_result = should_filter(
            &event,
            &IgnoreTransactionsFilterConfig {
                patterns: _get_patterns(),
                is_enabled: true,
            },
        );
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have not matched"
        )
    }

    #[test]
    fn test_only_filters_transactions_not_anything_else() {
        let config = IgnoreTransactionsFilterConfig {
            patterns: _get_patterns(),
            is_enabled: true,
        };

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
