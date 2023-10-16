//! Implements generic filtering based on the [`RuleCondition`] DSL.
//!
//! Multiple generic filters can be defined and they are going to be checked in FIFO order. The
//! first one that matches, will result in the event being discarded with a [`FilterStatKey`]
//! identifying the matching filter.

use crate::{FilterStatKey, GenericFiltersConfig};
use relay_event_schema::protocol::Event;
use relay_protocol::RuleCondition;

/// Version of the generic filters schema.
pub const VERSION: u16 = 1;

/// Checks events by patterns in their error messages.
pub fn matches(event: &Event, condition: Option<&RuleCondition>) -> bool {
    // TODO: the condition DSL needs to be extended to support more complex semantics, such as
    //  collections operations.
    condition.map_or(false, |condition| condition.matches(event))
}

/// Filters events by patterns in their error messages.
pub fn should_filter(event: &Event, config: &GenericFiltersConfig) -> Result<(), FilterStatKey> {
    for (filter_name, filter_config) in config.filters.0.iter() {
        if !filter_config.is_empty() && matches(event, filter_config.condition.as_ref()) {
            return Err(FilterStatKey::GenericFilter(filter_name.clone()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::generic_filters::should_filter;
    use crate::{FilterStatKey, GenericFilterConfig, GenericFiltersConfig, OrderedFilters};
    use relay_event_schema::protocol::{Event, LenientString};
    use relay_protocol::Annotated;
    use relay_protocol::RuleCondition;

    fn mock_filters() -> Vec<(String, GenericFilterConfig)> {
        vec![
            (
                "firstReleases".to_string(),
                GenericFilterConfig {
                    is_enabled: true,
                    condition: Some(RuleCondition::eq("event.release", "1.0")),
                },
            ),
            (
                "helloTransactions".to_string(),
                GenericFilterConfig {
                    is_enabled: true,
                    condition: Some(RuleCondition::eq("event.transaction", "/hello")),
                },
            ),
        ]
    }

    #[test]
    fn test_should_filter_event() {
        let config = GenericFiltersConfig {
            version: 1,
            filters: OrderedFilters(mock_filters()),
        };

        // Matching first rule.
        let event = Event {
            release: Annotated::new(LenientString("1.0".to_string())),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config),
            Err(FilterStatKey::GenericFilter("firstReleases".to_string()))
        );

        // Matching second rule.
        let event = Event {
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config),
            Err(FilterStatKey::GenericFilter(
                "helloTransactions".to_string()
            ))
        );

        // Matching both rules (first is taken).
        let event = Event {
            release: Annotated::new(LenientString("1.0".to_string())),
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config),
            Err(FilterStatKey::GenericFilter("firstReleases".to_string()))
        );

        // Matching no rule.
        let event = Event {
            transaction: Annotated::new("/world".to_string()),
            ..Default::default()
        };
        assert_eq!(should_filter(&event, &config), Ok(()));
    }
}
