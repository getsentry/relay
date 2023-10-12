//! Implements generic filtering based on the [`RuleCondition`] DSL.
//!
//! Multiple generic filters can be defined and they are going to be checked in FIFO order. The
//! first one that matches, will result in the event being discarded with a [`FilterStatKey`]
//! identifying the matching filter.

use crate::{FilterStatKey, GenericFiltersConfig};
use relay_event_schema::protocol::Event;
use relay_sampling::condition::RuleCondition;

/// Checks events by patterns in their error messages.
pub fn matches(event: &Event, condition: &RuleCondition) -> bool {
    // TODO: check how to augment the getter in order to support an equivalent behavior of existing
    //  filters.
    condition.matches(event)
}

/// Filters events by patterns in their error messages.
pub fn should_filter(event: &Event, config: &GenericFiltersConfig) -> Result<(), FilterStatKey> {
    for (filter_name, filter_config) in config.0.iter() {
        if matches(event, &filter_config.condition) {
            return Err(FilterStatKey::GenericFilters(filter_name.clone()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::generic_filters::should_filter;
    use crate::{FilterStatKey, GenericFilterConfig, GenericFiltersConfig};
    use relay_event_schema::protocol::{Event, LenientString};
    use relay_protocol::Annotated;
    use relay_sampling::condition::RuleCondition;
    use std::collections::HashMap;
    use std::hash::Hash;

    fn mock_filters() -> HashMap<String, GenericFilterConfig> {
        let mut generic_filters_map = HashMap::new();

        generic_filters_map.insert(
            "hydrationError".to_string(),
            GenericFilterConfig {
                is_enabled: true,
                condition: RuleCondition::eq("event.release", "1.0"),
            },
        );
        generic_filters_map.insert(
            "chunkLoadError".to_string(),
            GenericFilterConfig {
                is_enabled: true,
                condition: RuleCondition::eq("event.transaction", "/hello"),
            },
        );

        generic_filters_map
    }

    #[test]
    fn test_should_filter_event() {
        let config = GenericFiltersConfig(mock_filters());

        // Matching first rule.
        let event = Event {
            release: Annotated::new(LenientString("1.0".to_string())),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config),
            Err(FilterStatKey::GenericFilters("hydrationError".to_string()))
        );

        // Matching second rule.
        let event = Event {
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config),
            Err(FilterStatKey::GenericFilters("chunkLoadError".to_string()))
        );

        // Matching both rules (first is taken).
        let event = Event {
            release: Annotated::new(LenientString("1.0".to_string())),
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config),
            Err(FilterStatKey::GenericFilters("hydrationError".to_string()))
        );

        // Matching no rule.
        let event = Event {
            transaction: Annotated::new("/world".to_string()),
            ..Default::default()
        };
        assert_eq!(should_filter(&event, &config), Ok(()));
    }
}
