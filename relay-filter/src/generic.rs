//! Implements generic filtering based on the [`RuleCondition`] DSL.
//!
//! Multiple generic filters can be defined and they are going to be checked in FIFO order. The
//! first one that matches, will result in the event being discarded with a [`FilterStatKey`]
//! identifying the matching filter.

use crate::{FilterStatKey, GenericFiltersConfig};
use relay_event_schema::protocol::Event;
use relay_protocol::RuleCondition;

/// Maximum supported version of the generic filters schema. If the version in the project config
/// is higher, no generic filters are applied.
pub const VERSION: u16 = 1;

fn is_enabled(config: &GenericFiltersConfig) -> bool {
    config.version > 0 && config.version <= VERSION
}

/// Checks events by patterns in their error messages.
fn matches(event: &Event, condition: Option<&RuleCondition>) -> bool {
    // TODO: the condition DSL needs to be extended to support more complex semantics, such as
    //  collections operations.
    condition.map_or(false, |condition| condition.matches(event))
}

/// Filters events by patterns in their error messages.
pub(crate) fn should_filter(
    event: &Event,
    config: &GenericFiltersConfig,
) -> Result<(), FilterStatKey> {
    // We check if the configuration is enabled, since we support only configuration with a version
    // <= than the maximum one in this Relay instance.
    if !is_enabled(config) {
        return Ok(());
    }

    for filter_config in config.filters.iter() {
        if !filter_config.is_empty() && matches(event, filter_config.condition.as_ref()) {
            return Err(FilterStatKey::GenericFilter(filter_config.id.clone()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::generic::{should_filter, VERSION};
    use crate::{FilterStatKey, GenericFilterConfig, GenericFiltersConfig};
    use relay_event_schema::protocol::{Event, LenientString};
    use relay_protocol::Annotated;
    use relay_protocol::RuleCondition;

    fn mock_filters() -> Vec<GenericFilterConfig> {
        vec![
            GenericFilterConfig {
                id: "firstReleases".to_string(),
                is_enabled: true,
                condition: Some(RuleCondition::eq("event.release", "1.0")),
            },
            GenericFilterConfig {
                id: "helloTransactions".to_string(),
                is_enabled: true,
                condition: Some(RuleCondition::eq("event.transaction", "/hello")),
            },
        ]
    }

    #[test]
    fn test_should_filter_match_rules() {
        let config = GenericFiltersConfig {
            version: 1,
            filters: mock_filters(),
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
    }

    #[test]
    fn test_should_filter_fifo_match_rules() {
        let config = GenericFiltersConfig {
            version: 1,
            filters: mock_filters(),
        };

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
    }

    #[test]
    fn test_should_filter_match_no_rules() {
        let config = GenericFiltersConfig {
            version: 1,
            filters: mock_filters(),
        };

        // Matching no rule.
        let event = Event {
            transaction: Annotated::new("/world".to_string()),
            ..Default::default()
        };
        assert_eq!(should_filter(&event, &config), Ok(()));
    }

    #[test]
    fn test_should_filter_with_higher_config_version() {
        let config = GenericFiltersConfig {
            // We simulate receiving a higher configuration version, which we don't support.
            version: VERSION + 1,
            filters: mock_filters(),
        };

        let event = Event {
            release: Annotated::new(LenientString("1.0".to_string())),
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };
        assert_eq!(should_filter(&event, &config), Ok(()));
    }
}
