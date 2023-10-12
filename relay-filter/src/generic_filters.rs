use crate::{FilterStatKey, GenericFiltersConfig};
use relay_event_schema::protocol::Event;
use relay_sampling::condition::RuleCondition;

/// Checks events by patterns in their error messages.
pub fn matches(event: &Event, condition: &RuleCondition) -> bool {
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
