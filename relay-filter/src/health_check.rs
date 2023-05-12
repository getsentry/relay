//! Implements event filtering based on whether the endpoint called is a healthcheck endpoint
//!
//! If this filter is enabled messages to healthcheck endpoints will be filtered out

use relay_general::protocol::Event;

use crate::{FilterConfig, FilterStatKey};

fn matches(_event: &Event) -> bool {
    //TODO RaduW implement matching for filtering on healthcheck endpoint
    false
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
