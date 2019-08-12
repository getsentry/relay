//! Implements event filtering based on the error message
//!
//! Specific values in the error message or in the exception values can be used to
//! filter messages with this filter.

use crate::protocol::Event;

use crate::filter::config::ErrorMessagesFilterConfig;

/// Filters events by patterns in their error messages.
pub fn should_filter(_event: &Event, config: &ErrorMessagesFilterConfig) -> Result<(), String> {
    let patterns = &config.patterns;
    if patterns.is_empty() {
        return Ok(());
    }

    // TODO: Implement
    Ok(())
}
