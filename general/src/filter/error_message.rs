//! Implements event filtering based on the error message
//!
//! Specific values in the error message or in the exception values can be used to
//! filter messages with this filter.

use crate::protocol::Event;

/// Should filter event
pub fn should_filter(_event: &Event, _error_messages: &[String]) -> Result<(), String> {
    Ok(())
}
