//! Implements event filtering for events originating from CSP endpoints
//!
//! Events originating from a CSP message can be filtered based on the source URL
//!
use crate::protocol::Event;

/// Should filter event
pub fn should_filter(_event: &Event, _csp_disallowed_sources: &[String]) -> Result<(), String> {
    Ok(())
}
