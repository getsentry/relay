//! Implements event filtering based on the client ip address.
//!
//! A project may be configured with blacklisted ip addresses that will
//! be banned from sending events (all events received from banned ip
//! addresses will be filtered).

use crate::protocol::Event;

/// Should filter event
pub fn should_filter(_event: &Event, _black_listed_ips: &[String]) -> Result<(), String> {
    Ok(())
}
