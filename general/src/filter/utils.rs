//! Utilities for event filtering.

use crate::protocol::{Event, Headers};

fn get_user_agent_from_headers(headers: &Headers) -> Option<&str> {
    for item in headers.iter() {
        if let Some((ref o_k, ref v)) = item.value() {
            if let Some(ref k) = o_k.as_str() {
                if k.to_lowercase() == "user-agent" {
                    return v.as_str();
                }
            }
        }
    }

    None
}

/// Returns the user agent from an event (if available) or None (if not present).
pub fn get_user_agent(event: &Event) -> Option<&str> {
    let request = event.request.value()?;
    let headers = request.headers.value()?;
    get_user_agent_from_headers(headers)
}
