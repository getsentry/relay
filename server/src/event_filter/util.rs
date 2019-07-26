//! Utilities for event filtering

use semaphore_general::protocol::{Event, Headers, Request};

/// Returns the user agent from an event (if available) or None (if not present)
pub fn get_user_agent(event: &Event) -> Option<&str> {
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
        return None;
    }

    return event
        .request
        .value()
        .and_then(|req: &Request| req.headers.value())
        .and_then(|headers: &Headers| get_user_agent_from_headers(headers));
}

#[cfg(test)]
/// Utilities used by the event filter tests.
pub mod test_utils {
    use semaphore_general::protocol::PairList;
    use semaphore_general::protocol::{Event, Headers, Request};

    use crate::actors::project::FilterConfig;
    use semaphore_general::types::Annotated;

    /// Create a FilterConfig with the specified enabled state.
    pub fn get_f_config(is_enabled: bool) -> FilterConfig {
        FilterConfig { is_enabled }
    }

    /// Creates an Event with the specified user agent.
    pub fn get_event_with_user_agent(user_agent: &str) -> Event {
        let mut headers = Vec::new();

        headers.push(Annotated::new((
            Annotated::new("Accept".to_string().into()),
            Annotated::new("application/json".to_string().into()),
        )));

        headers.push(Annotated::new((
            Annotated::new("UsEr-AgeNT".to_string().into()),
            Annotated::new(user_agent.to_string().into()),
        )));
        headers.push(Annotated::new((
            Annotated::new("WWW-Authenticate".to_string().into()),
            Annotated::new("basic".to_string().into()),
        )));

        return Event {
            request: Annotated::new(Request {
                headers: Annotated::new(Headers(PairList(headers))),
                ..Default::default()
            }),
            ..Default::default()
        };
    }
}
