//! Utilities used by the event filter tests.

use semaphore_general::protocol::{Event, Headers, PairList, Request};
use semaphore_general::types::Annotated;

use crate::actors::project::FilterConfig;

/// Create a FilterConfig with the specified enabled state.
pub(super) fn get_f_config(is_enabled: bool) -> FilterConfig {
    FilterConfig { is_enabled }
}

/// Creates an Event with the specified user agent.
pub(super) fn get_event_with_user_agent(user_agent: &str) -> Event {
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

    Event {
        request: Annotated::new(Request {
            headers: Annotated::new(Headers(PairList(headers))),
            ..Request::default()
        }),
        ..Event::default()
    }
}
