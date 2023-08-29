//! Utilities used by the event filter tests.

use relay_event_schema::protocol::{Event, Headers, Request};
use relay_protocol::Annotated;

/// Creates an Event with the specified user agent.
pub fn get_event_with_user_agent(user_agent: &str) -> Event {
    let headers = vec![Annotated::new((
        Annotated::new("UsEr-AgeNT".to_string().into()),
        Annotated::new(user_agent.to_string().into()),
    ))];

    Event {
        request: Annotated::new(Request {
            headers: Annotated::new(Headers(headers.into())),
            ..Request::default()
        }),
        ..Event::default()
    }
}
