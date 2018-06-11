use actix_web::error::Error;
use failure::Fail;
use sentry::integrations::failure::exception_from_single_fail;
use sentry::protocol::{Event, Level};
use sentry::with_client_and_scope;
use uuid::Uuid;

/// Creates an error event from an actix error.
pub fn event_from_actix_error(err: &Error) -> Event<'static> {
    let mut exceptions = Vec::new();

    let mut index = 0;
    let mut fail: Option<&Fail> = Some(err.as_fail());
    while let Some(cause) = fail {
        let backtrace = match index {
            0 => Some(err.backtrace()),
            _ => cause.backtrace(),
        };

        exceptions.push(exception_from_single_fail(cause, backtrace));
        fail = Some(cause);
        index += 1;
    }

    exceptions.reverse();

    Event {
        exceptions: exceptions,
        level: Level::Error,
        ..Default::default()
    }
}

/// Reports an actix error to sentry if it's relevant.
pub fn report_actix_error_to_sentry(err: &Error) -> Uuid {
    with_client_and_scope(|client, scope| {
        client.capture_event(event_from_actix_error(err), Some(scope))
    })
}
