use actix_web::error::Error;
use failure::Fail;
use sentry::integrations::failure::exception_from_single_fail;
use sentry::protocol::{Event, Level};
use sentry::with_client_and_scope;
use uuid::Uuid;

/// Creates an error event from an actix error.
pub fn event_from_actix_error(err: &Error) -> Event<'static> {
    let mut exceptions = vec![exception_from_single_fail(
        err.as_fail(),
        Some(err.backtrace()),
    )];

    let mut ptr: Option<&Fail> = err.as_fail().cause();
    while let Some(cause) = ptr {
        exceptions.push(exception_from_single_fail(cause, cause.backtrace()));
        ptr = Some(cause);
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
