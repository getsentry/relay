use actix_web::error::Error;
use failure::Fail;
use sentry::integrations::failure::exception_from_single_fail;
use sentry::protocol::{Event, Level};
use sentry::with_client_and_scope;
use uuid::Uuid;

/// Reports an actix error to sentry.
pub fn report_actix_error_to_sentry(err: &Error) -> Uuid {
    with_client_and_scope(|client, scope| {
        let mut exceptions = vec![exception_from_single_fail(
            err.cause(),
            Some(err.backtrace()),
        )];

        let mut ptr: Option<&Fail> = err.cause().cause();
        while let Some(cause) = ptr {
            exceptions.push(exception_from_single_fail(cause, cause.backtrace()));
            ptr = Some(cause);
        }

        exceptions.reverse();

        client.capture_event(
            Event {
                exceptions: exceptions,
                level: Level::Error,
                ..Default::default()
            },
            Some(scope),
        )
    })
}
