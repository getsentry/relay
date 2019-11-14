//! Returns captured events.

use ::actix::prelude::*;
use actix_web::{http::Method, HttpResponse, Path, Scope};
use futures::future::Future;

use crate::actors::events::GetCapturedEvent;
use crate::envelope;
use crate::extractors::CurrentServiceState;
use crate::service::ServiceState;

use semaphore_general::protocol::EventId;

#[allow(clippy::needless_pass_by_value)]
fn get_captured_event(
    state: CurrentServiceState,
    event_id: Path<EventId>,
) -> ResponseFuture<HttpResponse, actix::MailboxError> {
    let future = state
        .event_manager()
        .send(GetCapturedEvent {
            event_id: *event_id,
        })
        .map(|captured_event| match captured_event {
            Some(Ok(envelope)) => HttpResponse::Ok()
                .content_type(envelope::CONTENT_TYPE)
                .body(envelope.to_vec().unwrap()),
            Some(Err(error)) => HttpResponse::BadRequest()
                .content_type("text/plain")
                .body(error),
            None => HttpResponse::NotFound().finish(),
        });

    Box::new(future)
}

pub fn configure_scope(scope: Scope<ServiceState>) -> Scope<ServiceState> {
    scope.resource("/events/{event_id}/", |r| {
        r.method(Method::GET).with(get_captured_event);
    })
}
