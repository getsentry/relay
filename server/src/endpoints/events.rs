//! Returns captured events.

use ::actix::prelude::*;
use actix_web::{http::Method, http::StatusCode, HttpResponse, Json, Path, ResponseError, Scope};
use failure::Fail;
use futures::future::{Future, IntoFuture};
use serde::Serialize;
use serde_json::Value;

use crate::actors::events::GetCapturedEvent;
use crate::extractors::CurrentServiceState;
use crate::service::ServiceState;
use crate::utils::ApiErrorResponse;

use semaphore_general::protocol::EventId;

#[derive(Fail, Debug)]
enum BadEventsRequest {
    #[fail(display = "could not schedule lookup")]
    ScheduleFailed(#[cause] MailboxError),
    #[fail(display = "event was not found")]
    NotFound,
    #[fail(display = "event sending failed: {}", _0)]
    EventFailed(String),
}

impl ResponseError for BadEventsRequest {
    fn error_response(&self) -> HttpResponse {
        let body = ApiErrorResponse::from_fail(self);

        match self {
            BadEventsRequest::NotFound => HttpResponse::build(StatusCode::NOT_FOUND).json(&body),
            _ => HttpResponse::BadRequest().json(&body),
        }
    }
}

#[derive(Serialize)]
struct CapturedEventResponse {
    event: Value,
    event_id: EventId,
}

#[allow(clippy::needless_pass_by_value)]
fn get_captured_event(
    state: CurrentServiceState,
    event_id: Path<EventId>,
) -> ResponseFuture<Json<CapturedEventResponse>, BadEventsRequest> {
    Box::new(
        state
            .event_manager()
            .send(GetCapturedEvent {
                event_id: *event_id,
            })
            .map_err(BadEventsRequest::ScheduleFailed)
            .and_then(|event| {
                match event {
                    None => Err(BadEventsRequest::NotFound),
                    Some(event) => match (event.payload, event.error) {
                        (Some(payload), None) => Ok(Json(CapturedEventResponse {
                            event: serde_json::from_slice(&payload).unwrap(),
                            event_id: event.event_id,
                        })),
                        (None, Some(error)) => Err(BadEventsRequest::EventFailed(error)),
                        _ => unreachable!(),
                    },
                }
                .into_future()
            }),
    )
}

pub fn configure_scope(scope: Scope<ServiceState>) -> Scope<ServiceState> {
    scope.resource("/events/{event_id}/", |r| {
        r.method(Method::GET).with(get_captured_event);
    })
}
