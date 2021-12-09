//! Returns captured events.

use actix_web::actix::*;
use actix_web::{http::Method, HttpResponse, Path};
use futures::future::Future;
use serde::Serialize;

use crate::actors::envelopes::{EnvelopeManager, GetCapturedEnvelope};
use crate::envelope::{self, ContentType, Envelope, EnvelopeHeaders, ItemHeaders};
use crate::service::ServiceApp;

use relay_general::protocol::EventId;

fn get_captured_event(event_id: Path<EventId>) -> ResponseFuture<HttpResponse, MailboxError> {
    let future = EnvelopeManager::from_registry()
        .send(GetCapturedEnvelope {
            event_id: Some(*event_id),
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

#[derive(Debug, Serialize)]
struct JsonEnvelope {
    headers: EnvelopeHeaders,
    items: Vec<JsonItem>,
}

#[derive(Debug, Serialize)]
struct JsonItem {
    headers: ItemHeaders,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
}

fn envelope_as_json(envelope: Result<Envelope, String>) -> Result<JsonEnvelope, String> {
    envelope.map(|envelope| {
        let items = envelope
            .items()
            .map(|item| {
                let payload = if let Some(ContentType::Json) = item.content_type() {
                    Some(serde_json::from_slice(item.payload().as_ref()).unwrap())
                } else {
                    None
                };

                JsonItem {
                    headers: item.headers().clone(),
                    payload,
                }
            })
            .collect::<Vec<_>>();

        JsonEnvelope {
            headers: envelope.headers().clone(),
            items,
        }
    })
}

fn get_last_event(_: ()) -> ResponseFuture<HttpResponse, MailboxError> {
    let future = EnvelopeManager::from_registry()
        .send(GetCapturedEnvelope { event_id: None })
        .map(|result| match result {
            Some(result) => {
                let envelope = envelope_as_json(result);

                match serde_json::to_string(&envelope) {
                    Ok(json) => HttpResponse::Ok()
                        .content_type("application/json")
                        .body(json),
                    Err(e) => HttpResponse::BadGateway()
                        .content_type("text/plain")
                        .body(e.to_string()),
                }
            }
            None => HttpResponse::NotFound().finish(),
        });

    Box::new(future)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/relay/events/{event_id}/", |r| {
        r.name("internal-events");
        r.method(Method::GET).with(get_captured_event);
    })
    .resource("/api/relay/last-event/", |r| {
        r.name("internal-events-last");
        r.method(Method::GET).with(get_last_event);
    })
}
