//! Returns captured events.

use actix_web::http::header::ACCEPT;
use actix_web::{actix::*, HttpMessage, HttpRequest};
use actix_web::{http::Method, HttpResponse};
use futures::future::Future;
use serde::Serialize;

use crate::actors::envelopes::{EnvelopeManager, GetEnvelope};
use crate::envelope::{self, ContentType, Envelope, EnvelopeHeaders, ItemHeaders};
use crate::service::{ServiceApp, ServiceState};

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

fn envelope_as_json(envelope: Envelope) -> JsonEnvelope {
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
}

fn get_envelope(request: &HttpRequest<ServiceState>) -> ResponseFuture<HttpResponse, MailboxError> {
    let is_json = request
        .headers()
        .get(ACCEPT)
        .map(|h| {
            h.to_str()
                .unwrap_or("")
                .contains(ContentType::Json.as_str())
        })
        .unwrap_or_default();

    let future = EnvelopeManager::from_registry()
        .send(GetEnvelope {})
        .map(move |captured_event| match captured_event {
            Some(Ok(envelope)) => {
                if is_json {
                    let envelope = envelope_as_json(envelope);

                    match serde_json::to_string(&envelope) {
                        Ok(json) => HttpResponse::Ok()
                            .content_type(ContentType::Json.as_str())
                            .body(json),
                        Err(e) => HttpResponse::BadGateway()
                            .content_type("text/plain")
                            .body(e.to_string()),
                    }
                } else {
                    HttpResponse::Ok()
                        .content_type(envelope::CONTENT_TYPE)
                        .body(envelope.to_vec().unwrap())
                }
            }
            Some(Err(error)) => HttpResponse::BadRequest()
                .content_type("text/plain")
                .body(error),
            None => HttpResponse::NotFound().finish(),
        });

    Box::new(future)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/relay/get-envelope/", |r| {
        r.name("internal-envelope");
        r.method(Method::GET).h(get_envelope)
    })
}
