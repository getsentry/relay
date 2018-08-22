use actix_web::error::Error;
use actix_web::http::header;
use actix_web::{FromRequest, HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use futures::{future, Future};
use url::Url;

use semaphore_aorta::{ApiErrorResponse, EventMeta, EventVariant, StoreChangeset};
use semaphore_common::Auth;

use body::{EncodedEvent, EncodedEventPayloadError};
use service::ServiceState;

use sentry;

#[derive(Fail, Debug)]
pub enum BadStoreRequest {
    #[fail(display = "bad event payload")]
    BadEvent(#[cause] EncodedEventPayloadError),
    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),
}

impl ResponseError for BadStoreRequest {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(&ApiErrorResponse::from_fail(self))
    }
}

/// Extracts an event variant from the payload.
pub struct IncomingEvent {
    event: EventVariant,
    meta: EventMeta,
    public_key: String,
}

impl IncomingEvent {
    fn new(event: EventVariant, meta: EventMeta, public_key: String) -> IncomingEvent {
        IncomingEvent {
            event,
            meta,
            public_key,
        }
    }
}

impl From<IncomingEvent> for StoreChangeset {
    fn from(mut inc: IncomingEvent) -> StoreChangeset {
        inc.event.ensure_id();
        StoreChangeset {
            event: inc.event,
            meta: inc.meta,
            public_key: inc.public_key,
        }
    }
}

fn log_failed_payload(err: &EncodedEventPayloadError) {
    let causes: Vec<_> = Fail::iter_causes(err).skip(1).collect();
    for cause in causes.iter().rev() {
        warn!("payload processing issue: {}", cause);
    }
    warn!("failed processing event: {}", err);
    debug!(
        "bad event payload: {}",
        err.utf8_body().unwrap_or("<broken>")
    );

    // sentry::with_client_and_scope(|client, scope| {
    //     let mut event = sentry::integrations::failure::event_from_fail(err);
    //     let last = event.exceptions.len() - 1;
    //     event.exceptions[last].ty = "BadEventPayload".into();
    //     if let Some(body) = err.utf8_body() {
    //         event.message = Some(format!("payload: {}", body));
    //     };
    //     client.capture_event(event, Some(scope));
    // });
}

fn parse_header_origin<T>(req: &HttpRequest<T>, header: header::HeaderName) -> Option<Url> {
    req.headers()
        .get(header)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<Url>().ok())
        .and_then(|u| match u.scheme() {
            "http" | "https" => Some(u),
            _ => None,
        })
}

fn event_meta_from_req(req: &HttpRequest<ServiceState>) -> EventMeta {
    let extensions = req.extensions();
    let auth: &Auth = extensions.get().unwrap();
    EventMeta {
        remote_addr: req.peer_addr().map(|sock_addr| sock_addr.ip()),
        sentry_client: auth.client_agent().map(|x| x.to_string()),
        origin: parse_header_origin(req, header::ORIGIN)
            .or_else(|| parse_header_origin(req, header::REFERER)),
    }
}

impl FromRequest<ServiceState> for IncomingEvent {
    type Config = ();
    type Result = Box<Future<Item = Self, Error = Error>>;

    #[inline]
    fn from_request(req: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        let extensions = req.extensions();
        let auth: &Auth = extensions.get().unwrap();
        let max_payload = req.state().aorta_config().max_event_payload_size;
        let log_failed_payloads = req.state().config().log_failed_payloads();

        // anything up to 8 is considered sentry v8
        if auth.version() <= 8 {
            let meta = event_meta_from_req(req);
            let public_key = auth.public_key().to_string();
            Box::new(
                EncodedEvent::new(req.clone())
                    .limit(max_payload)
                    .map_err(move |e| {
                        if log_failed_payloads {
                            log_failed_payload(&e);
                        }
                        BadStoreRequest::BadEvent(e).into()
                    })
                    .map(move |e| IncomingEvent::new(e, meta, public_key)),
            )

        // for now don't handle unsupported versions.  later fallback to raw
        } else {
            Box::new(future::err(
                BadStoreRequest::UnsupportedProtocolVersion(auth.version()).into(),
            ))
        }
    }
}
