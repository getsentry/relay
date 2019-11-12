//! Handles event store requests.

use actix::prelude::*;
use actix_web::http::Method;
use actix_web::middleware::cors::Cors;
use actix_web::{HttpRequest, HttpResponse};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use semaphore_common::Uuid;
use semaphore_general::protocol::EventId;

use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::{ContentType, IncomingEnvelope, IncomingItem, IncomingItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

// Transparent 1x1 gif
// See http://probablyprogramming.com/2009/03/15/the-tiniest-gif-ever
static PIXEL: &[u8] =
    b"GIF89a\x01\x00\x01\x00\x00\xff\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;";

#[derive(Deserialize)]
struct EventIdHelper {
    #[serde(default, rename = "event_id")]
    id: Option<EventId>,
}

fn extract_envelope(mut data: Bytes) -> Result<IncomingEnvelope, BadStoreRequest> {
    if data.is_empty() {
        return Err(BadStoreRequest::EmptyBody);
    }

    let meta: EventMeta = unimplemented!();

    // python clients are well known to send crappy JSON in the Sentry world.  The reason
    // for this is that they send NaN and Infinity as invalid JSON tokens.  The code sentry
    // server could deal with this but we cannot.  To work around this issue we run a basic
    // character substitution on the input stream but only if we detect a Python agent.
    //
    // this is done here so that the rest of the code can assume valid JSON.
    if meta.needs_legacy_python_json_support() {
        let mut data_mut = BytesMut::from(data);
        json_forensics::translate_slice(&mut data_mut[..]);
        data = data_mut.freeze();
    }

    // Ensure that the event has a UUID. It will be returned from this message and from the
    // incoming store request. To uncouple it from the workload on the processing workers, this
    // requires to synchronously parse a minimal part of the JSON payload. If the JSON payload
    // is invalid, processing can be skipped altogether.
    let event_id = serde_json::from_slice::<EventIdHelper>(&data)
        .map(|event| event.id)
        .map_err(BadStoreRequest::InvalidJson)?
        .unwrap_or_else(EventId::new);

    let mut event_item = IncomingItem::new(IncomingItemType::Event);
    event_item.set_payload(ContentType::Json, data);

    let mut envelope = IncomingEnvelope::new(event_id);
    envelope.add_item(event_item);

    Ok(envelope)
}

#[derive(Serialize)]
struct StoreResponse {
    id: EventId,
}

/// Handler for the event store endpoint.
///
/// This simply delegates to `store_event` which does all the work.
/// `handle_store_event` is an adaptor for `store_event` which cannot
/// be used directly as a request handler since not all of its arguments
/// implement the FromRequest trait.
fn store_event(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let is_get_request = request.method() == "GET";

    let future =
        handle_store_like_request(meta, start_time, request, extract_envelope, move |id| {
            if is_get_request {
                HttpResponse::Ok().content_type("image/gif").body(PIXEL)
            } else {
                HttpResponse::Ok().json(StoreResponse { id })
            }
        });

    Box::new(future)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    // XXX: does not handle the legacy /api/store/ endpoint
    Cors::for_app(app)
        .allowed_methods(vec!["POST"])
        .allowed_headers(vec![
            "x-sentry-auth",
            "x-requested-with",
            "x-forwarded-for",
            "origin",
            "referer",
            "accept",
            "content-type",
            "authentication",
        ])
        .expose_headers(vec!["X-Sentry-Error", "Retry-After"])
        .max_age(3600)
        .resource(r"/api/{project:\d+}/store{trailing_slash:/?}", |r| {
            r.method(Method::POST).with(store_event);
            r.method(Method::GET).with(store_event);
        })
        .register()
}
