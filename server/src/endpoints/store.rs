//! Handles event store requests.

use actix::prelude::*;
use actix_web::http::Method;
use actix_web::middleware::cors::Cors;
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use semaphore_general::protocol::EventId;

use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::{self, ContentType, Envelope, Item, ItemType};
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

fn extract_envelope(
    mut data: Bytes,
    meta: EventMeta,
    content_type: String,
) -> Result<Envelope, BadStoreRequest> {
    if data.is_empty() {
        return Err(BadStoreRequest::EmptyBody);
    }

    // Clients may send full envelopes to /store. In this case, just parse the envelope and assume
    // that it has sufficient headers. However, we're using `parse_request` here to ensure that
    // we're merging available request headers into the envelope's headers.
    if content_type == envelope::CONTENT_TYPE {
        return Envelope::parse_request(data, meta).map_err(BadStoreRequest::InvalidEnvelope);
    }

    // Python clients are well known to send crappy JSON in the Sentry world.  The reason
    // for this is that they send NaN and Infinity as invalid JSON tokens.  The code sentry
    // server could deal with this but we cannot.  To work around this issue, we run a basic
    // character substitution on the input stream but only if we detect a Python agent.
    //
    // This is done here so that the rest of the code can assume valid JSON.
    let is_legacy_python_json = meta.auth().client_agent().map_or(false, |agent| {
        agent.starts_with("raven-python/") || agent.starts_with("sentry-python/")
    });

    if is_legacy_python_json {
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

    // Use the request's content type. If the content type is missing, assume "application/json".
    let content_type = match content_type {
        ct if ct.is_empty() => ContentType::Json,
        ct => ContentType::from(ct),
    };

    let mut event_item = Item::new(ItemType::Event);
    event_item.set_payload(content_type, data);

    let mut envelope = Envelope::from_request(event_id, meta);
    envelope.add_item(event_item);

    Ok(envelope)
}

#[derive(Serialize)]
struct StoreResponse {
    id: EventId,
}

fn create_response(id: EventId, is_get_request: bool) -> HttpResponse {
    if is_get_request {
        HttpResponse::Ok().content_type("image/gif").body(PIXEL)
    } else {
        HttpResponse::Ok().json(StoreResponse { id })
    }
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
    let content_type = request.content_type().to_owned();

    Box::new(handle_store_like_request(
        meta,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, content_type),
        move |id| create_response(id, is_get_request),
    ))
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
        .resource(r"/api/{project:\d+}/store{trailing_slash:/*}", |r| {
            r.method(Method::POST).with(store_event);
            r.method(Method::GET).with(store_event);
        })
        .register()
}
