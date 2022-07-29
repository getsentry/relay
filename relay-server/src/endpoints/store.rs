//! Handles event store requests.

use actix::prelude::*;
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use bytes::{Bytes, BytesMut};
use futures01::Future;
use serde::Serialize;

use relay_general::protocol::EventId;

use crate::body::StoreBody;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};

// Transparent 1x1 gif
// See http://probablyprogramming.com/2009/03/15/the-tiniest-gif-ever
static PIXEL: &[u8] =
    b"GIF89a\x01\x00\x01\x00\x00\xff\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;";

/// Parses a full `Envelope` request body.
fn parse_envelope(meta: RequestMeta, data: Bytes) -> Result<Envelope, BadStoreRequest> {
    // Use `parse_request` here to ensure that we're merging available request headers into the
    // envelope's headers.
    Envelope::parse_request(data, meta).map_err(BadStoreRequest::InvalidEnvelope)
}

/// Parses a JSON event body into an `Envelope`.
fn parse_event(
    meta: RequestMeta,
    content_type: ContentType,
    mut data: Bytes,
) -> Result<Envelope, BadStoreRequest> {
    // Python clients are well known to send crappy JSON in the Sentry world.  The reason
    // for this is that they send NaN and Infinity as invalid JSON tokens.  The code sentry
    // server could deal with this but we cannot.  To work around this issue, we run a basic
    // character substitution on the input stream but only if we detect a Python agent.
    //
    // This is done here so that the rest of the code can assume valid JSON.
    let is_legacy_python_json = meta.client().map_or(false, |agent| {
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
    let minimal = common::minimal_event_from_json(&data)?;

    // Old SDKs used to send transactions to the store endpoint with an explicit `Transaction` event
    // type. The processing queue expects those in an explicit item.
    let item_type = ItemType::from_event_type(minimal.ty);
    let mut event_item = Item::new(item_type);
    event_item.set_payload(content_type, data);

    let event_id = minimal.id.unwrap_or_else(EventId::new);
    let mut envelope = Envelope::from_request(Some(event_id), meta);
    envelope.add_item(event_item);

    Ok(envelope)
}

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let max_payload_size = request.state().config().max_event_size();

    // If the content type is missing, assume "application/json".
    let content_type = match request.content_type() {
        content_type if content_type.is_empty() => ContentType::Json,
        content_type => ContentType::from(content_type),
    };

    let future = StoreBody::new(request, max_payload_size)
        .map_err(BadStoreRequest::PayloadError)
        .and_then(move |data| {
            if data.is_empty() {
                return Err(BadStoreRequest::EmptyBody);
            }

            match content_type {
                ContentType::Envelope => parse_envelope(meta, data),
                _ => parse_event(meta, content_type, data),
            }
        });

    Box::new(future)
}

#[derive(Serialize)]
struct StoreResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<EventId>,
}

fn create_response(id: Option<EventId>, is_get_request: bool) -> HttpResponse {
    if is_get_request {
        HttpResponse::Ok().content_type("image/gif").body(PIXEL)
    } else {
        HttpResponse::Ok().json(StoreResponse { id })
    }
}

/// Handler for the JSON event store endpoint.
///
/// This simply delegates to `store_event` which does all the work.
/// `handle_store_event` is an adaptor for `store_event` which cannot
/// be used directly as a request handler since not all of its arguments
/// implement the FromRequest trait.
fn store_event(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let is_get_request = request.method() == "GET";

    common::handle_store_like_request(
        meta,
        request,
        extract_envelope,
        move |id| create_response(id, is_get_request),
        true,
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        // Standard store endpoint. Some SDKs send multiple leading or trailing slashes due to bugs
        // in their URL handling. Since actix does not normalize such paths, allow any number of
        // slashes. The trailing slash can also be omitted, optionally.
        .resource(&common::normpath(r"/api/{project:\d+}/store/"), |r| {
            r.name("store-default");
            r.post().with(store_event);
            r.get().with(store_event);
        })
        // Legacy store path. Since it is missing the project parameter, the `RequestMeta` extractor
        // will use `ProjectKeyLookup` to map the public key to a project id before handling the
        // request.
        .resource(&common::normpath(r"/api/store/"), |r| {
            r.name("store-legacy");
            r.post().with(store_event);
            r.get().with(store_event);
        })
        .register()
}
