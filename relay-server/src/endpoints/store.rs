//! Handles event store requests.

use actix::prelude::*;
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use bytes::BytesMut;
use futures::Future;
use serde::Serialize;

use relay_general::protocol::EventId;

use crate::body::StoreBody;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{self, ContentType, Envelope, Item, ItemType};
use crate::extractors::{RequestMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

// Transparent 1x1 gif
// See http://probablyprogramming.com/2009/03/15/the-tiniest-gif-ever
static PIXEL: &[u8] =
    b"GIF89a\x01\x00\x01\x00\x00\xff\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;";

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
    max_event_payload_size: usize,
    content_type: String,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let future = StoreBody::new(&request, max_event_payload_size)
        .map_err(BadStoreRequest::PayloadError)
        .and_then(move |mut data| {
            if data.is_empty() {
                return Err(BadStoreRequest::EmptyBody);
            }

            // Clients may send full envelopes to /store. In this case, just parse the envelope and assume
            // that it has sufficient headers. However, we're using `parse_request` here to ensure that
            // we're merging available request headers into the envelope's headers.
            if content_type == envelope::CONTENT_TYPE {
                return Envelope::parse_request(data, meta)
                    .map_err(BadStoreRequest::InvalidEnvelope);
            }

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
            let event_id = common::event_id_from_json(&data)?.unwrap_or_else(EventId::new);

            // Use the request's content type. If the content type is missing, assume "application/json".
            let content_type = match &content_type {
                ct if ct.is_empty() => ContentType::Json,
                _ct => ContentType::from(content_type),
            };

            let mut event_item = Item::new(ItemType::Event);
            event_item.set_payload(content_type, data);

            let mut envelope = Envelope::from_request(Some(event_id), meta);
            envelope.add_item(event_item);

            Ok(envelope)
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

/// Handler for the event store endpoint.
///
/// This simply delegates to `store_event` which does all the work.
/// `handle_store_event` is an adaptor for `store_event` which cannot
/// be used directly as a request handler since not all of its arguments
/// implement the FromRequest trait.
fn store_event(
    meta: RequestMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let is_get_request = request.method() == "GET";
    let content_type = request.content_type().to_owned();
    let event_size = request.state().config().max_event_payload_size();

    common::handle_store_like_request(
        meta,
        // XXX: This is wrong. In case of external relays, store can receive event-less envelopes.
        // We need to fix this before external relays go live or we will create outcomes and rate
        // limits for individual attachment requests.
        true,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, event_size, content_type),
        move |id| create_response(id, is_get_request),
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        // Standard store endpoint. Some SDKs send multiple leading or trailing slashes due to bugs
        // in their URL handling. Since actix does not normalize such paths, allow any number of
        // slashes. The trailing slash can also be omitted, optionally.
        .resource(r"/{l:/*}api/{project:\d+}/store{t:/*}", |r| {
            r.name("store-default");
            r.post().with(store_event);
            r.get().with(store_event);
        })
        // Legacy store path. Since it is missing the project parameter, the `RequestMeta` extractor
        // will use `ProjectKeyLookup` to map the public key to a project id before handling the
        // request.
        .resource(r"/{l:/*}api/store{t:/*}", |r| {
            r.name("store-legacy");
            r.post().with(store_event);
            r.get().with(store_event);
        })
        .register()
}
