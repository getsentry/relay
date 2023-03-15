//! Handles event store requests.

use axum::extract::FromRequest;
use axum::http::{header, Method};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{headers, Json, Router, TypedHeader};
use bytes::{Bytes, BytesMut};
use relay_general::protocol::EventId;
use serde::Serialize;

use crate::body;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;

// Transparent 1x1 gif
// See http://probablyprogramming.com/2009/03/15/the-tiniest-gif-ever
static PIXEL: &[u8] =
    b"GIF89a\x01\x00\x01\x00\x00\xff\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;";

#[derive(Debug, FromRequest)]
struct EnvelopeParams {
    state: ServiceState,
    meta: RequestMeta,
    content_type: TypedHeader<headers::ContentType>,
    body: Bytes,
}

impl EnvelopeParams {
    /// Parses a full `Envelope` request body.
    fn parse_envelope(self) -> Result<Box<Envelope>, BadStoreRequest> {
        // Use `parse_request` here to ensure that we're merging available request headers into the
        // envelope's headers.
        Envelope::parse_request(self.body, self.meta).map_err(BadStoreRequest::InvalidEnvelope)
    }

    /// Parses a JSON event body into an `Envelope`.
    fn parse_event(mut self, content_type: ContentType) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self { body, meta, .. } = self;

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
            let mut data_mut = BytesMut::from(body);
            json_forensics::translate_slice(&mut data_mut[..]);
            body = data_mut.freeze();
        }

        // Ensure that the event has a UUID. It will be returned from this message and from the
        // incoming store request. To uncouple it from the workload on the processing workers, this
        // requires to synchronously parse a minimal part of the JSON payload. If the JSON payload
        // is invalid, processing can be skipped altogether.
        let minimal = common::minimal_event_from_json(&body)?;

        // Old SDKs used to send transactions to the store endpoint with an explicit `Transaction` event
        // type. The processing queue expects those in an explicit item.
        let item_type = ItemType::from_event_type(minimal.ty);
        let mut event_item = Item::new(item_type);
        event_item.set_payload(content_type, body);

        let event_id = minimal.id.unwrap_or_else(EventId::new);
        let mut envelope = Envelope::from_request(Some(event_id), meta);
        envelope.add_item(event_item);

        Ok(envelope)
    }

    async fn extract_envelope(self) -> Result<Box<Envelope>, BadStoreRequest> {
        // TODO(ja): Limit content size
        // let max_payload_size = state.config().max_event_size();
        // let data = body::store_body(request, max_payload_size).await?;

        if self.body.is_empty() {
            return Err(BadStoreRequest::EmptyBody);
        }

        // If the content type is missing, assume "application/json".
        let content_type = match self.content_type {
            content_type if content_type.is_empty() => ContentType::Json,
            content_type => ContentType::from(content_type),
        };

        match content_type {
            ContentType::Envelope => self.parse_envelope(),
            _ => self.parse_event(content_type),
        }
    }
}

#[derive(Serialize)]
struct StoreResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<EventId>,
}

/// Handler for the JSON event store endpoint.
///
/// This simply delegates to `store_event` which does all the work.
/// `handle_store_event` is an adaptor for `store_event` which cannot
/// be used directly as a request handler since not all of its arguments
/// implement the FromRequest trait.
async fn store_event(
    method: Method,
    envelope: EnvelopeParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let state = envelope.state.clone();
    let envelope = envelope.extract_envelope().await?;
    let id = common::handle_envelope(&state, envelope).await?;

    Ok(match method {
        Method::GET => ([(header::CONTENT_TYPE, "image/gif")], PIXEL).into_response(),
        _ => Json(StoreResponse { id }).into_response(),
    })
}

// pub fn configure_app(app: App<ServiceState>) -> App<ServiceState> {
//     common::cors(app)
//         // Standard store endpoint. Some SDKs send multiple leading or trailing slashes due to bugs
//         // in their URL handling. Since actix does not normalize such paths, allow any number of
//         // slashes. The trailing slash can also be omitted, optionally.
//         .resource(&common::normpath(r"/api/{project:\d+}/store/"), |r| {
//             r.name("store-default");
//             r.post()
//                 .with_async(|m, r| common::handler(store_event(m, r)));
//             r.get()
//                 .with_async(|m, r| common::handler(store_event(m, r)));
//         })
//         // Legacy store path. Since it is missing the project parameter, the `RequestMeta` extractor
//         // will use `ProjectKeyLookup` to map the public key to a project id before handling the
//         // request.
//         .resource(&common::normpath(r"/api/store/"), |r| {
//             r.name("store-legacy");
//             r.post()
//                 .with_async(|m, r| common::handler(store_event(m, r)));
//             r.get()
//                 .with_async(|m, r| common::handler(store_event(m, r)));
//         })
//         .register()
// }

pub fn routes() -> Router<ServiceState> {
    Router::new()
        // Standard store endpoint. Some SDKs send multiple leading or trailing slashes due to bugs
        // in their URL handling. Since actix does not normalize such paths, allow any number of
        // slashes. The trailing slash can also be omitted, optionally.
        // r.name("store-default");
        .route(
            "/api/:project_id/store/",
            post(store_event).get(store_event),
        )
        // Legacy store path. Since it is missing the project parameter, the `RequestMeta` extractor
        // will use `ProjectKeyLookup` to map the public key to a project id before handling the
        // request.
        // r.name("store-legacy");
        .route("/api/store/", post(store_event).get(store_event))
        .route_layer(common::cors())
}
