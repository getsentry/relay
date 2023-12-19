//! Handles event store requests.

use std::io::{self, Read};

use axum::extract::{DefaultBodyLimit, Query};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use bytes::Bytes;
use data_encoding::BASE64;
use flate2::bufread::ZlibDecoder;
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use serde::{Deserialize, Serialize};

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{self, ContentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, RequestMeta};
use crate::service::ServiceState;

/// Decodes a base64-encoded zlib compressed request body.
///
/// If the body is not encoded with base64 or not zlib-compressed, this function returns the body
/// without modification.
///
/// If the body exceeds the given `limit` during streaming or decompression, an error is returned.
fn decode_bytes(body: Bytes, limit: usize) -> Result<Bytes, io::Error> {
    if body.is_empty() || body.starts_with(b"{") {
        return Ok(body);
    }

    // TODO: Switch to a streaming decoder
    // see https://github.com/alicemaz/rust-base64/pull/56
    let binary_body = BASE64
        .decode(body.as_ref())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    if binary_body.starts_with(b"{") {
        return Ok(binary_body.into());
    }

    let mut decode_stream = ZlibDecoder::new(binary_body.as_slice()).take(limit as u64);
    let mut bytes = vec![];
    decode_stream.read_to_end(&mut bytes)?;

    Ok(Bytes::from(bytes))
}

/// Parses an event body into an `Envelope`.
///
/// If the body is encoded with base64 or zlib, it will be transparently decoded.
fn parse_event(
    mut body: Bytes,
    meta: RequestMeta,
    config: &Config,
) -> Result<Box<Envelope>, BadStoreRequest> {
    // The body may be zlib compressed and encoded as base64. Decode it transparently if this is the
    // case.
    body = decode_bytes(body, config.max_event_size()).map_err(BadStoreRequest::InvalidBody)?;
    if body.is_empty() {
        return Err(BadStoreRequest::EmptyBody);
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
        let mut data_mut = body.to_vec();
        json_forensics::translate_slice(&mut data_mut[..]);
        body = data_mut.into();
    }

    // Ensure that the event has a UUID. It will be returned from this message and from the
    // incoming store request. To uncouple it from the workload on the processing workers, this
    // requires to synchronously parse a minimal part of the JSON payload. If the JSON payload
    // is invalid, processing can be skipped altogether.
    let minimal = common::minimal_event_from_json(&body)?;

    // Old SDKs used to send transactions to the store endpoint with an explicit `Transaction`
    // event type. The processing queue expects those in an explicit item.
    let item_type = ItemType::from_event_type(minimal.ty);
    let mut event_item = Item::new(item_type);
    event_item.set_payload(ContentType::Json, body);

    let event_id = minimal.id.unwrap_or_else(EventId::new);
    let mut envelope = Envelope::from_request(Some(event_id), meta);
    envelope.add_item(event_item);

    Ok(envelope)
}

#[derive(Serialize)]
struct PostResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<EventId>,
}

/// Handler for the JSON event store endpoint.
async fn handle_post(
    state: ServiceState,
    meta: RequestMeta,
    content_type: RawContentType,
    body: Bytes,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = match content_type.as_ref() {
        envelope::CONTENT_TYPE => Envelope::parse_request(body, meta)?,
        _ => parse_event(body, meta, state.config())?,
    };

    let id = common::handle_envelope(&state, envelope).await?;
    Ok(axum::Json(PostResponse { id }).into_response())
}

/// Query params of the GET store endpoint.
#[derive(Debug, Deserialize)]
struct GetQuery {
    sentry_data: String,
}

// Transparent 1x1 gif
// See http://probablyprogramming.com/2009/03/15/the-tiniest-gif-ever
static PIXEL: &[u8] =
    b"GIF89a\x01\x00\x01\x00\x00\xff\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;";

/// Handler for the GET event store endpoint.
///
/// In this version, the event payload is sent in a `sentry_data` query parameter. The response is a
/// transparent pixel GIF.
async fn handle_get(
    state: ServiceState,
    meta: RequestMeta,
    Query(query): Query<GetQuery>,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = parse_event(query.sentry_data.into(), meta, state.config())?;
    common::handle_envelope(&state, envelope).await?;
    Ok(([(header::CONTENT_TYPE, "image/gif")], PIXEL))
}

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
{
    (post(handle_post).get(handle_get)).route_layer(DefaultBodyLimit::max(config.max_event_size()))
}
