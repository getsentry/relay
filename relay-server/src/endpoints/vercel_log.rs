use axum::extract::{DefaultBodyLimit, FromRequest};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;
use serde_json::value::RawValue;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, RequestMeta};
use crate::service::ServiceState;

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
struct VercelLogParams {
    meta: RequestMeta,
    body: Bytes,
}

async fn handle(
    state: ServiceState,
    content_type: RawContentType,
    params: VercelLogParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let ContentType::Json = ContentType::from(content_type.as_ref()) else {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response());
    };

    let mut envelope = Envelope::from_request(Some(EventId::new()), params.meta);
    envelope.require_feature(Feature::VercelLogsEndpoint);

    // Parse newline-delimited JSON (NDJSON)
    let payload_str = std::str::from_utf8(&params.body).map_err(|_| {
        // Convert UTF-8 error to a JSON parsing error
        BadStoreRequest::InvalidJson(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid UTF-8 in request body",
        )))
    })?;
    for line in payload_str.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let json_value: &RawValue =
            serde_json::from_str(line).map_err(BadStoreRequest::InvalidJson)?;
        let mut item = Item::new(ItemType::VercelLog);
        item.set_payload(ContentType::Json, json_value.to_owned().to_string());
        envelope.add_item(item);
    }

    common::handle_envelope(&state, envelope).await?;
    Ok(().into_response())
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_event_size()))
}
