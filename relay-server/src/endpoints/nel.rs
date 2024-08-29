//! Endpoint for Network Error Logging (NEL) reports.
//!
//! It split list of incoming events from the envelope into separate envelope with 1 item inside.
//! Which later get failed by the service infrastructure.

use axum::extract::{DefaultBodyLimit, FromRequest};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use bytes::Bytes;
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use serde_json::value::RawValue;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{Mime, RequestMeta};
use crate::service::ServiceState;

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
struct NelReportParams {
    meta: RequestMeta,
    body: Bytes,
}

fn is_nel_mime(mime: Mime) -> bool {
    let ty = mime.type_().as_str();
    let subty = mime.subtype().as_str();
    let suffix = mime.suffix().map(|suffix| suffix.as_str());

    matches!(
        (ty, subty, suffix),
        ("application", "json", None) | ("application", "reports", Some("json"))
    )
}

/// Handles all messages coming on the NEL endpoint.
async fn handle(
    state: ServiceState,
    mime: Mime,
    params: NelReportParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    if !is_nel_mime(mime) {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response());
    }

    let items: Vec<&RawValue> =
        serde_json::from_slice(&params.body).map_err(BadStoreRequest::InvalidJson)?;

    let mut envelope = Envelope::from_request(Some(EventId::new()), params.meta.clone());
    for item in items {
        let mut report_item = Item::new(ItemType::Nel);
        report_item.set_payload(ContentType::Json, item.to_owned().to_string());
        envelope.add_item(report_item);
    }

    common::handle_envelope(&state, envelope).await?;
    Ok(().into_response())
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_event_size()))
}
