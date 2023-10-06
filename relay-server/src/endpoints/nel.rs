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
use serde_json::Value;

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

/// This handles all messages coming on the NEL endpoint.
async fn handle(
    state: ServiceState,
    mime: Mime,
    params: NelReportParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    if !is_nel_mime(mime) {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response());
    }

    let items: Value =
        serde_json::from_slice(&params.body).map_err(BadStoreRequest::InvalidJson)?;

    // Iterate only if the body contains the list of the items.
    let handlers = items.as_array().into_iter().flatten().map(|item| {
        let mut envelope = Envelope::from_request(Some(EventId::new()), params.meta.clone());
        let mut report_item = Item::new(ItemType::Nel);
        report_item.set_payload(ContentType::Json, item.to_owned().to_string());
        envelope.add_item(report_item);
        common::handle_envelope(&state, envelope)
    });

    // Check the results of the handled envelopes.
    for handle_result in futures::future::join_all(handlers).await {
        handle_result?;
    }

    Ok(().into_response())
}

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
{
    post(handle).route_layer(DefaultBodyLimit::max(config.max_event_size()))
}
