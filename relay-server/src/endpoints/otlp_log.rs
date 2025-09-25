use axum::RequestExt;
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use relay_config::Config;
use relay_dynamic_config::Feature;

use crate::endpoints::common;
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, RequestMeta};
use crate::integrations::{Integration, LogsIntegration, OtelFormat};
use crate::service::ServiceState;

async fn handle(
    state: ServiceState,
    content_type: RawContentType,
    meta: RequestMeta,
    request: Request,
) -> axum::response::Result<impl IntoResponse> {
    let format = match ContentType::from(content_type.as_ref()) {
        ContentType::Json => OtelFormat::Json,
        ContentType::Protobuf => OtelFormat::Protobuf,
        _ => return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE),
    };
    let integration = Integration::Logs(LogsIntegration::OtelV1 { format });

    let payload: Bytes = request.extract().await?;
    let mut envelope = Envelope::from_request(None, meta);
    envelope.require_feature(Feature::OtelLogsEndpoint);

    envelope.add_item({
        let mut item = Item::new(ItemType::Integration);
        item.set_payload(integration.into(), payload);
        item
    });

    common::handle_envelope(&state, envelope).await?;

    Ok(StatusCode::ACCEPTED)
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_envelope_size()))
}
