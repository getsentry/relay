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
use crate::service::ServiceState;

async fn handle(
    state: ServiceState,
    content_type: RawContentType,
    meta: RequestMeta,
    request: Request,
) -> axum::response::Result<impl IntoResponse> {
    let content_type @ (ContentType::Json | ContentType::Protobuf) =
        ContentType::from(content_type.as_ref())
    else {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    };
    let payload: Bytes = request.extract().await?;
    let mut envelope = Envelope::from_request(None, meta);
    envelope.require_feature(Feature::OtelEndpoint);

    envelope.add_item({
        let mut item = Item::new(ItemType::OtelTracesData);
        item.set_payload(content_type, payload);
        item
    });

    common::handle_envelope(&state, envelope).await?;

    Ok(StatusCode::ACCEPTED)
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_envelope_size()))
}
