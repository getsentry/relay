use axum::extract;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use bytes::Bytes;

use relay_config::Config;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, RequestMeta};
use crate::service::ServiceState;

async fn handle(
    state: ServiceState,
    content_type: RawContentType,
    meta: RequestMeta,
    body: Bytes,
) -> Result<impl IntoResponse, BadStoreRequest> {
    if !content_type.as_ref().starts_with("application/json") {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    if body.is_empty() {
        return Err(BadStoreRequest::EmptyBody);
    }

    let mut item = Item::new(ItemType::OtelSpan);
    item.set_payload(ContentType::Json, body);
    let mut envelope = Envelope::from_request(None, meta);
    envelope.add_item(item);
    common::handle_envelope(&state, envelope).await?;

    Ok(StatusCode::ACCEPTED)
}

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send + Into<Bytes>,
    B::Error: Into<axum::BoxError>,
{
    post(handle).route_layer(extract::DefaultBodyLimit::max(config.max_span_size()))
}
