use axum::extract;
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use axum::RequestExt;
use bytes::Bytes;

use relay_config::Config;
use relay_event_schema::protocol::{EventId, Span as EventSpan};
use relay_protocol::Annotated;
use relay_spans::Span;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, RequestMeta};
use crate::service::ServiceState;

async fn handle<B>(
    state: ServiceState,
    content_type: RawContentType,
    meta: RequestMeta,
    request: Request<B>,
) -> axum::response::Result<impl IntoResponse>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send + Into<Bytes>,
    B::Error: Into<axum::BoxError>,
{
    if !content_type.as_ref().starts_with("application/json") {
        return Ok(StatusCode::ACCEPTED);
    }

    // Parse OTel span.
    let payload: String = request.extract().await?;
    let span: Annotated<Span> =
        Annotated::from_json(payload.as_str()).map_err(BadStoreRequest::InvalidJson)?;

    // Convert to an Event span.
    let event_span: Annotated<EventSpan> = Annotated::new(span.value().unwrap().into());

    // Pack into an envelope for further processing.
    let span_json = event_span.to_json().map_err(BadStoreRequest::InvalidJson)?;
    let mut item = Item::new(ItemType::Span);
    item.set_payload(ContentType::Json, span_json);
    let mut envelope = Envelope::from_request(Some(EventId::new()), meta);
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
