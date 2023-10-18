use axum::extract;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use bytes::Bytes;

use relay_config::Config;
use relay_event_schema::protocol::{EventId, Span as EventSpan};
use relay_protocol::Annotated;
use relay_spans::Span;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;

async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    extract::Json(span): extract::Json<Span>,
) -> Result<impl IntoResponse, BadStoreRequest> {
    println!("{:#?}", span);

    // Convert to an Event span.
    let event_span: Annotated<EventSpan> = Annotated::new(span.into());

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
