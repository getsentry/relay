use axum::extract::{DefaultBodyLimit, Json, Request};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use axum::RequestExt;
use axum_extra::protobuf::Protobuf;
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_spans::otel_trace::TracesData;

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
    let trace: TracesData = if content_type.as_ref().starts_with("application/json") {
        let Json(trace) = request.extract().await?;
        trace
    } else if content_type.as_ref().starts_with("application/x-protobuf") {
        let Protobuf(trace) = request.extract().await?;
        trace
    } else {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    };

    let mut envelope = Envelope::from_request(None, meta);
    envelope.require_feature(Feature::OtelEndpoint);
    for resource_span in trace.resource_spans {
        for scope_span in resource_span.scope_spans {
            for span in scope_span.spans {
                let Ok(payload) = serde_json::to_vec(&span) else {
                    continue;
                };
                let mut item = Item::new(ItemType::OtelSpan);
                item.set_payload(ContentType::Json, payload);
                envelope.add_item(item);
            }
        }
    }
    common::handle_envelope(&state, envelope).await?;

    Ok(StatusCode::ACCEPTED)
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_span_size()))
}
