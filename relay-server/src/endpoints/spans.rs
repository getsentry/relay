use axum::extract::{DefaultBodyLimit, Json};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};

use relay_config::Config;
use relay_spans::TracesData;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;

async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    Json(trace): Json<TracesData>,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let mut envelope = Envelope::from_request(None, meta);
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
