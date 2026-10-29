use axum::extract::{DefaultBodyLimit, Query};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::ConfigSnapshot;
use relay_event_schema::protocol::EventId;
use serde::Deserialize;

use crate::constants::UNREAL_USER_HEADER;
use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::extractors::IntegrationBuilder;
use crate::integrations::ErrorsIntegration;
use crate::middlewares;
use crate::service::ServiceState;

#[derive(Debug, Deserialize)]
struct UnrealQuery {
    #[serde(rename = "UserID")]
    user_id: Option<String>,
}

async fn handle(
    state: ServiceState,
    Query(query): Query<UnrealQuery>,
    mut builder: IntegrationBuilder,
) -> axum::response::Result<impl IntoResponse> {
    if builder.is_empty() {
        return Err(BadStoreRequest::EmptyBody.into());
    }

    let id = EventId::new();
    builder = builder.with_event_id(id);
    if let Some(user_id) = query.user_id {
        builder = builder.with_header(UNREAL_USER_HEADER, user_id);
    }

    let envelope = builder.with_type(ErrorsIntegration::Unreal).build();

    // Never respond with a 429 since clients often retry these
    common::handle_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();

    // The return here is only useful for consistency because the UE4 crash reporter doesn't
    // care about it.
    Ok(TextResponse(Some(id)))
}

pub fn route(config: &ConfigSnapshot) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}
