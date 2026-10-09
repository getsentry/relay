use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::ConfigSnapshot;

use crate::endpoints::common;
use crate::extractors::{IntegrationBuilder, Mime};
use crate::integrations::LogsIntegration;
use crate::service::ServiceState;

pub fn route(config: &ConfigSnapshot) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_container_size()))
}

fn is_integrity_mime(mime: Mime) -> bool {
    let ty = mime.type_().as_str();
    let subty = mime.subtype().as_str();
    let suffix = mime.suffix().map(|suffix| suffix.as_str());

    matches!(
        (ty, subty, suffix),
        ("application", "json", None) | ("application", "reports", Some("json"))
    )
}

async fn handle(
    state: ServiceState,
    mime: Mime,
    builder: IntegrationBuilder,
) -> axum::response::Result<impl IntoResponse> {
    if !is_integrity_mime(mime) {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    let envelope = builder.with_type(LogsIntegration::Integrity).build();

    common::handle_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();

    Ok(StatusCode::OK)
}
