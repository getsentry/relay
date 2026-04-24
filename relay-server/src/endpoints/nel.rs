//! Endpoint for Network Error Logging (NEL) reports.
//!
//! It split list of incoming events from the envelope into separate envelope with 1 item inside.
//! Which later get failed by the service infrastructure.

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::Config;

use crate::endpoints::common;
use crate::extractors::{IntegrationBuilder, Mime};
use crate::integrations::LogsIntegration;
use crate::service::ServiceState;

fn is_nel_mime(mime: Mime) -> bool {
    let ty = mime.type_().as_str();
    let subty = mime.subtype().as_str();
    let suffix = mime.suffix().map(|suffix| suffix.as_str());

    matches!(
        (ty, subty, suffix),
        ("application", "json", None) | ("application", "reports", Some("json"))
    )
}

/// Handles all messages coming on the NEL endpoint.
async fn handle(
    state: ServiceState,
    mime: Mime,
    builder: IntegrationBuilder,
) -> axum::response::Result<impl IntoResponse> {
    if !is_nel_mime(mime) {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    let envelope = builder.with_type(LogsIntegration::Nel).build();

    common::handle_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();

    Ok(StatusCode::OK)
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_logs_integration_size()))
}
