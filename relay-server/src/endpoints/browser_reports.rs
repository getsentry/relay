//! Endpoint for browser reports sent through the Reporting API.
//!
//! All reports received on this endpoint are converted into logs, regardless of their type. This
//! includes CSP violations, network errors (NEL), integrity violations, deprecations,
//! interventions and crashes.
//!
//! Legacy CSP reports sent via the `report-uri` directive are also accepted.

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::ConfigSnapshot;

use crate::endpoints::common;
use crate::extractors::{IntegrationBuilder, Mime};
use crate::integrations::LogsIntegration;
use crate::service::ServiceState;

fn is_browser_report_mime(mime: Mime) -> bool {
    let ty = mime.type_().as_str();
    let subty = mime.subtype().as_str();
    let suffix = mime.suffix().map(|suffix| suffix.as_str());

    matches!(
        (ty, subty, suffix),
        ("application", "json", None)
            | ("application", "reports", Some("json"))
            | ("application", "csp-report", None)
    )
}

async fn handle(
    state: ServiceState,
    mime: Mime,
    builder: IntegrationBuilder,
) -> axum::response::Result<impl IntoResponse> {
    if !is_browser_report_mime(mime) {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    let envelope = builder.with_type(LogsIntegration::BrowserReports).build();

    // Browsers do not retry reports, there is no point in responding with a rate limit.
    common::handle_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();

    Ok(StatusCode::OK)
}

pub fn route(config: &ConfigSnapshot) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_container_size()))
}
