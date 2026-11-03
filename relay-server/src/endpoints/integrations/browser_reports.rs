use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::ConfigSnapshot;
use relay_dynamic_config::Feature;

use crate::endpoints::common;
use crate::envelope::ContentType;
use crate::extractors::{IntegrationBuilder, RawContentType};
use crate::integrations::LogsIntegration;
use crate::service::ServiceState;

async fn handle(
    content_type: RawContentType,
    state: ServiceState,
    builder: IntegrationBuilder,
) -> axum::response::Result<impl IntoResponse> {
    if !matches!(
        content_type.as_ref().parse::<ContentType>(),
        Ok(ContentType::BrowserReport)
    ) {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    let envelope = builder
        .with_type(LogsIntegration::BrowserReport)
        .with_required_feature(Feature::BrowserReportIngestion)
        .build();

    common::handle_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();

    Ok(StatusCode::OK)
}

pub fn route(config: &ConfigSnapshot) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_container_size()))
}
