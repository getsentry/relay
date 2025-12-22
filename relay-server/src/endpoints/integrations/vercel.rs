use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::Config;

use crate::endpoints::common;
use crate::envelope::ContentType;
use crate::extractors::{IntegrationBuilder, RawContentType};
use crate::integrations::{LogsIntegration, VercelLogDrainFormat};
use crate::service::ServiceState;

/// All routes configured for the Vercel integration.
///
/// The integration currently supports the following endpoints:
///  - Vercel Log Drain
pub fn routes(config: &Config) -> axum::Router<ServiceState> {
    axum::Router::new()
        .route("/logs", logs::route(config))
        .route("/logs/", logs::route(config))
}

mod logs {
    use super::*;

    async fn handle(
        content_type: RawContentType,
        state: ServiceState,
        builder: IntegrationBuilder,
    ) -> axum::response::Result<impl IntoResponse> {
        let format = match ContentType::from(content_type.as_ref()) {
            ContentType::Json => VercelLogDrainFormat::Json,
            ContentType::NdJson => VercelLogDrainFormat::NdJson,
            _ => return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE),
        };

        let envelope = builder
            .with_type(LogsIntegration::VercelDrainLog { format })
            .build();

        common::handle_envelope(&state, envelope)
            .await?
            .ensure_rate_limits()?;

        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_logs_integration_size()))
    }
}
