use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::Config;
use relay_dynamic_config::Feature;

use crate::endpoints::common;
use crate::extractors::IntegrationBuilder;
use crate::integrations::LogsIntegration;
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
        state: ServiceState,
        builder: IntegrationBuilder,
    ) -> axum::response::Result<impl IntoResponse> {
        let envelope = builder
            .with_type(LogsIntegration::VercelDrainLog)
            .with_required_feature(Feature::VercelLogDrainEndpoint)
            .build();

        common::handle_envelope(&state, envelope).await?;

        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_envelope_size()))
    }
}
