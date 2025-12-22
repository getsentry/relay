use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::Config;
use relay_dynamic_config::Feature;

use crate::endpoints::common;
use crate::envelope::ContentType;
use crate::extractors::{IntegrationBuilder, RawContentType};
use crate::integrations::{LogsIntegration, OtelFormat, SpansIntegration};
use crate::service::ServiceState;

/// All routes configured for the OTLP integration.
///
/// The integration currently supports the following endpoints:
///  - V1 Traces
///  - V1 Logs
pub fn routes(config: &Config) -> axum::Router<ServiceState> {
    axum::Router::new()
        .route("/v1/traces", traces::route(config))
        .route("/v1/traces/", traces::route(config))
        .route("/v1/logs", logs::route(config))
        .route("/v1/logs/", logs::route(config))
}

mod traces {
    use super::*;

    async fn handle(
        content_type: RawContentType,
        state: ServiceState,
        builder: IntegrationBuilder,
    ) -> axum::response::Result<impl IntoResponse> {
        let format = match ContentType::from(content_type.as_ref()) {
            ContentType::Json => OtelFormat::Json,
            ContentType::Protobuf => OtelFormat::Protobuf,
            _ => return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE),
        };

        let envelope = builder
            .with_type(SpansIntegration::OtelV1 { format })
            .with_required_feature(Feature::OtelTracesEndpoint)
            .build();

        common::handle_envelope(&state, envelope)
            .await?
            .ensure_rate_limits()?;

        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_spans_integration_size()))
    }
}

mod logs {
    use super::*;

    async fn handle(
        content_type: RawContentType,
        state: ServiceState,
        builder: IntegrationBuilder,
    ) -> axum::response::Result<impl IntoResponse> {
        let format = match ContentType::from(content_type.as_ref()) {
            ContentType::Json => OtelFormat::Json,
            ContentType::Protobuf => OtelFormat::Protobuf,
            _ => return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE),
        };

        let envelope = builder
            .with_type(LogsIntegration::OtelV1 { format })
            .with_required_feature(Feature::OtelLogsEndpoint)
            .build();

        common::handle_envelope(&state, envelope)
            .await?
            .ignore_rate_limits();

        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_logs_integration_size()))
    }
}
