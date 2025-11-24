use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::Config;
use relay_dynamic_config::Feature;

use crate::endpoints::common;
use crate::envelope::ContentType;
use crate::extractors::{IntegrationBuilder, RawContentType};
use crate::integrations::LogsIntegration;
use crate::service::ServiceState;

/// All routes configured for the Heroku integration.
///
/// The integration currently supports the following endpoints:
///  - Heroku Log Drain
pub fn routes(config: &Config) -> axum::Router<ServiceState> {
    axum::Router::new()
        .route("/logs", logs::route(config))
        .route("/logs/", logs::route(config))
}

mod logs {
    use super::*;

    /// Helper function to extract a header value as a String.
    fn extract_header(headers: &HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    async fn handle(
        content_type: RawContentType,
        headers: HeaderMap,
        state: ServiceState,
        builder: IntegrationBuilder,
    ) -> axum::response::Result<impl IntoResponse> {
        // Validate content type
        if !matches!(
            ContentType::from(content_type.as_ref()),
            ContentType::Integration(crate::integrations::Integration::Logs(
                LogsIntegration::HerokuLogDrain { .. }
            ))
        ) {
            return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }

        // Extract Heroku-specific headers
        let msg_count = extract_header(&headers, "Logplex-Msg-Count");
        let frame_id = extract_header(&headers, "Logplex-Frame-Id");
        let drain_token = extract_header(&headers, "Logplex-Drain-Token");
        let user_agent = extract_header(&headers, "User-Agent");

        let envelope = builder
            .with_type(LogsIntegration::HerokuLogDrain {
                msg_count,
                frame_id,
                drain_token,
                user_agent,
            })
            .with_required_feature(Feature::HerokuLogDrainEndpoint)
            .build();

        common::handle_envelope(&state, envelope).await?;

        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_envelope_size()))
    }
}
