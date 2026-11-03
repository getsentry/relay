//! Heroku Log Drain integration endpoint.
//!
//! This module handles HTTPS log drains from Heroku's Logplex system.
//! Logplex sends batches of syslog-formatted messages via POST requests.
//!
//! Heroku cannot attach custom headers to drain requests, the DSN key must be passed in the
//! `sentry_key` query parameter.

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::ConfigSnapshot;
use relay_dynamic_config::Feature;
use relay_ourlogs::{
    HEROKU_LOGPLEX_HEADER_DRAIN_TOKEN as DRAIN_TOKEN, HEROKU_LOGPLEX_HEADER_MSG_COUNT as MSG_COUNT,
};

use crate::endpoints::common;
use crate::envelope::ContentType;
use crate::extractors::{IntegrationBuilder, RawContentType};
use crate::integrations::LogsIntegration;
use crate::service::ServiceState;

/// All routes configured for the Heroku integration.
///
/// The integration currently supports the following endpoints:
///  - Heroku Log Drain (Logplex HTTPS drain)
pub fn routes(config: &ConfigSnapshot) -> axum::Router<ServiceState> {
    axum::Router::new()
        .route("/logs", logs::route(config))
        .route("/logs/", logs::route(config))
}

mod logs {
    use super::*;

    async fn handle(
        content_type: RawContentType,
        headers: http::HeaderMap,
        state: ServiceState,
        builder: IntegrationBuilder,
    ) -> axum::response::Result<impl IntoResponse> {
        if !matches!(
            content_type.as_ref().parse::<ContentType>(),
            Ok(ContentType::Logplex)
        ) {
            return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }

        let header = |name| headers.get(name).and_then(|h| h.to_str().ok());

        let envelope = builder
            .with_type(LogsIntegration::HerokuLogDrain)
            .with_header_opt(MSG_COUNT, header(MSG_COUNT))
            .with_header_opt(DRAIN_TOKEN, header(DRAIN_TOKEN))
            .with_required_feature(Feature::HerokuLogDrainEndpoint)
            .build();

        common::handle_envelope(&state, envelope)
            .await?
            .check_rate_limits()?;

        // Logplex expects an empty response body.
        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &ConfigSnapshot) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_container_size()))
    }
}
