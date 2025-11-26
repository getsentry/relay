//! Heroku Log Drain integration endpoint.
//!
//! This module handles HTTPS log drains from Heroku's Logplex system.
//! Logplex sends batches of syslog-formatted messages via POST requests.

use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_ourlogs::HerokuHeader;
use relay_protocol::Value;

use crate::endpoints::common;
use crate::envelope::ContentType;
use crate::extractors::{IntegrationBuilder, RawContentType};
use crate::integrations::LogsIntegration;
use crate::service::ServiceState;

/// All routes configured for the Heroku integration.
///
/// The integration currently supports the following endpoints:
///  - Heroku Log Drain (Logplex HTTPS drain)
pub fn routes(config: &Config) -> axum::Router<ServiceState> {
    axum::Router::new()
        .route("/logs", logs::route(config))
        .route("/logs/", logs::route(config))
}

mod logs {
    use super::*;

    async fn handle(
        content_type: RawContentType,
        headers: HeaderMap,
        state: ServiceState,
        builder: IntegrationBuilder,
    ) -> axum::response::Result<impl IntoResponse> {
        if ContentType::Logplex != content_type.as_ref() {
            return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }

        let item_headers: Vec<_> = [
            HerokuHeader::FrameId,
            HerokuHeader::DrainToken,
            HerokuHeader::UserAgent,
        ]
        .into_iter()
        .filter_map(|header| {
            let value = headers.get(header.http_header_name())?.to_str().ok()?;
            Some((header.as_str().to_owned(), Value::String(value.to_owned())))
        })
        .collect();

        let envelope = builder
            .with_type_and_headers(LogsIntegration::HerokuLogDrain, item_headers)
            .with_required_feature(Feature::HerokuLogDrainEndpoint)
            .build();

        common::handle_envelope(&state, envelope).await?;

        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_envelope_size()))
    }
}
