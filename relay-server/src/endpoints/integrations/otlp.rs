use axum::RequestExt;
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use relay_config::Config;
use relay_dynamic_config::Feature;

use crate::endpoints::common;
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{IntegrationBuilder, RawContentType, RequestMeta};
use crate::integrations::{LogsIntegration, OtelFormat};
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

// Public until we can remove the old, pre integration, routes.
pub mod traces {
    use super::*;

    async fn handle(
        state: ServiceState,
        content_type: RawContentType,
        meta: RequestMeta,
        request: Request,
    ) -> axum::response::Result<impl IntoResponse> {
        let content_type @ (ContentType::Json | ContentType::Protobuf) =
            ContentType::from(content_type.as_ref())
        else {
            return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        };

        let payload: Bytes = request.extract().await?;
        let mut envelope = Envelope::from_request(None, meta);
        envelope.require_feature(Feature::OtelEndpoint);

        envelope.add_item({
            let mut item = Item::new(ItemType::OtelTracesData);
            item.set_payload(content_type, payload);
            item
        });

        common::handle_envelope(&state, envelope).await?;

        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_envelope_size()))
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

        common::handle_envelope(&state, envelope).await?;

        Ok(StatusCode::ACCEPTED)
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_envelope_size()))
    }
}
