//! Endpoints for security reports.

use axum::extract::{DefaultBodyLimit, FromRequest, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use bytes::Bytes;
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{Mime, RequestMeta};
use crate::service::ServiceState;

#[derive(Debug, Deserialize)]
struct SecurityReportQuery {
    sentry_release: Option<String>,
    sentry_environment: Option<String>,
}

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
struct SecurityReportParams {
    meta: RequestMeta,
    #[from_request(via(Query))]
    query: SecurityReportQuery,
    body: Bytes,
}

impl SecurityReportParams {
    fn extract_envelope(self) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self { meta, query, body } = self;

        if body.is_empty() {
            return Err(BadStoreRequest::EmptyBody);
        }

        let mut report_item = Item::new(ItemType::RawSecurity);
        report_item.set_payload(ContentType::Json, body);

        if let Some(sentry_release) = query.sentry_release {
            report_item.set_header("sentry_release", sentry_release);
        }

        if let Some(sentry_environment) = query.sentry_environment {
            report_item.set_header("sentry_environment", sentry_environment);
        }

        let mut envelope = Envelope::from_request(Some(EventId::new()), meta);
        envelope.add_item(report_item);

        Ok(envelope)
    }
}

fn is_security_mime(mime: Mime) -> bool {
    let ty = mime.type_().as_str();
    let subty = mime.subtype().as_str();
    let suffix = mime.suffix().map(|suffix| suffix.as_str());

    matches!(
        (ty, subty, suffix),
        ("application", "json", None)
            | ("application", "csp-report", None)
            | ("application", "expect-ct-report", None)
            | ("application", "expect-ct-report", Some("json"))
            | ("application", "expect-staple-report", None)
    )
}

/// This handles all messages coming on the Security endpoint.
///
/// The security reports will be checked.
async fn handle(
    state: ServiceState,
    mime: Mime,
    params: SecurityReportParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    if !is_security_mime(mime) {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response());
    }

    let envelope = params.extract_envelope()?;
    common::handle_envelope(&state, envelope).await?;
    Ok(().into_response())
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_event_size()))
}
