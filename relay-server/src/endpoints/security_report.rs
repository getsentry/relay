//! Endpoints for security reports.

use axum::extract::{FromRequest, Query};
use axum::handler::Handler;
use axum::response::IntoResponse;
use axum::{headers, RequestExt, TypedHeader};
use bytes::Bytes;
use relay_general::protocol::EventId;
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::endpoints::forward;
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;

#[derive(Debug, Deserialize)]
struct SecurityReportQuery {
    sentry_release: Option<String>,
    sentry_environment: Option<String>,
}

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
pub struct SecurityReportParams {
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

/// This handles all messages coming on the Security endpoint.
///
/// The security reports will be checked.
async fn inner(
    state: ServiceState,
    params: SecurityReportParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = params.extract_envelope()?;
    common::handle_envelope(&state, envelope).await?;
    Ok(())
}

fn predicate(TypedHeader(_): TypedHeader<headers::ContentType>) -> bool {
    // let ty = content_type.type_().as_str();
    // let subty = content_type.subtype().as_str();
    // let suffix = content_type.suffix().map(|suffix| suffix.as_str());

    // matches!(
    //     (ty, subty, suffix),
    //     ("application", "json", None)
    //         | ("application", "csp-report", None)
    //         | ("application", "expect-ct-report", None)
    //         | ("application", "expect-ct-report", Some("json"))
    //         | ("application", "expect-staple-report", None)
    // )
    todo!("get mime type and run predicate")
}

pub async fn handle<B>(
    state: ServiceState,
    mut req: axum::http::Request<B>,
) -> axum::response::Result<impl IntoResponse>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
{
    let data = req.extract_parts().await?;
    Ok(if predicate(data) {
        inner.call(req, state).await
    } else {
        forward::handle.call(req, state).await
    })
}
