//! Endpoints for security reports.

use axum::extract::{FromRequest, Query};
use axum::response::IntoResponse;
use bytes::Bytes;
use relay_general::protocol::EventId;
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
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
pub async fn handle(
    state: ServiceState,
    params: SecurityReportParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = params.extract_envelope()?;
    common::handle_envelope(&state, envelope).await?;
    Ok(())
}

// TODO(ja): Filter

// #[derive(Debug)]
// struct SecurityReportFilter;

// impl pred::Predicate<ServiceState> for SecurityReportFilter {
//     fn check(&self, request: &Request, _: &ServiceState) -> bool {
//         let mime_type = match request.mime_type() {
//             Ok(Some(mime)) => mime,
//             _ => return false,
//         };

//         let ty = mime_type.type_().as_str();
//         let subty = mime_type.subtype().as_str();
//         let suffix = mime_type.suffix().map(|suffix| suffix.as_str());

//         matches!(
//             (ty, subty, suffix),
//             ("application", "json", None)
//                 | ("application", "csp-report", None)
//                 | ("application", "expect-ct-report", None)
//                 | ("application", "expect-ct-report", Some("json"))
//                 | ("application", "expect-staple-report", None)
//         )
//     }
// }

// pub fn configure_app(app: App<ServiceState>) -> App<ServiceState> {
//     common::cors(app)
//         // Default security endpoint
//         .resource(&common::normpath(r"/api/{project:\d+}/security/"), |r| {
//             r.name("store-security-report");
//             r.post()
//                 .filter(SecurityReportFilter)
//                 .with_async(|m, r, p| common::handler(store_security_report(m, r, p)));
//         })
//         // Legacy security endpoint
//         .resource(&common::normpath(r"/api/{project:\d+}/csp-report/"), |r| {
//             r.name("store-csp-report");
//             r.post()
//                 .filter(SecurityReportFilter)
//                 .with_async(|m, r, p| common::handler(store_security_report(m, r, p)));
//         })
//         .register()
// }
