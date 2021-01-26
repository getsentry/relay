//! Endpoints for security reports.

use actix_web::actix::ResponseFuture;
use actix_web::{pred, HttpMessage, HttpRequest, HttpResponse, Query, Request};
use futures::Future;
use serde::Deserialize;

use relay_general::protocol::EventId;

use crate::body::StoreBody;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};

#[derive(Debug, Deserialize)]
struct SecurityReportParams {
    sentry_release: Option<String>,
    sentry_environment: Option<String>,
}

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
    params: SecurityReportParams,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let max_payload_size = request.state().config().max_event_size();
    let future = StoreBody::new(&request, max_payload_size)
        .map_err(BadStoreRequest::PayloadError)
        .and_then(move |data| {
            if data.is_empty() {
                return Err(BadStoreRequest::EmptyBody);
            }

            let mut report_item = Item::new(ItemType::RawSecurity);
            report_item.set_payload(ContentType::Json, data);

            if let Some(sentry_release) = params.sentry_release {
                report_item.set_header("sentry_release", sentry_release);
            }

            if let Some(sentry_environment) = params.sentry_environment {
                report_item.set_header("sentry_environment", sentry_environment);
            }

            let mut envelope = Envelope::from_request(Some(EventId::new()), meta);
            envelope.add_item(report_item);

            Ok(envelope)
        });
    Box::new(future)
}

fn create_response() -> HttpResponse {
    HttpResponse::Ok().finish()
}

/// This handles all messages coming on the Security endpoint.
///
/// The security reports will be checked.
fn store_security_report(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
    params: Query<SecurityReportParams>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    common::handle_store_like_request(
        meta,
        true,
        request,
        move |data, meta| extract_envelope(data, meta, params.into_inner()),
        |_| create_response(),
        true,
    )
}

#[derive(Debug)]
struct SecurityReportFilter;

impl pred::Predicate<ServiceState> for SecurityReportFilter {
    fn check(&self, request: &Request, _: &ServiceState) -> bool {
        let mime_type = match request.mime_type() {
            Ok(Some(mime)) => mime,
            _ => return false,
        };

        let ty = mime_type.type_().as_str();
        let subty = mime_type.subtype().as_str();
        let suffix = mime_type.suffix().map(|suffix| suffix.as_str());

        matches!(
            (ty, subty, suffix),
            ("application", "json", None)
                | ("application", "csp-report", None)
                | ("application", "expect-ct-report", None)
                | ("application", "expect-ct-report", Some("json"))
                | ("application", "expect-staple-report", None)
        )
    }
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        // Default security endpoint
        .resource(&common::normpath(r"/api/{project:\d+}/security/"), |r| {
            r.name("store-security-report");
            r.post()
                .filter(SecurityReportFilter)
                .with(store_security_report);
        })
        // Legacy security endpoint
        .resource(&common::normpath(r"/api/{project:\d+}/csp-report/"), |r| {
            r.name("store-csp-report");
            r.post()
                .filter(SecurityReportFilter)
                .with(store_security_report);
        })
        .register()
}
