//! Endpoints for security reports.

use actix_web::actix::ResponseFuture;
use actix_web::{pred, HttpRequest, HttpResponse, Query, Request};
use futures::Future;
use serde::Deserialize;

use relay_general::protocol::EventId;

use crate::body::StoreBody;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

#[derive(Debug, Deserialize)]
struct SecurityReportParams {
    sentry_release: Option<String>,
    sentry_environment: Option<String>,
}

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: EventMeta,
    max_event_payload_size: usize,
    params: SecurityReportParams,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let future = StoreBody::new(&request, max_event_payload_size)
        .map_err(BadStoreRequest::PayloadError)
        .and_then(move |data| {
            if data.is_empty() {
                return Err(BadStoreRequest::EmptyBody);
            }

            let mut report_item = Item::new(ItemType::SecurityReport);
            report_item.set_payload(ContentType::Json, data);

            if let Some(sentry_release) = params.sentry_release {
                report_item.set_header("sentry_release", sentry_release);
            }

            if let Some(sentry_environment) = params.sentry_environment {
                report_item.set_header("sentry_environment", sentry_environment);
            }

            let mut envelope = Envelope::from_request(EventId::new(), meta);
            envelope.add_item(report_item);

            Ok(envelope)
        });
    Box::new(future)
}

fn create_response() -> HttpResponse {
    HttpResponse::Created()
        .content_type("application/javascript")
        .finish()
}

/// This handles all messages coming on the Security endpoint.
///
/// The security reports will be checked.
fn store_security_report(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
    params: Query<SecurityReportParams>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let event_size = request.state().config().max_event_payload_size();
    common::handle_store_like_request(
        meta,
        true,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, event_size, params.into_inner()),
        |_| create_response(),
    )
}

#[derive(Debug)]
struct SecurityReportFilter;

impl pred::Predicate<ServiceState> for SecurityReportFilter {
    fn check(&self, request: &Request, _: &ServiceState) -> bool {
        let content_type = request
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");

        match content_type {
            "application/csp-report"
            | "application/json"
            | "application/expect-ct-report"
            | "application/expect-ct-report+json"
            | "application/expect-staple-report" => true,
            _ => false,
        }
    }
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    // Default security endpoint
    app.resource(r"/api/{project:\d+}/security/", |r| {
        r.name("store-security-report");
        r.post()
            .filter(SecurityReportFilter)
            .with(store_security_report);
    })
    // Legacy security endpoint
    .resource(r"/api/{project:\d+}/csp-report/", |r| {
        r.name("store-csp-report");
        r.post()
            .filter(SecurityReportFilter)
            .with(store_security_report);
    })
}
