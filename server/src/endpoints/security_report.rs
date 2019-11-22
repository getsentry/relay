//! Endpoints for security reports.

use actix_web::actix::ResponseFuture;
use actix_web::http::Method;
use actix_web::{pred, HttpRequest, HttpResponse, Query, Request};
use bytes::Bytes;
use serde::Deserialize;

use semaphore_general::protocol::EventId;

use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

#[derive(Debug, Deserialize)]
struct SecurityReportParams {
    sentry_release: Option<String>,
    sentry_environment: Option<String>,
}

fn extract_envelope(
    data: Bytes,
    meta: EventMeta,
    params: SecurityReportParams,
) -> Result<Envelope, BadStoreRequest> {
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
    Box::new(handle_store_like_request(
        meta,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, params.into_inner()),
        |_| create_response(),
    ))
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
    app.resource(r"/api/{project:\d+}/security/", |r| {
        //hook security endpoint
        r.method(Method::POST)
            .filter(SecurityReportFilter)
            .with(store_security_report);
    })
    .resource(r"/api/{project:\d+}/csp-report/", |r| {
        //legacy security endpoint
        r.method(Method::POST)
            .filter(SecurityReportFilter)
            .with(store_security_report);
    })
}
