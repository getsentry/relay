//! Endpoints for security reports.

use actix_web::actix::ResponseFuture;
use actix_web::http::Method;
use actix_web::{pred, HttpRequest, HttpResponse, Request};
use bytes::Bytes;
use serde::Serialize;

use semaphore_general::protocol::{
    Csp, Event, EventId, ExpectCt, ExpectStaple, Hpkp, SecurityReportType,
};
use semaphore_general::types::Annotated;

use crate::actors::events::EventError;
use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

fn event_to_bytes(event: Event) -> Result<Bytes, serde_json::Error> {
    let json_string = Annotated::new(event).to_json()?;
    Ok(Bytes::from(json_string))
}

fn security_report_to_event(data: Bytes) -> Result<Bytes, serde_json::Error> {
    let event = match SecurityReportType::from_json(&data)? {
        SecurityReportType::Csp => Csp::parse_event(&data)?,
        SecurityReportType::ExpectCt => ExpectCt::parse_event(&data)?,
        SecurityReportType::ExpectStaple => ExpectStaple::parse_event(&data)?,
        SecurityReportType::Hpkp => Hpkp::parse_event(&data)?,
    };

    event_to_bytes(event)
}

fn process_security_report(data: Bytes) -> Result<Bytes, BadStoreRequest> {
    security_report_to_event(data)
        // Return a ProcessingFailed error here to avoid introducing an `InvalidJson` error variant
        // into BadStoreRequest. Eventually, this logic will be moved into `EventProcessor` and all
        // these errors will happen during processing, rather than in the endpoint.
        .map_err(|e| BadStoreRequest::ProcessingFailed(EventError::InvalidJson(e)))
}

#[derive(Serialize)]
struct SecurityReportResponse {
    id: EventId,
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

/// This handles all messages coming on the Security endpoint.
///
/// The security reports will be checked.
fn store_security_report(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let future =
        handle_store_like_request(meta, start_time, request, process_security_report, |id| {
            HttpResponse::Ok().json(SecurityReportResponse { id })
        });

    Box::new(future)
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
