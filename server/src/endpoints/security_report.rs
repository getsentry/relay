//! Endpoints for security reports.

use actix_web::actix::ResponseFuture;
use actix_web::http::Method;
use actix_web::{pred, HttpRequest, HttpResponse, Query, Request};
use bytes::Bytes;
use serde::Deserialize;

use semaphore_general::protocol::EventId;
// use semaphore_general::protocol::{
//     Csp, ExpectCt, ExpectStaple, Hpkp, LenientString, SecurityReportType,
// };
// use semaphore_general::types::Annotated;

use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

#[derive(Debug, Deserialize)]
struct SecurityReportParams {
    sentry_release: Option<String>,
    sentry_environment: Option<String>,
}

// fn process_security_report(
//     data: Bytes,
//     params: SecurityReportParams,
// ) -> Result<Bytes, BadStoreRequest> {
//     let security_report_type = SecurityReportType::from_json(&data)
//         .map_err(|_| BadStoreRequest::InvalidSecurityReportType)?;

//     let mut event = match security_report_type {
//         SecurityReportType::Csp => Csp::parse_event(&data),
//         SecurityReportType::ExpectCt => ExpectCt::parse_event(&data),
//         SecurityReportType::ExpectStaple => ExpectStaple::parse_event(&data),
//         SecurityReportType::Hpkp => Hpkp::parse_event(&data),
//     }
//     .map_err(|e| BadStoreRequest::InvalidSecurityReport(e))?;

//     event.release = Annotated::from(params.sentry_release.map(LenientString));
//     event.environment = Annotated::from(params.sentry_environment);

//     let json_string = Annotated::new(event)
//         .to_json()
//         .map_err(|e| BadStoreRequest::InvalidJson(e))?;

//     Ok(Bytes::from(json_string))
// }

fn extract_envelope(
    data: Bytes,
    params: SecurityReportParams,
) -> Result<Envelope, BadStoreRequest> {
    if data.is_empty() {
        return Err(BadStoreRequest::EmptyBody);
    }

    let mut report_item = Item::new(ItemType::SecurityReport);
    report_item.set_payload(ContentType::Json, data);

    // TODO(ja): Type these out?
    if let Some(sentry_release) = params.sentry_release {
        report_item.set_header("sentry_release", sentry_release);
    }
    if let Some(sentry_environment) = params.sentry_environment {
        report_item.set_header("sentry_environment", sentry_environment);
    }

    let mut envelope = Envelope::new(EventId::new());
    envelope.add_item(report_item);

    Ok(envelope)
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
    params: Query<SecurityReportParams>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let future = handle_store_like_request(
        meta,
        start_time,
        request,
        move |data| extract_envelope(data, params.into_inner()),
        |_| {
            HttpResponse::Created()
                .content_type("application/javascript")
                .finish()
        },
    );

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
