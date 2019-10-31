//! Endpoints for security reports.

use actix_web::actix::ResponseFuture;
use actix_web::http::Method;
use actix_web::{HttpRequest, HttpResponse};
use bytes::Bytes;
use serde::Serialize;

use semaphore_general::protocol::{
    CspReportRaw, Event, EventId, ExpectCtReportRaw, ExpectStapleReportRaw, HpkpRaw,
    SecurityReportType,
};
use semaphore_general::types::Annotated;

use crate::actors::events::EventError;
use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

fn security_report_to_event(data: Bytes) -> Result<Bytes, BadStoreRequest> {
    let str_body = String::from_utf8_lossy(&data);
    let security_report_type = get_security_report_type(str_body.as_ref())?;

    match security_report_type {
        SecurityReportType::Csp => csp_report_to_event_data(str_body.as_ref()),
        SecurityReportType::ExpectCt => expect_ct_report_to_event_data(str_body.as_ref()),
        SecurityReportType::ExpectStaple => expect_staple_report_to_event_data(str_body.as_ref()),
        SecurityReportType::Hpkp => hpkp_report_to_event_data(str_body.as_ref()),
    }
}

// Look into the body
fn get_security_report_type(data: &str) -> Result<SecurityReportType, BadStoreRequest> {
    serde_json::from_str::<SecurityReportType>(data)
        .map_err(|e| BadStoreRequest::ProcessingFailed(EventError::InvalidJson(e)))
}

fn csp_report_to_event_data(data: &str) -> Result<Bytes, BadStoreRequest> {
    let CspReportRaw { csp_report } = serde_json::from_str::<CspReportRaw>(data)
        .map_err(|e| BadStoreRequest::ProcessingFailed(EventError::InvalidJson(e)))?;

    //TODO finish add message and any other derived fields
    event_to_bytes(Event {
        logentry: Annotated::new(csp_report.get_message().into()),
        csp: Annotated::new(csp_report.into()),
        ..Event::default()
    })
}

fn expect_ct_report_to_event_data(data: &str) -> Result<Bytes, BadStoreRequest> {
    let ExpectCtReportRaw { expect_ct_report } = serde_json::from_str::<ExpectCtReportRaw>(data)
        .map_err(|e| BadStoreRequest::ProcessingFailed(EventError::InvalidJson(e)))?;

    //TODO finish add message and any other derived fields
    event_to_bytes(Event {
        logentry: Annotated::new(expect_ct_report.get_message().into()),
        expectct: Annotated::new(expect_ct_report.into()),
        ..Event::default()
    })
}

fn expect_staple_report_to_event_data(data: &str) -> Result<Bytes, BadStoreRequest> {
    let ExpectStapleReportRaw {
        expect_staple_report,
    } = serde_json::from_str::<ExpectStapleReportRaw>(data)
        .map_err(|e| BadStoreRequest::ProcessingFailed(EventError::InvalidJson(e)))?;

    //TODO finish add message and any other derived fields
    event_to_bytes(Event {
        logentry: Annotated::new(expect_staple_report.get_message().into()),
        expectstaple: Annotated::new(expect_staple_report.into()),
        ..Event::default()
    })
}

fn hpkp_report_to_event_data(data: &str) -> Result<Bytes, BadStoreRequest> {
    let hpkp_raw = serde_json::from_str::<HpkpRaw>(data)
        .map_err(|e| BadStoreRequest::ProcessingFailed(EventError::InvalidJson(e)))?;

    //TODO finish add message and any other derived fields
    event_to_bytes(Event {
        logentry: Annotated::new(hpkp_raw.get_message().into()),
        hpkp: Annotated::new(hpkp_raw.into()),
        ..Event::default()
    })
}

fn event_to_bytes(event: Event) -> Result<Bytes, BadStoreRequest> {
    let json_string = Annotated::new(event)
        .to_json()
        .map_err(|e| BadStoreRequest::ProcessingFailed(EventError::InvalidJson(e)))?;
    Ok(Bytes::from(json_string))
}

#[derive(Serialize)]
struct SecurityReportResponse {
    id: EventId,
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
        handle_store_like_request(meta, start_time, request, security_report_to_event, |id| {
            HttpResponse::Ok().json(SecurityReportResponse { id })
        });

    Box::new(future)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    //hook security endpoint
    app.resource(r"/api/{project:\d+}/security/", |r| {
        r.method(Method::POST).with(store_security_report);
    })
    //legacy security endpoint
    .resource(r"/api/{project:\d+}/csp-report/", |r| {
        r.method(Method::POST).with(store_security_report);
    })
}
