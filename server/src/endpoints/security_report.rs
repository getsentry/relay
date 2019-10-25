//! Endpoints for security reports.

use actix_web::actix::ResponseFuture;
use actix_web::http::Method;
use actix_web::{HttpRequest, HttpResponse, ResponseError};
use bytes::Bytes;
use failure::Fail;
use futures::future::Future;

use semaphore_general::protocol::{Csp, CspReportRaw, Event, SecurityReportType};

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::endpoints::store::{store_event, BadStoreRequest, ToOutcome};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};
use crate::utils::ApiErrorResponse;
use semaphore_general::types::Annotated;

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    //hook security endpoint
    app.resource(r"/api/{project:\d+}/security/", |r| {
        r.method(Method::POST).with(handle_security_report);
    })
    //legacy security endpoint
    .resource(r"/api/{project:\d+}/csp-report/", |r| {
        r.method(Method::POST).with(handle_security_report);
    })
}

#[derive(Fail, Debug)]
pub enum SecurityError {
    #[fail(display = "empty security report")]
    EmptyReport,

    #[fail(display = "error parsing security report")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "failed to process message in store")]
    StoreError(#[cause] BadStoreRequest),
}

impl ResponseError for SecurityError {
    fn error_response(&self) -> HttpResponse {
        match self {
            SecurityError::EmptyReport | SecurityError::InvalidJson(_) => {
                HttpResponse::BadRequest().json(ApiErrorResponse::from_fail(self))
            }
            SecurityError::StoreError(se) => se.error_response(),
        }
    }
}

impl From<BadStoreRequest> for SecurityError {
    fn from(e: BadStoreRequest) -> SecurityError {
        SecurityError::StoreError(e)
    }
}

impl ToOutcome for SecurityError {
    fn to_outcome(&self) -> Outcome {
        match self {
            SecurityError::StoreError(se) => se.to_outcome(),
            _ => Outcome::Invalid(DiscardReason::SecurityReport),
        }
    }
}

/// This handles all messages coming on the Security endpoint.
/// The security reports will be checked
fn handle_security_report(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, SecurityError> {
    store_event(meta, start_time, request, Some(security_report_to_event))
}

fn security_report_to_event(data: Bytes) -> Result<Bytes, SecurityError> {
    let str_body = String::from_utf8_lossy(&data);
    let security_report_type = get_security_report_type(str_body.as_ref())?;

    match security_report_type {
        SecurityReportType::Csp => csp_report_to_event_data(str_body.as_ref()),
        SecurityReportType::ExpectCt => expect_ct_report_to_event_data(str_body.as_ref()),
        SecurityReportType::ExpectStaple => expect_staple_report_to_event_data(str_body.as_ref()),
        SecurityReportType::HpKp => hpkp_report_to_event_data(str_body.as_ref()),
    }
}

// Look into the body
fn get_security_report_type(data: &str) -> Result<SecurityReportType, SecurityError> {
    serde_json::from_str::<SecurityReportType>(data).map_err(SecurityError::InvalidJson)
}

fn csp_report_to_event_data(data: &str) -> Result<Bytes, SecurityError> {
    let CspReportRaw { csp_report } =
        serde_json::from_str::<CspReportRaw>(data).map_err(SecurityError::InvalidJson)?;

    event_to_bytes(Event {
        csp: Annotated::new(csp_report.into()),
        ..Event::default()
    })
}

fn expect_ct_report_to_event_data(data: &str) -> Result<Bytes, SecurityError> {
    //TODO implement
    unimplemented!();
}

fn expect_staple_report_to_event_data(data: &str) -> Result<Bytes, SecurityError> {
    //TODO implement
    unimplemented!();
}

fn hpkp_report_to_event_data(data: &str) -> Result<Bytes, SecurityError> {
    //TODO implement
    unimplemented!();
}

fn event_to_bytes(event: Event) -> Result<Bytes, SecurityError> {
    let json_string = Annotated::new(event)
        .to_json()
        .map_err(SecurityError::InvalidJson)?;
    Ok(Bytes::from(json_string))
}
