//! Endpoints for security reports.

use actix_web::actix::ResponseFuture;
use actix_web::http::Method;
use actix_web::{HttpRequest, HttpResponse, ResponseError};
use bytes::Bytes;
use failure::Fail;
use futures::future::Future;

use semaphore_general::protocol::{Csp, Event, SecurityReportType};

use crate::endpoints::security_report::SecurityError::{InvalidSecurityMessage, StoreError};
use crate::endpoints::store::{store_event, BadStoreRequest};
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
    #[fail(display = "invalid security message")]
    InvalidSecurityMessage(String),
    #[fail(display = "failed to process message in store")]
    StoreError(BadStoreRequest),
}

impl ResponseError for SecurityError {
    fn error_response(&self) -> HttpResponse {
        match self {
            SecurityError::StoreError(se) => se.error_response(),
            SecurityError::InvalidSecurityMessage(_) => {
                HttpResponse::BadRequest().json(ApiErrorResponse::from_fail(self))
            }
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
    Box::new(
        store_event(
            meta,
            start_time,
            request,
            Some(security_report_to_sentry_request),
        )
        .map_err(|e| match e {
            BadStoreRequest::EventBodyProcessingError(s) => InvalidSecurityMessage(s),
            other => StoreError(other),
        }),
    )
}

fn security_report_to_sentry_request(data: Bytes) -> Result<Bytes, String> {
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
fn get_security_report_type(data: &str) -> Result<SecurityReportType, String> {
    serde_json::from_str::<SecurityReportType>(data).map_err(|json_err| {
        log::error!("Invalid json for security report: {:?}", json_err);
        "invalid security report".to_string()
    })
}

fn csp_report_to_event_data(data: &str) -> Result<Bytes, String> {
    unimplemented!();
    //    let annotated_csp = Annotated::<CspReport>::from_json(data).map_err(|json_err| {
    //        log::error!("Invalid json for CSP report: {:?}", json_err);
    //        "invalid CPS report".to_string()
    //    })?;
    //
    //    match annotated_csp {
    //        Annotated(Some(csp), _) => event_to_bytes(make_csp_event(csp)),
    //        Annotated(None, _) => Err("Empty CSP report".to_string()),
    //    }
}

fn make_csp_event(csp: Csp) -> Event {
    Event {
        csp: Annotated::new(csp),
        ..Default::default()
    }
}

fn expect_ct_report_to_event_data(data: &str) -> Result<Bytes, String> {
    //TODO implement
    unimplemented!();
}

fn expect_staple_report_to_event_data(data: &str) -> Result<Bytes, String> {
    //TODO implement
    unimplemented!();
}

fn hpkp_report_to_event_data(data: &str) -> Result<Bytes, String> {
    //TODO implement
    unimplemented!();
}

fn event_to_bytes(event: Event) -> Result<Bytes, String> {
    let event = Annotated::new(event).to_json();
    match event {
        Err(_serde_err) => Err("Internal error, could not convert Event to string".to_string()),
        Ok(json_string) => Ok(Bytes::from(json_string)),
    }
}
