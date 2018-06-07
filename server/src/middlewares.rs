use std::time::Instant;

use actix_web::error::Error;
use actix_web::middleware::{Finished, Middleware, Response, Started};
use actix_web::{http::header, Body, HttpRequest, HttpResponse};

use semaphore_aorta::ApiErrorResponse;

use constants::SERVER;
use service::ServiceState;
use utils::report_actix_error_to_sentry;

/// Basic metrics
pub struct Metrics;

struct StartTime(Instant);

impl<S> Middleware<S> for Metrics {
    fn start(&mut self, req: &mut HttpRequest<S>) -> Result<Started, Error> {
        req.extensions_mut().insert(StartTime(Instant::now()));
        Ok(Started::Done)
    }

    fn finish(&mut self, req: &mut HttpRequest<S>, resp: &HttpResponse) -> Finished {
        let start_time = req.extensions().get::<StartTime>().unwrap().0;
        metric!(timer("requests.duration") = start_time.elapsed());
        metric!(counter(&format!("responses.status_code.{}", resp.status())) += 1);
        Finished::Done
    }
}

/// Reports certain failures to sentry.
pub struct CaptureSentryError;

impl Middleware<ServiceState> for CaptureSentryError {
    fn response(
        &mut self,
        _req: &mut HttpRequest<ServiceState>,
        mut resp: HttpResponse,
    ) -> Result<Response, Error> {
        let mut event_id = None;
        if let Some(error) = resp.error() {
            if resp.status().is_server_error() {
                event_id = Some(report_actix_error_to_sentry(error));
            }
        }
        if let Some(event_id) = event_id {
            resp.headers_mut()
                .insert("x-sentry-event-id", event_id.to_string().parse().unwrap());
        }
        Ok(Response::Done(resp))
    }
}

/// Adds the common relay headers.
pub struct AddCommonHeaders;

impl<S> Middleware<S> for AddCommonHeaders {
    fn response(
        &mut self,
        _req: &mut HttpRequest<S>,
        mut resp: HttpResponse,
    ) -> Result<Response, Error> {
        resp.headers_mut()
            .insert(header::SERVER, header::HeaderValue::from_static(SERVER));
        Ok(Response::Done(resp))
    }
}

/// Registers the default error handlers.
pub struct ErrorHandlers;

impl<S> Middleware<S> for ErrorHandlers {
    fn response(&mut self, _: &mut HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
        if (resp.status().is_server_error() || resp.status().is_client_error())
            && resp.body() == &Body::Empty
        {
            let reason = resp.status().canonical_reason().unwrap_or("unknown error");
            Ok(Response::Done(
                resp.into_builder()
                    .json(ApiErrorResponse::with_detail(reason)),
            ))
        } else {
            Ok(Response::Done(resp))
        }
    }
}
