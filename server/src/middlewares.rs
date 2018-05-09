use std::time::Instant;

use actix_web::error::Error;
use actix_web::middleware::{Finished, Middleware, Response, Started};
use actix_web::{Body, HttpRequest, HttpResponse, http::header};
use sentry::integrations::failure::capture_fail;

use semaphore_aorta::ApiErrorResponse;

use constants::SERVER;

/// Basic metrics
pub struct Metrics;

struct StartTime(Instant);

impl<S> Middleware<S> for Metrics {
    fn start(&self, req: &mut HttpRequest<S>) -> Result<Started, Error> {
        req.extensions_mut().insert(StartTime(Instant::now()));
        Ok(Started::Done)
    }

    fn finish(&self, req: &mut HttpRequest<S>, resp: &HttpResponse) -> Finished {
        let start_time = req.extensions().get::<StartTime>().unwrap().0;
        metric!(timer("requests.duration") = start_time.elapsed());
        metric!(counter(&format!("responses.status_code.{}", resp.status())) += 1);
        Finished::Done
    }
}

/// Reports certain failures to sentry.
pub struct CaptureSentryError;

impl<S> Middleware<S> for CaptureSentryError {
    fn response(&self, _: &mut HttpRequest<S>, mut resp: HttpResponse) -> Result<Response, Error> {
        // TODO: newer versions of actix will support the backtrace on the actix
        // error.  In that case we want to emit a custom error event to sentry
        // that includes that backtrace (maybe also have a sentry-actix package).
        let mut event_id = None;
        if resp.status().is_server_error() {
            if let Some(error) = resp.error() {
                event_id = Some(capture_fail(error.cause()));
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
        &self,
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
    fn response(&self, _: &mut HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
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
