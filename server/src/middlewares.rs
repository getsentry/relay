use std::time::Instant;

use actix_web::error::Error;
use actix_web::middleware::{Finished, Middleware, Response, Started};
use actix_web::{http::header, Body, HttpRequest, HttpResponse};

use crate::constants::SERVER;
use crate::utils::ApiErrorResponse;

/// Basic metrics
pub struct Metrics;

struct StartTime(Instant);

impl<S> Middleware<S> for Metrics {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started, Error> {
        req.extensions_mut().insert(StartTime(Instant::now()));
        Ok(Started::Done)
    }

    fn finish(&self, req: &HttpRequest<S>, resp: &HttpResponse) -> Finished {
        let start_time = req.extensions().get::<StartTime>().unwrap().0;
        metric!(timer("requests.duration") = start_time.elapsed());
        metric!(counter(&format!("responses.status_code.{}", resp.status())) += 1);
        Finished::Done
    }
}

/// Adds the common relay headers.
pub struct AddCommonHeaders;

impl<S> Middleware<S> for AddCommonHeaders {
    fn response(
        &self,
        _request: &HttpRequest<S>,
        mut response: HttpResponse,
    ) -> Result<Response, Error> {
        response
            .headers_mut()
            .insert(header::SERVER, header::HeaderValue::from_static(SERVER));

        Ok(Response::Done(response))
    }
}

/// Registers the default error handlers.
pub struct ErrorHandlers;

impl<S> Middleware<S> for ErrorHandlers {
    fn response(&self, _: &HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
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
