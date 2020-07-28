use std::time::Instant;

use actix_web::error::Error;
use actix_web::middleware::{Finished, Middleware, Response, Started};
use actix_web::{http::header, Body, HttpMessage, HttpRequest, HttpResponse};
use futures::prelude::*;

use relay_common::metric;

use crate::constants::SERVER;
use crate::metrics::{RelayCounters, RelayTimers};
use crate::utils::ApiErrorResponse;

/// Basic metrics
pub struct Metrics;

/// The time at which the request started.
#[derive(Clone, Copy, Debug)]
pub struct StartTime(Instant);

impl StartTime {
    /// Returns the `Instant` of this start time.
    #[inline]
    pub fn into_inner(self) -> Instant {
        self.0
    }
}

impl<S> Middleware<S> for Metrics {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started, Error> {
        req.extensions_mut().insert(StartTime(Instant::now()));
        metric!(
            counter(RelayCounters::Requests) += 1,
            route = req.resource().name(),
            method = req.method().as_str()
        );
        Ok(Started::Done)
    }

    fn finish(&self, req: &HttpRequest<S>, resp: &HttpResponse) -> Finished {
        let start_time = req.extensions().get::<StartTime>().unwrap().0;

        metric!(
            timer(RelayTimers::RequestsDuration) = start_time.elapsed(),
            route = req.resource().name(),
            method = req.method().as_str()
        );
        metric!(
            counter(RelayCounters::ResponsesStatusCodes) += 1,
            status_code = &resp.status().as_str(),
            route = req.resource().name(),
            method = req.method().as_str()
        );

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

/// Read request before returning response. This is not required for RFC compliance, however:
///
/// * a lot of clients are not able to deal with unread request payloads
/// * in HTTP over unix domain sockets, not reading the request payload might cause the client's
///   write() to block forever due to a filled up socket buffer
pub struct ReadRequestMiddleware;

impl<S> Middleware<S> for ReadRequestMiddleware {
    fn response(&self, req: &HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
        let future = req
            .payload()
            .for_each(|_| Ok(()))
            .map(|_| resp)
            .map_err(Error::from);

        Ok(Response::Future(Box::new(future)))
    }
}
