// #![allow(deprecated)]

// use std::borrow::Cow;
// use std::cell::RefCell;
// use std::sync::{Arc, Mutex};
// use std::time::Instant;

// use actix_web::error::Error;
// use actix_web::http::header;
// use actix_web::middleware::{Finished, Middleware, Response, Started};
// use actix_web::{Body, HttpMessage, HttpRequest, HttpResponse};
// use failure::Fail;
// use futures::TryFutureExt;
// use relay_log::_sentry::integrations::backtrace::parse_stacktrace;
// use relay_log::_sentry::types::Uuid;
// use relay_log::_sentry::{parse_type_from_debug, Hub, Level, ScopeGuard};
// use relay_log::protocol::{ClientSdkPackage, Event, Exception, Request};
// use relay_statsd::metric;

// use crate::extractors::SharedPayload;
// use crate::statsd::{RelayCounters, RelayTimers};
// use crate::utils::ApiErrorResponse;

use axum::http::{header, Request};
use axum::middleware::Next;
use axum::response::Response;

use crate::constants::SERVER;
use crate::extractors::StartTime;
use crate::statsd::{RelayCounters, RelayTimers};

/// A middleware that logs web request timings as statsd metrics.
///
/// Use this with [`axum::middleware::from_fn`].
pub async fn metrics<B>(mut request: Request<B>, next: Next<B>) -> Response {
    let start_time = StartTime::now();
    request.extensions_mut().insert(start_time);

    // TODO(ja): Resource name
    let route = "";
    let method = request.method().clone();

    relay_statsd::metric!(
        counter(RelayCounters::Requests) += 1,
        route = route,
        method = method.as_str(),
    );

    let response = next.run(request).await;

    relay_statsd::metric!(
        timer(RelayTimers::RequestsDuration) = start_time.into_inner().elapsed(),
        route = route,
        method = method.as_str(),
    );
    relay_statsd::metric!(
        counter(RelayCounters::ResponsesStatusCodes) += 1,
        status_code = response.status().as_str(),
        route = route,
        method = method.as_str(),
    );

    response
}

/// Middleware function that adds common response headers.
///
/// Use this with [`axum::middleware::from_fn`].
pub async fn common_headers<B>(request: Request<B>, next: Next<B>) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::SERVER, header::HeaderValue::from_static(SERVER));
    response
}

// pub struct AddCommonHeaders;

// impl<S> Middleware<S> for AddCommonHeaders {
//     fn response(
//         &self,
//         _request: &HttpRequest<S>,
//         mut response: HttpResponse,
//     ) -> Result<Response, Error> {
//         response
//             .headers_mut()
//             .insert(header::SERVER, header::HeaderValue::from_static(SERVER));

//         Ok(Response::Done(response))
//     }
// }

// /// Registers the default error handlers.
// pub struct ErrorHandlers;

// impl<S> Middleware<S> for ErrorHandlers {
//     fn response(&self, _: &HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
//         if (resp.status().is_server_error() || resp.status().is_client_error())
//             && resp.body() == &Body::Empty
//         {
//             let reason = resp.status().canonical_reason().unwrap_or("unknown error");
//             Ok(Response::Done(
//                 resp.into_builder()
//                     .json(ApiErrorResponse::with_detail(reason)),
//             ))
//         } else {
//             Ok(Response::Done(resp))
//         }
//     }
// }

// TODO(ja): Check this. Is there a test?
// /// Read request before returning response. This is not required for RFC compliance, however:
// ///
// /// * a lot of clients are not able to deal with unread request payloads
// /// * in HTTP over unix domain sockets, not reading the request payload might cause the client's
// ///   write() to block forever due to a filled up socket buffer
// pub struct ReadRequestMiddleware;

// impl<S> Middleware<S> for ReadRequestMiddleware {
//     fn response(&self, req: &HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
//         let payload = SharedPayload::get(req);
//         let future = async move {
//             payload.consume().await;
//             Ok::<_, actix_web::Error>(resp)
//         };

//         Ok(Response::Future(Box::new(Box::pin(future).compat())))
//     }
// }

pub async fn normalize_uri<B>(request: Request<B>, next: Next<B>) -> Response {
    // TODO(ja): Normalize the URI. See common module.
    // https://docs.rs/tower-http/latest/tower_http/normalize_path/index.html
    todo!()
}
