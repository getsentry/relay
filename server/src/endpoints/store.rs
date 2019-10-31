//! Handles event store requests.

use actix::prelude::*;
use actix_web::http::Method;
use actix_web::middleware::cors::Cors;
use actix_web::{HttpRequest, HttpResponse};
use serde::Serialize;

use semaphore_general::protocol::EventId;

use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

// Transparent 1x1 gif
// See http://probablyprogramming.com/2009/03/15/the-tiniest-gif-ever
static PIXEL: &[u8] =
    b"GIF89a\x01\x00\x01\x00\x00\xff\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;";

#[derive(Serialize)]
struct StoreResponse {
    id: EventId,
}

/// Handler for the event store endpoint.
///
/// This simply delegates to `store_event` which does all the work.
/// `handle_store_event` is an adaptor for `store_event` which cannot
/// be used directly as a request handler since not all of its arguments
/// implement the FromRequest trait.
fn store_event(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let is_get_request = request.method() == "GET";

    let future = handle_store_like_request(meta, start_time, request, Ok, move |id| {
        if is_get_request {
            HttpResponse::Ok().content_type("image/gif").body(PIXEL)
        } else {
            HttpResponse::Ok().json(StoreResponse { id })
        }
    });

    Box::new(future)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    // XXX: does not handle the legacy /api/store/ endpoint
    Cors::for_app(app)
        .allowed_methods(vec!["POST"])
        .allowed_headers(vec![
            "x-sentry-auth",
            "x-requested-with",
            "x-forwarded-for",
            "origin",
            "referer",
            "accept",
            "content-type",
            "authentication",
        ])
        .expose_headers(vec!["X-Sentry-Error", "Retry-After"])
        .max_age(3600)
        .resource(r"/api/{project:\d+}/store{trailing_slash:/?}", |r| {
            r.method(Method::POST).with(store_event);
            r.method(Method::GET).with(store_event);
        })
        .register()
}
