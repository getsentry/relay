use actix_web::error::Error;
use actix_web::middleware::{Middleware, Response, Started};
use actix_web::{HttpRequest, HttpResponse};
use http::header;
use sentry::integrations::failure::capture_fail;

/// forces the mimetype to json for some cases.
pub struct ForceJson;

impl<S> Middleware<S> for ForceJson {
    fn start(&self, req: &mut HttpRequest<S>) -> Result<Started, Error> {
        req.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        Ok(Started::Done)
    }
}

pub struct CaptureSentryError;

impl<S> Middleware<S> for CaptureSentryError {
    fn response(&self, _req: &mut HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
        if let Some(error) = resp.error() {
            capture_fail(error.cause());
        }

        Ok(Response::Done(resp))
    }
}
