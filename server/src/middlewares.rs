use uuid::Uuid;

use actix_web::error::Error;
use actix_web::middleware::{Middleware, Response, Started};
use actix_web::{Body, HttpRequest, HttpResponse, http::header};
use sentry::integrations::failure::capture_fail;

use constants::SERVER;

/// forces the mimetype to json for some cases.
pub struct ForceJson;

impl<S> Middleware<S> for ForceJson {
    fn start(&self, req: &mut HttpRequest<S>) -> Result<Started, Error> {
        let headers = req.headers_mut();

        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            "x-content-type-options",
            header::HeaderValue::from_static("nosniff"),
        );

        Ok(Started::Done)
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

#[derive(Serialize, Debug)]
pub struct ServerError {
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_id: Option<Uuid>,
}

impl<S> Middleware<S> for ErrorHandlers {
    fn response(&self, _: &mut HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
        if (resp.status().is_server_error() || resp.status().is_client_error())
            && resp.body() == &Body::Empty
        {
            let err = ServerError {
                reason: resp.status().canonical_reason(),
                detail: None,
                event_id: resp.headers()
                    .get("x-sentry-event-id")
                    .and_then(|x| x.to_str().ok())
                    .and_then(|x| x.parse().ok()),
            };
            Ok(Response::Done(resp.into_builder().json(err)))
        } else {
            Ok(Response::Done(resp))
        }
    }
}
