use http::header;
use actix_web::HttpRequest;
use actix_web::middleware::{Middleware, Started};
use actix_web::error::Error;

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
