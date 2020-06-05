use actix_web::{HttpRequest, HttpResponse};

use crate::service::ServiceState;

/// An endpoint function that always responds with `404 Not Found`.
///
/// This function has a signature compatible with `App::handler` and `Resource::f`.
pub fn not_found(_: &HttpRequest<ServiceState>) -> HttpResponse {
    HttpResponse::NotFound().finish()
}
