use std::any::Any;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
pub use tower_http::catch_panic::CatchPanicLayer;

use crate::utils::ApiErrorResponse;

/// Handler function for the [`CatchPanicLayer`] middleware.
pub fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response {
    let detail = if let Some(s) = err.downcast_ref::<String>() {
        s.as_str()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s
    } else {
        "no error details"
    };

    relay_log::error!("panic in web handler: {detail}");

    let response = (
        StatusCode::INTERNAL_SERVER_ERROR,
        ApiErrorResponse::with_detail(detail),
    );

    response.into_response()
}
