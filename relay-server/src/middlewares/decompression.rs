pub use axum::error_handling::HandleErrorLayer;
use axum::http::{header, Request, StatusCode};
use axum::response::IntoResponse;
pub use tower_http::decompression::RequestDecompressionLayer;
use tower_http::BoxError;

use crate::utils::ApiErrorResponse;

/// Map request middleware that removes empty content encoding headers.
///
/// This is to be used along with the [`RequestDecompressionLayer`].
pub fn remove_empty_encoding<B>(mut request: Request<B>) -> Request<B> {
    if let header::Entry::Occupied(entry) = request.headers_mut().entry(header::CONTENT_ENCODING) {
        if should_ignore_encoding(entry.get().as_bytes()) {
            entry.remove();
        }
    }

    request
}

/// Returns `true` if this content-encoding value should be ignored.
fn should_ignore_encoding(value: &[u8]) -> bool {
    // sentry-ruby/5.x sends an empty string
    // sentry.java.android/2.0.0 sends "UTF-8"
    value == b"" || value.eq_ignore_ascii_case(b"utf-8")
}

/// Error function to be used with [`RequestDecompressionLayer`].
///
/// To use decompression in axum, wrap it in a [`HandleErrorLayer`] with this function.
pub async fn decompression_error(error: BoxError) -> impl IntoResponse {
    (
        StatusCode::BAD_REQUEST,
        ApiErrorResponse::from_error(error.as_ref()),
    )
}
