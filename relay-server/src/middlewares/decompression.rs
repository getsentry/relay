pub use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::response::IntoResponse;
pub use tower_http::decompression::RequestDecompressionLayer;
use tower_http::BoxError;

use crate::utils::ApiErrorResponse;

/// Error function to be used with [`RequestDecompressionLayer`].
///
/// To use decompression in axum, wrap it in a [`HandleErrorLayer`] with this function.
pub async fn decompression_error(error: BoxError) -> impl IntoResponse {
    (
        StatusCode::BAD_REQUEST,
        ApiErrorResponse::from_error(error.as_ref()),
    )
}
