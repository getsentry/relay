use axum::http::StatusCode;
use axum::response::IntoResponse;
use tower_http::BoxError;

use crate::utils::ApiErrorResponse;

pub async fn decompression_error(error: BoxError) -> impl IntoResponse {
    (
        StatusCode::BAD_REQUEST,
        ApiErrorResponse::from_error(error.as_ref()),
    )
}
