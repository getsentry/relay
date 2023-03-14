use axum::http::StatusCode;
use axum::response::IntoResponse;

/// An endpoint function that always responds with `404 Not Found`.
pub fn not_found() -> impl IntoResponse {
    StatusCode::NOT_FOUND
}
