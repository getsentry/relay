//! A simple health check endpoint for the relay.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;

use crate::actors::health_check::{HealthCheck, IsHealthy};

#[derive(Serialize)]
struct Status {
    is_healthy: bool,
}

pub async fn handle(Path(kind): Path<IsHealthy>) -> impl IntoResponse {
    match HealthCheck::from_registry().send(kind).await {
        Ok(true) => (StatusCode::OK, axum::Json(Status { is_healthy: true })),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(Status { is_healthy: false }),
        ),
    }
}
