//! A simple health check endpoint for the relay.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;

use crate::service::ServiceState;
use crate::services::health_check::{IsHealthy, Status as HealthStatus};

#[derive(Serialize)]
struct Status {
    is_healthy: bool,
}

const UNHEALTHY: (hyper::StatusCode, axum::Json<Status>) = (
    StatusCode::SERVICE_UNAVAILABLE,
    axum::Json(Status { is_healthy: false }),
);

pub async fn handle(state: ServiceState, Path(kind): Path<IsHealthy>) -> impl IntoResponse {
    if state.has_crashed() {
        return UNHEALTHY;
    }
    match state.health_check().send(kind).await {
        Ok(HealthStatus::Healthy) => (StatusCode::OK, axum::Json(Status { is_healthy: true })),
        _ => UNHEALTHY,
    }
}

pub async fn handle_live(state: ServiceState) -> impl IntoResponse {
    handle(state, Path(IsHealthy::Liveness)).await
}
