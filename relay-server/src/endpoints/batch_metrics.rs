use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::extractors::{SignedBytes, StartTime};
use crate::service::ServiceState;
use crate::services::processor::ProcessBatchedMetrics;

#[derive(Debug, Serialize, Deserialize)]
struct SendMetricsResponse {}

pub async fn handle(
    state: ServiceState,
    start_time: StartTime,
    body: SignedBytes,
) -> impl IntoResponse {
    if !body.relay.internal {
        return StatusCode::FORBIDDEN.into_response();
    }

    state.processor().send(ProcessBatchedMetrics {
        payload: body.body,
        start_time: start_time.into_inner(),
        sent_at: None,
    });

    (StatusCode::ACCEPTED, axum::Json(SendMetricsResponse {})).into_response()
}
