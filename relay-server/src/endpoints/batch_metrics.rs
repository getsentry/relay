use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::extractors::{ReceivedAt, SignedBytes};
use crate::service::ServiceState;
use crate::services::processor::{BucketSource, ProcessBatchedMetrics};

#[derive(Debug, Serialize, Deserialize)]
struct SendMetricsResponse {}

pub async fn handle(
    state: ServiceState,
    ReceivedAt(received_at): ReceivedAt,
    body: SignedBytes,
) -> impl IntoResponse {
    if !body.relay.internal {
        return StatusCode::FORBIDDEN.into_response();
    }

    state.processor().send(ProcessBatchedMetrics {
        payload: body.body,
        source: BucketSource::Internal,
        received_at,
        sent_at: None,
    });

    (StatusCode::ACCEPTED, axum::Json(SendMetricsResponse {})).into_response()
}
