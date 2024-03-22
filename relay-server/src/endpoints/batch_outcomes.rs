use axum::http::StatusCode;
use axum::response::IntoResponse;
use relay_config::EmitOutcomes;

use crate::extractors::SignedJson;
use crate::service::ServiceState;
use crate::services::outcome::{SendOutcomes, SendOutcomesResponse};

pub async fn handle(state: ServiceState, body: SignedJson<SendOutcomes>) -> impl IntoResponse {
    if !body.relay.internal || state.config().emit_outcomes() != EmitOutcomes::AsOutcomes {
        return StatusCode::FORBIDDEN.into_response();
    }

    let producer = &state.outcome_producer();
    for outcome in body.inner.outcomes {
        producer.send(outcome);
    }

    (StatusCode::ACCEPTED, axum::Json(SendOutcomesResponse {})).into_response()
}
