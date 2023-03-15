use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use relay_config::EmitOutcomes;

use crate::actors::outcome::{OutcomeProducer, SendOutcomes, SendOutcomesResponse};
use crate::extractors::SignedJson;
use crate::service::ServiceState;

async fn send_outcomes(state: ServiceState, body: SignedJson<SendOutcomes>) -> impl IntoResponse {
    if !body.relay.internal || state.config().emit_outcomes() != EmitOutcomes::AsOutcomes {
        return StatusCode::FORBIDDEN.into_response();
    }

    let producer = OutcomeProducer::from_registry();
    for outcome in body.inner.outcomes {
        producer.send(outcome);
    }

    (StatusCode::ACCEPTED, Json(SendOutcomesResponse {})).into_response()
}

pub fn routes() -> Router<ServiceState> {
    // r.name("relay-outcomes");
    Router::new().route("/api/0/relays/outcomes/", post(send_outcomes))
}
