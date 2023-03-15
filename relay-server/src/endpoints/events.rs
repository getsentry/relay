//! Returns captured events.

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Result};
use axum::routing::get;
use axum::Router;
use relay_general::protocol::EventId;

use crate::actors::test_store::{GetCapturedEnvelope, TestStore};
use crate::envelope;

async fn get_captured_event(Path(event_id): Path<EventId>) -> Result<impl IntoResponse> {
    let envelope_opt = TestStore::from_registry()
        .send(GetCapturedEnvelope { event_id })
        .await
        .map_err(|_| ())?; // TODO(ja): Proper error handler

    Ok(match envelope_opt {
        Some(Ok(envelope)) => {
            let headers = [(header::CONTENT_TYPE, envelope::CONTENT_TYPE)];
            (StatusCode::OK, headers, envelope.to_vec().unwrap()).into_response()
        }
        Some(Err(error)) => (StatusCode::BAD_REQUEST, error).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    })
}

pub fn routes<S>() -> Router<S> {
    // r.name("internal-events");
    Router::new()
        .route("/api/relay/events/:event_id/", get(get_captured_event))
        .with_state(())
}
