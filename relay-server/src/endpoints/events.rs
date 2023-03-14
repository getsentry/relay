//! Returns captured events.

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{Response, Result};
use axum::routing::get;
use axum::Router;
use relay_general::protocol::EventId;

use crate::actors::test_store::{GetCapturedEnvelope, TestStore};
use crate::envelope;

async fn get_captured_event(Path(event_id): Path<EventId>) -> Result<Response> {
    let envelope_opt = TestStore::from_registry()
        .send(GetCapturedEnvelope { event_id })
        .await?;

    let response = match envelope_opt {
        Some(Ok(envelope)) => Response::builder()
            .header(header::CONTENT_TYPE, envelope::CONTENT_TYPE)
            .body(envelope.to_vec().unwrap())?,
        Some(Err(error)) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(error)?,
        None => Response::builder().status(StatusCode::NOT_FOUND).body(())?,
    };

    Ok(response)
}

pub fn routes() -> Router {
    // r.name("internal-events");
    Router::new().route("/api/relay/events/:event_id/", get(get_captured_event))
}
