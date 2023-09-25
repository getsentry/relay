//! Returns captured events.

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use relay_event_schema::protocol::EventId;

use crate::actors::test_store::GetCapturedEnvelope;
use crate::endpoints::common::ServiceUnavailable;
use crate::envelope;
use crate::service::ServiceState;

pub async fn handle(
    state: ServiceState,
    Path(event_id): Path<EventId>,
) -> Result<impl IntoResponse, ServiceUnavailable> {
    let envelope_opt = state
        .test_store()
        .send(GetCapturedEnvelope { event_id })
        .await?;

    Ok(match envelope_opt {
        Some(Ok(envelope)) => {
            let headers = [(header::CONTENT_TYPE, envelope::CONTENT_TYPE)];
            (StatusCode::OK, headers, envelope.to_vec().unwrap()).into_response()
        }
        Some(Err(error)) => (StatusCode::BAD_REQUEST, error).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    })
}
