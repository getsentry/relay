//! Handles envelope store requests.

use axum::extract::FromRequest;
use axum::response::IntoResponse;
use axum::Json;
use bytes::Bytes;
use relay_general::protocol::EventId;
use serde::Serialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::EnvelopeMeta;
use crate::service::ServiceState;

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
pub struct EnvelopeParams {
    meta: EnvelopeMeta,
    body: Bytes,
}

impl EnvelopeParams {
    fn extract_envelope(self) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self { body, meta } = self;
        // let max_payload_size = request.state().config().max_envelope_size();
        // let data = body::store_body(request, max_payload_size).await?;

        if body.is_empty() {
            return Err(BadStoreRequest::EmptyBody);
        }

        Envelope::parse_request(body, meta.into_inner()).map_err(BadStoreRequest::InvalidEnvelope)
    }
}

#[derive(Serialize)]
struct StoreResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<EventId>,
}

/// Handler for the envelope store endpoint.
pub async fn handle(
    state: ServiceState,
    params: EnvelopeParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = params.extract_envelope()?;
    let id = common::handle_envelope(&state, envelope).await?;
    Ok(Json(StoreResponse { id }))
}
