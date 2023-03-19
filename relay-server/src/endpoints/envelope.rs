//! Handles envelope store requests.

use axum::extract::FromRequest;
use axum::http::Request;
use axum::response::IntoResponse;
use axum::{Json, RequestExt};
use bytes::Bytes;
use relay_general::protocol::EventId;
use serde::Serialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::{BadEventMeta, PartialMeta, RequestMeta};
use crate::service::ServiceState;

#[derive(Debug)]
pub struct EnvelopeParams {
    meta: RequestMeta,
    body: Bytes,
}

impl EnvelopeParams {
    fn extract_envelope(self) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self { meta, body } = self;
        // let max_payload_size = request.state().config().max_envelope_size();
        // let data = body::store_body(request, max_payload_size).await?;

        if body.is_empty() {
            return Err(BadStoreRequest::EmptyBody);
        }

        Envelope::parse_request(body, meta).map_err(BadStoreRequest::InvalidEnvelope)
    }
}

#[axum::async_trait]
impl<B> FromRequest<ServiceState, B> for EnvelopeParams
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
{
    type Rejection = BadEventMeta;

    async fn from_request(
        mut request: Request<B>,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let result = request.extract_parts_with_state(state).await;

        if !matches!(result, Err(BadEventMeta::MissingAuth)) {
            return Ok(Self {
                meta: result?,
                body: request.extract().await.unwrap(),
            });
        }

        // TODO(ja): Improve PartialMeta / RequestMeta to parse Auth and meta separately, then merge
        // into RequestMeta.
        let partial_meta = request.extract_parts::<PartialMeta>().await?;
        let body: Bytes = request.extract().await.unwrap();

        let line = body
            .splitn(2, |b| *b == b'\n')
            .next()
            .ok_or(BadEventMeta::MissingAuth)?;

        let request_meta = serde_json::from_slice(line).map_err(BadEventMeta::BadEnvelopeAuth)?;

        Ok(Self {
            meta: partial_meta.copy_to(request_meta),
            body,
        })
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
