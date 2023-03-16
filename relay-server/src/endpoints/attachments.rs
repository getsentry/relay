use axum::extract::{Multipart, Path};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use relay_general::protocol::EventId;
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::RequestMeta;
use crate::service::ServiceState;
use crate::utils;

#[derive(Debug, Deserialize)]
pub struct AttachmentPath {
    event_id: EventId,
}

async fn extract_envelope(
    meta: RequestMeta,
    path: AttachmentPath,
    multipart: Multipart,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let event_id = path.event_id;

    // let max_payload_size = request.state().config().max_attachments_size();
    let items = utils::multipart_items(multipart).await?;

    let mut envelope = Envelope::from_request(Some(event_id), meta);
    for item in items {
        envelope.add_item(item);
    }
    Ok(envelope)
}

pub async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    Path(path): Path<AttachmentPath>,
    multipart: Multipart,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = extract_envelope(meta, path, multipart).await?;
    common::handle_envelope(&state, envelope).await?;
    Ok(StatusCode::CREATED)
}
