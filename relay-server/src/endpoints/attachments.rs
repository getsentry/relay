use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use multer::Multipart;
use relay_event_schema::protocol::EventId;
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{AttachmentType, Envelope};
use crate::extractors::{Remote, RequestMeta};
use crate::service::ServiceState;
use crate::utils;

#[derive(Debug, Deserialize)]
pub struct AttachmentPath {
    event_id: EventId,
}

async fn extract_envelope(
    meta: RequestMeta,
    path: AttachmentPath,
    multipart: Multipart<'static>,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let items = utils::multipart_items(multipart, |_| AttachmentType::default()).await?;

    let mut envelope = Envelope::from_request(Some(path.event_id), meta);
    for item in items {
        envelope.add_item(item);
    }

    Ok(envelope)
}

pub async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    Path(path): Path<AttachmentPath>,
    Remote(multipart): Remote<Multipart<'static>>,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = extract_envelope(meta, path, multipart).await?;
    common::handle_envelope(&state, envelope).await?;
    Ok(StatusCode::CREATED)
}
