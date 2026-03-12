use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{AttachmentType, Envelope};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;
use crate::utils::ConstrainedMultipart;

#[derive(Debug, Deserialize)]
pub struct AttachmentPath {
    event_id: EventId,
}

async fn extract_envelope(
    meta: RequestMeta,
    path: AttachmentPath,
    multipart: ConstrainedMultipart,
    config: &Config,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let items = multipart
        .items(|_, _| AttachmentType::default(), config)
        .await?;

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
    multipart: ConstrainedMultipart,
) -> axum::response::Result<impl IntoResponse> {
    let envelope = extract_envelope(meta, path, multipart, state.config()).await?;
    common::handle_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();
    Ok(StatusCode::CREATED)
}
