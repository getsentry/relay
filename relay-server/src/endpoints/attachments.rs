use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use multer::Field;
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{AttachmentType, Envelope, Item};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;
use crate::utils::{AttachmentStrategy, ConstrainedMultipart, read_attachment_bytes_into_item};

#[derive(Debug, Deserialize)]
pub struct AttachmentPath {
    event_id: EventId,
}

struct AttachmentsAttachmentStrategy;

impl AttachmentStrategy for AttachmentsAttachmentStrategy {
    fn infer_type(&self, _: &Field) -> AttachmentType {
        AttachmentType::default()
    }

    fn add_to_item(
        &self,
        field: Field<'static>,
        item: Item,
        config: &Config,
    ) -> impl Future<Output = Result<Option<Item>, multer::Error>> + Send {
        read_attachment_bytes_into_item(field, item, config)
    }
}

async fn extract_envelope(
    meta: RequestMeta,
    path: AttachmentPath,
    multipart: ConstrainedMultipart,
    config: &Config,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let items = multipart
        .items(config, AttachmentsAttachmentStrategy)
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
