use axum::extract::{DefaultBodyLimit, Path, Request};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use multer::{Field, Multipart};
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use relay_quotas::DataCategory;
use serde::Deserialize;
use tower_http::limit::RequestBodyLimitLayer;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{AttachmentType, Envelope, Item};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::service::ServiceState;
use crate::utils::{self, AttachmentStrategy, read_attachment_bytes_into_item};

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
        item: Managed<Item>,
        config: &Config,
    ) -> impl Future<Output = Result<Option<Managed<Item>>, multer::Error>> + Send {
        read_attachment_bytes_into_item(field, item, config, false)
    }
}

async fn multipart_to_envelope(
    meta: RequestMeta,
    path: AttachmentPath,
    multipart: Multipart<'static>,
    state: &ServiceState,
) -> Result<Managed<Box<Envelope>>, BadStoreRequest> {
    let items = utils::multipart_items(
        multipart,
        state.config(),
        AttachmentsAttachmentStrategy,
        &meta,
        state.outcome_aggregator(),
    )
    .await?;

    let envelope = Envelope::from_request(Some(path.event_id), meta);
    let mut envelope = Managed::from_envelope(envelope, state.outcome_aggregator().clone());
    let mut has_event = false;
    for item in items {
        envelope.merge_with(item, |envelope, item, records| {
            if !has_event && item.creates_event() {
                records.modify_by(DataCategory::Error, 1);
                has_event = true;
            }
            envelope.add_item(item);
        });
    }

    Ok(envelope)
}

pub async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    Path(path): Path<AttachmentPath>,
    request: Request,
) -> axum::response::Result<impl IntoResponse> {
    let multipart = utils::multipart_from_request(request)?;
    let envelope = multipart_to_envelope(meta, path, multipart, &state).await?;
    common::handle_managed_envelope(&state, envelope)
        .await?
        .check_rate_limits()?;
    Ok(StatusCode::CREATED)
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(RequestBodyLimitLayer::new(config.max_attachments_size()))
        .route_layer(DefaultBodyLimit::disable())
}
