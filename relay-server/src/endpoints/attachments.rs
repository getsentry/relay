use axum::extract::{DefaultBodyLimit, Path, Request};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use multer::{Field, Multipart};
use relay_config::Config;
use relay_event_schema::protocol::EventId;
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
) -> Result<Box<Envelope>, BadStoreRequest> {
    let items = utils::multipart_items(
        multipart,
        state.config(),
        state.outcome_aggregator().clone(),
        &meta,
        AttachmentsAttachmentStrategy,
    )
    .await?;

    let mut envelope = Envelope::from_request(Some(path.event_id), meta);
    for item in items {
        item.accept(|i| envelope.add_item(i));
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
    common::handle_envelope(&state, envelope)
        .await?
        .check_rate_limits()?;
    Ok(StatusCode::CREATED)
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(RequestBodyLimitLayer::new(config.max_attachments_size()))
        .route_layer(DefaultBodyLimit::disable())
}
