//! Implements the PlayStation crash uploading endpoint.
//!
//! Crashes are received as multipart uploads in this [format](https://game.develop.playstation.net/resources/documents/SDK/12.000/Core_Dump_System-Overview/ps5-core-dump-file-set-sending-format.html).
use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use http::StatusCode;
use multer::{Field, Multipart};
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;
use relay_quotas::{DataCategory, Scoping};
use relay_system::Addr;
use serde::Serialize;
use tower_http::limit::RequestBodyLimitLayer;

use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::ContentType::OctetStream;
use crate::envelope::{AttachmentType, Envelope, Item};
use crate::extractors::{RawContentType, RequestMeta};
use crate::middlewares;
use crate::service::ServiceState;
use crate::services::outcome::DiscardReason;
use crate::services::projects::cache::Project;
use crate::services::upload::Upload;
use crate::utils::{self, AttachmentStrategy};

/// The extension of a prosperodump in the multipart form-data upload.
const PROSPERODUMP_EXTENSION: &str = ".prosperodmp";

/// Prosperodump attachments should have these magic bytes.
const PROSPERODUMP_MAGIC_HEADER: &[u8] = b"\x7FELF";

/// Magic bytes for lz4 compressed prosperodump containers.
const LZ4_MAGIC_HEADER: &[u8] = b"\x04\x22\x4d\x18";

/// Content type of the data request payload.
const DATA_REQUEST_CONTENT_TYPE: &str = "application/vnd.sce.crs.datareq-request+json";

/// Files that are currently supported for uploading.
const UPLOAD_PARTS: &[&str] = &["corefile", "screenshot"];

/// Implementation of the `getRequestCoreDumpData` [response](https://game.develop.playstation.net/resources/documents/SDK/11.000/Core_Dump_Data_Request_API-Reference/0002.html).
///
/// The optional `partParameters` section is omitted as we make no use of it. This is because any
/// parameter specified there only applies to on demand generated videos. So it can't be used to
/// truncate videos that are already present on the device.
#[derive(Serialize)]
struct DataRequestResponse {
    parts: Parts,
}

/// Partial implementation of the `parts` section in the `getRequestCoreDumpData` response.
///
/// The optional `additional` and `no_upload` sections are currently omitted as they are not used.
#[derive(Serialize)]
struct Parts {
    /// List of files that can be uploaded.
    ///
    /// Note: It is not guaranteed that all of these will be uploaded every time. Rather, they are
    /// only uploaded if already present on the device. To force the device to generate a specific
    /// file it needs to be added to the `additional` section.
    upload: &'static [&'static str],
}

struct PlaystationAttachmentStrategy<'a> {
    upload_service: Option<&'a Addr<Upload>>,
    scoping: Scoping,
}

impl<'a> AttachmentStrategy for PlaystationAttachmentStrategy<'a> {
    fn infer_type(&self, field: &Field) -> AttachmentType {
        if field
            .file_name()
            .is_some_and(|f| f.ends_with(PROSPERODUMP_EXTENSION))
        {
            AttachmentType::Prosperodump
        } else {
            AttachmentType::Attachment
        }
    }

    async fn add_to_item(
        &self,
        field: Field<'static>,
        item: Item,
        config: &Config,
    ) -> Result<Option<Item>, multer::Error> {
        match self.upload_service {
            Some(upload) if self.infer_type(&field) != AttachmentType::Prosperodump => {
                Ok(common::upload_to_objectstore(
                    field,
                    item,
                    config,
                    self.scoping,
                    upload,
                    "playstation",
                )
                .await)
            }
            _ => utils::read_attachment_bytes_into_item(field, item, config, true).await,
        }
    }
}

fn create_data_request_response() -> DataRequestResponse {
    DataRequestResponse {
        parts: Parts {
            upload: UPLOAD_PARTS,
        },
    }
}

fn validate_prosperodump(data: &[u8]) -> Result<(), BadStoreRequest> {
    if !data.starts_with(LZ4_MAGIC_HEADER) && !data.starts_with(PROSPERODUMP_MAGIC_HEADER) {
        relay_log::trace!("invalid prosperodump file");
        return Err(BadStoreRequest::InvalidProsperodump);
    }

    Ok(())
}

async fn extract_multipart(
    multipart: Multipart<'static>,
    meta: RequestMeta,
    state: &ServiceState,
    project: &Project<'_>,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let project_config = project
        .state()
        .clone()
        .enabled()
        .ok_or(BadStoreRequest::EventRejected(DiscardReason::ProjectId))?;
    let scoping = project_config
        .scoping(meta.public_key())
        .ok_or(BadStoreRequest::EventRejected(DiscardReason::ProjectId))?;
    let attachment_rate_limits = || {
        project.rate_limits().current_limits().check_with_quotas(
            project_config.get_quotas(),
            scoping.item(DataCategory::Attachment),
        )
    };
    let upload_service = (project_config.has_feature(Feature::PlaystationUploads)
        && !attachment_rate_limits().is_limited())
    .then_some(state.upload());
    let attachment_strategy = PlaystationAttachmentStrategy {
        scoping,
        upload_service,
    };
    let mut items = utils::multipart_items(multipart, state.config(), attachment_strategy).await?;

    let prosperodump_item = items
        .iter_mut()
        .find(|item| item.attachment_type() == Some(AttachmentType::Prosperodump))
        .ok_or(BadStoreRequest::MissingProsperodump)?;

    prosperodump_item.set_payload(OctetStream, prosperodump_item.payload());

    validate_prosperodump(&prosperodump_item.payload())?;

    let event_id = common::event_id_from_items(&items)?.unwrap_or_else(EventId::new);
    let mut envelope = Envelope::from_request(Some(event_id), meta);

    for item in items {
        envelope.add_item(item);
    }

    Ok(envelope)
}

async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    content_type: RawContentType,
    request: Request,
) -> axum::response::Result<impl IntoResponse> {
    // Handle either a data request (send as json) or a crash dump (send as a multi-part)
    if content_type.as_ref().contains(DATA_REQUEST_CONTENT_TYPE) {
        return Ok(axum::Json(create_data_request_response()).into_response());
    }

    let config = state.config();
    let project = state
        .project_cache_handle()
        .ready(meta.public_key(), config.query_timeout())
        .await
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let multipart = utils::multipart_from_request(request)?;
    let mut envelope = extract_multipart(multipart, meta, &state, &project).await?;
    envelope.require_feature(Feature::PlaystationIngestion);

    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    common::handle_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();

    // Return here needs to be a 200 with arbitrary text to make the sender happy.
    Ok(TextResponse(id).into_response())
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(RequestBodyLimitLayer::new(
            config.max_upload_size() + config.max_attachments_size(),
        ))
        .route_layer(DefaultBodyLimit::disable())
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}
