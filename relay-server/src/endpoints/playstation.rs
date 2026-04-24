//! Implements the PlayStation crash uploading endpoint.
//!
//! Crashes are received as multipart uploads in this [format](https://game.develop.playstation.net/resources/documents/SDK/12.000/Core_Dump_System-Overview/ps5-core-dump-file-set-sending-format.html).
use std::io;

use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use futures::stream::BoxStream;
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
use crate::envelope::ContentType;
use crate::envelope::{AttachmentPlaceholder, AttachmentType, Envelope, Item};
use crate::extractors::{RawContentType, RequestMeta};
use crate::managed::{self, Managed};
use crate::middlewares;
use crate::service::ServiceState;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::projects::cache::Project;
use crate::services::upload::{Create, Stream, Upload};
use crate::utils::{self, MeteredStream};
use crate::utils::{AttachmentStrategy, BoundedStream};

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
        mut item: Managed<Item>,
        config: &Config,
    ) -> Result<Option<Managed<Item>>, multer::Error> {
        let upload = match self.upload_service {
            Some(upload) if self.infer_type(&field) != AttachmentType::Prosperodump => upload,
            _ => return utils::read_attachment_bytes_into_item(field, item, config, true).await,
        };
        let content_type = field.content_type().cloned();
        let stream: BoxStream<'static, io::Result<Bytes>> =
            Box::pin(field.map_err(io::Error::other));
        let stream = MeteredStream::new(stream, "playstation");
        let stream = BoundedStream::new(stream, 1, config.max_upload_size());
        let byte_counter = stream.byte_counter();
        let Ok(Ok(location)) = upload
            .send(Create {
                scoping: self.scoping,
                length: None,
            })
            .await
        else {
            return Ok(None);
        };
        let result = upload
            .send(Stream {
                received: Utc::now(),
                scoping: self.scoping,
                location,
                stream,
            })
            .await;
        if let Ok(result) = result
            && let Ok(location) = result.inspect_err(|e| {
                relay_log::warn!(error = e as &dyn std::error::Error, "Upload failed");
            })
            && let Ok(location) = location.into_header_value()
            && let Ok(location) = location.to_str()
            && let Ok(placeholder) = serde_json::to_vec(&AttachmentPlaceholder {
                location,
                content_type: content_type.as_ref().map(|c| c.to_string()),
            })
        {
            let attachment_size = byte_counter.get();
            item.set_attachment_ref_placeholder(placeholder, attachment_size);
            Ok(Some(item))
        } else {
            Ok(None)
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

async fn multipart_to_envelope(
    multipart: Multipart<'static>,
    meta: RequestMeta,
    project: &Project<'_>,
    state: &ServiceState,
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
    let mut items = utils::multipart_items(
        multipart,
        state.config(),
        state.outcome_aggregator().clone(),
        &meta,
        attachment_strategy,
    )
    .await?;

    let Some(prosperodump_item) = items
        .iter_mut()
        .find(|item| item.attachment_type() == Some(AttachmentType::Prosperodump))
    else {
        managed::reject_all(
            &items,
            Outcome::Invalid(DiscardReason::MissingProsperodumpUpload),
        );
        return Err(BadStoreRequest::MissingProsperodump);
    };
    prosperodump_item
        .set_attachment_payload(Some(ContentType::OctetStream), prosperodump_item.payload());

    validate_prosperodump(&prosperodump_item.payload()).inspect_err(|_| {
        managed::reject_all(&items, Outcome::Invalid(DiscardReason::InvalidProsperodump));
    })?;

    let event_id = common::event_id_from_items(&items)?.unwrap_or_else(EventId::new);
    let mut envelope = Envelope::from_request(Some(event_id), meta);
    for item in items {
        item.accept(|i| envelope.add_item(i));
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
    let mut envelope = multipart_to_envelope(multipart, meta, &project, &state).await?;
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
