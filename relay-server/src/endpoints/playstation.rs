use std::io;

use axum::RequestExt;
use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use http::StatusCode;
use multer::Field;
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;
use serde::Serialize;

use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::ContentType::{self, OctetStream};
use crate::envelope::{AttachmentType, Envelope, Item};
use crate::extractors::{RawContentType, RequestMeta};
use crate::middlewares;
use crate::service::ServiceState;
use crate::services::projects::project::ProjectInfo;
use crate::services::upload::Stream;
use crate::utils::{
    AttachmentStrategy, BoundedStream, UnconstrainedMultipart, read_attachment_bytes_into_item,
};

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
    state: &'a ServiceState,
    request_meta: &'a RequestMeta,
    project_config: &'a ProjectInfo,
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
        mut item: Item,
        config: &Config,
    ) -> Result<Option<Item>, multer::Error> {
        let attachment_type = self.infer_type(&field);
        if self.project_config.has_feature(Feature::PlaystationUploads)
            && attachment_type != AttachmentType::Prosperodump
        {
            let content_type = field.content_type().cloned();
            let stream: BoxStream<'static, io::Result<Bytes>> =
                Box::pin(field.map_err(io::Error::other));
            let stream = BoundedStream::new(stream, 1, config.max_upload_size());
            let byte_counter = stream.byte_counter();
            let stream = Stream {
                scoping: self
                    .project_config
                    .scoping(self.request_meta.public_key())
                    .unwrap_or_else(|| self.request_meta.get_partial_scoping().into_scoping()),
                stream,
            };
            let result = self.state.upload().send(stream).await;
            if let Ok(result) = result
                && let Ok(location) = result.inspect_err(|e| {
                    relay_log::warn!(error = e as &dyn std::error::Error, "upload failed");
                })
                && let Ok(location) = location.into_header_value()
                && let Ok(location) = location.to_str()
            {
                let mut payload = serde_json::json!({"location": location});
                if let Some(content_type) = content_type {
                    payload["content_type"] = serde_json::Value::String(content_type.to_string());
                }
                let payload = payload.to_string().into_bytes();
                item.set_payload(ContentType::AttachmentRef, payload);
                item.set_attachment_length(byte_counter.get());
                Ok(Some(item))
            } else {
                Ok(None)
            }
        } else {
            read_attachment_bytes_into_item(field, item, config).await
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
    multipart: UnconstrainedMultipart,
    meta: RequestMeta,
    state: &ServiceState,
    project_config: &ProjectInfo,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let attachment_strategy = PlaystationAttachmentStrategy {
        state,
        request_meta: &meta,
        project_config,
    };
    let mut items = multipart.items(state.config(), attachment_strategy).await?;

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

    let project_config = state
        .project_cache_handle()
        .ready(meta.public_key(), state.config().query_timeout())
        .await
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?
        .state()
        .clone()
        .enabled()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let multipart = request.extract_with_state(&state).await?;
    let mut envelope = extract_multipart(multipart, meta, &state, &project_config).await?;
    envelope.require_feature(Feature::PlaystationIngestion);

    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    match common::handle_envelope(&state, envelope)
        .await
        .map_err(|err| err.into_inner())
    {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error.into()),
    };

    // Return here needs to be a 200 with arbitrary text to make the sender happy.
    Ok(TextResponse(id).into_response())
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}
