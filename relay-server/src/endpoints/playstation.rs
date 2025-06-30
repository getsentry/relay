use axum::RequestExt;
use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;
use serde::Serialize;

use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::ContentType::OctetStream;
use crate::envelope::{AttachmentType, Envelope};
use crate::extractors::{RawContentType, RequestMeta};
use crate::service::ServiceState;
use crate::utils::UnconstrainedMultipart;

/// The extension of a prosperodump in the multipart form-data upload.
const PROSPERODUMP_EXTENSION: &str = ".prosperodmp";

/// Prosperodump attachments should have these magic bytes.
const PROSPERODUMP_MAGIC_HEADER: &[u8] = b"\x7FELF";

/// Magic bytes for lz4 compressed prosperodump containers.
const LZ4_MAGIC_HEADER: &[u8] = b"\x04\x22\x4d\x18";

/// Content type of the data request payload.
const DATA_REQUEST_CONTENT_TYPE: &str = "application/vnd.sce.crs.datareq-request+json";

/// Files that are currently supported for uploading.
const UPLOAD_PARTS: &[&str] = &["corefile", "video", "screenshot"];

/// Files that are not currently supported for uploading.
const NO_UPLOAD_PARTS: &[&str] = &[
    "memorydump",
    "usermemoryfile",
    "gpudump",
    "gpucapture",
    "gpuextdump",
];

/// The maximum video length that is currently support for uploading.
const MAX_VIDEO_LENGTH: u32 = 4;

/// Implementation of the `getRequestCoreDumpData` [response](https://game.develop.playstation.net/resources/documents/SDK/11.000/Core_Dump_Data_Request_API-Reference/0002.html).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DataRequestResponse {
    parts: Parts,
    #[serde(skip_serializing_if = "Option::is_none")]
    part_parameters: Option<PartParameters>,
}

/// Partial implementation of the `parts` section in the `getRequestCoreDumpData` response.
///
/// The optional `additional` section is currently omitted as we make no use of it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Parts {
    /// List of files that can be uploaded.
    ///
    /// Note: It is not guaranteed that all of these will be uploaded every time. If they are not
    /// already created on the device, we need to tell the device to create them by adding the files
    /// to the `additional` section.
    upload: &'static [&'static str],
    /// List of files that should not be uploaded.
    ///
    /// This takes precedence over any setting on the device itself.
    no_upload: &'static [&'static str],
}

#[derive(Serialize)]
struct PartParameters {
    video: VideoParameters,
}

#[derive(Serialize)]
struct VideoParameters {
    duration: DurationParameters,
}

/// Partial implementation of the `duration` section in the `getRequestCoreDumpData` response.
///
/// The optional `min` section is currently omitted as we make no use of it.
#[derive(Serialize)]
struct DurationParameters {
    /// The maximum length a video that is uploaded is allowed to have.
    ///
    /// If a longer video is present on the device only the last `max` seconds will be  uploaded.
    max: u32,
}

fn create_data_request_response() -> DataRequestResponse {
    DataRequestResponse {
        parts: Parts {
            upload: UPLOAD_PARTS,
            no_upload: NO_UPLOAD_PARTS,
        },
        part_parameters: Some(PartParameters {
            video: VideoParameters {
                duration: DurationParameters {
                    max: MAX_VIDEO_LENGTH,
                },
            },
        }),
    }
}

fn validate_prosperodump(data: &[u8]) -> Result<(), BadStoreRequest> {
    if !data.starts_with(LZ4_MAGIC_HEADER) && !data.starts_with(PROSPERODUMP_MAGIC_HEADER) {
        relay_log::trace!("invalid prosperodump file");
        return Err(BadStoreRequest::InvalidProsperodump);
    }

    Ok(())
}

fn infer_attachment_type(_field_name: Option<&str>, file_name: &str) -> AttachmentType {
    if file_name.ends_with(PROSPERODUMP_EXTENSION) {
        AttachmentType::Prosperodump
    } else {
        AttachmentType::Attachment
    }
}

async fn extract_multipart(
    multipart: UnconstrainedMultipart,
    meta: RequestMeta,
    config: &Config,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let mut items = multipart.items(infer_attachment_type, config).await?;

    let prosperodump_item = items
        .iter_mut()
        .find(|item| item.attachment_type() == Some(&AttachmentType::Prosperodump))
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

    let multipart = request.extract_with_state(&state).await?;
    let mut envelope = extract_multipart(multipart, meta, state.config()).await?;
    envelope.require_feature(Feature::PlaystationIngestion);

    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    match common::handle_envelope(&state, envelope).await {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error.into()),
    };

    // Return here needs to be a 200 with arbitrary text to make the sender happy.
    Ok(TextResponse(id).into_response())
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
}
