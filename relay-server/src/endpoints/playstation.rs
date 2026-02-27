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
use crate::middlewares;
use crate::service::ServiceState;
use crate::utils::upload::Sink;
use crate::utils::{AttachmentStrategy, UnconstrainedMultipart, UploadExemptions};

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
    state: &ServiceState,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let upload_sink = Sink::new(state);
    let scoping = meta.get_partial_scoping().into_scoping();
    let attachment_strategy = AttachmentStrategy::Upload {
        upload_sink,
        scoping,
        exemptions: UploadExemptions {
            exempt_types: &[AttachmentType::Prosperodump],
            ignore_size_limit_exceeded: true,
        },
    };
    let mut items = multipart
        .items(infer_attachment_type, state.config(), attachment_strategy)
        .await?;

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
    let mut envelope = extract_multipart(multipart, meta, &state).await?;
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
