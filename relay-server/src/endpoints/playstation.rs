use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use axum::RequestExt;
use multer::Multipart;
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;

use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::ContentType::OctetStream;
use crate::envelope::{AttachmentType, Envelope};
use crate::extractors::{RawContentType, Remote, RequestMeta};
use crate::service::ServiceState;
use crate::utils;

/// The extension of a prosperodump in the multipart form-data upload.
const PROSPERODUMP_EXTENSION: &str = "prosperodmp";

/// Prosperodump attachments should have these magic bytes
const PROSPERODUMP_MAGIC_HEADER: &[u8] = b"\x7FELF";

/// Magic bytes for lz4 compressed prosperodump containers.
const LZ4_MAGIC_HEADER: &[u8] = b"\x04\x22\x4d\x18";

fn validate_prosperodump(data: &[u8]) -> Result<(), BadStoreRequest> {
    if !data.starts_with(LZ4_MAGIC_HEADER) && !data.starts_with(PROSPERODUMP_MAGIC_HEADER) {
        relay_log::trace!("invalid prosperodump file");
        return Err(BadStoreRequest::InvalidProsperodump);
    }

    Ok(())
}

fn infer_attachment_type(field_name: Option<&str>) -> AttachmentType {
    match field_name.unwrap_or("") {
        PROSPERODUMP_EXTENSION => AttachmentType::Prosperodump,
        _ => AttachmentType::Attachment,
    }
}

async fn extract_multipart(
    multipart: Multipart<'static>,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let mut items = utils::multipart_items_by_extension(multipart, infer_attachment_type).await?;

    let prosperodump_item = items
        .iter_mut()
        .find(|item| item.attachment_type() == Some(&AttachmentType::Prosperodump))
        .ok_or(BadStoreRequest::MissingProsperodump)?;

    // TODO: Think about if we want a ContentType::Prosperodump ?
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
    _content_type: RawContentType,
    request: Request,
) -> axum::response::Result<impl IntoResponse> {
    // The crash dumps are transmitted as `...` in a multipart form-data/ request.
    let Remote(multipart) = request.extract_with_state(&state).await?;
    let mut envelope = extract_multipart(multipart, meta).await?;
    envelope.require_feature(Feature::PlaystationIngestion);

    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    match common::handle_envelope(&state, envelope).await {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error.into()),
    };

    // Return here needs to be a 200 with arbitrary text to make the sender happy.
    // TODO: Think about if there is something else to return here
    Ok(TextResponse(id))
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    // TODO: Check if this even has an effect since we will always have a multipart message.
    post(handle).route_layer(DefaultBodyLimit::max(config.max_attachment_size()))
}
