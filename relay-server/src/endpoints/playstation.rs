use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use axum::RequestExt;
use bytes::Bytes;
use lz4_flex::frame::FrameDecoder as lz4Decoder;
use multer::Multipart;
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;
use std::io::Cursor;
use std::io::Read;

use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::ContentType::OctetStream;
use crate::envelope::{AttachmentType, Envelope};
use crate::extractors::{RawContentType, Remote, RequestMeta};
use crate::service::ServiceState;
use crate::utils;

/// The extension of a prosperodump in the multipart form-data upload.
const PROSPERODUMP_EXTENSION: &str = "prosperodmp";

/// The extension of a screenshot in the multipart form-data upload.
const SCREENSHOT_EXTENSION: &str = "jpg";

/// The extension of a video in the multipart form-data upload.
const VIDEO_EXTENSION: &str = "webm";

/// The extension of a memorydump in the multipart form-data upload.
const MEMORYDUMP_EXTENSION: &str = "prosperomemdmp";

/// Prosperodump attachments should have these magic bytes
const PROSPERODUMP_MAGIC_HEADER: &[u8] = b"\x7FELF";

/// Magic bytes for lz4 compressed prosperodump containers.
const LZ4_MAGIC_HEADER: &[u8] = b"\x04\x22\x4d\x18";

fn validate_prosperodump(data: &[u8]) -> Result<(), BadStoreRequest> {
    if !data.starts_with(PROSPERODUMP_MAGIC_HEADER) {
        relay_log::trace!("invalid prosperodump file");
        return Err(BadStoreRequest::InvalidProsperodump);
    }

    Ok(())
}

// TODO: Decide if we want to move this into a utils since it is duplicate.
/// Convenience wrapper to let a decoder decode its full input into a buffer
fn run_decoder(decoder: &mut Box<dyn Read>) -> std::io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    decoder.read_to_end(&mut buffer)?;
    Ok(buffer)
}

// TODO: Decide if this extra function is worth it if we only have one encoder
/// Creates a decoder based on the magic bytes the prosperodump payload
fn decoder_from(prosperodump_data: Bytes) -> Option<Box<dyn Read>> {
    if prosperodump_data.starts_with(LZ4_MAGIC_HEADER) {
        return Some(Box::new(lz4Decoder::new(Cursor::new(prosperodump_data))));
    }
    None
}

/// Tries to decode a prosperodump using any of the supported compression formats
/// or returns the provided minidump payload untouched if no format where detected
fn decode_prosperodump(prosperodump_data: Bytes) -> Result<Bytes, BadStoreRequest> {
    match decoder_from(prosperodump_data.clone()) {
        Some(mut decoder) => {
            match run_decoder(&mut decoder) {
                Ok(decoded) => Ok(Bytes::from(decoded)),
                Err(err) => {
                    // we detected a compression container but failed to decode it
                    relay_log::trace!("invalid compression container");
                    Err(BadStoreRequest::InvalidCompressionContainer(err))
                }
            }
        }
        None => {
            // this means we haven't detected any compression container
            // TODO: Decide if we want to fail here
            Ok(prosperodump_data)
        }
    }
}

fn infer_attachment_type(field_name: Option<&str>) -> AttachmentType {
    match field_name.unwrap_or("") {
        PROSPERODUMP_EXTENSION => AttachmentType::Prosperodump,
        // TODO: Think about if we want these to be a special attachment type.
        SCREENSHOT_EXTENSION | VIDEO_EXTENSION | MEMORYDUMP_EXTENSION | _ => {
            AttachmentType::Attachment
        }
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
    prosperodump_item.set_payload(
        OctetStream,
        decode_prosperodump(prosperodump_item.payload())?,
    );

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
    envelope.require_feature(Feature::PlaystationEndpoint);

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
