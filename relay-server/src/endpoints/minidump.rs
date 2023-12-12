use std::convert::Infallible;

use axum::extract::{DefaultBodyLimit, Multipart, Request};
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use axum::RequestExt;
use bytes::Bytes;
use futures::{future, FutureExt};
use relay_config::Config;
use relay_event_schema::protocol::EventId;

use crate::constants::{ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT};
use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, RequestMeta};
use crate::service::ServiceState;
use crate::utils;

/// The field name of a minidump in the multipart form-data upload.
///
/// Sentry requires
const MINIDUMP_FIELD_NAME: &str = "upload_file_minidump";

/// File name for a standalone minidump upload.
///
/// In contrast to the field name, this is used when a standalone minidump is uploaded not in a
/// multipart request. The file name is later used to display the event attachment.
const MINIDUMP_FILE_NAME: &str = "Minidump";

/// Minidump attachments should have these magic bytes, little- and big-endian.
const MINIDUMP_MAGIC_HEADER_LE: &[u8] = b"MDMP";
const MINIDUMP_MAGIC_HEADER_BE: &[u8] = b"PMDM";

/// Content types by which standalone uploads can be recognized.
const MINIDUMP_RAW_CONTENT_TYPES: &[&str] = &["application/octet-stream", "application/x-dmp"];

fn validate_minidump(data: &[u8]) -> Result<(), BadStoreRequest> {
    if !data.starts_with(MINIDUMP_MAGIC_HEADER_LE) && !data.starts_with(MINIDUMP_MAGIC_HEADER_BE) {
        relay_log::trace!("invalid minidump file");
        return Err(BadStoreRequest::InvalidMinidump);
    }

    Ok(())
}

fn infer_attachment_type(field_name: Option<&str>) -> AttachmentType {
    match field_name.unwrap_or("") {
        self::MINIDUMP_FIELD_NAME => AttachmentType::Minidump,
        self::ITEM_NAME_BREADCRUMBS1 => AttachmentType::Breadcrumbs,
        self::ITEM_NAME_BREADCRUMBS2 => AttachmentType::Breadcrumbs,
        self::ITEM_NAME_EVENT => AttachmentType::EventPayload,
        _ => AttachmentType::Attachment,
    }
}

/// Extract a minidump from a nested multipart form.
///
/// This field is not a minidump (i.e. it doesn't start with the minidump magic header). It could be
/// a multipart field containing a minidump; this happens in old versions of the Linux Electron SDK.
///
/// Unfortunately, the embedded multipart field is not recognized by the multipart parser as a
/// multipart field containing a multipart body. For this case we will look if the field starts with
/// a '--' and manually extract the boundary (which is what follows '--' up to the end of line) and
/// manually construct a multipart with the detected boundary. If we can extract a multipart with an
/// embedded minidump, then use that field.
async fn extract_embedded_minidump(payload: Bytes) -> Result<Option<Bytes>, BadStoreRequest> {
    let boundary = match utils::get_multipart_boundary(&payload) {
        Some(boundary) => boundary,
        None => return Ok(None),
    };

    let stream = future::ok::<_, Infallible>(payload.clone()).into_stream();
    let mut multipart = multer::Multipart::new(stream, boundary);

    while let Some(field) = multipart.next_field().await? {
        if field.name() == Some(MINIDUMP_FIELD_NAME) {
            return Ok(Some(field.bytes().await?));
        }
    }

    Ok(None)
}

async fn extract_multipart(
    config: &Config,
    multipart: Multipart,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let max_size = config.max_attachment_size();
    let mut items = utils::multipart_items(multipart, max_size, infer_attachment_type).await?;

    let minidump_item = items
        .iter_mut()
        .find(|item| item.attachment_type() == Some(&AttachmentType::Minidump))
        .ok_or(BadStoreRequest::MissingMinidump)?;

    let embedded_opt = extract_embedded_minidump(minidump_item.payload()).await?;
    if let Some(embedded) = embedded_opt {
        minidump_item.set_payload(ContentType::Minidump, embedded);
    }

    validate_minidump(&minidump_item.payload())?;

    let event_id = common::event_id_from_items(&items)?.unwrap_or_else(EventId::new);
    let mut envelope = Envelope::from_request(Some(event_id), meta);

    for item in items {
        envelope.add_item(item);
    }

    Ok(envelope)
}

fn extract_raw_minidump(data: Bytes, meta: RequestMeta) -> Result<Box<Envelope>, BadStoreRequest> {
    validate_minidump(&data)?;

    let mut item = Item::new(ItemType::Attachment);
    item.set_payload(ContentType::Minidump, data);
    item.set_filename(MINIDUMP_FILE_NAME);
    item.set_attachment_type(AttachmentType::Minidump);

    // Create an envelope with a random event id.
    let mut envelope = Envelope::from_request(Some(EventId::new()), meta);
    envelope.add_item(item);
    Ok(envelope)
}

async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    content_type: RawContentType,
    request: Request,
) -> axum::response::Result<impl IntoResponse> {
    // The minidump can either be transmitted as the request body, or as
    // `upload_file_minidump` in a multipart form-data/ request.
    // Minidump request payloads do not have the same structure as usual events from other SDKs. The
    // minidump can either be transmitted as request body, or as `upload_file_minidump` in a
    // multipart formdata request.
    let envelope = if MINIDUMP_RAW_CONTENT_TYPES.contains(&content_type.as_ref()) {
        extract_raw_minidump(request.extract().await?, meta)?
    } else {
        extract_multipart(state.config(), request.extract().await?, meta).await?
    };

    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    match common::handle_envelope(&state, envelope).await {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error.into()),
    };

    // The return here is only useful for consistency because the UE4 crash reporter doesn't
    // care about it.
    Ok(TextResponse(id))
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_minidump() {
        let be_minidump = b"PMDMxxxxxx";
        assert!(validate_minidump(be_minidump).is_ok());

        let le_minidump = b"MDMPxxxxxx";
        assert!(validate_minidump(le_minidump).is_ok());

        let garbage = b"xxxxxx";
        assert!(validate_minidump(garbage).is_err());
    }
}
