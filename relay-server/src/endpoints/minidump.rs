use actix_web::multipart::{Multipart, MultipartItem};
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use bytes::Bytes;
use futures::compat::Future01CompatExt;
use futures01::{stream, Stream};

use relay_general::protocol::EventId;

use crate::body;
use crate::constants::{ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT};
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};
use crate::utils::{consume_field, get_multipart_boundary, MultipartError, MultipartItems};

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
async fn extract_embedded_minidump(
    payload: Bytes,
    max_size: usize,
) -> Result<Option<Bytes>, BadStoreRequest> {
    let boundary = match get_multipart_boundary(&payload) {
        Some(boundary) => boundary,
        None => return Ok(None),
    };

    let future = Multipart::new(Ok(boundary.to_string()), stream::once(Ok(payload)))
        .map_err(MultipartError::InvalidMultipart)
        .filter_map(|item| {
            if let MultipartItem::Field(field) = item {
                if let Some(content_disposition) = field.content_disposition() {
                    if content_disposition.get_name() == Some(MINIDUMP_FIELD_NAME) {
                        return Some(field);
                    }
                }
            }
            None
        })
        .and_then(move |field| consume_field(field, max_size))
        .into_future()
        .compat();

    let (data, _) = future
        .await
        .map_err(|(err, _)| BadStoreRequest::InvalidMultipart(err))?;

    Ok(data.map(Bytes::from))
}

async fn extract_multipart(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let max_multipart_size = request.state().config().max_attachments_size();
    let mut items = MultipartItems::new(max_multipart_size)
        .infer_attachment_type(infer_attachment_type)
        .handle_request(request)
        .await
        .map_err(BadStoreRequest::InvalidMultipart)?;

    let minidump_item = items
        .iter_mut()
        .find(|item| item.attachment_type() == Some(&AttachmentType::Minidump))
        .ok_or(BadStoreRequest::MissingMinidump)?;

    let max_single_size = request.state().config().max_attachment_size();
    let embedded_opt = extract_embedded_minidump(minidump_item.payload(), max_single_size).await?;
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

async fn extract_raw_minidump(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let max_single_size = request.state().config().max_attachment_size();
    let data = body::request_body(request, max_single_size).await?;

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

/// Creates an evelope from a minidump request.
///
/// Minidump request payloads do not have the same structure as usual
/// events from other SDKs.
///
/// The minidump can either be transmitted as the request body, or as
/// `upload_file_minidump` in a multipart form-data/ request.
///
/// Optionally, an event payload can be sent in the `sentry` form
/// field, either as JSON or as nested form data.
async fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    // Minidump request payloads do not have the same structure as usual events from other SDKs. The
    // minidump can either be transmitted as request body, or as `upload_file_minidump` in a
    // multipart formdata request.
    if MINIDUMP_RAW_CONTENT_TYPES.contains(&request.content_type()) {
        extract_raw_minidump(request, meta).await
    } else {
        extract_multipart(request, meta).await
    }
}

async fn store_minidump(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
) -> Result<HttpResponse, BadStoreRequest> {
    let envelope = extract_envelope(&request, meta).await?;
    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    match common::handle_envelope(request.state(), envelope).await {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error),
    };

    // The return here is only useful for consistency because the UE4 crash reporter doesn't
    // care about it.
    Ok(common::create_text_event_id_response(id))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        // No mandatory trailing slash here because people already use it like this.
        .resource(&common::normpath(r"/api/{project:\d+}/minidump"), |r| {
            r.name("store-minidump");
            r.post()
                .with_async(|m, r| common::handler(store_minidump(m, r)));
        })
        .register()
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
