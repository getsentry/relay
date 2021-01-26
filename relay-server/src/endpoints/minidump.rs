use actix_web::multipart::{Multipart, MultipartItem};
use actix_web::{actix::ResponseFuture, HttpMessage, HttpRequest, HttpResponse};
use bytes::Bytes;
use futures::{future, Future, Stream};

use relay_general::protocol::EventId;

use crate::body::ForwardBody;
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

fn get_embedded_minidump(
    payload: Bytes,
    max_size: usize,
) -> ResponseFuture<Option<Bytes>, BadStoreRequest> {
    let boundary = match get_multipart_boundary(&payload) {
        Some(boundary) => boundary,
        None => return Box::new(future::ok(None)),
    };

    let f = Multipart::new(Ok(boundary.to_string()), futures::stream::once(Ok(payload)))
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
        .map_err(|(err, _)| BadStoreRequest::InvalidMultipart(err))
        .map(|(data, _)| data.map(Bytes::from));

    Box::new(f)
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
fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let max_single_size = request.state().config().max_attachment_size();
    let max_multipart_size = request.state().config().max_attachments_size();

    // Minidump request payloads do not have the same structure as usual events from other SDKs. The
    // minidump can either be transmitted as request body, or as `upload_file_minidump` in a
    // multipart formdata request.
    if MINIDUMP_RAW_CONTENT_TYPES.contains(&request.content_type()) {
        let future = ForwardBody::new(request, max_single_size)
            .map_err(|_| BadStoreRequest::InvalidMinidump)
            .and_then(move |data| {
                validate_minidump(&data)?;

                let mut item = Item::new(ItemType::Attachment);
                item.set_payload(ContentType::Minidump, data);
                item.set_filename(MINIDUMP_FILE_NAME);
                item.set_attachment_type(AttachmentType::Minidump);

                // Create an envelope with a random event id.
                let mut envelope = Envelope::from_request(Some(EventId::new()), meta);
                envelope.add_item(item);
                Ok(envelope)
            });

        return Box::new(future);
    }

    let future = MultipartItems::new(max_multipart_size)
        .infer_attachment_type(infer_attachment_type)
        .handle_request(request)
        .map_err(BadStoreRequest::InvalidMultipart)
        .and_then(move |mut items| {
            let minidump_index = items
                .iter()
                .position(|item| item.attachment_type() == Some(AttachmentType::Minidump));

            let mut minidump_item = match minidump_index {
                Some(index) => items.swap_remove(index),
                None => {
                    return Box::new(future::err(BadStoreRequest::MissingMinidump))
                        as ResponseFuture<_, _>
                }
            };

            // HACK !!
            //
            // This field is not a minidump (.i.e. it doesn't start with the minidump magic header).
            // It could be a multipart field containing a minidump; this happens in old versions of
            // the Linux Electron SDK.
            //
            // Unfortunately, the embedded multipart field is not recognized by the multipart parser
            // as a multipart field containing a multipart body. For this case we will look if field
            // the field starts with a '--' and manually extract the boundary (which is what follows
            // '--' up to the end of line) and manually construct a multipart with the detected
            // boundary. If we can extract a multipart with an embedded minidump, then use that
            // field.
            let future = get_embedded_minidump(minidump_item.payload(), max_single_size).and_then(
                move |embedded_opt| {
                    if let Some(embedded) = embedded_opt {
                        minidump_item.set_payload(ContentType::Minidump, embedded);
                    }

                    validate_minidump(&minidump_item.payload())?;
                    items.push(minidump_item);

                    Ok(items)
                },
            );

            Box::new(future)
        })
        .and_then(move |items| {
            let event_id = common::event_id_from_items(&items)?.unwrap_or_else(EventId::new);
            let mut envelope = Envelope::from_request(Some(event_id), meta);

            for item in items {
                envelope.add_item(item);
            }

            Ok(envelope)
        });

    Box::new(future)
}

fn store_minidump(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    common::handle_store_like_request(
        meta,
        true,
        request,
        extract_envelope,
        common::create_text_event_id_response,
        false, // Never respond with a 429 since clients often retry these
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        // No mandatory trailing slash here because people already use it like this.
        .resource(&common::normpath(r"/api/{project:\d+}/minidump"), |r| {
            r.name("store-minidump");
            r.post().with(store_minidump);
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
