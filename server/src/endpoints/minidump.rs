use actix_web::{actix::ResponseFuture, http::Method, HttpMessage, HttpRequest, HttpResponse};
use futures::Future;

use semaphore_general::protocol::EventId;

use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};
use crate::utils::MultipartEnvelope;

/// The field name of a minidump in the multipart form-data upload.
///
/// Sentry requires
const MINIDUMP_FIELD_NAME: &str = "upload_file_minidump";

/// File name for a standalone minidump upload.
///
/// In contrast to the field name, this is used when a standalone minidump is uploaded not in a
/// multipart request. The file name is later used to display the event attachment.
const MINIDUMP_FILE_NAME: &str = "Minidump";

/// Minidump attachments should have these magic bytes.
const MINIDUMP_MAGIC_HEADER: &[u8] = b"MDMP";

/// Content types by which standalone uploads can be recognized.
const MINIDUMP_RAW_CONTENT_TYPES: &[&str] = &["application/octet-stream", "application/x-dmp"];

fn validate_minidump(data: &[u8]) -> Result<(), BadStoreRequest> {
    if !data.starts_with(MINIDUMP_MAGIC_HEADER) {
        log::trace!("invalid minidump file");
        return Err(BadStoreRequest::InvalidMinidump);
    }

    Ok(())
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
///
fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: EventMeta,
    max_payload_size: usize,
) -> ResponseFuture<Envelope, BadStoreRequest>
where {
    // TODO: at the moment we override any pre exsiting (set by an SDK) event id here.
    //  We shouldn't do that. The problem is that at this stage we cannot afford to parse the
    //  request and look for the event id so we simply set one.
    //  We need to somehow preserve SDK set event ids ( the current behaviour needs to change).
    let mut envelope = Envelope::from_request(EventId::new(), meta);

    // Minidump request payloads do not have the same structure as usual events from other SDKs. The
    // minidump can either be transmitted as request body, or as `upload_file_minidump` in a
    // multipart formdata request.
    if MINIDUMP_RAW_CONTENT_TYPES.contains(&request.content_type()) {
        let future = request
            .body()
            .limit(max_payload_size)
            .map_err(|_| BadStoreRequest::InvalidMinidump)
            .and_then(move |data| {
                validate_minidump(&data)?;

                let mut item = Item::new(ItemType::Attachment);
                item.set_payload(ContentType::OctetStream, data);
                item.set_filename(MINIDUMP_FILE_NAME);
                item.set_name(MINIDUMP_FIELD_NAME);

                envelope.add_item(item);

                Ok(envelope)
            });

        return Box::new(future);
    }

    let future = MultipartEnvelope::new(envelope, max_payload_size)
        .handle_request(request)
        .map_err(BadStoreRequest::InvalidMultipart)
        .and_then(|envelope| {
            // Check that the envelope contains a minidump item.
            match envelope.get_item_by_name(MINIDUMP_FIELD_NAME) {
                Some(item) => validate_minidump(&item.payload())?,
                None => return Err(BadStoreRequest::MissingMinidump),
            }

            Ok(envelope)
        });

    Box::new(future)
}

fn store_minidump(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let event_size = request.state().config().max_attachment_payload_size();

    Box::new(handle_store_like_request(
        meta,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, event_size),
        move |id| {
            // the minidump client expects the response to contain an event id as a hyphenated UUID
            // i.e. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            let hyphenated = id.0.to_hyphenated();
            HttpResponse::Ok()
                .content_type("text/plain")
                .body(format!("{}", hyphenated))
        },
    ))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource(r"/api/{project:\d+}/minidump{trailing_slash:/?}", |r| {
        r.method(Method::POST).with(store_minidump);
    })
}
