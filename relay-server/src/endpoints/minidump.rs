use std::convert::Infallible;

use axum::extract::{DefaultBodyLimit, Multipart};
use axum::http::Request;
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
        MINIDUMP_FIELD_NAME => AttachmentType::Minidump,
        ITEM_NAME_BREADCRUMBS1 => AttachmentType::Breadcrumbs,
        ITEM_NAME_BREADCRUMBS2 => AttachmentType::Breadcrumbs,
        ITEM_NAME_EVENT => AttachmentType::EventPayload,
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

async fn handle<B>(
    state: ServiceState,
    meta: RequestMeta,
    content_type: RawContentType,
    request: Request<B>,
) -> axum::response::Result<impl IntoResponse>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send + Into<Bytes>,
    B::Error: Into<axum::BoxError>,
{
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

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send + Into<Bytes>,
    B::Error: Into<axum::BoxError>,
{
    post(handle).route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
}

#[cfg(test)]
mod tests {
    use axum::body::Full;
    use axum::extract::FromRequest;

    use relay_config::ByteSize;

    use crate::utils::{multipart_items, FormDataIter};

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

    #[tokio::test]
    async fn test_minidump_multipart_attachments() -> anyhow::Result<()> {
        let multipart_body: &[u8] =
            b"-----MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw---\x0d\x0a\
            Content-Disposition: form-data; name=\"guid\"\x0d\x0a\x0d\x0add46bb04-bb27-448c-aad0-0deb0c134bdb\x0d\x0a\
            -----MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw---\x0d\x0a\
            Content-Disposition: form-data; name=\"config.json\"; filename=\"config.json\"\x0d\x0a\x0d\x0a\
            \"Sentry\": { \"Dsn\": \"https://ingest.us.sentry.io/xxxxxxx\", \"MaxBreadcrumbs\": 50, \"Debug\": true }\x0d\x0a\
            -----MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw---\x0d\x0a\
            Content-Disposition: form-data; name=\"__sentry-breadcrumb1\"; filename=\"__sentry-breadcrumb1\"\x0d\x0a\
            Content-Type: application/octet-stream\x0d\x0a\x0d\x0a\
            \x82\
            \xa9timestamp\xb82024-03-12T16:59:33.069Z\
            \xa7message\xb5default level is info\x0d\x0a\
            -----MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw---\x0d\x0a\
            Content-Disposition: form-data; name=\"__sentry-breadcrumb2\"; filename=\"__sentry-breadcrumb2\"\x0d\x0a\
            Content-Type: application/octet-stream\x0d\x0a\x0d\x0a\
            \x0d\x0a\
            -----MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw---\x0d\x0a\
            Content-Disposition: form-data; name=\"__sentry-event\"; filename=\"__sentry-event\"\x0d\x0a\
            Content-Type: application/octet-stream\x0d\x0a\x0d\x0a\
            \x82\xa5level\xa5fatal\xa8platform\xa6native\x0d\x0a\
            -----MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw-----\x0d\x0a";

        let request = Request::builder()
            .header(
                "content-type",
                "multipart/form-data; boundary=---MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw---",
            )
            .body(Full::new(multipart_body))
            .unwrap();

        let multipart = Multipart::from_request(request, &()).await?;

        let items = multipart_items(
            multipart,
            ByteSize::mebibytes(100).as_bytes(),
            infer_attachment_type,
        )
        .await?;

        // we expect the multipart body to contain
        // * one arbitrary attachment from the user (a `config.json`)
        // * two breadcrumb files
        // * one event file
        // * one form-data item
        assert_eq!(5, items.len());

        // `config.json` has no content-type. MIME-detection in later processing will assign this.
        let item = &items[0];
        assert_eq!(item.filename().unwrap(), "config.json");
        assert!(item.content_type().is_none());
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(item.attachment_type().unwrap(), &AttachmentType::Attachment);
        assert_eq!(item.payload().len(), 95);

        // the first breadcrumb buffer
        let item = &items[1];
        assert_eq!(item.filename().unwrap(), "__sentry-breadcrumb1");
        assert_eq!(item.content_type().unwrap(), &ContentType::OctetStream);
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(
            item.attachment_type().unwrap(),
            &AttachmentType::Breadcrumbs
        );
        assert_eq!(item.payload().len(), 66);

        // the second breadcrumb buffer is empty since we haven't reached our max in the first
        let item = &items[2];
        assert_eq!(item.filename().unwrap(), "__sentry-breadcrumb2");
        assert_eq!(item.content_type().unwrap(), &ContentType::OctetStream);
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(
            item.attachment_type().unwrap(),
            &AttachmentType::Breadcrumbs
        );
        assert_eq!(item.payload().len(), 0);

        // the msg-pack encoded event file
        let item = &items[3];
        assert_eq!(item.filename().unwrap(), "__sentry-event");
        assert_eq!(item.content_type().unwrap(), &ContentType::OctetStream);
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(
            item.attachment_type().unwrap(),
            &AttachmentType::EventPayload
        );
        assert_eq!(item.payload().len(), 29);

        // the last item is the form-data if any and contains a `guid` from the `crashpad_handler`
        let item = &items[4];
        assert!(item.filename().is_none());
        assert_eq!(item.content_type().unwrap(), &ContentType::Text);
        assert_eq!(item.ty(), &ItemType::FormData);
        assert!(item.attachment_type().is_none());
        let form_payload = item.payload();
        let form_data_entry = FormDataIter::new(form_payload.as_ref()).next().unwrap();
        assert_eq!(form_data_entry.key(), "guid");
        assert_eq!(
            form_data_entry.value(),
            "dd46bb04-bb27-448c-aad0-0deb0c134bdb"
        );

        Ok(())
    }
}
