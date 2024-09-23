use std::convert::Infallible;
use std::io::Read;
use std::io::{Cursor, Error};

use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use axum::RequestExt;
use bytes::Bytes;
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use liblzma::read::XzDecoder;
use multer::Multipart;
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use zstd::stream::Decoder as ZstdDecoder;

use crate::constants::{ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT};
use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::ContentType::Minidump;
use crate::envelope::{AttachmentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, Remote, RequestMeta};
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

/// Convenience wrapper to let a decoder decode its full input into a buffer
fn run_decoder(decoder: &mut Box<dyn Read>) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    decoder.read_to_end(&mut buffer)?;
    Ok(buffer)
}

/// Creates a decoder based on the magic bytes the minidump payload
fn decoder_from(minidump_data: Bytes) -> Option<Box<dyn Read>> {
    if minidump_data.starts_with(b"\x1F\x8B") {
        return Some(Box::new(GzDecoder::new(Cursor::new(minidump_data))));
    } else if minidump_data.starts_with(b"\xFD\x37\x7A\x58\x5A\x00") {
        return Some(Box::new(XzDecoder::new(Cursor::new(minidump_data))));
    } else if minidump_data.starts_with(b"\x42\x5A\x68") {
        return Some(Box::new(BzDecoder::new(Cursor::new(minidump_data))));
    } else if minidump_data.starts_with(b"\x28\xB5\x2F\xFD") {
        return Some(Box::new(
            ZstdDecoder::new(Cursor::new(minidump_data)).unwrap(),
        ));
    }

    None
}

/// Tries to decode a minidump using any of the supported compression formats
/// or returns the provided minidump payload untouched if no format where detected
fn decode_minidump(minidump_data: Bytes) -> Result<Bytes, BadStoreRequest> {
    match decoder_from(minidump_data.clone()) {
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
            // proceed to process the payload untouched (as a plain minidump).
            Ok(minidump_data)
        }
    }
}

/// Removes any compression container file extensions from the minidump
/// filename so it can be updated in the item. Otherwise, attachments that
/// have been decoded would still show the extension in the UI, which is misleading.
fn remove_container_extension(filename: &str) -> &str {
    if filename.ends_with("gz") || filename.ends_with("xz") {
        &filename[..filename.len() - 3]
    } else if filename.ends_with("bz2") || filename.ends_with("zst") {
        &filename[..filename.len() - 4]
    } else {
        filename
    }
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

    let stream = futures::stream::once(async { Ok::<_, Infallible>(payload.clone()) });
    let mut multipart = Multipart::new(stream, boundary);

    while let Some(field) = multipart.next_field().await? {
        if field.name() == Some(MINIDUMP_FIELD_NAME) {
            return Ok(Some(field.bytes().await?));
        }
    }

    Ok(None)
}

async fn extract_multipart(
    multipart: Multipart<'static>,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let mut items = utils::multipart_items(multipart, infer_attachment_type).await?;

    let minidump_item = items
        .iter_mut()
        .find(|item| item.attachment_type() == Some(&AttachmentType::Minidump))
        .ok_or(BadStoreRequest::MissingMinidump)?;

    let embedded_opt = extract_embedded_minidump(minidump_item.payload()).await?;
    if let Some(embedded) = embedded_opt {
        minidump_item.set_payload(Minidump, embedded);
    }

    minidump_item.set_payload(Minidump, decode_minidump(minidump_item.payload())?);

    validate_minidump(&minidump_item.payload())?;

    if let Some(minidump_filename) = minidump_item.filename() {
        minidump_item.set_filename(remove_container_extension(minidump_filename).to_owned());
    }

    let event_id = common::event_id_from_items(&items)?.unwrap_or_else(EventId::new);
    let mut envelope = Envelope::from_request(Some(event_id), meta);

    for item in items {
        envelope.add_item(item);
    }

    Ok(envelope)
}

fn extract_raw_minidump(data: Bytes, meta: RequestMeta) -> Result<Box<Envelope>, BadStoreRequest> {
    let mut item = Item::new(ItemType::Attachment);

    item.set_payload(Minidump, decode_minidump(data)?);
    validate_minidump(&item.payload())?;
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
        let Remote(multipart) = request.extract_with_state(&state).await?;
        extract_multipart(multipart, meta).await?
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
    // Set the single-attachment limit that applies only for raw minidumps. Multipart bypasses the
    // limited body and applies its own limits.
    post(handle).route_layer(DefaultBodyLimit::max(config.max_attachment_size()))
}

#[cfg(test)]
mod tests {
    use crate::envelope::ContentType;
    use crate::utils::{multipart_items, FormDataIter};
    use axum::body::Body;
    use bzip2::write::BzEncoder;
    use bzip2::Compression as BzCompression;
    use flate2::write::GzEncoder;
    use flate2::Compression as GzCompression;
    use liblzma::write::XzEncoder;
    use relay_config::Config;
    use std::io::Write;
    use zstd::stream::Encoder as ZstdEncoder;

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

    type EncodeFunction = fn(&[u8]) -> Result<Bytes, Box<dyn std::error::Error>>;

    fn encode_gzip(be_minidump: &[u8]) -> Result<Bytes, Box<dyn std::error::Error>> {
        let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
        encoder.write_all(be_minidump)?;
        let compressed = encoder.finish()?;
        Ok(Bytes::from(compressed))
    }
    fn encode_bzip(be_minidump: &[u8]) -> Result<Bytes, Box<dyn std::error::Error>> {
        let mut encoder = BzEncoder::new(Vec::new(), BzCompression::default());
        encoder.write_all(be_minidump)?;
        let compressed = encoder.finish()?;
        Ok(Bytes::from(compressed))
    }
    fn encode_xz(be_minidump: &[u8]) -> Result<Bytes, Box<dyn std::error::Error>> {
        let mut encoder = XzEncoder::new(Vec::new(), 6);
        encoder.write_all(be_minidump)?;
        let compressed = encoder.finish()?;
        Ok(Bytes::from(compressed))
    }
    fn encode_zst(be_minidump: &[u8]) -> Result<Bytes, Box<dyn std::error::Error>> {
        let mut encoder = ZstdEncoder::new(Vec::new(), 0)?;
        encoder.write_all(be_minidump)?;
        let compressed = encoder.finish()?;
        Ok(Bytes::from(compressed))
    }

    #[test]
    fn test_validate_encoded_minidump() -> Result<(), Box<dyn std::error::Error>> {
        let encoders: Vec<EncodeFunction> = vec![encode_gzip, encode_zst, encode_bzip, encode_xz];

        for encoder in &encoders {
            let be_minidump = b"PMDMxxxxxx";
            let compressed = encoder(be_minidump)?;
            let mut decoder = decoder_from(compressed).unwrap();
            assert!(run_decoder(&mut decoder).is_ok());

            let le_minidump = b"MDMPxxxxxx";
            let compressed = encoder(le_minidump)?;
            let mut decoder = decoder_from(compressed).unwrap();
            assert!(run_decoder(&mut decoder).is_ok());

            let garbage = b"xxxxxx";
            let compressed = encoder(garbage)?;
            let mut decoder = decoder_from(compressed).unwrap();
            let decoded = run_decoder(&mut decoder);
            assert!(decoded.is_ok());
            assert!(validate_minidump(&decoded.unwrap()).is_err());
        }

        Ok(())
    }

    #[test]
    fn test_remove_container_extension() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(remove_container_extension("minidump"), "minidump");
        assert_eq!(remove_container_extension("minidump.gz"), "minidump");
        assert_eq!(remove_container_extension("minidump.bz2"), "minidump");
        assert_eq!(remove_container_extension("minidump.xz"), "minidump");
        assert_eq!(remove_container_extension("minidump.zst"), "minidump");
        assert_eq!(remove_container_extension("minidump.dmp"), "minidump.dmp");
        assert_eq!(
            remove_container_extension("minidump.dmp.gz"),
            "minidump.dmp"
        );
        assert_eq!(
            remove_container_extension("minidump.dmp.bz2"),
            "minidump.dmp"
        );
        assert_eq!(
            remove_container_extension("minidump.dmp.xz"),
            "minidump.dmp"
        );
        assert_eq!(
            remove_container_extension("minidump.dmp.zst"),
            "minidump.dmp"
        );

        Ok(())
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
            .body(Body::from(multipart_body))?;

        let config = Config::default();

        let multipart = utils::multipart_from_request(request, &config)?;
        let items = multipart_items(multipart, infer_attachment_type).await?;

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
