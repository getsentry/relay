use axum::RequestExt;
use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use liblzma::read::XzDecoder;
use multer::{Field, Multipart};
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;
use relay_quotas::{DataCategory, RateLimits, Scoping};
use relay_system::Addr;
use std::convert::Infallible;
use std::error::Error;
use std::io::Cursor;
use std::io::Read;
use tower_http::limit::RequestBodyLimitLayer;
use zstd::stream::Decoder as ZstdDecoder;

use crate::constants::{ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT};
use crate::endpoints::common::{self, BadStoreRequest, TextResponse, upload_to_objectstore};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, RequestMeta};
use crate::managed::{Managed, ManagedResult};
use crate::middlewares;
use crate::service::ServiceState;
use crate::services::outcome::{DiscardAttachmentType, DiscardItemType, DiscardReason, Outcome};
use crate::services::upload::Upload;
use crate::statsd::RelayCounters;
use crate::utils::{self, AttachmentStrategy, read_attachment_bytes_into_item};

/// The field name of a minidump in the multipart form-data upload.
///
/// Sentry requires
const MINIDUMP_FIELD_NAME: &str = "upload_file_minidump";

/// The field name of a view hierarchy file in the multipart form-data upload.
/// It matches the expected file name of the view hierarchy, as outlined in RFC#33
/// <https://github.com/getsentry/rfcs/blob/main/text/0033-view-hierarchy.md>
const VIEW_HIERARCHY_FIELD_NAME: &str = "view-hierarchy.json";

/// File name for a standalone minidump upload.
///
/// In contrast to the field name, this is used when a standalone minidump is uploaded not in a
/// multipart request. The file name is later used to display the event attachment.
const MINIDUMP_FILE_NAME: &str = "Minidump";

/// Minidump attachments should have these magic bytes, little- and big-endian.
const MINIDUMP_MAGIC_HEADER_LE: &[u8] = b"MDMP";
const MINIDUMP_MAGIC_HEADER_BE: &[u8] = b"PMDM";

/// Magic bytes for gzip compressed minidump containers.
const GZIP_MAGIC_HEADER: &[u8] = b"\x1F\x8B";
/// Magic bytes for xz compressed minidump containers.
const XZ_MAGIC_HEADER: &[u8] = b"\xFD\x37\x7A\x58\x5A\x00";
/// Magic bytes for bzip2 compressed minidump containers.
const BZIP2_MAGIC_HEADER: &[u8] = b"\x42\x5A\x68";
/// Magic bytes for zstd compressed minidump containers.
const ZSTD_MAGIC_HEADER: &[u8] = b"\x28\xB5\x2F\xFD";

/// Content types by which standalone uploads can be recognized.
const MINIDUMP_RAW_CONTENT_TYPES: &[&str] = &["application/octet-stream", "application/x-dmp"];

fn validate_minidump(data: &[u8]) -> Result<(), BadStoreRequest> {
    if !data.starts_with(MINIDUMP_MAGIC_HEADER_LE) && !data.starts_with(MINIDUMP_MAGIC_HEADER_BE) {
        relay_log::trace!("invalid minidump file");
        return Err(BadStoreRequest::InvalidMinidump);
    }

    Ok(())
}

/// Convenience wrapper to let a decoder decode its full input into a buffer.
///
/// Stops reading once `max_size` is exceeded and returns an error. This prevents
/// decompression bombs from exhausting memory.
fn run_decoder(mut decoder: impl Read) -> std::io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    decoder.read_to_end(&mut buffer)?;
    Ok(buffer)
}

/// Creates a decoder based on the magic bytes the minidump payload
fn decoder_from(minidump_data: Bytes) -> Option<Box<dyn Read>> {
    if minidump_data.starts_with(GZIP_MAGIC_HEADER) {
        return Some(Box::new(GzDecoder::new(Cursor::new(minidump_data))));
    } else if minidump_data.starts_with(XZ_MAGIC_HEADER) {
        return Some(Box::new(XzDecoder::new(Cursor::new(minidump_data))));
    } else if minidump_data.starts_with(BZIP2_MAGIC_HEADER) {
        return Some(Box::new(BzDecoder::new(Cursor::new(minidump_data))));
    } else if minidump_data.starts_with(ZSTD_MAGIC_HEADER) {
        return match ZstdDecoder::new(Cursor::new(minidump_data)) {
            Ok(decoder) => Some(Box::new(decoder)),
            Err(ref err) => {
                relay_log::error!(error = err as &dyn Error, "failed to create ZstdDecoder");
                None
            }
        };
    }

    None
}

/// Tries to decode a minidump using any of the supported compression formats
/// or returns the provided minidump payload untouched if no format where detected.
///
/// Returns an `Overflow` error if the decompressed size exceeds `max_size`.
fn decode_minidump(minidump_data: Bytes, max_size: usize) -> Result<Bytes, BadStoreRequest> {
    let Some(decoder) = decoder_from(minidump_data.clone()) else {
        // this means we haven't detected any compression container
        // proceed to process the payload untouched (as a plain minidump).
        return Ok(minidump_data);
    };

    // Determine if this is a niche use-case of if this happens frequently.
    relay_statsd::metric!(counter(RelayCounters::CompressedMinidump) += 1);

    let decoder = decoder.take(max_size.saturating_add(1) as u64);

    match run_decoder(decoder) {
        Ok(decoded) => {
            if decoded.len() > max_size {
                let item_type = DiscardItemType::Attachment(DiscardAttachmentType::Minidump);
                return Err(BadStoreRequest::Overflow(item_type));
            }
            Ok(Bytes::from(decoded))
        }
        Err(err) => {
            // we detected a compression container but failed to decode it
            relay_log::trace!("invalid compression container");
            Err(BadStoreRequest::InvalidCompressionContainer(err))
        }
    }
}

/// Removes any compression container file extensions from the minidump
/// filename so it can be updated in the item. Otherwise, attachments that
/// have been decoded would still show the extension in the UI, which is misleading.
fn remove_container_extension(filename: &str) -> &str {
    [".gz", ".xz", ".bz2", ".zst"]
        .into_iter()
        .find_map(|suffix| filename.strip_suffix(suffix))
        .unwrap_or(filename)
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

#[derive(Clone, Debug)]
enum UploadDecision {
    /// Put the item into the envelope as-is (default behavior).
    Inline,
    /// Upload the item to objectstore and put a placeholder into the envelope.
    ///
    /// The behavior for supersized items.
    Upload,
    /// Drop the item rather than uploading it or putting it in an envelope.
    ///
    /// Since we already check the quota in the endpoint an item can be dropped even before being
    /// uploaded or put into an envelope.
    Drop(RateLimits),
}

struct UploadContext<'a> {
    upload: &'a Addr<Upload>,
    scoping: Scoping,
    upload_attachments: UploadDecision,
    upload_minidumps: UploadDecision,
}

impl UploadContext<'_> {
    fn upload_decision(&self, attachment_type: Option<AttachmentType>) -> &UploadDecision {
        match attachment_type {
            Some(AttachmentType::Attachment) => &self.upload_attachments,
            Some(AttachmentType::Minidump) => &self.upload_minidumps,
            _ => &UploadDecision::Inline,
        }
    }
}

struct MinidumpAttachmentStrategy<'a> {
    /// Information necessary to upload to the objectstore.
    ///
    /// This is optional since uploading to the objectstore might not be enabled for a project.
    upload_context: Option<UploadContext<'a>>,
}

impl<'a> AttachmentStrategy for MinidumpAttachmentStrategy<'a> {
    async fn add_to_item(
        &self,
        field: Field<'static>,
        item: Managed<Item>,
        config: &Config,
    ) -> Result<Option<Managed<Item>>, multer::Error> {
        // If we have no upload context just fall back to the old behavior.
        let Some(ref upload_context) = self.upload_context else {
            return read_attachment_bytes_into_item(field, item, config, false).await;
        };

        match upload_context.upload_decision(item.attachment_type()) {
            UploadDecision::Upload => {
                let content_type = field.content_type().map(ToString::to_string);
                Ok(upload_to_objectstore(
                    field,
                    content_type,
                    item,
                    config,
                    upload_context.scoping,
                    upload_context.upload,
                    "minidump",
                )
                .await)
            }
            UploadDecision::Inline => {
                read_attachment_bytes_into_item(field, item, config, false).await
            }
            UploadDecision::Drop(limits) => {
                // This is best effort, the item here does not yet have its content set hence size
                // is not correct.
                let _ = item.reject_err(Outcome::RateLimited(
                    limits.longest().and_then(|l| l.reason_code.clone()),
                ));
                Ok(None)
            }
        }
    }

    fn infer_type(&self, field: &Field) -> AttachmentType {
        match field.name().unwrap_or("") {
            MINIDUMP_FIELD_NAME => AttachmentType::Minidump,
            ITEM_NAME_BREADCRUMBS1 => AttachmentType::Breadcrumbs,
            ITEM_NAME_BREADCRUMBS2 => AttachmentType::Breadcrumbs,
            ITEM_NAME_EVENT => AttachmentType::EventPayload,
            VIEW_HIERARCHY_FIELD_NAME => AttachmentType::ViewHierarchy,
            _ => AttachmentType::Attachment,
        }
    }
}

async fn multipart_to_envelope(
    multipart: Multipart<'static>,
    meta: RequestMeta,
    state: &ServiceState,
    config: &Config,
    upload_context: Option<UploadContext<'_>>,
) -> Result<Managed<Box<Envelope>>, BadStoreRequest> {
    let minidump_attachment_strategy = MinidumpAttachmentStrategy { upload_context };

    let mut items = utils::multipart_items(
        multipart,
        config,
        minidump_attachment_strategy,
        &meta,
        state.outcome_aggregator(),
    )
    .await?;

    let minidump_idx = items
        .iter()
        .position(|item| item.attachment_type() == Some(AttachmentType::Minidump))
        .ok_or(BadStoreRequest::MissingMinidump)
        .reject(&items)?;

    // Doing these operations does not make sense if we already streamed the minidump to objectstore.
    if !items[minidump_idx].is_attachment_ref() {
        let payload = items[minidump_idx].payload();
        let payload = extract_embedded_minidump(payload.clone())
            .await?
            .unwrap_or(payload);
        let payload = decode_minidump(payload, config.max_attachment_size()).reject(&items)?;

        items.try_modify(|items, records| -> Result<(), BadStoreRequest> {
            let minidump_item = &mut items[minidump_idx];
            minidump_item.set_payload(ContentType::Minidump, payload);
            records.lenient(DataCategory::Attachment); // decoding the minidump changes its size
            if let Some(minidump_filename) = minidump_item.filename() {
                minidump_item.set_filename(remove_container_extension(minidump_filename).to_owned())
            }
            validate_minidump(&minidump_item.payload())?;
            Ok(())
        })?;
    }

    let event_id = common::event_id_from_items(&items)?.unwrap_or_else(EventId::new);
    let envelope = items.map(|items, records| {
        records.modify_by(DataCategory::Error, 1);
        Box::new(Envelope::from_request(Some(event_id), meta).with_items(items))
    });
    Ok(envelope)
}

/// Creates an [UploadContext].
async fn upload_context<'a>(
    meta: &RequestMeta,
    state: &'a ServiceState,
) -> Result<Option<UploadContext<'a>>, BadStoreRequest> {
    if !state
        .global_config_handle()
        .current()
        .options
        .endpoint_fetch_config_enabled
    {
        return Ok(None);
    }

    let project = state
        .project_cache_handle()
        .ready(meta.public_key(), state.config().query_timeout())
        .await
        .ok_or(BadStoreRequest::ProjectUnavailable)?;

    let project_config = project
        .state()
        .clone()
        .enabled()
        .ok_or(BadStoreRequest::EventRejected(DiscardReason::ProjectId))?;

    let scoping = project_config
        .scoping(meta.public_key())
        .ok_or(BadStoreRequest::EventRejected(DiscardReason::ProjectId))?;

    let rate_limits = project.rate_limits().current_limits().check_with_quotas(
        project_config.get_quotas(),
        scoping.item(DataCategory::Error),
    );

    let attachment_rate_limits = project.rate_limits().current_limits().check_with_quotas(
        project_config.get_quotas(),
        scoping.item(DataCategory::Attachment),
    );

    let upload_minidumps = if !project_config.has_feature(Feature::MinidumpUploads) {
        UploadDecision::Inline
    } else if rate_limits.is_limited() {
        UploadDecision::Drop(rate_limits.clone())
    } else {
        UploadDecision::Upload
    };

    let upload_attachments = if matches!(upload_minidumps, UploadDecision::Drop(_)) {
        UploadDecision::Drop(rate_limits)
    } else if !project_config.has_feature(Feature::MinidumpAttachmentUploads) {
        UploadDecision::Inline
    } else if attachment_rate_limits.is_limited() {
        UploadDecision::Drop(attachment_rate_limits)
    } else {
        UploadDecision::Upload
    };

    Ok(Some(UploadContext {
        upload: state.upload(),
        scoping,
        upload_attachments,
        upload_minidumps,
    }))
}

async fn raw_minidump_to_envelope(
    request: Request,
    meta: RequestMeta,
    state: &ServiceState,
    content_type: RawContentType,
    config: &Config,
    upload_context: Option<UploadContext<'_>>,
) -> Result<Managed<Box<Envelope>>, BadStoreRequest> {
    debug_assert!(!matches!(
        upload_context.as_ref().map(|c| &c.upload_minidumps),
        Some(UploadDecision::Drop(_))
    ));

    let mut item = Item::new(ItemType::Attachment);
    item.set_filename(MINIDUMP_FILE_NAME);
    item.set_attachment_type(AttachmentType::Minidump);
    let mut item = Managed::with_meta_from_request_meta(&meta, state.outcome_aggregator(), item);
    if let Some(upload_context) = upload_context
        && matches!(upload_context.upload_minidumps, UploadDecision::Upload)
    {
        item = upload_to_objectstore(
            request.into_body().into_data_stream(),
            Some(content_type.to_string()).filter(|s| !s.is_empty()),
            item,
            config,
            upload_context.scoping,
            upload_context.upload,
            "minidump",
        )
        .await
        .ok_or(BadStoreRequest::ObjectstoreUploadFailed)?;
    } else {
        let minidump_data = request.extract().await?;
        item.try_modify(|inner, records| -> Result<(), BadStoreRequest> {
            let payload = decode_minidump(minidump_data, state.config().max_attachment_size())?;
            inner.set_payload(ContentType::Minidump, payload);
            records.lenient(DataCategory::Attachment); // decoding the minidump changes its size
            validate_minidump(&inner.payload())?;
            Ok(())
        })?;
    };

    // Create an envelope with a random event id.
    let envelope = item.map(|item, records| {
        records.modify_by(DataCategory::Error, 1);
        Box::new(Envelope::from_request(Some(EventId::new()), meta).with_items(vec![item]))
    });
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
    let config = state.config();
    let upload_context = upload_context(&meta, &state).await?;

    if let Some(upload_context) = &upload_context
        && let UploadDecision::Drop(limits) = &upload_context.upload_minidumps
    {
        // This is a best effort outcome, in the sense that we also drop some attachments but
        // since we know neither the amount or size we can't emit an accurate outcome for them.
        let _ = Managed::with_meta_from_request_meta(
            &meta,
            state.outcome_aggregator(),
            (DataCategory::Error, 1),
        )
        .reject_err(Outcome::RateLimited(
            limits.longest().and_then(|l| l.reason_code.clone()),
        ));
        return Ok(TextResponse(Some(EventId::new())));
    }

    let envelope = if MINIDUMP_RAW_CONTENT_TYPES.contains(&content_type.as_ref()) {
        raw_minidump_to_envelope(request, meta, &state, content_type, config, upload_context)
            .await?
    } else {
        let multipart = utils::multipart_from_request(request)?;
        multipart_to_envelope(multipart, meta, &state, config, upload_context).await?
    };

    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    common::handle_managed_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();

    // The return here is only useful for consistency because the UE4 crash reporter doesn't
    // care about it.
    Ok(TextResponse(id))
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(RequestBodyLimitLayer::new(
            config.max_upload_size() + config.max_attachments_size(),
        ))
        .route_layer(DefaultBodyLimit::disable())
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}

#[cfg(test)]
mod tests {
    use crate::envelope::ContentType;
    use crate::utils::FormDataIter;
    use axum::body::Body;
    use bzip2::Compression as BzCompression;
    use bzip2::write::BzEncoder;
    use flate2::Compression as GzCompression;
    use flate2::write::GzEncoder;
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
            let decoder = decoder_from(compressed).unwrap();
            assert!(run_decoder(decoder).is_ok());

            let le_minidump = b"MDMPxxxxxx";
            let compressed = encoder(le_minidump)?;
            let decoder = decoder_from(compressed).unwrap();
            assert!(run_decoder(decoder).is_ok());

            let garbage = b"xxxxxx";
            let compressed = encoder(garbage)?;
            let decoder = decoder_from(compressed).unwrap();
            let decoded = run_decoder(decoder);
            assert!(decoded.is_ok());
            assert!(validate_minidump(&decoded.unwrap()).is_err());
        }

        Ok(())
    }

    #[test]
    fn test_decode_minidump_size_limit() -> Result<(), Box<dyn std::error::Error>> {
        // Create a minidump that will decompress to 100 bytes
        let minidump_data = b"xxxxxxxxxx".repeat(10);
        let compressed = encode_gzip(&minidump_data)?;

        // With a limit larger than the decompressed size, decoding should succeed
        let result = decode_minidump(compressed.clone(), 200);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 100);

        // With a limit smaller than the decompressed size, decoding should fail with Overflow
        let result = decode_minidump(compressed, 50);
        assert!(matches!(result, Err(BadStoreRequest::Overflow(_))));

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
    async fn test_minidump_multipart_attachments() {
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
            -----MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw---\x0d\x0a\
            Content-Disposition: form-data; name=\"view-hierarchy.json\"; filename=\"view-hierarchy.json\"\x0d\x0a\
            Content-Type: application/json\x0d\x0a\x0d\x0a\
            {\"rendering_system\":\"android_view_system\",\"windows\":[{\"type\":\"com.android.internal.policy.DecorView\",\"width\":768.0,\"height\":1280.0,\"x\":0.0,\"y\":0.0,\"visibility\":\"visible\",\"alpha\":1.0}]}\x0d\x0a\
            -----MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw-----\x0d\x0a";

        let request = Request::builder()
            .header(
                "content-type",
                "multipart/form-data; boundary=---MultipartBoundary-sQ95dYmFvVzJ2UcOSdGPBkqrW0syf0Uw---",
            )
            .body(Body::from(multipart_body)).unwrap();

        let config = Config::default();

        let request_meta = RequestMeta::new(
            "https://a94ae32be2582e0bbd7a4cbb95971fee:@sentry.io/42"
                .parse()
                .unwrap(),
        );
        let multipart = utils::multipart_from_request(request).unwrap();
        let items = utils::multipart_items(
            multipart,
            &config,
            MinidumpAttachmentStrategy {
                upload_context: None,
            },
            &request_meta,
            &Addr::dummy(),
        )
        .await
        .unwrap();

        // we expect the multipart body to contain
        // * one arbitrary attachment from the user (a `config.json`)
        // * two breadcrumb files
        // * one event file
        // * one form-data item
        // * one view-hierarchy file
        assert_eq!(6, items.len());

        // `config.json` has no content-type. MIME-detection in later processing will assign this.
        let item = &items[0];
        assert_eq!(item.filename().unwrap(), "config.json");
        assert!(item.content_type().is_none());
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(item.attachment_type().unwrap(), AttachmentType::Attachment);
        assert_eq!(item.payload().len(), 95);

        // the first breadcrumb buffer
        let item = &items[1];
        assert_eq!(item.filename().unwrap(), "__sentry-breadcrumb1");
        assert_eq!(item.content_type().unwrap(), ContentType::OctetStream);
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(item.attachment_type().unwrap(), AttachmentType::Breadcrumbs);
        assert_eq!(item.payload().len(), 66);

        // the second breadcrumb buffer is empty since we haven't reached our max in the first
        let item = &items[2];
        assert_eq!(item.filename().unwrap(), "__sentry-breadcrumb2");
        assert_eq!(item.content_type().unwrap(), ContentType::OctetStream);
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(item.attachment_type().unwrap(), AttachmentType::Breadcrumbs);
        assert_eq!(item.payload().len(), 0);

        // the msg-pack encoded event file
        let item = &items[3];
        assert_eq!(item.filename().unwrap(), "__sentry-event");
        assert_eq!(item.content_type().unwrap(), ContentType::OctetStream);
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(
            item.attachment_type().unwrap(),
            AttachmentType::EventPayload
        );
        assert_eq!(item.payload().len(), 29);

        // the next item is the view-hierarchy file
        let item = &items[4];
        assert_eq!(item.filename().unwrap(), "view-hierarchy.json");
        assert_eq!(item.content_type().unwrap(), ContentType::Json);
        assert_eq!(item.ty(), &ItemType::Attachment);
        assert_eq!(
            item.attachment_type().unwrap(),
            AttachmentType::ViewHierarchy
        );
        assert_eq!(item.payload().len(), 184);

        // the last item is the form-data if any and contains a `guid` from the `crashpad_handler`
        let item = &items[5];
        assert!(item.filename().is_none());
        assert_eq!(item.content_type().unwrap(), ContentType::Text);
        assert_eq!(item.ty(), &ItemType::FormData);
        assert!(item.attachment_type().is_none());
        let form_payload = item.payload();
        let form_data_entry = FormDataIter::new(form_payload.as_ref()).next().unwrap();
        assert_eq!(form_data_entry.key(), "guid");
        assert_eq!(
            form_data_entry.value(),
            "dd46bb04-bb27-448c-aad0-0deb0c134bdb"
        );
    }
}
