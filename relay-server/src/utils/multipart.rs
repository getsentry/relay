use std::future::Future;
use std::io;
use std::sync::Arc;

use axum::extract::Request;
use bytes::Bytes;
use futures::TryStreamExt;
use multer::{Field, Multipart};
use relay_config::Config;
use relay_system::Addr;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

use crate::endpoints::common::BadStoreRequest;
use crate::envelope::{AttachmentType, ContentType, Item, ItemType, ManagedItems};
use crate::extractors::RequestMeta;
use crate::managed::{self, Managed, Meta};
use crate::services::outcome::{
    DiscardAttachmentType, DiscardItemType, DiscardReason, Outcome, TrackOutcome,
};

/// Type used for encoding string lengths.
type Len = u32;

/// Serializes a Pascal-style string with a 4 byte little-endian length prefix.
fn write_string<W>(mut writer: W, string: &str) -> io::Result<()>
where
    W: io::Write,
{
    writer.write_all(&(string.len() as Len).to_le_bytes())?;
    writer.write_all(string.as_bytes())?;

    Ok(())
}

/// Safely consumes a slice of the given length.
fn split_front<'a>(data: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    if data.len() < len {
        *data = &[];
        return None;
    }

    let (slice, rest) = data.split_at(len);
    *data = rest;
    Some(slice)
}

/// Consumes the 4-byte length prefix of a string.
fn consume_len(data: &mut &[u8]) -> Option<usize> {
    let len = std::mem::size_of::<Len>();
    let slice = split_front(data, len)?;
    let bytes = slice.try_into().ok();
    bytes.map(|b| Len::from_le_bytes(b) as usize)
}

/// Consumes a Pascal-style string with a 4 byte little-endian length prefix.
fn consume_string<'a>(data: &mut &'a [u8]) -> Option<&'a str> {
    let len = consume_len(data)?;
    let bytes = split_front(data, len)?;
    std::str::from_utf8(bytes).ok()
}

/// An entry in a serialized form data item.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FormDataEntry<'a>(&'a str, &'a str);

impl<'a> FormDataEntry<'a> {
    pub fn new(key: &'a str, value: &'a str) -> Self {
        Self(key, value)
    }

    pub fn key(&self) -> &'a str {
        self.0
    }

    pub fn value(&self) -> &'a str {
        self.1
    }

    fn to_writer<W: io::Write>(&self, mut writer: W) {
        write_string(&mut writer, self.key()).ok();
        write_string(&mut writer, self.value()).ok();
    }

    fn read(data: &mut &'a [u8]) -> Option<Self> {
        let key = consume_string(data)?;
        let value = consume_string(data)?;
        Some(Self::new(key, value))
    }
}

/// A writer for serialized form data.
///
/// This writer is used to serialize multiple plain fields from a multipart form data request into a
/// single envelope item. Use `FormDataIter` to iterate all entries.
struct FormDataWriter {
    data: Vec<u8>,
}

impl FormDataWriter {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn append(&mut self, key: &str, value: &str) {
        let entry = FormDataEntry::new(key, value);
        entry.to_writer(&mut self.data);
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.data
    }
}

/// Iterates through serialized form data written with `FormDataWriter`.
pub struct FormDataIter<'a> {
    data: &'a [u8],
}

impl<'a> FormDataIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> Iterator for FormDataIter<'a> {
    type Item = FormDataEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.data.is_empty() {
            match FormDataEntry::read(&mut self.data) {
                Some(entry) => return Some(entry),
                None => relay_log::error!("form data deserialization failed"),
            }
        }

        None
    }
}

/// Looks for a multipart boundary at the beginning of the data
/// and returns it as a `&str` if it is found
///
/// A multipart boundary starts at the beginning of the data (possibly
/// after some blank lines) and it is prefixed by '--' (two dashes)
///
/// ```ignore
/// let boundary = get_multipart_boundary(b"--The boundary\r\n next line");
/// assert_eq!(Some("The boundary"), boundary);
///
/// let invalid_boundary = get_multipart_boundary(b"The boundary\r\n next line");
/// assert_eq!(None, invalid_boundary);
/// ```
pub fn get_multipart_boundary(data: &[u8]) -> Option<&str> {
    data.split(|&byte| byte == b'\r' || byte == b'\n')
        // Get the first non-empty line
        .find(|slice| !slice.is_empty())
        // Check for the form boundary indicator
        .filter(|slice| slice.len() > 2 && slice.starts_with(b"--"))
        // Form boundaries must be valid UTF-8 strings
        .and_then(|slice| std::str::from_utf8(&slice[2..]).ok())
}

/// Strategy for how to infer attachment type and add a multipart attachment to an envelope item.
///
/// This enables different endpoints to have different ways of dealing with multipart attachments,
/// for instance, one endpoint can upload attachments and add a ref to the item, while another
/// endpoint can add attachments to items directly.
pub trait AttachmentStrategy {
    fn infer_type(&self, field: &Field) -> AttachmentType;

    /// Defines how individual multipart items should be handled.
    ///
    /// Returns
    ///  - `Ok(Some(item))` if everything was successful.
    ///  - `Ok(None)` if there was an error adding the attachment, but the rest of the request
    ///    should still be handled.
    ///  - `Err(..)` if there was an unexpected error adding the attachment and the request should
    ///    be cancelled.
    fn add_to_item(
        &self,
        field: Field<'static>,
        item: Managed<Item>,
        config: &Config,
    ) -> impl Future<Output = Result<Option<Managed<Item>>, multer::Error>> + Send;
}

pub async fn read_attachment_bytes_into_item(
    field: Field<'static>,
    mut item: Managed<Item>,
    config: &Config,
    ignore_size_exceeded: bool,
) -> Result<Option<Managed<Item>>, multer::Error> {
    let content_type = field
        .content_type()
        .map(|ct| ct.as_ref().parse().unwrap_or(ContentType::OctetStream));
    let field_name = field.name().map(String::from);
    let limit = config.max_attachment_size();
    let mut buf = Vec::new();
    StreamReader::new(field.map_err(io::Error::other))
        .take((limit + 1) as u64) // Extra byte needed to determine if limit was exceeded.
        .read_to_end(&mut buf)
        .await
        .map_err(|e| multer::Error::StreamReadFailed(Box::new(e)))?;
    let bytes = Bytes::from(buf);
    let n_bytes = bytes.len();
    item.set_attachment_payload(content_type, bytes);
    if n_bytes > limit {
        let attachment_type = item.attachment_type().unwrap_or(AttachmentType::Attachment);
        let _ = item.reject_err(Outcome::Invalid(DiscardReason::TooLarge(
            DiscardItemType::Attachment(DiscardAttachmentType::from(attachment_type)),
        )));
        return match ignore_size_exceeded {
            true => Ok(None),
            false => Err(multer::Error::FieldSizeExceeded {
                limit: limit as u64,
                field_name,
            }),
        };
    }
    Ok(Some(item))
}

pub async fn multipart_items(
    mut multipart: Multipart<'static>,
    config: &Config,
    outcome_aggregator: Addr<TrackOutcome>,
    request_meta: &RequestMeta,
    attachment_strategy: impl AttachmentStrategy,
) -> Result<ManagedItems, multer::Error> {
    let mut items = ManagedItems::new();
    let mut form_data = FormDataWriter::new();
    let mut attachments_size = 0;
    let outcome_meta = Arc::new(Meta::from_request_meta(request_meta, outcome_aggregator));

    while let Some(field) = multipart.next_field().await? {
        if let Some(file_name) = field.file_name() {
            let mut item = Item::new(ItemType::Attachment);
            let attachment_type = attachment_strategy.infer_type(&field);
            item.set_attachment_type(attachment_type);
            item.set_filename(file_name);
            let item = Managed::from_parts(item, outcome_meta.clone());
            let item = attachment_strategy.add_to_item(field, item, config).await?;
            if let Some(item) = item {
                // This increases the attachments byte count even if the item is an attachment ref.
                // This is by design as the total number of bytes read into memory should be
                // constrained.
                attachments_size += item.len();
                items.push(item);
                if attachments_size > config.max_attachments_size() {
                    managed::reject_all(
                        &items,
                        Outcome::Invalid(DiscardReason::TooLarge(DiscardItemType::Attachment(
                            DiscardAttachmentType::Attachment,
                        ))),
                    );
                    return Err(multer::Error::StreamSizeExceeded {
                        limit: config.max_attachments_size() as u64,
                    });
                }
            }
        } else if let Some(field_name) = field.name().map(str::to_owned) {
            // Ensure to decode this SAFELY to match Django's POST data behavior. This allows us to
            // process sentry event payloads even if they contain invalid encoding.
            let string = field.text().await?;
            form_data.append(&field_name, &string);
        } else {
            relay_log::trace!("multipart content without name or file_name");
        }
    }

    let form_data = form_data.into_inner();
    if !form_data.is_empty() {
        let mut item = Item::new(ItemType::FormData);
        // Content type is `Text` (since it is not a json object but multiple
        // json arrays serialized one after the other).
        item.set_payload(ContentType::Text, form_data);
        let item = Managed::from_parts(item, outcome_meta);
        items.push(item);
    }

    Ok(items)
}

pub fn multipart_from_request(request: Request) -> Result<Multipart<'static>, BadStoreRequest> {
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let boundary =
        multer::parse_boundary(content_type).map_err(BadStoreRequest::InvalidMultipart)?;
    Ok(Multipart::new(
        request.into_body().into_data_stream(),
        boundary,
    ))
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    #[test]
    fn test_get_boundary() {
        let examples: &[(&[u8], Option<&str>)] = &[
            (b"--some_val", Some("some_val")),
            (b"--\nsecond line", None),
            (b"\n\r--some_val", Some("some_val")),
            (b"\n\r--some_val\nadfa", Some("some_val")),
            (b"\n\r--some_val\rfasdf", Some("some_val")),
            (b"\n\r--some_val\r\nfasdf", Some("some_val")),
            (b"\n\rsome_val", None),
            (b"", None),
            (b"--", None),
        ];

        for (input, expected) in examples {
            let boundary = get_multipart_boundary(input);
            assert_eq!(*expected, boundary);
        }
    }

    #[test]
    fn test_formdata() {
        let mut writer = FormDataWriter::new();
        writer.append("foo", "foo");
        writer.append("bar", "");
        writer.append("blub", "blub");

        let payload = writer.into_inner();
        let iter = FormDataIter::new(&payload);
        let entries: Vec<_> = iter.collect();

        assert_eq!(
            entries,
            vec![
                FormDataEntry::new("foo", "foo"),
                FormDataEntry::new("bar", ""),
                FormDataEntry::new("blub", "blub"),
            ]
        );
    }

    #[test]
    fn test_empty_formdata() {
        let writer = FormDataWriter::new();
        let payload = writer.into_inner();

        let iter = FormDataIter::new(&payload);
        let entries: Vec<_> = iter.collect();

        assert_eq!(entries, vec![]);
    }

    /// Regression test for multipart payloads without a trailing newline.
    #[tokio::test]
    async fn missing_trailing_newline() {
        let data = "--X-BOUNDARY\r\nContent-Disposition: form-data; \
        name=\"my_text_field\"\r\n\r\nabcd\r\n--X-BOUNDARY--"; // No trailing newline

        let stream = futures::stream::once(async { Ok::<_, Infallible>(data) });
        let mut multipart = Multipart::new(stream, "X-BOUNDARY");

        assert!(multipart.next_field().await.unwrap().is_some());
        assert!(multipart.next_field().await.unwrap().is_none());
    }

    fn test_request_meta() -> RequestMeta {
        let dsn = "https://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.io/42"
            .parse()
            .unwrap();
        RequestMeta::new(dsn)
    }

    #[tokio::test]
    async fn test_individual_size_limit_exceeded() {
        let data = "--X-BOUNDARY\r\n\
              Content-Disposition: form-data; name=\"file\"; filename=\"large.txt\"\r\n\
              Content-Type: text/plain\r\n\
              \r\n\
              content too large for limit\r\n\
              --X-BOUNDARY\r\n\
              Content-Disposition: form-data; name=\"small_file\"; filename=\"small.txt\"\r\n\
              Content-Type: text/plain\r\n\
              \r\n\
              ok\r\n\
              --X-BOUNDARY--\r\n";

        let stream = futures::stream::once(async move { Ok::<_, Infallible>(data) });
        let multipart = Multipart::new(stream, "X-BOUNDARY");

        let config = Config::from_json_value(serde_json::json!({
            "limits": {
                "max_attachment_size": 5
            }
        }))
        .unwrap();

        struct MockAttachmentStrategy;
        impl AttachmentStrategy for MockAttachmentStrategy {
            fn add_to_item(
                &self,
                field: Field<'static>,
                item: Managed<Item>,
                config: &Config,
            ) -> impl Future<Output = Result<Option<Managed<Item>>, multer::Error>> + Send
            {
                read_attachment_bytes_into_item(field, item, config, false)
            }

            fn infer_type(&self, _: &Field) -> AttachmentType {
                AttachmentType::Attachment
            }
        }

        let (outcome_aggregator, _rx) = Addr::custom();
        let meta = test_request_meta();
        let res = multipart_items(
            multipart,
            &config,
            outcome_aggregator,
            &meta,
            MockAttachmentStrategy,
        )
        .await;
        assert!(res.is_err_and(|x| matches!(x, multer::Error::FieldSizeExceeded { .. })));
    }

    #[tokio::test]
    async fn test_collective_size_limit_exceeded() {
        let data = "--X-BOUNDARY\r\n\
              Content-Disposition: form-data; name=\"file\"; filename=\"large.txt\"\r\n\
              Content-Type: text/plain\r\n\
              \r\n\
              content too large for limit\r\n\
              --X-BOUNDARY\r\n\
              Content-Disposition: form-data; name=\"small_file\"; filename=\"small.txt\"\r\n\
              Content-Type: text/plain\r\n\
              \r\n\
              ok\r\n\
              --X-BOUNDARY--\r\n";

        let stream = futures::stream::once(async move { Ok::<_, Infallible>(data) });

        let config = &Config::from_json_value(serde_json::json!({
            "limits": {
                "max_attachments_size": 5
            }
        }))
        .unwrap();

        let multipart = Multipart::new(stream, "X-BOUNDARY");

        struct MockAttachmentStrategy;
        impl AttachmentStrategy for MockAttachmentStrategy {
            fn add_to_item(
                &self,
                field: Field<'static>,
                item: Managed<Item>,
                config: &Config,
            ) -> impl Future<Output = Result<Option<Managed<Item>>, multer::Error>> + Send
            {
                read_attachment_bytes_into_item(field, item, config, true)
            }

            fn infer_type(&self, _: &Field) -> AttachmentType {
                AttachmentType::Attachment
            }
        }

        let (outcome_aggregator, _rx) = Addr::custom();
        let meta = test_request_meta();
        let result = multipart_items(
            multipart,
            config,
            outcome_aggregator,
            &meta,
            MockAttachmentStrategy,
        )
        .await;

        // Should be warned if the overall stream limit is being breached.
        assert!(result.is_err_and(|x| matches!(x, multer::Error::StreamSizeExceeded { limit: _ })));
    }
}
