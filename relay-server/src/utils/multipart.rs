use std::io;
use std::task::Poll;

use axum::extract::Request;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use multer::{Field, Multipart};
use relay_config::Config;
use serde::{Deserialize, Serialize};

use crate::envelope::{AttachmentType, ContentType, Item, ItemType, Items};

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

pub async fn multipart_items<F>(
    mut multipart: Multipart<'_>,
    mut infer_type: F,
) -> Result<Items, multer::Error>
where
    F: FnMut(Option<&str>, &str) -> AttachmentType,
{
    let mut items = Items::new();
    let mut form_data = FormDataWriter::new();

    while let Some(field) = multipart.next_field().await? {
        if let Some(file_name) = field.file_name() {
            let mut item = Item::new(ItemType::Attachment);
            item.set_attachment_type(infer_type(field.name(), file_name));
            item.set_filename(file_name);
            // Extract the body after the immutable borrow on `file_name` is gone.
            if let Some(content_type) = field.content_type() {
                item.set_payload(content_type.as_ref().into(), field.bytes().await?);
            } else {
                item.set_payload_without_content_type(field.bytes().await?);
            }
            items.push(item);
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
        items.push(item);
    }

    Ok(items)
}

/// Wrapper around `multer::Field` which consumes the entire underlying stream even when the
/// size limit is exceeded.
///
/// The idea being that you can process fields in a multi-part form even if one fields is too large.
struct LimitedField<'a> {
    field: Field<'a>,
    consumed_size: usize,
    size_limit: usize,
    inner_finished: bool,
}

impl<'a> LimitedField<'a> {
    fn new(field: Field<'a>, limit: usize) -> Self {
        LimitedField {
            field,
            consumed_size: 0,
            size_limit: limit,
            inner_finished: false,
        }
    }

    async fn bytes(self) -> Result<Bytes, multer::Error> {
        self.try_fold(BytesMut::new(), |mut acc, x| async move {
            acc.extend_from_slice(&x);
            Ok(acc)
        })
        .await
        .map(|x| x.freeze())
    }
}

impl futures::Stream for LimitedField<'_> {
    type Item = Result<Bytes, multer::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.inner_finished {
            return Poll::Ready(None);
        }

        match self.field.poll_next_unpin(cx) {
            err @ Poll::Ready(Some(Err(_))) => err,
            Poll::Ready(Some(Ok(t))) => {
                self.consumed_size += t.len();
                match self.consumed_size <= self.size_limit {
                    true => Poll::Ready(Some(Ok(t))),
                    false => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            Poll::Ready(None) if self.consumed_size > self.size_limit => {
                self.inner_finished = true;
                Poll::Ready(Some(Err(multer::Error::FieldSizeExceeded {
                    limit: self.size_limit as u64,
                    field_name: self.field.name().map(Into::into),
                })))
            }
            Poll::Ready(None) => {
                self.inner_finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// TODO: (Tobias we want to merge that with the existing function).
pub async fn filtered_multipart_items<F>(
    mut multipart: Multipart<'_>,
    mut infer_type: F,
    config: &Config,
) -> Result<Items, multer::Error>
where
    F: FnMut(Option<&str>, &str) -> AttachmentType,
{
    let mut items = Items::new();
    let mut form_data = FormDataWriter::new();

    while let Some(field) = multipart.next_field().await? {
        if let Some(file_name) = field.file_name() {
            let mut item = Item::new(ItemType::Attachment);
            item.set_attachment_type(infer_type(field.name(), file_name));
            item.set_filename(file_name);

            let content_type = field.content_type().cloned();
            let field = LimitedField::new(field, config.max_attachment_size());
            match field.bytes().await {
                Err(multer::Error::FieldSizeExceeded { .. }) => continue,
                Err(err) => return Err(err),
                Ok(bytes) => {
                    if let Some(content_type) = content_type {
                        item.set_payload(content_type.as_ref().into(), bytes);
                    } else {
                        item.set_payload_without_content_type(bytes);
                    }
                }
            }

            items.push(item);
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
        items.push(item);
    }

    Ok(items)
}

pub fn multipart_from_request(
    request: Request,
    config: &Config,
) -> Result<Multipart<'static>, multer::Error> {
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let boundary = multer::parse_boundary(content_type)?;

    // Only enforce the stream limit here as the `per_field` limit is enforced by `LimitedField`.
    let limits = multer::SizeLimit::new().whole_stream(config.max_attachments_size() as u64);

    Ok(Multipart::with_constraints(
        request.into_body().into_data_stream(),
        boundary,
        multer::Constraints::new().size_limit(limits),
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
    async fn missing_trailing_newline() -> anyhow::Result<()> {
        let data = "--X-BOUNDARY\r\nContent-Disposition: form-data; \
        name=\"my_text_field\"\r\n\r\nabcd\r\n--X-BOUNDARY--"; // No trailing newline

        let stream = futures::stream::once(async { Ok::<_, Infallible>(data) });
        let mut multipart = Multipart::new(stream, "X-BOUNDARY");

        assert!(multipart.next_field().await?.is_some());
        assert!(multipart.next_field().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_individual_size_limit_exceeded() -> anyhow::Result<()> {
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

        let stream = futures::stream::once(async { Ok::<_, Infallible>(data) });
        let multipart = Multipart::new(stream, "X-BOUNDARY");

        let config = Config::from_json_value(serde_json::json!({
            "limits": {
                "max_attachment_size": 5
            }
        }))?;

        let items =
            filtered_multipart_items(multipart, |_, _| AttachmentType::Attachment, &config).await?;

        // The large field is skipped so only the small one should make it through.
        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.filename(), Some("small.txt"));
        assert_eq!(item.payload(), Bytes::from("ok"));

        Ok(())
    }

    #[tokio::test]
    async fn test_collective_size_limit_exceeded() -> anyhow::Result<()> {
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

        let stream = futures::stream::once(async { Ok::<_, Infallible>(data) });

        let config = Config::from_json_value(serde_json::json!({
            "limits": {
                "max_attachments_size": 5
            }
        }))?;
        let limits = multer::SizeLimit::new().whole_stream(config.max_attachments_size() as u64);

        let multipart = Multipart::with_constraints(
            stream,
            "X-BOUNDARY",
            multer::Constraints::new().size_limit(limits),
        );

        let result =
            filtered_multipart_items(multipart, |_, _| AttachmentType::Attachment, &config).await;

        // Should be warned if the overall stream limit is being breached.
        assert!(result.is_err_and(|x| matches!(x, multer::Error::StreamSizeExceeded { limit: _ })));

        Ok(())
    }
}
