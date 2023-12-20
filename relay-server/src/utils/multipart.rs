use std::convert::TryInto;
use std::io;

use axum::extract::multipart::{Field, Multipart};
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

#[derive(Debug, thiserror::Error)]
pub enum MultipartError {
    #[error("field exceeded the size limit")]
    FieldSizeExceeded,
    #[error(transparent)]
    Raw(#[from] axum::extract::multipart::MultipartError),
}

async fn field_data<'a>(field: &mut Field<'a>, limit: usize) -> Result<Vec<u8>, MultipartError> {
    let mut body = Vec::new();

    while let Some(chunk) = field.chunk().await? {
        if body.len() + chunk.len() > limit {
            return Err(MultipartError::FieldSizeExceeded);
        }
        body.extend_from_slice(&chunk);
    }

    Ok(body)
}

pub async fn multipart_items<F>(
    mut multipart: Multipart,
    item_limit: usize,
    mut infer_type: F,
) -> Result<Items, MultipartError>
where
    F: FnMut(Option<&str>) -> AttachmentType,
{
    let mut items = Items::new();
    let mut form_data = FormDataWriter::new();

    while let Some(mut field) = multipart.next_field().await? {
        if let Some(file_name) = field.file_name() {
            let mut item = Item::new(ItemType::Attachment);
            item.set_attachment_type(infer_type(field.name()));
            item.set_filename(file_name);
            let content_type = match field.content_type() {
                Some(string) => string.into(),
                None => ContentType::OctetStream,
            };
            // Extract the body after the immutable borrow on `file_name` is gone.
            item.set_payload(content_type, field_data(&mut field, item_limit).await?);
            items.push(item);
        } else if let Some(field_name) = field.name().map(str::to_owned) {
            let data = field_data(&mut field, item_limit).await?;
            // Ensure to decode this safely to match Django's POST data behavior. This allows us to
            // process sentry event payloads even if they contain invalid encoding.
            let string = String::from_utf8_lossy(&data);
            form_data.append(&field_name, &string);
        } else {
            relay_log::trace!("multipart content without name or file_name");
        }
    }

    let form_data = form_data.into_inner();
    if !form_data.is_empty() {
        let mut item = Item::new(ItemType::FormData);
        // Content type is Text (since it is not a json object but multiple
        // json arrays serialized one after the other.
        item.set_payload(ContentType::Text, form_data);
        items.push(item);
    }

    Ok(items)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::Request;

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

        let request = Request::builder()
            .header("content-type", "multipart/form-data; boundary=X-BOUNDARY")
            .body(Body::from(data))
            .unwrap();

        let mut multipart = Multipart::from_request(request, &()).await?;
        assert!(multipart.next_field().await?.is_some());
        assert!(multipart.next_field().await?.is_none());

        Ok(())
    }
}
