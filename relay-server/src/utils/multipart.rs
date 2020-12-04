use std::convert::TryInto;
use std::io;

use actix::prelude::*;
use actix_web::{dev::Payload, error::PayloadError, multipart, HttpMessage, HttpRequest};
use bytes::Bytes;
use failure::Fail;
use futures::{future, Async, Future, Poll, Stream};
use serde::{Deserialize, Serialize};

use crate::envelope::{AttachmentType, ContentType, Item, ItemType, Items};
use crate::service::ServiceState;

#[derive(Debug, Fail)]
pub enum MultipartError {
    #[fail(display = "payload reached its size limit")]
    Overflow,

    #[fail(display = "{}", _0)]
    InvalidMultipart(actix_web::error::MultipartError),
}

/// A wrapper around an actix payload that always ends with a newline.
#[derive(Clone, Debug)]
struct TerminatedPayload {
    inner: Option<Payload>,
    end: Option<Bytes>,
}

impl TerminatedPayload {
    pub fn new(payload: Payload) -> Self {
        Self {
            inner: Some(payload),
            end: Some(Bytes::from_static(b"\r\n")),
        }
    }
}

impl Stream for TerminatedPayload {
    type Item = Bytes;
    type Error = PayloadError;

    #[inline]
    fn poll(&mut self) -> Poll<Option<Bytes>, PayloadError> {
        if let Some(ref mut inner) = self.inner {
            match inner.poll() {
                Ok(Async::Ready(option)) if option.is_none() => {
                    // Remove the stream to fuse, then fall through.
                    self.inner = None;
                }
                poll => return poll,
            }
        }

        Ok(Async::Ready(self.end.take()))
    }
}

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
        &self.0
    }

    pub fn value(&self) -> &'a str {
        &self.1
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

    pub fn into_item(self) -> Item {
        let mut item = Item::new(ItemType::FormData);
        // Content type is Text (since it is not a json object but multiple
        // json arrays serialized one after the other.
        item.set_payload(ContentType::Text, self.data);
        item
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

/// Reads data from a multipart field (coming from a HttpRequest).
///
/// This function returns `None` on overflow. The caller needs to coerce this into an appropriate
/// error. This function does not error directly on overflow to allow consuming the full stream.
pub fn consume_field<S>(
    field: multipart::Field<S>,
    max_size: usize,
) -> ResponseFuture<Vec<u8>, MultipartError>
where
    S: Stream<Item = Bytes, Error = PayloadError> + 'static,
{
    let future = field.map_err(MultipartError::InvalidMultipart).fold(
        Vec::with_capacity(512),
        move |mut body, chunk| {
            if (body.len() + chunk.len()) > max_size {
                Err(MultipartError::Overflow)
            } else {
                body.extend_from_slice(&chunk);
                Ok(body)
            }
        },
    );

    Box::new(future)
}

fn consume_item(
    mut content: MultipartItems,
    item: multipart::MultipartItem<TerminatedPayload>,
) -> ResponseFuture<MultipartItems, MultipartError> {
    let field = match item {
        multipart::MultipartItem::Nested(nested) => return consume_stream(content, nested),
        multipart::MultipartItem::Field(field) => field,
    };

    let content_type = field.content_type().to_string();
    let content_disposition = field.content_disposition();

    let future = consume_field(field, content.remaining_size).map(move |data| {
        content.remaining_size -= data.len();

        let field_name = content_disposition.as_ref().and_then(|d| d.get_name());
        let file_name = content_disposition.as_ref().and_then(|d| d.get_filename());

        if let Some(file_name) = file_name {
            let mut item = Item::new(ItemType::Attachment);
            item.set_attachment_type((*content.infer_type)(field_name));
            item.set_payload(content_type.into(), data);
            item.set_filename(file_name);
            content.items.push(item);
        } else if let Some(field_name) = field_name {
            // Ensure to decode this safely to match Django's POST data behavior. This allows us to
            // process sentry event payloads even if they contain invalid encoding.
            let string = String::from_utf8_lossy(&data);
            content.form_data.append(field_name, &string);
        } else {
            relay_log::trace!("multipart content without name or file_name");
        }

        content
    });

    Box::new(future)
}

fn consume_stream(
    content: MultipartItems,
    stream: multipart::Multipart<TerminatedPayload>,
) -> ResponseFuture<MultipartItems, MultipartError> {
    let future = stream
        .map_err(MultipartError::InvalidMultipart)
        .fold(content, consume_item);

    Box::new(future)
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

/// Internal structure used when constructing Envelopes to maintain a cap on the content size
pub struct MultipartItems {
    // Items of the envelope under construction.
    items: Items,
    // Keeps track of how much bigger is this envelope allowed to grow.
    remaining_size: usize,
    // Collect all form data in here.
    form_data: FormDataWriter,
    infer_type: Box<dyn Fn(Option<&str>) -> AttachmentType>,
}

impl MultipartItems {
    pub fn new(max_size: usize) -> Self {
        Self {
            items: Items::new(),
            remaining_size: max_size,
            form_data: FormDataWriter::new(),
            infer_type: Box::new(|_| AttachmentType::default()),
        }
    }

    pub fn infer_attachment_type<F>(mut self, f: F) -> Self
    where
        F: Fn(Option<&str>) -> AttachmentType + 'static,
    {
        self.infer_type = Box::new(f);
        self
    }

    pub fn handle_request(
        self,
        request: &HttpRequest<ServiceState>,
    ) -> ResponseFuture<Items, MultipartError> {
        // Do NOT use `request.multipart()` here. It calls request.payload() unconditionally, which
        // causes keep-alive streams to break. Instead, rely on the middleware to consume the
        // stream. This can happen, for instance, when the boundary is malformed.
        let boundary = match multipart::Multipart::boundary(request.headers()) {
            Ok(boundary) => boundary,
            Err(error) => return Box::new(future::err(MultipartError::InvalidMultipart(error))),
        };

        // The payload is internally clonable which allows to consume it at the end of this future.
        let payload = TerminatedPayload::new(request.payload());
        let multipart = multipart::Multipart::new(Ok(boundary), payload.clone());

        let future = consume_stream(self, multipart)
            .and_then(|multipart| {
                let mut items = multipart.items;

                let form_data = multipart.form_data.into_item();
                if !form_data.is_empty() {
                    items.push(form_data);
                }

                Ok(items)
            })
            .then(move |result| {
                // Consume the remaining stream but ignore errors.
                payload.for_each(|_| Ok(())).then(|_| result)
            });

        Box::new(future)
    }
}

#[cfg(test)]
mod tests {
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

        let item = writer.into_item();
        assert_eq!(item.ty(), ItemType::FormData);

        let payload = item.payload();
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
        let item = writer.into_item();

        let payload = item.payload();
        let iter = FormDataIter::new(&payload);
        let entries: Vec<_> = iter.collect();

        assert_eq!(entries, vec![]);
    }
}
