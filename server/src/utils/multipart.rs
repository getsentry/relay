use actix::prelude::*;
use actix_web::{dev::Payload, error::PayloadError, multipart, HttpMessage, HttpRequest};
use bytes::Bytes;
use failure::Fail;
use futures::{future, Future, Stream};
use serde::{Deserialize, Serialize};

use semaphore_common::LogError;

use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::service::ServiceState;

#[derive(Debug, Fail)]
pub enum MultipartError {
    #[fail(display = "payload reached its size limit")]
    Overflow,

    #[fail(display = "{}", _0)]
    InvalidMultipart(actix_web::error::MultipartError),
}

// An entry in a serialized form data item.
#[derive(Deserialize, Serialize)]
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
        serde_json::ser::to_writer(&mut self.data, &entry).ok();
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
    iter: serde_json::de::StreamDeserializer<'a, serde_json::de::SliceRead<'a>, FormDataEntry<'a>>,
}

impl<'a> FormDataIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            iter: serde_json::Deserializer::from_slice(data).into_iter(),
        }
    }
}

impl<'a> Iterator for FormDataIter<'a> {
    type Item = FormDataEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        for result in &mut self.iter {
            match result {
                Ok(entry) => return Some(entry),
                Err(error) => log::error!("form data deserialization failed: {}", LogError(&error)),
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
) -> ResponseFuture<Option<Vec<u8>>, MultipartError>
where
    S: Stream<Item = Bytes, Error = PayloadError> + 'static,
{
    let future = field.map_err(MultipartError::InvalidMultipart).fold(
        Some(Vec::with_capacity(512)),
        move |body_opt, chunk| {
            Ok(body_opt.and_then(|mut body| {
                if (body.len() + chunk.len()) > max_size {
                    None
                } else {
                    body.extend_from_slice(&chunk);
                    Some(body)
                }
            }))
        },
    );

    Box::new(future)
}

fn consume_item(
    mut content: MultipartEnvelope,
    item: multipart::MultipartItem<Payload>,
) -> ResponseFuture<Option<MultipartEnvelope>, MultipartError> {
    let field = match item {
        multipart::MultipartItem::Nested(nested) => return consume_stream(content, nested),
        multipart::MultipartItem::Field(field) => field,
    };

    let content_type = field.content_type().to_string();
    let content_disposition = field.content_disposition();

    let future = consume_field(field, content.remaining_size).map(move |data_opt| {
        let data = data_opt?;
        content.remaining_size -= data.len();

        let field_name = content_disposition.as_ref().and_then(|d| d.get_name());
        let file_name = content_disposition.as_ref().and_then(|d| d.get_filename());

        if let Some(file_name) = file_name {
            let mut item = Item::new(ItemType::Attachment);
            item.set_payload(content_type.into(), data);
            item.set_filename(file_name);
            if let Some(field_name) = field_name {
                item.set_name(field_name);
            }

            content.envelope.add_item(item);
        } else if let Some(field_name) = field_name {
            match std::str::from_utf8(&data) {
                Ok(value) => content.form_data.append(field_name, value),
                Err(_failure) => log::trace!("invalid text value in multipart item"),
            }
        } else {
            log::trace!("multipart content without name or file_name");
        }
        Some(content)
    });

    Box::new(future)
}

fn consume_stream(
    content: MultipartEnvelope,
    stream: multipart::Multipart<Payload>,
) -> ResponseFuture<Option<MultipartEnvelope>, MultipartError> {
    // Ensure that we consume the entire stream here. If we overflow at a certain point,
    // `consume_item` will return `None`. We need to continue folding, however, to ensure that we
    // consume the entire request payload.
    let future = stream.map_err(MultipartError::InvalidMultipart).fold(
        Some(content),
        move |content_opt, item| match content_opt {
            Some(content) => consume_item(content, item),
            None => Box::new(future::ok(None)),
        },
    );

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
pub struct MultipartEnvelope {
    // the envelope under construction
    envelope: Envelope,
    // keeps track of how much bigger is this envelope allowed to grow
    remaining_size: usize,
    // collect all form data in here
    form_data: FormDataWriter,
}

impl MultipartEnvelope {
    pub fn new(envelope: Envelope, max_size: usize) -> Self {
        Self {
            envelope,
            remaining_size: max_size,
            form_data: FormDataWriter::new(),
        }
    }

    pub fn handle_request(
        self,
        request: &HttpRequest<ServiceState>,
    ) -> ResponseFuture<Envelope, MultipartError> {
        let future = consume_stream(self, request.multipart()).and_then(|multipart_opt| {
            let multipart = multipart_opt.ok_or(MultipartError::Overflow)?;
            let mut envelope = multipart.envelope;

            let form_data = multipart.form_data.into_item();
            if !form_data.is_empty() {
                envelope.add_item(form_data);
            }

            Ok(envelope)
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
}
