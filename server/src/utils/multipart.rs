use actix::prelude::*;
use actix_web::{dev::Payload, multipart, HttpMessage, HttpRequest};
use failure::Fail;
use futures::future;
use futures::prelude::*;

use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::service::ServiceState;

#[derive(Debug, Fail)]
pub enum MultipartError {
    #[fail(display = "payload reached its size limit")]
    Overflow,

    #[fail(display = "{}", _0)]
    InvalidMultipart(actix_web::error::MultipartError),
}

fn handle_multipart_item(
    mut content: MultipartEnvelope,
    item: multipart::MultipartItem<Payload>,
) -> ResponseFuture<MultipartEnvelope, MultipartError> {
    let field = match item {
        multipart::MultipartItem::Field(field) => field,
        multipart::MultipartItem::Nested(nested) => {
            return handle_multipart_stream(content, nested);
        }
    };

    let (name, file_name) = field
        .content_disposition()
        .as_ref()
        .map_or((None, None), |d| {
            (
                d.get_name().map(String::from),
                d.get_filename().map(String::from),
            )
        });

    match (name, file_name) {
        (name, Some(file_name)) => {
            let result = read_multipart_data(field, content.remaining_size).and_then(move |data| {
                content.remaining_size -= data.len();

                let mut item = Item::new(ItemType::Attachment);
                item.set_payload(ContentType::OctetStream, data);
                item.set_filename(file_name);
                if let Some(name) = name {
                    item.set_name(name);
                }

                content.envelope.add_item(item);
                Ok(content)
            });
            Box::new(result)
        }
        (Some(name), None) => {
            let result = read_multipart_data(field, content.remaining_size).and_then(move |data| {
                content.remaining_size -= data.len();
                match std::str::from_utf8(&data) {
                    Ok(value) => {
                        serde_json::ser::to_writer(&mut content.form_data, &[name.as_str(), value])
                            .ok();
                    }
                    Err(_failure) => {
                        log::trace!("invalid text value in multipart item");
                    }
                }
                Ok(content)
            });
            Box::new(result)
        }
        (None, None) => {
            log::trace!("multipart content without name or file_name");
            Box::new(future::ok(content))
        }
    }
}

fn handle_multipart_stream(
    content: MultipartEnvelope,
    stream: multipart::Multipart<Payload>,
) -> ResponseFuture<MultipartEnvelope, MultipartError> {
    let future = stream
        .map_err(MultipartError::InvalidMultipart)
        .fold(content, move |content, item| {
            handle_multipart_item(content, item)
        });

    Box::new(future)
}

/// Reads data from a multipart field (coming from a HttpRequest)
fn read_multipart_data(
    field: multipart::Field<Payload>,
    max_size: usize,
) -> ResponseFuture<Vec<u8>, MultipartError> {
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

/// Internal structure used when constructing Envelopes to maintain a cap on the content size
pub struct MultipartEnvelope {
    // the envelope under construction
    envelope: Envelope,
    // keeps track of how much bigger is this envelope allowed to grow
    remaining_size: usize,
    // collect all form data in here
    form_data: Vec<u8>,
}

impl MultipartEnvelope {
    pub fn new(envelope: Envelope, max_size: usize) -> Self {
        Self {
            envelope,
            remaining_size: max_size,
            form_data: Vec::new(),
        }
    }

    pub fn handle_request(
        self,
        request: &HttpRequest<ServiceState>,
    ) -> ResponseFuture<Envelope, MultipartError> {
        let future = handle_multipart_stream(self, request.multipart()).and_then(|multipart| {
            let mut envelope = multipart.envelope;

            if !multipart.form_data.is_empty() {
                let mut item = Item::new(ItemType::FormData);
                // Content type is Text (since it is not a json object but multiple
                // json arrays serialized one after the other.
                item.set_payload(ContentType::Text, multipart.form_data);
                envelope.add_item(item);
            }

            Ok(envelope)
        });

        Box::new(future)
    }
}
