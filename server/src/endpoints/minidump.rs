use std::str::from_utf8;

use actix_web::{
    actix::ResponseFuture,
    error::{MultipartError, PayloadError},
    http::Method,
    multipart::{self, MultipartItem},
    HttpMessage, HttpRequest, HttpResponse,
};
use bytes::Bytes;
use futures::{
    future::{ok, Future},
    Stream,
};

use semaphore_general::protocol::EventId;

use crate::body::StorePayloadError;
use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

const MULTIPART_DATA_INITIAL_CHUNK_SIZE: usize = 512;
const FORM_DATA: &str = "form_data";
const MINIDUMP_ENTRY_NAME: &str = "upload_file_minidump";
// minidump attachments should have this name
const MINIDUMP_MAGIC_HEADER: &[u8] = b"MDMP"; // all minidumps start with this header

const MINIDUMP_RAW_CONTENT_TYPES: &[&str] = &["application/octet-stream", "application/x-dmp"];

/// Internal structure used when constructing Envelopes to maintain a cap on the content size
struct SizeLimitedEnvelope {
    // the envelope under construction
    pub envelope: Envelope,
    // keeps track of how much bigger is this envelope allowed to grow
    pub remaining_size: usize,
    // collect all form data in here
    pub form_data: Vec<u8>,
}

impl SizeLimitedEnvelope {
    pub fn into_envelope(mut self) -> Result<Envelope, serde_json::Error> {
        if !self.form_data.is_empty() {
            let mut item = Item::new(ItemType::FormData);
            item.set_name(FORM_DATA);
            // Content type is Text (since it is not a json object but multiple
            // json arrays serialized one after the other.
            item.set_payload(ContentType::Text, self.form_data);
            self.envelope.add_item(item);
        }
        Ok(self.envelope)
    }
}

/// Reads data from a multipart field (coming from a HttpRequest)
fn read_multipart_data<S>(field: S, max_size: usize) -> ResponseFuture<Vec<u8>, BadStoreRequest>
where
    S: Stream<Item = Bytes, Error = MultipartError> + 'static,
{
    let future = field.map_err(|_| BadStoreRequest::InvalidMultipart).fold(
        Vec::with_capacity(MULTIPART_DATA_INITIAL_CHUNK_SIZE),
        move |mut body, chunk| {
            if (body.len() + chunk.len()) > max_size {
                Err(BadStoreRequest::PayloadError(StorePayloadError::Overflow))
            } else {
                body.extend_from_slice(&chunk);
                Ok(body)
            }
        },
    );
    Box::new(future)
}

fn add_minidump_to_envelope(
    envelope: &mut Envelope,
    data: Vec<u8>,
    file_name: Option<&str>,
) -> Result<(), BadStoreRequest> {
    if !data.starts_with(MINIDUMP_MAGIC_HEADER) {
        log::trace!("Did not find magic minidump header");
        return Err(BadStoreRequest::InvalidMinidump);
    }

    let mut item = Item::new(ItemType::Attachment);
    item.set_payload(ContentType::OctetStream, data);
    item.set_filename(file_name.unwrap_or(MINIDUMP_ENTRY_NAME)); // add a default file name
    item.set_name(MINIDUMP_ENTRY_NAME);
    envelope.add_item(item);
    Ok(())
}

fn handle_multipart_stream<S>(
    content: SizeLimitedEnvelope,
    stream: multipart::Multipart<S>,
) -> ResponseFuture<SizeLimitedEnvelope, BadStoreRequest>
where
    S: Stream<Item = Bytes, Error = PayloadError> + 'static,
{
    let future = stream
        .map_err(|_| BadStoreRequest::InvalidMultipart)
        .fold(content, move |content, item| {
            handle_multipart_item(content, item)
        });
    Box::new(future)
}

fn handle_multipart_item<S>(
    mut content: SizeLimitedEnvelope,
    item: MultipartItem<S>,
) -> ResponseFuture<SizeLimitedEnvelope, BadStoreRequest>
where
    S: Stream<Item = Bytes, Error = PayloadError> + 'static,
{
    let field = match item {
        MultipartItem::Field(field) => field,
        MultipartItem::Nested(nested) => {
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
                let value = from_utf8(&data);
                match value {
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
            Box::new(ok(content))
        }
    }
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
    // TODO at the moment we override any pre exsiting (set by an SDK) event id here.
    //  We shouldn't do that. The problem is that at this stage we cannot afford to parse the
    //  request and look for the event id so we simply set one.
    //  We need to somehow preserve SDK set event ids ( the current behaviour needs to change).

    let mut envelope = Envelope::from_request(EventId::new(), meta);

    if MINIDUMP_RAW_CONTENT_TYPES.contains(&request.content_type()) {
        let future = request
            .body()
            .limit(max_payload_size)
            .map_err(|_| BadStoreRequest::InvalidMinidump)
            .and_then(move |data| {
                add_minidump_to_envelope(&mut envelope, data.to_vec(), None)?;
                Ok(envelope)
            });

        return Box::new(future);
    }

    let size_limited_envelope = SizeLimitedEnvelope {
        envelope,
        remaining_size: max_payload_size,
        form_data: Vec::new(),
    };

    let future = handle_multipart_stream(size_limited_envelope, request.multipart()).and_then(
        |size_limited_envelope| {
            let envelope = size_limited_envelope
                .into_envelope()
                .map_err(|_| BadStoreRequest::InvalidMultipart)?;

            //check that the envelope contains a minidump item
            if let Some(item) =
                envelope.get_item_by(|item| item.name() == Some(MINIDUMP_ENTRY_NAME))
            {
                if item.payload().as_ref().starts_with(MINIDUMP_MAGIC_HEADER) {
                    return Ok(envelope);
                } else {
                    log::trace!("Invalid minidump content");
                    return Err(BadStoreRequest::InvalidMinidump);
                }
            }
            Err(BadStoreRequest::MissingMinidump)
        },
    );

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
            // the minidump client expects the response to contain an event id as a hyphenated UUID i.e.
            // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[test]
    fn test_read_multipart_data_does_not_read_more_that_specified() {
        let y = vec![Bytes::from("12345"), Bytes::from("123456")];
        let input = stream::iter_ok::<_, ()>(y.into_iter()).map_err(|_| MultipartError::Incomplete);
        let result = read_multipart_data(input, 10).wait();
        assert!(result.is_err())
    }

    #[test]
    fn test_read_multipart_data_does_read_the_body() {
        let y = vec![Bytes::from("12345"), Bytes::from("123456")];
        let input = stream::iter_ok::<_, ()>(y.into_iter()).map_err(|_| MultipartError::Incomplete);
        let result = read_multipart_data(input, 20).wait();
        assert_eq!(result.unwrap(), b"12345123456");
    }
}
