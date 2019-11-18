use std::str::from_utf8;

use actix_web::{
    actix::ResponseFuture,
    error::{MultipartError, PayloadError},
    http::Method,
    multipart::{self, MultipartItem},
    pred, HttpMessage, HttpRequest, HttpResponse,
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
const COLLECTOR_NAME: &str = "data_collector";

/// Internal structure used when constructing Envelopes to maintain a cap on the content size
struct SizeLimitedEnvelope {
    // the envelope under construction
    pub envelope: Envelope,
    // keeps track of how much bigger is this envelope allowed to grow
    pub remaining_size: usize,
    // collect all form data in here
    pub form_data: Vec<(String, String)>,
}

impl SizeLimitedEnvelope {
    pub fn into_envelope(mut self) -> Result<Envelope, serde_json::Error> {
        if !self.form_data.is_empty() {
            let data = serde_json::to_string(&self.form_data)?;
            let mut item = Item::new(ItemType::FormData);
            item.set_header("name", COLLECTOR_NAME);
            item.set_payload(ContentType::Json, data);
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

    if name.is_none() && file_name.is_none() {
        // log::trace!("multipart content without name or file_name");
        Box::new(ok(content))
    } else {
        let result = read_multipart_data(field, content.remaining_size).map(|data| {
            content.remaining_size -= data.len();
            match (name, file_name) {
                (name, Some(file_name)) => {
                    let mut item = Item::new(ItemType::Attachment);
                    item.set_payload(ContentType::OctetStream, data);
                    item.set_filename(file_name);
                    name.map(|name| item.set_header("name", name));
                    content.envelope.add_item(item)
                }
                (Some(name), None) => {
                    let value = from_utf8(&data);
                    match value {
                        Ok(value) => {
                            content.form_data.push((name, value.to_string()));
                        }
                        Err(_failure) => {

                            // log::trace!("invalid text value in multipart item");
                        }
                    }
                }
                (None, None) => {
                    //already checked on the if branch
                    unreachable!();
                }
            }
            content
        });
        Box::new(result)
    }
}

fn extract_envelope_from_multipart_request<T, S>(
    request: &T,
    meta: EventMeta,
    max_payload_size: usize,
) -> ResponseFuture<Envelope, BadStoreRequest>
where
    T: HttpMessage<Stream = S>,
    S: Stream<Item = Bytes, Error = PayloadError> + 'static,
{
    let size_limited_envelope = SizeLimitedEnvelope {
        envelope: Envelope::from_request(EventId::new(), meta),
        remaining_size: max_payload_size,
        form_data: Vec::new(),
    };
    Box::new(
        handle_multipart_stream(size_limited_envelope, request.multipart()).and_then(
            |size_limited_envelope| {
                SizeLimitedEnvelope::into_envelope(size_limited_envelope)
                    //TODO RaduW check if this is the error we want
                    .map_err(|_| BadStoreRequest::InvalidMultipart)
            },
        ),
    )
}

fn store_minidump(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    Box::new(handle_store_like_request(
        meta,
        start_time,
        request,
        move |data, meta, max_event_payload_size| {
            extract_envelope_from_multipart_request(data, meta, max_event_payload_size)
        },
        move |id| {
            HttpResponse::Ok()
                .content_type("text/plain")
                .body(format!("{}", id))
        },
    ))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource(r"/api/{project:\d+}/minidump/", |r| {
        //hook security endpoint
        r.method(Method::POST)
            .filter(pred::Header("content-type", "multipart/form-data"))
            .with(store_minidump);
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
