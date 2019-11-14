use actix_web::dev::Payload;
use actix_web::{
    actix::ResponseFuture, http::Method, multipart, pred, HttpMessage, HttpRequest, HttpResponse,
};
use futures::{
    future::{self, Future},
    Stream,
};

use future::ok;

use crate::body::StorePayloadError;
use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};
use semaphore_general::protocol::EventId;
use std::str::from_utf8;

const MULTIPART_DATA_INITIAL_CHUNK_SIZE: usize = 512;

/// Internal structure used when constructing Envelopes to maintain a cap on the content size
struct SizeLimitedEnvelope {
    pub envelope: Envelope,
    pub remaining_size: usize, // how much bigger is this envelope allowed to grow
}

/// Reads data from a multipart field (coming from a HttpRequest)
fn read_multipart_data(
    field: multipart::Field<Payload>,
    max_size: usize,
) -> ResponseFuture<Vec<u8>, BadStoreRequest> {
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

fn handle_multipart_stream(
    content: SizeLimitedEnvelope,
    stream: multipart::Multipart<Payload>,
) -> ResponseFuture<SizeLimitedEnvelope, BadStoreRequest> {
    let future = stream
        .map_err(|_| BadStoreRequest::InvalidMultipart)
        .fold(content, move |content, item| {
            handle_multipart_item(content, item)
        });
    Box::new(future)
}

fn handle_multipart_item(
    mut content: SizeLimitedEnvelope,
    item: multipart::MultipartItem<Payload>,
) -> ResponseFuture<SizeLimitedEnvelope, BadStoreRequest> {
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

    if name.is_none() && file_name.is_none() {
        // log::trace!("multipart content without name or file_name");
        return Box::new(ok(content));
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
                        Ok(_value) => {
                            let mut item = Item::new(ItemType::FormData);
                            item.set_header("name", name);
                            item.set_payload(ContentType::Text, data);
                            content.envelope.add_item(item);
                        }
                        Err(_failure) => {

                            // log::trace!("invalid text value in multipart item");
                        }
                    }
                }
                (None, None) => {
                    unreachable!();
                }
            }
            content
        });
        Box::new(result)
    }
}

fn extract_envelope_from_multipart_request(
    request: &HttpRequest<ServiceState>,
    meta: EventMeta,
    max_payload_size: usize,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let size_limited_envelope = SizeLimitedEnvelope {
        envelope: Envelope::from_request(EventId::new(), meta),
        remaining_size: max_payload_size,
    };
    Box::new(
        handle_multipart_stream(size_limited_envelope, request.multipart())
            .map(|size_limited_envelope| size_limited_envelope.envelope),
    )
}

fn mini_dump_handler(
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
            .with(mini_dump_handler);
    })
}
