use actix_web::actix::ResponseFuture;
use actix_web::http::Method;
use actix_web::{HttpRequest, HttpResponse, Path, Query, Request};
use bytes::Bytes;
use serde::Deserialize;

use semaphore_general::protocol::EventId;

use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};

#[derive(Clone, Debug, Deserialize)]
struct AttachmentParams {
    event_id: EventId,
}

fn extract_envelope(
    data: Bytes,
    meta: EventMeta,
    params: AttachmentParams,
) -> Result<Envelope, BadStoreRequest> {
    Ok(unimplemented!())
}

fn create_response() -> HttpResponse {
    HttpResponse::Created().finish()
}

fn store_attachment(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
    params: Path<AttachmentParams>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    Box::new(handle_store_like_request(
        meta,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, params.into_inner()),
        |_| create_response(),
    ))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource(r"/api/{project:\d+}/events/{event_id}/attachments/", |r| {
        r.method(Method::POST).with(store_attachment);
    })
}
