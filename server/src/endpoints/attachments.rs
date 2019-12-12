use actix_web::{actix::ResponseFuture, http::Method, HttpRequest, HttpResponse};
use futures::Future;

use semaphore_common::tryf;
use semaphore_general::protocol::EventId;

use crate::endpoints::common::{handle_store_like_request, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};
use crate::utils::MultipartEnvelope;

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: EventMeta,
    max_payload_size: usize,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let event_id = tryf!(request
        .match_info()
        .get("event_id")
        .unwrap_or_default()
        .parse::<EventId>()
        .map_err(|_| BadStoreRequest::InvalidEventId));

    let envelope = Envelope::from_request(event_id, meta);

    let future = MultipartEnvelope::new(envelope, max_payload_size)
        .handle_request(request)
        .map_err(BadStoreRequest::InvalidMultipart);

    Box::new(future)
}

fn create_response(_: EventId) -> HttpResponse {
    HttpResponse::Created().finish()
}

fn store_attachment(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let attachment_size = request.state().config().max_attachment_payload_size();

    Box::new(handle_store_like_request(
        meta,
        false,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, attachment_size),
        create_response,
    ))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource(
        r"/api/{project:\d+}/events/{event_id:[^/]+}/attachments{trailing_slash:/}",
        |r| {
            r.name("store-attachment");
            r.method(Method::POST).with(store_attachment);
        },
    )
}
