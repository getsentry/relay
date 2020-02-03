use actix_web::{actix::ResponseFuture, http::Method, HttpRequest, HttpResponse};
use futures::Future;

use relay_common::tryf;
use relay_general::protocol::EventId;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::{RequestMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};
use crate::utils::MultipartItems;

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
    max_payload_size: usize,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let event_id = tryf!(request
        .match_info()
        .get("event_id")
        .unwrap_or_default()
        .parse::<EventId>()
        .map_err(|_| BadStoreRequest::InvalidEventId));

    let future = MultipartItems::new(max_payload_size)
        .handle_request(request)
        .map_err(BadStoreRequest::InvalidMultipart)
        .map(move |items| {
            let mut envelope = Envelope::from_request(Some(event_id), meta);

            for item in items {
                envelope.add_item(item);
            }

            envelope
        });

    Box::new(future)
}

fn create_response(_: Option<EventId>) -> HttpResponse {
    HttpResponse::Created().finish()
}

fn store_attachment(
    meta: RequestMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let attachment_size = request.state().config().max_attachment_payload_size();

    common::handle_store_like_request(
        meta,
        false,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, attachment_size),
        create_response,
    )
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
