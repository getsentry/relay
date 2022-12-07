use actix_web::{actix::ResponseFuture, http::Method, HttpRequest, HttpResponse};
use futures01::Future;

use relay_common::tryf;
use relay_general::protocol::EventId;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};
use crate::utils::MultipartItems;

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let event_id = tryf!(request
        .match_info()
        .get("event_id")
        .unwrap_or_default()
        .parse::<EventId>()
        .map_err(|_| BadStoreRequest::InvalidEventId));

    let max_payload_size = request.state().config().max_attachments_size();
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
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    common::handle_store_like_request(meta, request, extract_envelope, create_response, true)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    let url_pattern = common::normpath(r"/api/{project:\d+}/events/{event_id:[\w-]+}/attachments/");

    common::cors(app)
        .resource(&url_pattern, |r| {
            r.name("store-attachment");
            r.method(Method::POST).with(store_attachment);
        })
        .register()
}
