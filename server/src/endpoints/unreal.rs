use actix_web::{actix::ResponseFuture, HttpRequest, HttpResponse};

use semaphore_general::protocol::EventId;

use crate::body::ForwardBody;
use crate::endpoints::common::{
    create_text_event_id_response, handle_store_like_request, BadStoreRequest,
};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};
use futures::Future;

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: EventMeta,
    max_payload_size: usize,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let future = ForwardBody::new(request, max_payload_size)
        .map_err(|_| BadStoreRequest::InvalidUnrealReport)
        .and_then(move |data| {
            let mut envelope = Envelope::from_request(EventId::new(), meta);
            let mut item = Item::new(ItemType::UnrealReport);
            item.set_payload(ContentType::OctetStream, data);
            envelope.add_item(item);
            Ok(envelope)
        });

    Box::new(future)
}

fn store_unreal(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let event_size = request.state().config().max_attachment_payload_size();

    Box::new(handle_store_like_request(
        meta,
        true,
        start_time,
        request,
        move |data, meta| extract_envelope(data, meta, event_size),
        create_text_event_id_response,
    ))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource(
        r"/api/{project:\d+}/unreal/{sentry_key:\d+}{trailing_slash:/?}",
        |r| {
            r.name("store-unreal");
            r.post().with(store_unreal);
        },
    )
}
