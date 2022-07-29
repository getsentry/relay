use actix_web::{actix::ResponseFuture, HttpRequest, HttpResponse};
use futures01::Future;

use relay_general::protocol::EventId;

use crate::body::RequestBody;
use crate::constants::UNREAL_USER_HEADER;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let user_id = request.query().get("UserID").cloned();
    let max_payload_size = request.state().config().max_attachments_size();

    let future = RequestBody::new(request, max_payload_size)
        .map_err(BadStoreRequest::PayloadError)
        .and_then(move |data| {
            if data.is_empty() {
                return Err(BadStoreRequest::EmptyBody);
            }

            let mut envelope = Envelope::from_request(Some(EventId::new()), meta);

            let mut item = Item::new(ItemType::UnrealReport);
            item.set_payload(ContentType::OctetStream, data);
            envelope.add_item(item);

            if let Some(user_id) = user_id {
                envelope.set_header(UNREAL_USER_HEADER, user_id);
            }

            Ok(envelope)
        });

    Box::new(future)
}

fn store_unreal(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    common::handle_store_like_request(
        meta,
        request,
        extract_envelope,
        // The return here is only useful for consistency because the UE4 crash reporter doesn't
        // care about it.
        common::create_text_event_id_response,
        false,
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        .resource(
            &common::normpath(r"/api/{project:\d+}/unreal/{sentry_key:\w+}/"),
            |r| {
                r.name("store-unreal");
                r.post().with(store_unreal);
            },
        )
        .register()
}
