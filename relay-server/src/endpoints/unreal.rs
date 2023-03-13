use actix_web::{App, HttpRequest, HttpResponse};
use relay_general::protocol::EventId;

use crate::body;
use crate::constants::UNREAL_USER_HEADER;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;

async fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let user_id = request.query().get("UserID").cloned();
    let max_payload_size = request.state().config().max_attachments_size();

    let data = body::request_body(request, max_payload_size).await?;
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
}

async fn store_unreal(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
) -> Result<HttpResponse, BadStoreRequest> {
    let envelope = extract_envelope(&request, meta).await?;
    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    match common::handle_envelope(request.state(), envelope).await {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error),
    };

    // The return here is only useful for consistency because the UE4 crash reporter doesn't
    // care about it.
    Ok(common::create_text_event_id_response(id))
}

pub fn configure_app(app: App<ServiceState>) -> App<ServiceState> {
    common::cors(app)
        .resource(
            &common::normpath(r"/api/{project:\d+}/unreal/{sentry_key:\w+}/"),
            |r| {
                r.name("store-unreal");
                r.post()
                    .with_async(|m, r| common::handler(store_unreal(m, r)));
            },
        )
        .register()
}
