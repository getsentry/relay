use actix_web::{http::Method, App, HttpRequest, HttpResponse};

use relay_general::protocol::EventId;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::RequestMeta;
use crate::service::ServiceState;
use crate::utils::MultipartItems;

async fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let event_id = request
        .match_info()
        .get("event_id")
        .unwrap_or_default()
        .parse::<EventId>()
        .map_err(|_| BadStoreRequest::InvalidEventId)?;

    let max_payload_size = request.state().config().max_attachments_size();
    let items = MultipartItems::new(max_payload_size)
        .handle_request(request)
        .await
        .map_err(BadStoreRequest::InvalidMultipart)?;

    let mut envelope = Envelope::from_request(Some(event_id), meta);
    for item in items {
        envelope.add_item(item);
    }
    Ok(envelope)
}

async fn store_attachment(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
) -> Result<HttpResponse, BadStoreRequest> {
    let envelope = extract_envelope(&request, meta).await?;
    common::handle_envelope(request.state(), envelope).await?;
    Ok(HttpResponse::Created().finish())
}

pub fn configure_app(app: App<ServiceState>) -> App<ServiceState> {
    let url_pattern = common::normpath(r"/api/{project:\d+}/events/{event_id:[\w-]+}/attachments/");

    common::cors(app)
        .resource(&url_pattern, |r| {
            r.name("store-attachment");
            r.method(Method::POST)
                .with_async(|r, m| common::handler(store_attachment(r, m)));
        })
        .register()
}
