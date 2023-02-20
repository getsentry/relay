//! Handles envelope store requests.

use actix_web::{HttpRequest, HttpResponse};
use futures::TryFutureExt;
use serde::Serialize;

use relay_general::protocol::EventId;

use crate::body;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::{EnvelopeMeta, RequestMeta};
use crate::service::{ServiceApp, ServiceState};

async fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let max_payload_size = request.state().config().max_envelope_size();
    let data = body::store_body(request, max_payload_size).await?;

    if data.is_empty() {
        return Err(BadStoreRequest::EmptyBody);
    }

    Envelope::parse_request(data, meta).map_err(BadStoreRequest::InvalidEnvelope)
}

#[derive(Serialize)]
struct StoreResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<EventId>,
}

/// Handler for the envelope store endpoint.
async fn store_envelope(
    envelope_meta: EnvelopeMeta,
    request: HttpRequest<ServiceState>,
) -> Result<HttpResponse, BadStoreRequest> {
    let envelope = extract_envelope(&request, envelope_meta.into_inner()).await?;
    let id = common::handle_envelope(request.state(), envelope).await?;
    Ok(HttpResponse::Ok().json(StoreResponse { id }))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        .resource(&common::normpath(r"/api/{project:\d+}/envelope/"), |r| {
            r.name("store-envelope");
            r.post()
                .with_async(|e, r| Box::pin(store_envelope(e, r)).compat());
        })
        .register()
}
