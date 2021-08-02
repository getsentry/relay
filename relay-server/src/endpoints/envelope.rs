//! Handles envelope store requests.

use actix::prelude::*;
use actix_web::{HttpRequest, HttpResponse};
use futures::Future;
use serde::Serialize;

use relay_general::protocol::EventId;

use crate::body::StoreBody;
use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::Envelope;
use crate::extractors::{EnvelopeMeta, RequestMeta};
use crate::service::{ServiceApp, ServiceState};

fn extract_envelope(
    request: &HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> ResponseFuture<Envelope, BadStoreRequest> {
    let max_payload_size = request.state().config().max_envelope_size();
    let future = StoreBody::new(request, max_payload_size)
        .map_err(BadStoreRequest::PayloadError)
        .and_then(move |data| {
            if data.is_empty() {
                return Err(BadStoreRequest::EmptyBody);
            }

            Envelope::parse_request(data, meta).map_err(BadStoreRequest::InvalidEnvelope)
        });

    Box::new(future)
}

#[derive(Serialize)]
struct StoreResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<EventId>,
}

fn create_response(id: Option<EventId>) -> HttpResponse {
    HttpResponse::Ok().json(StoreResponse { id })
}

/// Handler for the envelope store endpoint.
fn store_envelope(
    envelope_meta: EnvelopeMeta,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let meta = envelope_meta.into_inner();
    common::handle_store_like_request(meta, request, extract_envelope, create_response, true)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        .resource(&common::normpath(r"/api/{project:\d+}/envelope/"), |r| {
            r.name("store-envelope");
            r.post().with(store_envelope);
        })
        .register()
}
