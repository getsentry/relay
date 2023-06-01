use axum::extract::{DefaultBodyLimit, Multipart, Path};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use bytes::Bytes;
use relay_config::Config;
use relay_general::protocol::EventId;
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{AttachmentType, Envelope};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;
use crate::utils;

#[derive(Debug)]
struct MultipartWrapper(Multipart);

/// A wrapper overt [`axum::extract::Multipart`] with tracing instrumentation.
#[axum::async_trait]
impl<S, B> axum::extract::FromRequest<S, B> for MultipartWrapper
where
    Multipart: axum::extract::FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = <Multipart as axum::extract::FromRequest<S, B>>::Rejection;

    #[tracing::instrument(name = "Multipart::from_request", skip_all)]
    async fn from_request(req: axum::http::Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(Multipart::from_request(req, state).await?))
    }
}

#[derive(Debug, Deserialize)]
struct AttachmentPath {
    event_id: EventId,
}

async fn extract_envelope(
    config: &Config,
    meta: RequestMeta,
    path: AttachmentPath,
    multipart: Multipart,
) -> Result<Box<Envelope>, BadStoreRequest> {
    let max_size = config.max_attachment_size();
    let items = utils::multipart_items(multipart, max_size, |_| AttachmentType::default()).await?;

    let mut envelope = Envelope::from_request(Some(path.event_id), meta);
    for item in items {
        envelope.add_item(item);
    }
    Ok(envelope)
}

async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    Path(path): Path<AttachmentPath>,
    multipart: MultipartWrapper,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = extract_envelope(state.config(), meta, path, multipart.0).await?;
    common::handle_envelope(&state, envelope).await?;
    Ok(StatusCode::CREATED)
}

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send + Into<Bytes>,
    B::Error: Into<axum::BoxError>,
{
    post(handle).route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
}
