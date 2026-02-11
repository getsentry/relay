//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

mod tus;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use relay_config::Config;
use relay_system::Addr;
use tower_http::limit::RequestBodyLimitLayer;

use crate::Envelope;
use crate::endpoints::common::BadStoreRequest;
use crate::envelope::{ContentType, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::service::ServiceState;
use crate::services::upload::{Upload, UploadStream};
use crate::services::upstream::UpstreamRelay;
use crate::utils::{ForwardError, ForwardRequest, ForwardResponse};

use self::tus::{TUS_RESUMABLE, TUS_VERSION, UPLOAD_OFFSET, validate_headers};

/// Error type for TUS upload requests.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("TUS protocol violation: {0}")]
    Tus(#[from] tus::Error),

    #[error("payload too large: received more than {expected_length} bytes")]
    PayloadTooLarge { expected_length: usize },

    #[error("failed to read request body: {0}")]
    BodyReadError(axum::Error),

    #[error("upload failed: {0}")]
    UploadFailed(#[from] BadStoreRequest),

    #[error("upstream error: {0}")]
    Forward(#[from] ForwardError),

    #[error("upload service error: {0}")]
    UploadService(#[from] crate::services::upload::Error),

    #[error("service unavailable")]
    ServiceUnavailable,
}

impl IntoResponse for UploadError {
    fn into_response(self) -> axum::response::Response {
        // Delegate to inner error types that have their own status code mapping.
        match self {
            UploadError::UploadFailed(inner) => return inner.into_response(),
            UploadError::Forward(inner) => return inner.into_response(),
            _ => {}
        }

        let status = match &self {
            UploadError::Tus(_) | UploadError::BodyReadError(_) => StatusCode::BAD_REQUEST,
            UploadError::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            UploadError::UploadService(crate::services::upload::Error::LoadShed)
            | UploadError::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            UploadError::UploadService(crate::services::upload::Error::Timeout) => {
                StatusCode::GATEWAY_TIMEOUT
            }
            UploadError::UploadService(_) => StatusCode::INTERNAL_SERVER_ERROR,
            UploadError::UploadFailed(_) | UploadError::Forward(_) => unreachable!(),
        };

        let mut response = (status, self.to_string()).into_response();
        response
            .headers_mut()
            .insert(TUS_RESUMABLE, HeaderValue::from_static(TUS_VERSION));
        response
    }
}

/// Handles TUS upload requests (Creation With Upload).
///
/// This endpoint accepts POST requests with a body containing the complete upload data.
/// Unlike the full TUS protocol, this implementation only supports uploading all data
/// in a single request - partial uploads and resumption are not supported.
///
/// The body is processed as a stream to avoid loading the entire upload into memory.
async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> axum::response::Result<impl IntoResponse> {
    let expected_length = validate_headers(&headers).map_err(UploadError::from)?;

    let project = state.project_cache_handle().get(meta.public_key());

    // Create pseudo-envelope.
    // This is currently the easiest way to ensure that fast-path checks are applied for non-envelopes.
    let mut envelope = Envelope::from_request(None, meta);
    let mut item = Item::new(ItemType::Attachment);
    item.set_original_length(expected_length);
    item.set_payload(ContentType::AttachmentRef, vec![]);
    envelope.add_item(item);
    let mut envelope = Managed::from_envelope(envelope, state.outcome_aggregator().clone());
    let rate_limits = project
        .check_envelope(&mut envelope)
        .await
        .map_err(|err| err.map(BadStoreRequest::EventRejected))?;
    if envelope.is_empty() {
        return Err(envelope
            .reject_err((None, BadStoreRequest::RateLimited(rate_limits)))
            .into());
    }
    let scoping = envelope.scoping();
    let upload_stream = UploadStream {
        scoping,
        body,
        expected_length,
    };
    let managed_stream = envelope.wrap(upload_stream);
    envelope.accept(|_| ()); // We're not really processing an envelope here.

    let result = Sink::new(&state).upload(uri.path(), managed_stream).await?;

    match result {
        SinkResult::Forwarded(response) => Ok(response.into_response()),
        SinkResult::Uploaded => {
            let mut response_headers = HeaderMap::new();
            response_headers.insert(TUS_RESUMABLE, HeaderValue::from_static(TUS_VERSION));
            response_headers.insert(
                UPLOAD_OFFSET,
                HeaderValue::from_str(&expected_length.to_string()).unwrap(),
            );
            Ok((StatusCode::CREATED, response_headers, "").into_response())
        }
    }
}

enum SinkResult {
    /// The response from forwarding to upstream relay.
    Forwarded(ForwardResponse),
    /// The upload was handled locally by the upload service.
    Uploaded,
}

enum Sink {
    Upstream(Addr<UpstreamRelay>),
    Upload(Addr<Upload>),
}

impl Sink {
    fn new(state: &ServiceState) -> Self {
        if let Some(addr) = state.upload() {
            Self::Upload(addr.clone())
        } else {
            Self::Upstream(state.upstream_relay().clone())
        }
    }

    async fn upload(
        &self,
        path: &str,
        managed_stream: Managed<UploadStream>,
    ) -> Result<SinkResult, UploadError> {
        match self {
            Sink::Upstream(addr) => {
                let body = managed_stream.accept(|stream| stream.body);
                let response = ForwardRequest::builder(Method::POST, path.to_owned())
                    .with_body(body)
                    .send_to(addr)
                    .await?;
                Ok(SinkResult::Forwarded(response))
            }
            Sink::Upload(addr) => {
                addr.send(managed_stream)
                    .await
                    .map_err(|_| UploadError::ServiceUnavailable)?
                    .map_err(UploadError::UploadService)?;
                Ok(SinkResult::Uploaded)
            }
        }
    }
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(RequestBodyLimitLayer::new(config.max_upload_size()))
}
