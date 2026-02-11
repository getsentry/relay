//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

mod tus;

use axum::body::Body;

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::http::{Method, Uri};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use relay_config::Config;
use relay_system::Addr;
use tower_http::limit::RequestBodyLimitLayer;

use crate::endpoints::common::BadStoreRequest;
use crate::envelope::{ContentType, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::service::ServiceState;
use crate::services::upload::Upload;
use crate::services::upstream::UpstreamRelay;
use crate::utils::{ForwardRequest, ForwardRequestBuilder};
use crate::{Envelope, middlewares};

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
}

impl IntoResponse for UploadError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            UploadError::Tus(_) | UploadError::BodyReadError(_) => StatusCode::BAD_REQUEST,
            UploadError::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            UploadError::UploadFailed(inner) => {
                // Delegate to inner error for proper status code mapping
                return inner.to_string().into_response();
            }
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
    // Validate TUS protocol headers
    let expected_length = validate_headers(&headers).map_err(UploadError::from)?;

    let project = state.project_cache_handle().get(meta.public_key());

    // Create pseudo-envelope.
    // This is currently the easiest way to ensure that fast-path checks are applied for non-envelopes.
    let mut envelope = Envelope::from_request(None, meta);
    let mut item = Item::new(ItemType::Attachment);
    item.set_original_length(expected_length as u64);
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
    envelope.accept(|x| x); // We're not really processing an envelope here.

    // Sink::new(&state).upload(project, body).await?;

    // while let Some(chunk_result) = stream.next().await {
    //     let chunk = chunk_result.map_err(|e| UploadError::BodyReadError(e))?;
    //     bytes_received += chunk.len();

    //     if bytes_received > expected_length {
    //         return Err(UploadError::PayloadTooLarge { expected_length })?;
    //     }

    //     // TODO: Process each chunk as it arrives
    //     // - Write to storage
    //     // - Update progress tracking
    //     // For now, we just count bytes to validate the upload length
    // }

    // TODO: Implement actual upload handling
    // - Generate a unique upload ID
    // - Finalize the stored data
    // - Return the location of the created resource

    // Build success response with TUS headers
    let mut response_headers = HeaderMap::new();
    response_headers.insert(TUS_RESUMABLE, HeaderValue::from_static(TUS_VERSION));
    response_headers.insert(
        UPLOAD_OFFSET,
        HeaderValue::from_str(&expected_length.to_string()).unwrap(),
    );
    // TODO: Add Location header with the upload URL once we have upload ID generation

    Ok((StatusCode::CREATED, response_headers, ""))
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

    // async fn upload(
    //     &self,
    //     uri: Proj
    //     body: Body,
    // ) -> Result<(), BadStoreRequest> {
    //     match self {
    //         Sink::Upstream(addr) => {
    //             let request = ForwardRequest::builder(Method::POST, )
    //             // addr.send(UpstreamRelay::SendRequest(ForwardRequestBuilder))
    //         }
    //         Sink::Upload(addr) => {
    //             todo!();
    //         }
    //     }
    //     Ok(())
    // }
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(RequestBodyLimitLayer::new(config.max_upload_size()))
}
