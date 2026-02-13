//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

mod tus;

use std::io;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use futures::StreamExt;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_quotas::Scoping;
use relay_system::Addr;
use tower_http::limit::RequestBodyLimitLayer;

use crate::Envelope;
use crate::endpoints::common::BadStoreRequest;
use crate::envelope::{ContentType, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::service::ServiceState;
use crate::services::projects::cache::Project;
use crate::services::upload::{Error as UploadError, Upload, UploadKey, UploadStream};
use crate::services::upstream::UpstreamRelay;
use crate::utils::{ExactStream, ForwardError, ForwardRequest, ForwardResponse};

use self::tus::{TUS_RESUMABLE, TUS_VERSION, UPLOAD_OFFSET, validate_headers};

/// Error type for TUS upload requests.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("TUS protocol violation: {0}")]
    Tus(#[from] tus::Error),

    #[error("upstream error: {0}")]
    Forward(#[from] ForwardError),

    #[error("upload service error: {0}")]
    UploadService(#[from] UploadError),

    #[error("service unavailable")]
    ServiceUnavailable,
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        // Delegate to inner error types that have their own status code mapping.
        let status = match self {
            Error::Forward(inner) => return inner.into_response(),
            Error::Tus(_) => StatusCode::BAD_REQUEST,
            Error::UploadService(UploadError::LoadShed) | Error::ServiceUnavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Error::UploadService(UploadError::Timeout) => StatusCode::GATEWAY_TIMEOUT,
            Error::UploadService(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let mut response = status.into_response();
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
    let upload_length = validate_headers(&headers).map_err(Error::from)?;

    let project = get_project(&state, meta.public_key()).await;

    let scoping = check_request(&state, meta, upload_length, project).await?;
    let stream = body
        .into_data_stream()
        .map(|result| result.map_err(io::Error::other))
        .boxed();
    let stream = ExactStream::new(stream, upload_length);

    let path = uri.path();
    let result = Sink::new(&state)
        .upload(path, UploadStream { scoping, stream })
        .await?;

    match result {
        UploadResult::Forwarded(response) => Ok(response.into_response()),
        UploadResult::Success(key) => {
            relay_log::trace!("Successfully uploaded to objectstore");
            let mut response_headers = HeaderMap::new();
            response_headers.insert(TUS_RESUMABLE, HeaderValue::from_static(TUS_VERSION));
            response_headers.insert(
                UPLOAD_OFFSET,
                HeaderValue::from_str(&upload_length.to_string())
                    .expect("integer should always be a valid header"),
            );

            relay_log::trace!("Signing URL...");
            let location = signed(state.config(), path, key, upload_length)
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
            response_headers.insert(
                hyper::header::LOCATION,
                HeaderValue::from_str(&location).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            );
            Ok((StatusCode::CREATED, response_headers, "").into_response())
        }
    }
}

/// Check request by converting it into a pseudo-envelope.
///
/// This is currently the easiest way to guarantee that the upload gets checked the same way as
/// the envelope.
async fn check_request(
    state: &ServiceState,
    meta: RequestMeta,
    upload_length: u64,
    project: Project<'_>,
) -> Result<Scoping, axum::response::ErrorResponse> {
    let mut envelope = Envelope::from_request(None, meta);
    let mut item = Item::new(ItemType::Attachment);
    item.set_original_length(upload_length);
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

    // We are not really processing an envelope here, only keep the updated scoping:
    let scoping = envelope.scoping();
    envelope.accept(|x| x);
    Ok(scoping)
}

async fn get_project(state: &ServiceState, public_key: ProjectKey) -> Project<'_> {
    let mut project = state.project_cache_handle().get(public_key);

    // In non-procesing relays, it's OK to forward the request without waiting.
    if !state.config().processing_enabled() {
        return project;
    }

    // TODO: There should be a better way to await a project config.
    if project.state().is_pending() {
        state.project_cache_handle().fetch(public_key);
        while project.state().is_pending() {
            relay_log::trace!("Waiting for project state");
            let _ = state.project_cache_handle().changes().recv().await;
            project = state.project_cache_handle().get(public_key);
        }
    }
    debug_assert!(!project.state().is_pending());
    project
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(RequestBodyLimitLayer::new(config.max_upload_size()))
}
