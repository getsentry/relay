//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

use std::io;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, post};
use futures::StreamExt;
use http::header;
#[cfg(feature = "processing")]
use objectstore_client as objectstore;
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_quotas::Scoping;
use tower_http::limit::RequestBodyLimitLayer;

use crate::Envelope;
use crate::endpoints::common::BadStoreRequest;
use crate::envelope::{ContentType, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::service::ServiceState;
use crate::services::projects::cache::Project;
#[cfg(feature = "processing")]
use crate::services::upload::Error as ServiceError;
use crate::utils::upload::SignedLocation;
use crate::utils::{ExactStream, tus, upload};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("TUS protocol error: {0}")]
    Tus(#[from] tus::Error),

    #[error("request error: {0}")]
    Request(#[from] BadStoreRequest),

    #[error("upload error: {0}")]
    Upload(#[from] upload::Error),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Tus(_) => StatusCode::BAD_REQUEST,
            Error::Request(error) => return error.into_response(),
            Error::Upload(error) => match error {
                upload::Error::Forward(error) => return error.into_response(),
                upload::Error::Upstream(status) => status,
                upload::Error::InvalidLocation | upload::Error::SigningFailed => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                upload::Error::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
                #[cfg(feature = "processing")]
                upload::Error::UploadService(service_error) => match service_error {
                    ServiceError::Timeout => StatusCode::GATEWAY_TIMEOUT,
                    ServiceError::LoadShed => StatusCode::SERVICE_UNAVAILABLE,
                    ServiceError::UploadFailed(error) => match error {
                        objectstore::Error::Reqwest(error) => match error.status() {
                            Some(status) => status,
                            None => StatusCode::INTERNAL_SERVER_ERROR,
                        },
                        _ => StatusCode::INTERNAL_SERVER_ERROR,
                    },
                    ServiceError::Uuid(_) => StatusCode::INTERNAL_SERVER_ERROR,
                },
                upload::Error::Internal => {
                    debug_assert!(false);
                    relay_log::error!(error = &error as &dyn std::error::Error);
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            },
        }
        .into_response()
    }
}

impl IntoResponse for SignedLocation {
    fn into_response(self) -> Response {
        let mut headers = tus::response_headers();
        match self.into_header_value() {
            Ok(uri) => headers.insert(header::LOCATION, uri),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

        (StatusCode::CREATED, headers, ()).into_response()
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
    headers: HeaderMap,
    body: Body,
) -> axum::response::Result<impl IntoResponse> {
    let upload_length = tus::validate_headers(&headers).map_err(Error::from)?;
    let config = state.config();

    if upload_length > config.max_upload_size() {
        return Err(StatusCode::PAYLOAD_TOO_LARGE.into());
    }

    // There is no real "fast path" for streaming uploads. Always wait for the project config
    // to be loaded:
    let project = state
        .project_cache_handle()
        .ready(meta.public_key(), config.query_timeout()) // uses same timeout as `Upstream`
        .await
        .map_err(|()| StatusCode::SERVICE_UNAVAILABLE)?;

    let scoping = check_request(&state, meta, upload_length, project).await?;
    let stream = body
        .into_data_stream()
        .map(|result| result.map_err(io::Error::other))
        .boxed();
    let stream = ExactStream::new(stream, upload_length);

    let location = upload::Sink::new(&state)
        .upload(config, upload::Stream { scoping, stream })
        .await
        .map_err(Error::from)?;

    let mut response = location.into_response();
    response
        .headers_mut()
        .insert(tus::UPLOAD_OFFSET, upload_length.into());

    Ok(response)
}

/// Check request by converting it into a pseudo-envelope.
///
/// This is currently the easiest way to guarantee that the upload gets checked the same way as
/// the envelope.
async fn check_request(
    state: &ServiceState,
    meta: RequestMeta,
    upload_length: usize,
    project: Project<'_>,
) -> Result<Scoping, BadStoreRequest> {
    let mut envelope = Envelope::from_request(None, meta);
    envelope.require_feature(Feature::UploadEndpoint);
    let mut item = Item::new(ItemType::Attachment);
    item.set_original_length(upload_length as u64);
    item.set_payload(ContentType::AttachmentRef, vec![]);
    envelope.add_item(item);
    let mut envelope = Managed::from_envelope(envelope, state.outcome_aggregator().clone());
    let rate_limits = project
        .check_envelope(&mut envelope)
        .await
        .map_err(|err| err.map(BadStoreRequest::EventRejected).into_inner())?;
    if envelope.is_empty() {
        return Err(envelope
            .reject_err((None, BadStoreRequest::RateLimited(rate_limits)))
            .into_inner());
    }

    // We are not really processing an envelope here, only keep the updated scoping:
    let scoping = envelope.scoping();
    envelope.accept(|x| x);
    Ok(scoping)
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle).route_layer(RequestBodyLimitLayer::new(config.max_upload_size()))
}
