//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

use std::io;

use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, NoContent, Response};
use axum::routing::{MethodRouter, patch, post};
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::header;
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_quotas::Scoping;
use relay_system::SendError;
use tower_http::limit::RequestBodyLimitLayer;

use crate::Envelope;
use crate::endpoints::common::BadStoreRequest;
use crate::envelope::{ContentType, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::service::ServiceState;
#[cfg(feature = "processing")]
use crate::services::objectstore;
use crate::services::projects::cache::Project;
use crate::services::upload::{self, SignedLocation};
use crate::services::upstream::UpstreamRequestError;
use crate::utils::{BoundedStream, find_error_source, tus};

pub fn route_post(config: &Config) -> MethodRouter<ServiceState> {
    post(handle_post).route_layer(RequestBodyLimitLayer::new(config.max_upload_size()))
}

pub fn route_patch(config: &Config) -> MethodRouter<ServiceState> {
    patch(handle_patch).route_layer(RequestBodyLimitLayer::new(config.max_upload_size()))
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("TUS protocol error: {0}")]
    Tus(#[from] tus::Error),

    #[error("request error: {0}")]
    Request(#[from] BadStoreRequest),

    #[error("service error: {0}")]
    SendError(#[from] SendError),

    #[error("upload error: {0}")]
    Upload(#[from] upload::Error),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Tus(tus::Error::DeferLengthNotAllowed) => StatusCode::FORBIDDEN,
            Error::Tus(_) => StatusCode::BAD_REQUEST,
            Error::Request(error) => return error.into_response(),
            Error::SendError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Upload(error) => match error {
                upload::Error::Send(_) => StatusCode::SERVICE_UNAVAILABLE,
                upload::Error::UpstreamRequest(e) => match e {
                    UpstreamRequestError::SendFailed(e)
                        if find_error_source(&e, is_hyper_user_error).is_some() =>
                    {
                        return StatusCode::BAD_REQUEST.into_response();
                    }
                    _ => return e.into_response(),
                },
                upload::Error::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
                upload::Error::Upstream(error) => match error.status() {
                    _ if error.is_timeout() => StatusCode::GATEWAY_TIMEOUT,
                    Some(status) => status,
                    None => StatusCode::INTERNAL_SERVER_ERROR,
                },
                upload::Error::InvalidLocation(_) | upload::Error::SigningFailed => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                upload::Error::InvalidSignature => StatusCode::BAD_REQUEST,
                upload::Error::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
                #[cfg(feature = "processing")]
                upload::Error::Objectstore(service_error) => match service_error {
                    objectstore::Error::LoadShed => StatusCode::SERVICE_UNAVAILABLE,
                    objectstore::Error::UploadFailed(error) => match error {
                        objectstore_client::Error::Reqwest(error) => match error.status() {
                            _ if error.is_timeout() => StatusCode::GATEWAY_TIMEOUT,
                            Some(status) => status,
                            None if find_error_source(&error, is_hyper_user_error).is_some() => {
                                StatusCode::BAD_REQUEST
                            }
                            None => StatusCode::INTERNAL_SERVER_ERROR,
                        },
                        _ => StatusCode::INTERNAL_SERVER_ERROR,
                    },
                    objectstore::Error::Uuid(_) => StatusCode::INTERNAL_SERVER_ERROR,
                },
                upload::Error::LoadShed => StatusCode::SERVICE_UNAVAILABLE,
                upload::Error::Internal => {
                    debug_assert!(false);
                    relay_log::error!(
                        error = &error as &dyn std::error::Error,
                        "internal upload error"
                    );
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

/// Handles TUS upload requests (Creation or Creation With Upload).
///
/// This endpoint accepts POST requests with a body containing the complete upload data.
/// Unlike the full TUS protocol, this implementation only supports uploading all data
/// in a single request - partial uploads and resumption are not supported.
///
/// The body is processed as a stream to avoid loading the entire upload into memory.
async fn handle_post(
    state: ServiceState,
    meta: RequestMeta,
    headers: HeaderMap,
    body: Body,
) -> axum::response::Result<impl IntoResponse> {
    relay_log::trace!("Validating headers");
    let tus::ParsedHeaders {
        has_upload,
        upload_length,
    } = tus::validate_post_headers(&headers, meta.request_trust().is_trusted())
        .map_err(Error::from)?;
    let config = state.config();

    if upload_length.is_some_and(|len| len > config.max_upload_size()) {
        return Err(StatusCode::PAYLOAD_TOO_LARGE.into());
    }

    // There is no real "fast path" for streaming uploads. Always wait for the project config
    // to be loaded:
    relay_log::trace!("Awaiting project config");
    let project = state
        .project_cache_handle()
        .ready(meta.public_key(), config.query_timeout()) // uses same timeout as `Upstream`
        .await
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    relay_log::trace!("Checking request");
    let scoping = check_request(&state, meta, upload_length, project).await?;

    // Unconditionally create the upload location:
    let result = create(&state, scoping, upload_length).await;
    let mut location = result.inspect_err(|e| {
        relay_log::warn!(error = e as &dyn std::error::Error, "create failed");
    })?;
    let mut upload_offset = None;

    // If we already have bytes, upload them:
    if has_upload {
        let stream = body
            .into_data_stream()
            .map(|result| result.map_err(io::Error::other))
            .boxed();
        let (lower_bound, upper_bound) = match upload_length {
            None => (1, config.max_upload_size()),
            Some(u) => (u, u),
        };
        let stream = BoundedStream::new(stream, lower_bound, upper_bound);
        let byte_counter = stream.byte_counter();

        relay_log::trace!("Uploading");
        let result = upload(&state, scoping, location, stream).await;
        location = result.inspect_err(|e| {
            relay_log::warn!(error = e as &dyn std::error::Error, "upload failed");
        })?;
        upload_offset = Some(byte_counter.get());
    }

    let mut response = location.into_response();
    response
        .headers_mut()
        .insert(tus::TUS_RESUMABLE, tus::TUS_VERSION);
    if let Some(upload_offset) = upload_offset {
        response
            .headers_mut()
            .insert(tus::UPLOAD_OFFSET, upload_offset.into());
    }

    Ok(response)
}

async fn handle_patch(
    state: ServiceState,
    meta: RequestMeta,
    headers: HeaderMap,
    Path(upload::LocationPath { project_id, key }): Path<upload::LocationPath>,
    Query(upload::LocationQueryParams { length, signature }): Query<upload::LocationQueryParams>,
    body: Body,
) -> axum::response::Result<impl IntoResponse> {
    relay_log::trace!("Validating headers");
    tus::validate_patch_headers(&headers).map_err(Error::from)?;

    let location = SignedLocation::from_parts(project_id, key, length, signature);

    let config = state.config();

    // There is no real "fast path" for streaming uploads. Always wait for the project config
    // to be loaded:
    relay_log::trace!("Awaiting project config");
    let project = state
        .project_cache_handle()
        .ready(meta.public_key(), config.query_timeout()) // uses same timeout as `Upstream`
        .await
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    relay_log::trace!("Checking request");
    let scoping = check_request(&state, meta, length, project).await?;

    let stream = body
        .into_data_stream()
        .map(|result| result.map_err(io::Error::other))
        .boxed();
    let (lower_bound, upper_bound) = match length {
        None => (1, config.max_upload_size()),
        Some(u) => (u, u),
    };
    let stream = BoundedStream::new(stream, lower_bound, upper_bound);
    let byte_counter = stream.byte_counter();

    relay_log::trace!("Uploading");
    let result = upload(&state, scoping, location, stream).await;
    let location = result.inspect_err(|e| {
        relay_log::warn!(error = e as &dyn std::error::Error, "upload failed");
    })?;

    let upload_offset = byte_counter.get();

    let mut response = NoContent.into_response();

    // Not required by TUS, but we respond with the location header:
    response.headers_mut().insert(
        header::LOCATION,
        location
            .into_header_value()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    response
        .headers_mut()
        .insert(tus::TUS_RESUMABLE, tus::TUS_VERSION);
    response
        .headers_mut()
        .insert(tus::UPLOAD_OFFSET, upload_offset.into());

    Ok(response)
}

async fn create(
    state: &ServiceState,
    scoping: Scoping,
    upload_length: Option<usize>,
) -> Result<SignedLocation, Error> {
    let location = state
        .upload()
        .send(upload::Create {
            scoping,
            length: upload_length,
        })
        .await??;

    Ok(location)
}

async fn upload(
    state: &ServiceState,
    scoping: Scoping,
    location: SignedLocation,
    stream: BoundedStream<BoxStream<'static, std::io::Result<Bytes>>>,
) -> Result<SignedLocation, Error> {
    let location = state
        .upload()
        .send(upload::Stream {
            received: Utc::now(),
            scoping,
            location,
            stream,
        })
        .await??;

    Ok(location)
}

/// Check request by converting it into a pseudo-envelope.
///
/// This is currently the easiest way to guarantee that the upload gets checked the same way as
/// the envelope.
async fn check_request(
    state: &ServiceState,
    meta: RequestMeta,
    upload_length: Option<usize>,
    project: Project<'_>,
) -> Result<Scoping, BadStoreRequest> {
    let mut envelope = Envelope::from_request(None, meta);
    envelope.require_feature(Feature::UploadEndpoint);
    let mut item = Item::new(ItemType::Attachment);
    item.set_payload(ContentType::AttachmentRef, vec![]);
    item.set_attachment_length(upload_length.unwrap_or(1));
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

fn is_hyper_user_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<hyper::Error>()
        .is_some_and(hyper::Error::is_user)
}
