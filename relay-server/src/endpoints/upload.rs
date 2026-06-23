//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

use std::io;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, NoContent, Response};
use axum::routing::{MethodRouter, patch, post};
use chrono::Utc;
use futures::StreamExt;
use http::header;
use relay_config::{Config, UpstreamDescriptor};
use relay_dynamic_config::Feature;
use relay_quotas::Scoping;
use relay_system::SendError;
use tower_http::limit::RequestBodyLimitLayer;

use crate::Envelope;
use crate::endpoints::common::BadStoreRequest;
use crate::envelope::{AttachmentType, ContentType, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::service::ServiceState;
#[cfg(feature = "processing")]
use crate::services::objectstore;
use crate::services::projects::cache::Project;
use crate::services::projects::project::ProjectState;
use crate::services::upload::{
    self, ByteStream, Final, LocationQueryParams, Provisional, SignedLocation, UploadLength,
};
use crate::services::upstream::UpstreamRequestError;
use crate::statsd::RelayCounters;
use crate::utils::{ApiErrorResponse, MeteredStream};
use crate::utils::{BoundedStream, find_error_source, tus};

pub fn route_post(config: &Config) -> MethodRouter<ServiceState> {
    post(handle_post)
        .route_layer(RequestBodyLimitLayer::new(config.max_upload_size()))
        .route_layer(DefaultBodyLimit::disable())
}

pub fn route_patch(config: &Config) -> MethodRouter<ServiceState> {
    patch(handle_patch)
        .route_layer(RequestBodyLimitLayer::new(config.max_upload_size()))
        .route_layer(DefaultBodyLimit::disable())
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
        let body = ApiErrorResponse::from_error(&self);

        if let Error::Upload(upload::Error::Internal(_)) = &self {
            debug_assert!(false);
            relay_log::error!(
                error = &self as &dyn std::error::Error,
                "internal upload error"
            );
        }

        let status = match self {
            Error::Tus(_) => StatusCode::BAD_REQUEST,
            Error::Request(error) => return error.into_response(),
            Error::SendError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Upload(error) => match error {
                upload::Error::Send(_) => StatusCode::SERVICE_UNAVAILABLE,
                upload::Error::UpstreamRequest(e) => match e {
                    UpstreamRequestError::SendFailed(e)
                        if find_error_source(&e, is_hyper_user_error).is_some() =>
                    {
                        StatusCode::BAD_REQUEST
                    }
                    UpstreamRequestError::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
                    UpstreamRequestError::ResponseError(status, _) => status,
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
                upload::Error::SerializeFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
                upload::Error::InvalidSignature(_) => StatusCode::BAD_REQUEST,
                upload::Error::ObjectstoreServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
                #[cfg(feature = "processing")]
                upload::Error::Objectstore(service_error) => match service_error.kind {
                    objectstore::ErrorKind::InvalidScoping => StatusCode::INTERNAL_SERVER_ERROR,
                    objectstore::ErrorKind::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
                    objectstore::ErrorKind::LoadShed => StatusCode::SERVICE_UNAVAILABLE,
                    objectstore::ErrorKind::UploadFailed(error) => match error {
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
                    objectstore::ErrorKind::Uuid(_) => StatusCode::INTERNAL_SERVER_ERROR,
                },
                upload::Error::LoadShed => StatusCode::SERVICE_UNAVAILABLE,
                upload::Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
        };

        (status, body).into_response()
    }
}

impl<L: UploadLength> IntoResponse for SignedLocation<L> {
    fn into_response(self) -> Response {
        let mut headers = tus::response_headers();
        match self.into_header_value() {
            Ok(uri) => headers.insert(header::LOCATION, uri),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

        (StatusCode::CREATED, headers, ()).into_response()
    }
}

/// Handles TUS creation requests.
///
/// See <https://tus.io/protocols/resumable-upload#creation>.
async fn handle_post(
    state: ServiceState,
    meta: RequestMeta,
    headers: HeaderMap,
) -> axum::response::Result<impl IntoResponse> {
    relay_log::trace!("Checking project fetching kill switch");
    check_kill_switch(&state)?;

    relay_log::trace!("Validating headers");
    let headers = tus::validate_post_headers(&headers).map_err(Error::from)?;
    let config = state.config();

    if headers
        .upload_length
        .is_some_and(|len| len > config.max_upload_size())
    {
        return Err(StatusCode::PAYLOAD_TOO_LARGE.into());
    }

    // There is no real "fast path" for streaming uploads. Always wait for the project config
    // to be loaded:
    relay_log::trace!("Awaiting project config");
    let project = state
        .project_cache_handle()
        .ready(meta.public_key(), config.query_timeout()) // uses same timeout as `Upstream`
        .await
        .ok_or_else(|| {
            relay_log::warn!("timeout waiting for project config");
            StatusCode::SERVICE_UNAVAILABLE
        })?;

    relay_log::trace!("Checking request");
    let checked_upload = validate_and_limit(&state, meta, &headers, project).await?;

    // Unconditionally create the upload location:
    relay_log::trace!("Creating upload location");
    let result = create(
        &state,
        checked_upload.scoping,
        checked_upload.upstream,
        &headers,
    )
    .await;
    let location = result.inspect_err(|e| {
        relay_log::warn!(error = e as &dyn std::error::Error, "create failed");
    })?;

    let mut response = location.into_response();
    response
        .headers_mut()
        .insert(tus::TUS_RESUMABLE, tus::TUS_VERSION);

    Ok(response)
}

async fn handle_patch(
    state: ServiceState,
    meta: RequestMeta,
    headers: HeaderMap,
    Path(upload::LocationPath { project_id, key }): Path<upload::LocationPath>,
    Query(LocationQueryParams {
        upload_length,
        upload_signature,
        other,
    }): Query<LocationQueryParams<Provisional>>,
    body: Body,
) -> axum::response::Result<impl IntoResponse> {
    check_kill_switch(&state)?;

    relay_log::trace!("Validating headers");
    tus::validate_patch_headers(&headers).map_err(Error::from)?;

    let location =
        SignedLocation::from_parts(project_id, key, upload_length, upload_signature, other);

    let config = state.config();

    // There is no real "fast path" for streaming uploads. Always wait for the project config
    // to be loaded:
    relay_log::trace!("Awaiting project config");
    let project = state
        .project_cache_handle()
        .ready(meta.public_key(), config.query_timeout()) // uses same timeout as `Upstream`
        .await
        .ok_or_else(|| {
            relay_log::warn!("timeout waiting for project config");
            StatusCode::SERVICE_UNAVAILABLE
        })?;

    relay_log::trace!("Checking request");
    let checked_upload = validate(&state, meta, project).await?;

    let stream = body
        .into_data_stream()
        .map(|result| result.map_err(io::Error::other))
        .boxed();
    let stream = MeteredStream::new(stream, "upload");

    let (lower_bound, upper_bound) = match upload_length.value() {
        None => (1, config.max_upload_size()),
        Some(u) => (u, u),
    };
    let stream = BoundedStream::new(stream, lower_bound, upper_bound);
    let byte_counter = stream.byte_counter();

    relay_log::trace!("Uploading");
    let result = upload(
        &state,
        checked_upload.scoping,
        checked_upload.upstream,
        location,
        stream,
    )
    .await;
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

fn check_kill_switch(state: &ServiceState) -> Result<(), StatusCode> {
    if !state.global_config_handle().is_ready() {
        relay_log::warn!("global config not available");
    }

    if state.global_config_handle().current().is_none() {
        relay_log::info!("check_kill_switch global config is none");
    }

    if !state
        .global_config_handle()
        .current()
        .unwrap_or_default()
        .options
        .endpoint_fetch_config_enabled
    {
        relay_statsd::metric!(counter(RelayCounters::UploadKillswitched) += 1);
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(())
}

async fn create(
    state: &ServiceState,
    scoping: Scoping,
    upstream: Option<UpstreamDescriptor>,
    headers: &tus::Headers,
) -> Result<SignedLocation<Provisional>, Error> {
    let location = state
        .upload()
        .send(upload::Create {
            scoping,
            upstream,
            length: headers.upload_length,
            attachment_type: headers.metadata.map(|m| m.attachment_type),
        })
        .await??;

    Ok(location)
}

async fn upload(
    state: &ServiceState,
    scoping: Scoping,
    upstream: Option<UpstreamDescriptor>,
    location: SignedLocation<Provisional>,
    stream: BoundedStream<MeteredStream<ByteStream>>,
) -> Result<SignedLocation<Final>, Error> {
    let location = state
        .upload()
        .send(upload::Stream {
            received: Utc::now(),
            scoping,
            upstream,
            location,
            stream,
        })
        .await??;

    Ok(location)
}

struct CheckedUpload {
    scoping: Scoping,
    upstream: Option<UpstreamDescriptor>,
}

/// Check request by converting it into a pseudo-envelope.
///
/// This is currently the easiest way to guarantee that the upload gets checked the same way as
/// the envelope.
async fn validate_and_limit(
    state: &ServiceState,
    meta: RequestMeta,
    headers: &tus::Headers,
    project: Project<'_>,
) -> Result<CheckedUpload, BadStoreRequest> {
    let mut envelope = Envelope::from_request(None, meta);
    envelope.require_feature(Feature::UploadEndpoint);
    let mut item = Item::new(ItemType::Attachment);
    item.set_payload(ContentType::AttachmentRef, vec![]);
    item.set_attachment_length(headers.upload_length.unwrap_or(1));
    if let Some(ref metadata) = headers.metadata {
        item.set_attachment_type(metadata.attachment_type);

        if let Some(feature) = required_feature(metadata.attachment_type) {
            envelope.require_feature(feature);
        }
    }
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
    let upstream = project_upstream(&project);
    envelope.accept(|x| x);
    Ok(CheckedUpload { scoping, upstream })
}

/// Returns the feature a project must have enabled to upload attachments with the given type.
fn required_feature(attachment_type: AttachmentType) -> Option<Feature> {
    match attachment_type {
        AttachmentType::Minidump => Some(Feature::MinidumpUploads),
        _ => None,
    }
}

async fn validate(
    state: &ServiceState,
    meta: RequestMeta,
    project: Project<'_>,
) -> Result<CheckedUpload, BadStoreRequest> {
    let mut envelope = Envelope::from_request(None, meta);
    envelope.require_feature(Feature::UploadEndpoint);
    let mut envelope = Managed::from_envelope(envelope, state.outcome_aggregator().clone());

    let _ = project
        .check_envelope(&mut envelope)
        .await
        .map_err(|err| err.map(BadStoreRequest::EventRejected).into_inner())?;

    // We are not really processing an envelope here, only keep the updated scoping:
    let scoping = envelope.scoping();
    let upstream = project_upstream(&project);
    envelope.accept(|x| x);
    Ok(CheckedUpload { scoping, upstream })
}

fn project_upstream(project: &Project<'_>) -> Option<UpstreamDescriptor> {
    match project.state() {
        ProjectState::Enabled(info) => info.upstream.clone(),
        ProjectState::Dummy | ProjectState::Disabled | ProjectState::Pending => None,
    }
}

fn is_hyper_user_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<hyper::Error>()
        .is_some_and(hyper::Error::is_user)
}
