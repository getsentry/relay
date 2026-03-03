//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

use std::io;
use std::pin::Pin;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
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
                upload::Error::Upstream(status) => status,
                upload::Error::InvalidLocation | upload::Error::SigningFailed => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                upload::Error::InvalidSignature => StatusCode::BAD_REQUEST,
                upload::Error::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
                #[cfg(feature = "processing")]
                upload::Error::Objectstore(service_error) => match service_error {
                    objectstore::Error::Timeout => StatusCode::GATEWAY_TIMEOUT,
                    objectstore::Error::LoadShed => StatusCode::SERVICE_UNAVAILABLE,
                    objectstore::Error::UploadFailed(error) => match error {
                        objectstore_client::Error::Reqwest(error) => match error.status() {
                            Some(status) => status,
                            None => StatusCode::INTERNAL_SERVER_ERROR,
                        },
                        _ => StatusCode::INTERNAL_SERVER_ERROR,
                    },
                    objectstore::Error::Uuid(_) => StatusCode::INTERNAL_SERVER_ERROR,
                },
                upload::Error::LoadShed => StatusCode::SERVICE_UNAVAILABLE,
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
    let upload_length =
        tus::validate_headers(&headers, meta.request_trust().is_trusted()).map_err(Error::from)?;
    let config = state.config();

    if upload_length.is_some_and(|len| len > config.max_upload_size()) {
        return Err(StatusCode::PAYLOAD_TOO_LARGE.into());
    }

    // There is no real "fast path" for streaming uploads. Always wait for the project config
    // to be loaded:
    let project = state
        .project_cache_handle()
        .ready(meta.public_key(), config.query_timeout()) // uses same timeout as `Upstream`
        .await
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let received = meta.received_at();
    let scoping = check_request(&state, meta, upload_length, project).await?;
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

    let result = upload(&state, received, scoping, stream).await;

    let location = result.inspect_err(|e| {
        relay_log::warn!(error = e as &dyn std::error::Error, "upload failed");
    })?;

    let mut response = location.into_response();
    response
        .headers_mut()
        .insert(tus::UPLOAD_OFFSET, byte_counter.get().into());

    Ok(response)
}

async fn upload(
    state: &ServiceState,
    received: DateTime<Utc>,
    scoping: Scoping,
    stream: BoundedStream<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>>,
) -> Result<SignedLocation, Error> {
    let location = state
        .upload()
        .send(upload::Create {
            scoping,
            length: stream.length(),
        })
        .await??;

    let location = state
        .upload()
        .send(upload::Stream {
            received,
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
    item.set_attachment_length(upload_length.unwrap_or(1) as u64);
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

fn is_hyper_user_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<hyper::Error>()
        .is_some_and(hyper::Error::is_user)
}
