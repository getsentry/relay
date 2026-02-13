//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

mod tus;

use std::io;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, post};
use futures::StreamExt;
use http::header;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_quotas::Scoping;
use tower_http::limit::RequestBodyLimitLayer;

use crate::Envelope;
use crate::endpoints::common::BadStoreRequest;
use crate::envelope::{ContentType, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::service::ServiceState;
use crate::services::projects::cache::Project;
use crate::utils::upload::SignedLocation;
use crate::utils::{ExactStream, upload};

use self::tus::validate_headers;

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
        todo!()
    }
}

impl IntoResponse for SignedLocation {
    fn into_response(self) -> Response {
        tus::response()
            .header(header::LOCATION, self.into_header_value())
            .status(StatusCode::CREATED)
            .body(axum::body::Body::empty())
            .expect("failed to create body")
    }
}

async fn handle(
    state: ServiceState,
    meta: RequestMeta,
    headers: HeaderMap,
    body: Body,
) -> axum::response::Result<impl IntoResponse> {
    Ok(handle_upload(state, meta, headers, body)
        .await
        .into_response())
}

/// Handles TUS upload requests (Creation With Upload).
///
/// This endpoint accepts POST requests with a body containing the complete upload data.
/// Unlike the full TUS protocol, this implementation only supports uploading all data
/// in a single request - partial uploads and resumption are not supported.
///
/// The body is processed as a stream to avoid loading the entire upload into memory.
async fn handle_upload(
    state: ServiceState,
    meta: RequestMeta,
    headers: HeaderMap,
    body: Body,
) -> Result<SignedLocation, Error> {
    let upload_length = validate_headers(&headers)?;

    let project = get_project(&state, meta.public_key()).await;

    let scoping = check_request(&state, meta, upload_length, project).await?;
    let stream = body
        .into_data_stream()
        .map(|result| result.map_err(io::Error::other))
        .boxed();
    let stream = ExactStream::new(stream, upload_length);

    let location = upload::Sink::new(&state)
        .upload(state.config(), upload::Stream { scoping, stream })
        .await?;

    Ok(location)
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
