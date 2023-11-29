//! Common facilities for ingesting events through store-like endpoints.

use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use relay_event_schema::protocol::{EventId, EventType};
use relay_quotas::RateLimits;
use relay_statsd::metric;
use serde::Deserialize;

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::actors::processor::{ProcessMetricMeta, ProcessMetrics};
use crate::actors::project_cache::{CheckEnvelope, ValidateEnvelope};
use crate::envelope::{AttachmentType, Envelope, EnvelopeError, Item, ItemType, Items};
use crate::service::ServiceState;
use crate::statsd::RelayCounters;
use crate::utils::{
    self, ApiErrorResponse, BufferError, BufferGuard, FormDataIter, ManagedEnvelope, MultipartError,
};

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("the service is overloaded")]
pub struct ServiceUnavailable;

impl From<relay_system::SendError> for ServiceUnavailable {
    fn from(_: relay_system::SendError) -> Self {
        Self
    }
}

impl IntoResponse for ServiceUnavailable {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiErrorResponse::from_error(&self),
        )
            .into_response()
    }
}

/// Error type for all store-like requests.
#[derive(Debug, thiserror::Error)]
pub enum BadStoreRequest {
    #[error("could not schedule event processing")]
    ScheduleFailed,

    #[error("empty request body")]
    EmptyBody,

    #[error("invalid request body")]
    InvalidBody(#[source] std::io::Error),

    #[error("invalid JSON data")]
    InvalidJson(#[source] serde_json::Error),

    #[error("invalid messagepack data")]
    InvalidMsgpack(#[source] rmp_serde::decode::Error),

    #[error("invalid event envelope")]
    InvalidEnvelope(#[from] EnvelopeError),

    #[error("invalid multipart data")]
    InvalidMultipart(#[from] multer::Error),

    #[error("invalid multipart data")]
    InvalidMultipartAxum(#[from] MultipartError),

    #[error("invalid minidump")]
    InvalidMinidump,

    #[error("missing minidump")]
    MissingMinidump,

    #[error("invalid event id")]
    InvalidEventId,

    #[error("failed to queue envelope")]
    QueueFailed(#[from] BufferError),

    #[error(
        "envelope exceeded size limits (https://develop.sentry.dev/sdk/envelopes/#size-limits)"
    )]
    Overflow,

    #[error(
        "Sentry dropped data due to a quota or internal rate limit being reached. This will not affect your application. See https://docs.sentry.io/product/accounts/quotas/ for more information."
    )]
    RateLimited(RateLimits),

    #[error("event submission rejected with_reason: {0:?}")]
    EventRejected(DiscardReason),
}

impl IntoResponse for BadStoreRequest {
    fn into_response(self) -> axum::response::Response {
        let body = ApiErrorResponse::from_error(&self);

        let response = match &self {
            BadStoreRequest::RateLimited(rate_limits) => {
                let retry_after_header = rate_limits
                    .longest()
                    .map(|limit| limit.retry_after.remaining_seconds().to_string())
                    .unwrap_or_default();

                let rate_limits_header = utils::format_rate_limits(rate_limits);

                // For rate limits, we return a special status code and indicate the client to hold
                // off until the rate limit period has expired. Currently, we only support the
                // delay-seconds variant of the Rate-Limit header.
                let headers = [
                    (header::RETRY_AFTER.as_str(), retry_after_header),
                    (utils::RATE_LIMITS_HEADER, rate_limits_header),
                ];

                (StatusCode::TOO_MANY_REQUESTS, headers, body).into_response()
            }
            BadStoreRequest::ScheduleFailed | BadStoreRequest::QueueFailed(_) => {
                // These errors indicate that something's wrong with our service system, most likely
                // mailbox congestion or a faulty shutdown. Indicate an unavailable service to the
                // client. It might retry event submission at a later time.
                (StatusCode::SERVICE_UNAVAILABLE, body).into_response()
            }
            BadStoreRequest::EventRejected(_) => {
                // The event has been discarded, which is generally indicated with a 403 error.
                // Originally, Sentry also used this status code for event filters, but these are
                // now executed asynchronously in `EnvelopeProcessor`.
                (StatusCode::FORBIDDEN, body).into_response()
            }
            _ => {
                // In all other cases, we indicate a generic bad request to the client and render
                // the cause. This was likely the client's fault.
                (StatusCode::BAD_REQUEST, body).into_response()
            }
        };

        metric!(counter(RelayCounters::EnvelopeRejected) += 1);
        if response.status().is_server_error() {
            relay_log::error!(
                error = &self as &dyn std::error::Error,
                "error handling request"
            );
        }

        response
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct MinimalEvent {
    #[serde(default, rename = "event_id")]
    pub id: Option<EventId>,

    #[serde(default, rename = "type")]
    pub ty: EventType,
}

/// Parses a minimal subset of the event payload.
///
/// This function validates that the provided payload is valid and returns an `Err` on parse errors.
pub fn minimal_event_from_json(data: &[u8]) -> Result<MinimalEvent, BadStoreRequest> {
    serde_json::from_slice(data).map_err(BadStoreRequest::InvalidJson)
}

/// Extracts the event id from a JSON payload.
///
/// If the payload contains no event id, `Ok(None)` is returned. This function also validates that
/// the provided is valid and returns an `Err` on parse errors. If the event id itself is malformed,
/// an `Err` is returned.
pub fn event_id_from_json(data: &[u8]) -> Result<Option<EventId>, BadStoreRequest> {
    minimal_event_from_json(data).map(|event| event.id)
}

/// Extracts the event id from a MessagePack payload.
///
/// If the payload contains no event id, `Ok(None)` is returned. This function also validates that
/// the provided is valid and returns an `Err` on parse errors. If the event id itself is malformed,
/// an `Err` is returned.
pub fn event_id_from_msgpack(data: &[u8]) -> Result<Option<EventId>, BadStoreRequest> {
    rmp_serde::from_slice(data)
        .map(|MinimalEvent { id, .. }| id)
        .map_err(BadStoreRequest::InvalidMsgpack)
}

/// Extracts the event id from `sentry` JSON payload or the `sentry[event_id]` formdata key.
///
/// If the event id itself is malformed, an `Err` is returned. If there is a `sentry` key containing
/// malformed JSON, an error is returned.
pub fn event_id_from_formdata(data: &[u8]) -> Result<Option<EventId>, BadStoreRequest> {
    for entry in FormDataIter::new(data) {
        if entry.key() == "sentry" {
            return event_id_from_json(entry.value().as_bytes());
        } else if entry.key() == "sentry[event_id]" {
            return entry
                .value()
                .parse()
                .map(Some)
                .map_err(|_| BadStoreRequest::InvalidEventId);
        }
    }

    Ok(None)
}

/// Extracts the event id from multiple items.
///
/// Submitting multiple event payloads is undefined behavior. This function will check for an event
/// id in the following precedence:
///
///  1. The `Event` item.
///  2. The `__sentry-event` event attachment.
///  3. The `sentry` JSON payload.
///  4. The `sentry[event_id]` formdata key.
///
/// # Limitations
///
/// Extracting the event id from chunked formdata fields on the Minidump endpoint (`sentry__1`,
/// `sentry__2`, ...) is not supported. In this case, `None` is returned.
pub fn event_id_from_items(items: &Items) -> Result<Option<EventId>, BadStoreRequest> {
    if let Some(item) = items.iter().find(|item| item.ty() == &ItemType::Event) {
        if let Some(event_id) = event_id_from_json(&item.payload())? {
            return Ok(Some(event_id));
        }
    }

    if let Some(item) = items
        .iter()
        .find(|item| item.attachment_type() == Some(&AttachmentType::EventPayload))
    {
        if let Some(event_id) = event_id_from_msgpack(&item.payload())? {
            return Ok(Some(event_id));
        }
    }

    if let Some(item) = items.iter().find(|item| item.ty() == &ItemType::FormData) {
        // Swallow all other errors here since it is quite common to receive invalid secondary
        // payloads. `EnvelopeProcessor` also retains events in such cases.
        if let Ok(Some(event_id)) = event_id_from_formdata(&item.payload()) {
            return Ok(Some(event_id));
        }
    }

    Ok(None)
}

/// Queues an envelope for processing.
///
/// Depending on the items in the envelope, there are multiple outcomes:
///
/// - Events and event related items, such as attachments, are always queued together. See the
///   [crate-level documentation](crate) for a full description of how envelopes are
///   queued and processed.
/// - Sessions and Session batches are always queued separately. If they occur in the same envelope
///   as an event, they are split off. Their path is the same as other Envelopes.
/// - Metrics are directly sent to the [`crate::actors::processor::EnvelopeProcessor`], bypassing the manager's queue and
///   going straight into metrics aggregation. See [`ProcessMetrics`] for a full description.
///
/// Queueing can fail if the queue exceeds `envelope_buffer_size`. In this case, `Err` is
/// returned and the envelope is not queued.
fn queue_envelope(
    state: &ServiceState,
    mut managed_envelope: ManagedEnvelope,
    buffer_guard: &BufferGuard,
) -> Result<(), BadStoreRequest> {
    let envelope = managed_envelope.envelope_mut();

    // Remove metrics from the envelope and queue them directly on the project's `Aggregator`.
    let is_metric = |i: &Item| matches!(i.ty(), ItemType::Statsd | ItemType::MetricBuckets);
    let metric_items = envelope.take_items_by(is_metric);

    if !metric_items.is_empty() {
        relay_log::trace!("sending metrics into processing queue");
        state.processor().send(ProcessMetrics {
            items: metric_items.into_vec(),
            project_key: envelope.meta().public_key(),
            start_time: envelope.meta().start_time(),
            sent_at: envelope.sent_at(),
        });
    }

    // Remove metric meta from the envelope and send them directly to processing.
    let metric_meta = envelope.take_items_by(|item| matches!(item.ty(), ItemType::MetricMeta));
    if !metric_meta.is_empty() {
        relay_log::trace!("sending metric meta into processing queue");
        state.processor().send(ProcessMetricMeta {
            items: metric_meta.into_vec(),
            project_key: envelope.meta().public_key(),
        })
    }

    // Take all NEL reports and split them up into the separate envelopes with 1 item per
    // envelope.
    for nel_envelope in envelope.split_all_by(|item| matches!(item.ty(), ItemType::Nel)) {
        relay_log::trace!("queueing separate envelopes for NEL report");
        let buffer_guard = state.buffer_guard();
        let nel_envelope = buffer_guard
            .enter(
                nel_envelope,
                state.outcome_aggregator().clone(),
                state.test_store().clone(),
            )
            .map_err(BadStoreRequest::QueueFailed)?;
        state
            .project_cache()
            .send(ValidateEnvelope::new(nel_envelope));
    }

    // Split the envelope into event-related items and other items. This allows to fast-track:
    //  1. Envelopes with only session items. They only require rate limiting.
    //  2. Event envelope processing can bail out if the event is filtered or rate limited,
    //     since all items depend on this event.
    if let Some(event_envelope) = envelope.split_by(Item::requires_event) {
        relay_log::trace!("queueing separate envelope for non-event items");

        // The envelope has been split, so we need to fork the context.
        let event_context = buffer_guard.enter(
            event_envelope,
            state.outcome_aggregator().clone(),
            state.test_store().clone(),
        )?;
        state
            .project_cache()
            .send(ValidateEnvelope::new(event_context));
    }

    // Update the old context before continuing with the source envelope.
    managed_envelope.update();

    if managed_envelope.envelope().is_empty() {
        // The envelope can be empty here if it contained only metrics items which were removed
        // above. In this case, the envelope was accepted and needs no further queueing.
        managed_envelope.accept();
    } else {
        relay_log::trace!("queueing envelope");
        state
            .project_cache()
            .send(ValidateEnvelope::new(managed_envelope));
    }

    Ok(())
}

/// Handles an envelope store request.
///
/// Sentry envelopes may come either directly from an HTTP request (the envelope endpoint calls this
/// method directly) or are generated inside Relay from requests to other endpoints (e.g. the
/// security endpoint).
///
/// This returns `Some(EventId)` if the envelope contains an event, either explicitly as payload or
/// implicitly through an item that will create an event during ingestion.
pub async fn handle_envelope(
    state: &ServiceState,
    envelope: Box<Envelope>,
) -> Result<Option<EventId>, BadStoreRequest> {
    let buffer_guard = state.buffer_guard();
    let mut managed_envelope = buffer_guard
        .enter(
            envelope,
            state.outcome_aggregator().clone(),
            state.test_store().clone(),
        )
        .map_err(BadStoreRequest::QueueFailed)?;

    // If configured, remove unknown items at the very beginning. If the envelope is
    // empty, we fail the request with a special control flow error to skip checks and
    // queueing, that still results in a `200 OK` response.
    utils::remove_unknown_items(state.config(), &mut managed_envelope);

    let event_id = managed_envelope.envelope().event_id();
    if managed_envelope.envelope().is_empty() {
        managed_envelope.reject(Outcome::Invalid(DiscardReason::EmptyEnvelope));
        return Ok(event_id);
    }

    let checked = state
        .project_cache()
        .send(CheckEnvelope::new(managed_envelope))
        .await
        .map_err(|_| BadStoreRequest::ScheduleFailed)?
        .map_err(BadStoreRequest::EventRejected)?;

    let Some(mut managed_envelope) = checked.envelope else {
        return Err(BadStoreRequest::RateLimited(checked.rate_limits));
    };

    if !utils::check_envelope_size_limits(state.config(), managed_envelope.envelope()) {
        managed_envelope.reject(Outcome::Invalid(DiscardReason::TooLarge));
        return Err(BadStoreRequest::Overflow);
    }

    queue_envelope(state, managed_envelope, buffer_guard)?;

    if checked.rate_limits.is_limited() {
        Err(BadStoreRequest::RateLimited(checked.rate_limits))
    } else {
        Ok(event_id)
    }
}

#[derive(Debug)]
pub struct TextResponse(pub Option<EventId>);

impl IntoResponse for TextResponse {
    fn into_response(self) -> axum::response::Response {
        // Event id is set statically in the ingest path.
        let EventId(id) = self.0.unwrap_or_default();
        debug_assert!(!id.is_nil());

        // the minidump client expects the response to contain an event id as a hyphenated UUID
        // i.e. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        let text = id.as_hyphenated().to_string();
        text.into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_empty_event() {
        let json = r#"{}"#;
        let minimal = minimal_event_from_json(json.as_ref()).unwrap();
        assert_eq!(
            minimal,
            MinimalEvent {
                id: None,
                ty: EventType::Default,
            }
        );
    }

    #[test]
    fn test_minimal_event_id() {
        let json = r#"{"event_id": "037af9ac1b49494bacd7ec5114f801d9"}"#;
        let minimal = minimal_event_from_json(json.as_ref()).unwrap();
        assert_eq!(
            minimal,
            MinimalEvent {
                id: Some("037af9ac1b49494bacd7ec5114f801d9".parse().unwrap()),
                ty: EventType::Default,
            }
        );
    }

    #[test]
    fn test_minimal_event_type() {
        let json = r#"{"type": "expectct"}"#;
        let minimal = minimal_event_from_json(json.as_ref()).unwrap();
        assert_eq!(
            minimal,
            MinimalEvent {
                id: None,
                ty: EventType::ExpectCt,
            }
        );
    }

    #[test]
    fn test_minimal_event_invalid_type() {
        let json = r#"{"type": "invalid"}"#;
        let minimal = minimal_event_from_json(json.as_ref()).unwrap();
        assert_eq!(
            minimal,
            MinimalEvent {
                id: None,
                ty: EventType::Default,
            }
        );
    }
}
