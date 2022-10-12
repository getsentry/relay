//! Common facilities for ingesting events through store-like endpoints.

use std::cell::RefCell;
use std::fmt::Write;
use std::rc::Rc;

use actix::prelude::*;
use actix_web::http::{header, StatusCode};
use actix_web::middleware::cors::{Cors, CorsBuilder};
use actix_web::{error::PayloadError, HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use futures01::prelude::*;
use serde::Deserialize;

use relay_common::{clone, tryf};
use relay_general::protocol::{EventId, EventType};
use relay_log::LogError;
use relay_quotas::RateLimits;
use relay_statsd::metric;

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::actors::processor::{EnvelopeProcessor, ProcessMetrics};
use crate::actors::project_cache::{CheckEnvelope, ProjectCache, ValidateEnvelope};
use crate::envelope::{AttachmentType, Envelope, EnvelopeError, Item, ItemType, Items};
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};
use crate::statsd::RelayCounters;
use crate::utils::{
    self, ApiErrorResponse, BufferError, BufferGuard, EnvelopeContext, FormDataIter, MultipartError,
};

#[derive(Fail, Debug)]
pub enum BadStoreRequest {
    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),

    #[fail(display = "could not schedule event processing")]
    ScheduleFailed,

    #[fail(display = "empty request body")]
    EmptyBody,

    #[fail(display = "empty envelope")]
    EmptyEnvelope,

    #[fail(display = "invalid JSON data")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "invalid messagepack data")]
    InvalidMsgpack(#[cause] rmp_serde::decode::Error),

    #[fail(display = "invalid event envelope")]
    InvalidEnvelope(#[cause] EnvelopeError),

    #[fail(display = "invalid multipart data")]
    InvalidMultipart(#[cause] MultipartError),

    #[fail(display = "invalid minidump")]
    InvalidMinidump,

    #[fail(display = "missing minidump")]
    MissingMinidump,

    #[fail(display = "invalid event id")]
    InvalidEventId,

    #[fail(display = "failed to queue envelope")]
    QueueFailed(#[cause] BufferError),

    #[fail(display = "failed to read request body")]
    PayloadError(#[cause] PayloadError),

    #[fail(
        display = "Sentry dropped data due to a quota or internal rate limit being reached. This will not affect your application. See https://docs.sentry.io/product/accounts/quotas/ for more information."
    )]
    RateLimited(RateLimits),

    #[fail(display = "event submission rejected with_reason: {:?}", _0)]
    EventRejected(DiscardReason),
}

impl ResponseError for BadStoreRequest {
    fn error_response(&self) -> HttpResponse {
        let body = ApiErrorResponse::from_fail(self);

        match self {
            BadStoreRequest::RateLimited(rate_limits) => {
                let retry_after_header = rate_limits
                    .longest()
                    .map(|limit| limit.retry_after.remaining_seconds().to_string())
                    .unwrap_or_default();

                let rate_limits_header = utils::format_rate_limits(rate_limits);

                // For rate limits, we return a special status code and indicate the client to hold
                // off until the rate limit period has expired. Currently, we only support the
                // delay-seconds variant of the Rate-Limit header.
                HttpResponse::build(StatusCode::TOO_MANY_REQUESTS)
                    .header(header::RETRY_AFTER, retry_after_header)
                    .header(utils::RATE_LIMITS_HEADER, rate_limits_header)
                    .json(&body)
            }
            BadStoreRequest::ScheduleFailed | BadStoreRequest::QueueFailed(_) => {
                // These errors indicate that something's wrong with our actor system, most likely
                // mailbox congestion or a faulty shutdown. Indicate an unavailable service to the
                // client. It might retry event submission at a later time.
                HttpResponse::ServiceUnavailable().json(&body)
            }
            BadStoreRequest::EventRejected(_) => {
                // The event has been discarded, which is generally indicated with a 403 error.
                // Originally, Sentry also used this status code for event filters, but these are
                // now executed asynchronously in `EnvelopeProcessor`.
                HttpResponse::Forbidden().json(&body)
            }
            BadStoreRequest::PayloadError(PayloadError::Overflow) => {
                HttpResponse::PayloadTooLarge().json(&body)
            }
            _ => {
                // In all other cases, we indicate a generic bad request to the client and render
                // the cause. This was likely the client's fault.
                HttpResponse::BadRequest().json(&body)
            }
        }
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
        .find(|item| item.attachment_type() == Some(AttachmentType::EventPayload))
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

/// Creates a preconfigured CORS middleware builder for store requests.
///
/// To configure CORS, register endpoints using `resource()` and finalize by calling `register()`, which
/// returns an App. This configures POST as allowed method, allows default sentry headers, and
/// exposes the return headers.
pub fn cors(app: ServiceApp) -> CorsBuilder<ServiceState> {
    let mut builder = Cors::for_app(app);

    builder
        .allowed_methods(vec!["POST"])
        .allowed_headers(vec![
            "x-sentry-auth",
            "x-requested-with",
            "x-forwarded-for",
            "origin",
            "referer",
            "accept",
            "content-type",
            "authentication",
            "authorization",
            "content-encoding",
            "transfer-encoding",
        ])
        .expose_headers(vec![
            "x-sentry-error",
            "x-sentry-rate-limits",
            "retry-after",
        ])
        .max_age(3600);

    builder
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
/// - Metrics are directly sent to the [`EnvelopeProcessor`], bypassing the manager's queue and
///   going straight into metrics aggregation. See [`ProcessMetrics`] for a full description.
///
/// Queueing can fail if the queue exceeds `envelope_buffer_size`. In this case, `Err` is
/// returned and the envelope is not queued.
fn queue_envelope(
    mut envelope: Envelope,
    mut envelope_context: EnvelopeContext,
    buffer_guard: &BufferGuard,
) -> Result<(), BadStoreRequest> {
    // Remove metrics from the envelope and queue them directly on the project's `Aggregator`.
    let mut metric_items = Vec::new();
    let is_metric = |i: &Item| matches!(i.ty(), ItemType::Metrics | ItemType::MetricBuckets);
    while let Some(item) = envelope.take_item_by(is_metric) {
        metric_items.push(item);
    }

    if !metric_items.is_empty() {
        relay_log::trace!("sending metrics into processing queue");
        EnvelopeProcessor::from_registry().send(ProcessMetrics {
            items: metric_items,
            project_key: envelope.meta().public_key(),
            start_time: envelope.meta().start_time(),
            sent_at: envelope.sent_at(),
        });
    }

    // Split the envelope into event-related items and other items. This allows to fast-track:
    //  1. Envelopes with only session items. They only require rate limiting.
    //  2. Event envelope processing can bail out if the event is filtered or rate limited,
    //     since all items depend on this event.
    if let Some(event_envelope) = envelope.split_by(Item::requires_event) {
        relay_log::trace!("queueing separate envelope for non-event items");

        // The envelope has been split, so we need to fork the context.
        let event_context = buffer_guard
            .enter(&event_envelope)
            .map_err(BadStoreRequest::QueueFailed)?;

        // Update the old context after successful forking.
        envelope_context.update(&envelope);
        ProjectCache::from_registry().do_send(ValidateEnvelope::new(event_envelope, event_context));
    }

    if envelope.is_empty() {
        // The envelope can be empty here if it contained only metrics items which were removed
        // above. In this case, the envelope was accepted and needs no further queueing.
        envelope_context.accept();
    } else {
        relay_log::trace!("queueing envelope");
        ProjectCache::from_registry().do_send(ValidateEnvelope::new(envelope, envelope_context));
    }

    Ok(())
}

/// Handles Sentry events.
///
/// Sentry events may come either directly from an HTTP request (the store endpoint calls this
/// method directly) or are generated inside Relay from requests to other endpoints (e.g. the
/// security endpoint).
///
/// If store_event receives a non-empty store_body it will use it as the body of the event otherwise
/// it will try to create a store_body from the request.
pub fn handle_store_like_request<F, R, I>(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
    extract_envelope: F,
    create_response: R,
    emit_rate_limit: bool,
) -> ResponseFuture<HttpResponse, BadStoreRequest>
where
    F: FnOnce(&HttpRequest<ServiceState>, RequestMeta) -> I + 'static,
    I: IntoFuture<Item = Envelope, Error = BadStoreRequest> + 'static,
    R: FnOnce(Option<EventId>) -> HttpResponse + Copy + 'static,
{
    // For now, we only handle <= v8 and drop everything else
    let version = meta.version();
    if version > relay_common::PROTOCOL_VERSION {
        // TODO: Delegate to forward_upstream here
        tryf!(Err(BadStoreRequest::UnsupportedProtocolVersion(version)));
    }

    metric!(
        counter(RelayCounters::EventProtocol) += 1,
        version = &format!("{}", version)
    );

    let buffer_guard = request.state().buffer_guard();
    let config = request.state().config();
    let event_id = Rc::new(RefCell::new(None));

    let future = extract_envelope(&request, meta)
        .into_future()
        .and_then(clone!(config, event_id, |mut envelope| {
            *event_id.borrow_mut() = envelope.event_id();

            // If configured, remove unknown items at the very beginning. If the envelope is
            // empty, we fail the request with a special control flow error to skip checks and
            // queueing, that still results in a `200 OK` response.
            utils::remove_unknown_items(&config, &mut envelope);

            let mut envelope_context = request
                .state()
                .buffer_guard()
                .enter(&envelope)
                .map_err(BadStoreRequest::QueueFailed)?;

            if envelope.is_empty() {
                envelope_context.reject(Outcome::Invalid(DiscardReason::EmptyEnvelope));
                Err(BadStoreRequest::EmptyEnvelope)
            } else {
                Ok((envelope, envelope_context))
            }
        }))
        .and_then(move |(envelope, envelope_context)| {
            ProjectCache::from_registry()
                .send(CheckEnvelope::new(envelope, envelope_context))
                .map_err(|_| BadStoreRequest::ScheduleFailed)
        })
        .and_then(move |response| {
            let checked = response.map_err(BadStoreRequest::EventRejected)?;

            if let Some((envelope, mut envelope_context)) = checked.envelope {
                if !utils::check_envelope_size_limits(&config, &envelope) {
                    envelope_context.reject(Outcome::Invalid(DiscardReason::TooLarge));
                    return Err(BadStoreRequest::PayloadError(PayloadError::Overflow));
                }

                let event_id = envelope.event_id();
                queue_envelope(envelope, envelope_context, &buffer_guard)?;

                if !checked.rate_limits.is_limited() {
                    return Ok(create_response(event_id));
                }
            }

            Err(BadStoreRequest::RateLimited(checked.rate_limits))
        })
        .or_else(move |error: BadStoreRequest| {
            metric!(counter(RelayCounters::EnvelopeRejected) += 1);
            let event_id = *event_id.borrow();

            if !emit_rate_limit && matches!(error, BadStoreRequest::RateLimited(_)) {
                return Ok(create_response(event_id));
            }

            // This is a control-flow error without a bad status code.
            if matches!(error, BadStoreRequest::EmptyEnvelope) {
                return Ok(create_response(event_id));
            }

            let response = error.error_response();
            if response.status().is_server_error() {
                relay_log::error!("error handling request: {}", LogError(&error));
            }

            Ok(response)
        });

    Box::new(future)
}

/// Creates a HttpResponse containing the textual representation of the given EventId
pub fn create_text_event_id_response(id: Option<EventId>) -> HttpResponse {
    // Event id is set statically in the ingest path.
    let id = id.unwrap_or_default();
    debug_assert!(!id.is_nil());

    // the minidump client expects the response to contain an event id as a hyphenated UUID
    // i.e. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    HttpResponse::Ok()
        .content_type("text/plain")
        .body(format!("{}", id.0.to_hyphenated()))
}

/// A helper for creating Actix routes that are resilient against double-slashes
///
/// Write `normpath("api/store")` to create a route pattern that matches "/api/store/",
/// "api//store", "api//store////", etc.
pub fn normpath(route: &str) -> String {
    let mut pattern = String::new();
    for (i, segment) in route.trim_matches('/').split('/').enumerate() {
        // Apparently the leading slash needs to be explicit and cannot be part of a pattern
        let _ = write!(
            pattern,
            "/{{multislash{i}:/*}}{segment}",
            i = i,
            segment = segment
        );
    }

    if route.ends_with('/') {
        pattern.push_str("{trailing_slash:/+}");
    } else {
        pattern.push_str("{trailing_slash:/*}");
    }
    pattern
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normpath() {
        assert_eq!(
            normpath("/api/store/"),
            "/{multislash0:/*}api/{multislash1:/*}store{trailing_slash:/+}"
        );
        assert_eq!(
            normpath("/api/store"),
            "/{multislash0:/*}api/{multislash1:/*}store{trailing_slash:/*}"
        );
    }

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
