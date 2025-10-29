//! Common functionality for processing events and transactions.

use std::sync::OnceLock;

use chrono::Duration as SignedDuration;
use relay_auth::RelayVersion;
use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_event_normalization::ClockDriftProcessor;
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::{Event, Metrics, OtelContext, RelayInfo};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_statsd::metric;

use crate::envelope::{EnvelopeHeaders, Item};
use crate::processing::Context;
use crate::services::processor::{MINIMUM_CLOCK_DRIFT, ProcessingError};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils;

pub fn finalize<'a>(
    // managed_envelope: &mut TypedEnvelope<Group>,
    headers: &EnvelopeHeaders,
    attachments: impl Iterator<Item = &'a Item>,
    event: &mut Annotated<Event>,
    metrics: &mut Metrics,
    config: &Config,
) -> Result<(), ProcessingError> {
    let inner_event = match event.value_mut() {
        Some(event) => event,
        None if !config.processing_enabled() => return Ok(()),
        None => return Err(ProcessingError::NoEventPayload),
    };

    if !config.processing_enabled() {
        static MY_VERSION_STRING: OnceLock<String> = OnceLock::new();
        let my_version = MY_VERSION_STRING.get_or_init(|| RelayVersion::current().to_string());

        inner_event
            .ingest_path
            .get_or_insert_with(Default::default)
            .push(Annotated::new(RelayInfo {
                version: Annotated::new(my_version.clone()),
                public_key: config
                    .public_key()
                    .map_or(Annotated::empty(), |pk| Annotated::new(pk.to_string())),
                other: Default::default(),
            }));
    }

    // Event id is set statically in the ingest path.
    let event_id = headers.event_id().unwrap_or_default();
    debug_assert!(!event_id.is_nil());

    // Ensure that the event id in the payload is consistent with the envelope. If an event
    // id was ingested, this will already be the case. Otherwise, this will insert a new
    // event id. To be defensive, we always overwrite to ensure consistency.
    inner_event.id = Annotated::new(event_id);

    // In processing mode, also write metrics into the event. Most metrics have already been
    // collected at this state, except for the combined size of all attachments.
    if config.processing_enabled() {
        let mut metrics = std::mem::take(metrics);

        let attachment_size = attachments.map(Item::len).sum::<usize>() as u64;
        if attachment_size > 0 {
            metrics.bytes_ingested_event_attachment = Annotated::new(attachment_size);
        }

        inner_event._metrics = Annotated::new(metrics);

        if inner_event.ty.value() == Some(&EventType::Transaction) {
            let platform = utils::platform_tag(inner_event);
            let client_name = utils::client_name_tag(headers.meta().client_name());

            metric!(
                counter(RelayCounters::EventTransaction) += 1,
                source = utils::transaction_source_tag(inner_event),
                platform = platform,
                contains_slashes = if inner_event
                    .transaction
                    .as_str()
                    .unwrap_or_default()
                    .contains('/')
                {
                    "true"
                } else {
                    "false"
                }
            );

            let span_count = inner_event.spans.value().map(Vec::len).unwrap_or(0) as u64;
            metric!(
                histogram(RelayHistograms::EventSpans) = span_count,
                sdk = client_name,
                platform = platform,
            );

            let has_otel = inner_event
                .contexts
                .value()
                .is_some_and(|contexts| contexts.contains::<OtelContext>());

            if has_otel {
                metric!(
                    counter(RelayCounters::OpenTelemetryEvent) += 1,
                    sdk = client_name,
                    platform = platform,
                );
            }
        }

        if let Some(dsc) = headers.dsc()
            && let Ok(Some(value)) = relay_protocol::to_value(dsc)
        {
            inner_event._dsc = Annotated::new(value);
        }
    }

    let mut processor = ClockDriftProcessor::new(headers.sent_at(), headers.meta().received_at())
        .at_least(MINIMUM_CLOCK_DRIFT);
    processor::process_value(event, &mut processor, ProcessingState::root())
        .map_err(|_| ProcessingError::InvalidTransaction)?;

    // Log timestamp delays for all events after clock drift correction. This happens before
    // store processing, which could modify the timestamp if it exceeds a threshold. We are
    // interested in the actual delay before this correction.
    if let Some(timestamp) = event.value().and_then(|e| e.timestamp.value()) {
        let event_delay = headers.meta().received_at() - timestamp.into_inner();
        if event_delay > SignedDuration::minutes(1) {
            let category = event_category(event).unwrap_or(DataCategory::Unknown);
            metric!(
                timer(RelayTimers::TimestampDelay) = event_delay.to_std().unwrap(),
                category = category.name(),
            );
        }
    }

    Ok(())
}

/// Returns the data category if there is an event.
///
/// The data category is computed from the event type. Both `Default` and `Error` events map to
/// the `Error` data category. If there is no Event, `None` is returned.
fn event_category(event: &Annotated<Event>) -> Option<DataCategory> {
    event_type(event).map(DataCategory::from)
}

/// Returns the event type if there is an event.
///
/// If the event does not have a type, `Some(EventType::Default)` is assumed. If, in contrast, there
/// is no event, `None` is returned.
fn event_type(event: &Annotated<Event>) -> Option<EventType> {
    event
        .value()
        .map(|event| event.ty.value().copied().unwrap_or_default())
}
