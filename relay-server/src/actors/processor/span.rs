//! Processor code related to standalone spans.
use chrono::{DateTime, Utc};
use relay_base_schema::events::EventType;
use relay_event_normalization::NormalizeProcessorConfig;
use relay_event_schema::processor::{process_value, ProcessingState};
use relay_event_schema::protocol::{Contexts, Event, Span, TraceContext};

use relay_protocol::Annotated;

use crate::actors::processor::ProcessingError;

/// Config needed to normalize a standalone span.
pub struct NormalizeSpanConfig {
    /// The time at which the event was received in this Relay.
    pub received_at: DateTime<Utc>,
    /// The maximum amount of seconds an event can be dated in the past.
    pub max_secs_in_past: i64,
    /// The maximum amount of seconds an event can be predated into the future.
    pub max_secs_in_future: i64,
    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_length: usize,
}

/// Normalizes a standalone span.
///
/// Internally encapsulates the span in a temporary event in order to run
/// full normalization on it.
pub fn normalize_span(
    mut annotated_span: Annotated<Span>,
    config: NormalizeSpanConfig,
) -> Result<Annotated<Span>, ProcessingError> {
    let Some(span) = annotated_span.value_mut() else {
        return Err(ProcessingError::NoEventPayload);
    };

    let mut pseudo_event = Annotated::new(Event {
        ty: EventType::Transaction.into(),
        timestamp: span.timestamp.clone(),
        start_timestamp: span.start_timestamp.clone(),
        received: span.received.clone(),
        contexts: {
            let mut contexts = Contexts::new();
            contexts.add(TraceContext {
                trace_id: span.trace_id.clone(),
                span_id: span.span_id.clone(),
                ..Default::default()
            });
            contexts.into()
        },
        spans: Annotated::new(vec![annotated_span]),
        ..Default::default()
    });

    let NormalizeSpanConfig {
        received_at,
        max_secs_in_past,
        max_secs_in_future,
        max_tag_value_length,
    } = config;

    let processor_config = NormalizeProcessorConfig {
        received_at: Some(received_at),
        max_secs_in_past: Some(max_secs_in_past),
        max_secs_in_future: Some(max_secs_in_future),
        enrich_spans: true,
        max_tag_value_length,
        is_renormalize: false,
        light_normalize_spans: true,
        enable_trimming: true,
        ..Default::default()
    };

    process_value(
        &mut pseudo_event,
        &mut relay_event_normalization::NormalizeProcessor::new(processor_config),
        ProcessingState::root(),
    )?;

    // Take the span back from the event
    match pseudo_event
        .into_value()
        .and_then(|e| e.spans.into_value().and_then(|mut spans| spans.pop()))
    {
        Some(mut annotated_span) => {
            // HACK remove auto-generated transaction tag:
            if let Some(span) = annotated_span.value_mut() {
                if let Some(tags) = span.sentry_tags.value_mut() {
                    tags.remove("transaction");
                }
            }
            Ok(annotated_span)
        }
        None => Err(ProcessingError::NoEventPayload),
    }
}
