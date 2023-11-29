//! Processor code related to standalone spans.
use chrono::{DateTime, Utc};
use relay_base_schema::events::EventType;
use relay_event_normalization::span::tag_extraction;
use relay_event_normalization::{
    NormalizeProcessorConfig, SchemaProcessor, TimestampProcessor, TransactionsProcessor,
    TrimmingProcessor,
};
use relay_event_schema::processor::{process_value, ProcessingState};
use relay_event_schema::protocol::{Contexts, Event, Span, TraceContext};

use relay_metrics::UnixTimestamp;
use relay_protocol::Annotated;

use crate::actors::processor::ProcessingError;

/// Config needed to normalize a standalone span.
pub struct NormalizeSpanConfig {
    /// Allowed time range for transactions.
    pub transaction_range: std::ops::Range<UnixTimestamp>,
    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_size: usize,
}

/// Normalizes a standalone span.
pub fn normalize_span(
    annotated_span: &mut Annotated<Span>,
    config: NormalizeSpanConfig,
) -> Result<(), ProcessingError> {
    let NormalizeSpanConfig {
        transaction_range,
        max_tag_value_size,
    } = config;

    // This follows the steps of `NormalizeProcessor::process_event`.
    // Ideally, `NormalizeProcessor` would execute these steps generically, i.e. also when calling
    // `process_spans` on it.

    process_value(
        annotated_span,
        &mut SchemaProcessor,
        ProcessingState::root(),
    )?;

    process_value(
        annotated_span,
        &mut TimestampProcessor,
        ProcessingState::root(),
    )?;

    process_value(
        annotated_span,
        &mut TransactionsProcessor::new(Default::default(), Some(transaction_range)),
        ProcessingState::root(),
    )?;

    let Some(span) = annotated_span.value_mut() else {
        return Err(ProcessingError::NoEventPayload);
    };
    let config = tag_extraction::Config { max_tag_value_size };
    tag_extraction::extract_tags(span, &config, None, None);

    process_value(
        annotated_span,
        &mut TrimmingProcessor::new(),
        ProcessingState::root(),
    )?;

    Ok(())
}
