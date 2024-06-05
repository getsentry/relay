//! Processor code related to standalone spans.

use relay_dynamic_config::Feature;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;

use crate::services::processor::SpanGroup;
use crate::{services::processor::ProcessEnvelopeState, utils::ItemAction};

#[cfg(feature = "processing")]
mod processing;
#[cfg(feature = "processing")]
pub use processing::*;

pub fn filter(state: &mut ProcessEnvelopeState<SpanGroup>) {
    let standalone_span_ingestion_disabled = !state
        .project_state
        .has_feature(Feature::StandaloneSpanIngestion);
    state.managed_envelope.retain_items(|item| {
        if item.is_span() && standalone_span_ingestion_disabled {
            relay_log::debug!("dropping span because feature is disabled");
            ItemAction::DropSilently
        } else {
            ItemAction::Keep
        }
    });
}

/// Creates a span from the transaction and applies tag extraction on it.
///
/// Returns `None` when [`tag_extraction::extract_span_tags`] clears the span, which it shouldn't.
pub fn extract_transaction_span(event: &Event, max_tag_value_size: usize) -> Option<Span> {
    let mut spans = [Span::from(event).into()];

    tag_extraction::extract_span_tags(event, &mut spans, max_tag_value_size);
    tag_extraction::extract_segment_span_tags(event, &mut spans);

    spans.into_iter().next().and_then(Annotated::into_value)
}
