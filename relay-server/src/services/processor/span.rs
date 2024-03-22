//! Processor code related to standalone spans.

use relay_dynamic_config::Feature;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};

use crate::services::processor::SpanGroup;
use crate::{envelope::ItemType, services::processor::ProcessEnvelopeState, utils::ItemAction};

#[cfg(feature = "processing")]
mod processing;
#[cfg(feature = "processing")]
pub use processing::*;

pub fn filter(state: &mut ProcessEnvelopeState<SpanGroup>) {
    let standalone_span_ingestion_enabled = state
        .project_state
        .has_feature(Feature::StandaloneSpanIngestion);
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::OtelSpan | ItemType::Span => {
            if !standalone_span_ingestion_enabled {
                relay_log::warn!("dropping span because feature is disabled");
                ItemAction::DropSilently
            } else {
                ItemAction::Keep
            }
        }
        _ => ItemAction::Keep,
    });
}

/// Creates a span from the transaction and applies tag extraction on it.
pub fn extract_transaction_span(event: &Event, max_tag_value_size: usize) -> Span {
    let mut spans = [Span::from(event).into()];

    tag_extraction::extract_span_tags(event, &mut spans, max_tag_value_size);

    spans.into_iter().next().unwrap().into_value().unwrap() // TODO
}
