//! Processor code related to standalone spans.

use relay_dynamic_config::Feature;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;

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
pub fn extract_transaction_span(event: &Event) -> Span {
    let mut transaction_span: Span = event.into();

    let mut shared_tags = tag_extraction::extract_shared_tags(event);
    if let Some(span_op) = dbg!(transaction_span.op.value()) {
        shared_tags.insert(tag_extraction::SpanTagKey::SpanOp, span_op.to_owned());
    }

    transaction_span.sentry_tags = Annotated::new(
        shared_tags
            .clone()
            .into_iter()
            .map(|(k, v)| (k.sentry_tag_key().to_owned(), Annotated::new(v)))
            .collect(),
    );

    transaction_span
}
