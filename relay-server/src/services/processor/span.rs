//! Processor code related to standalone spans.

use prost::Message;
use relay_dynamic_config::Feature;
use relay_event_normalization::span::description::ScrubMongoDescription;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;
use relay_spans::otel_trace::TracesData;

use crate::envelope::{ContentType, Item, ItemType};
use crate::services::processor::SpanGroup;
use crate::{services::processor::ProcessEnvelopeState, utils::ItemAction};

#[cfg(feature = "processing")]
mod processing;
#[cfg(feature = "processing")]
pub use processing::*;

pub fn filter(state: &mut ProcessEnvelopeState<SpanGroup>) {
    let disabled = state.should_filter(Feature::StandaloneSpanIngestion);
    let otel_disabled = state.should_filter(Feature::OtelEndpoint);

    state.managed_envelope.retain_items(|item| {
        if disabled && item.is_span() {
            relay_log::debug!("dropping span because feature is disabled");
            ItemAction::DropSilently
        } else if otel_disabled && item.ty() == &ItemType::OtelTrace {
            relay_log::debug!("dropping otel trace because feature is disabled");
            ItemAction::DropSilently
        } else {
            ItemAction::Keep
        }
    });
}

pub fn convert_otel(state: &mut ProcessEnvelopeState<SpanGroup>) {
    let envelope = state.managed_envelope.envelope_mut();

    for item in envelope.take_items_by(|item| item.ty() == &ItemType::OtelTrace) {
        match convert_trace_data(item) {
            Ok(items) => {
                for item in items {
                    envelope.add_item(item);
                }
            }
            Err(_) => {
                todo!(); // report outcome
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ConvertOtelError {
    #[error("invalid content type: {0:?}")]
    InvalidContentType(Option<ContentType>),

    #[error("parse error: {0}")]
    ParseError(#[from] prost::DecodeError),
}

fn convert_trace_data(item: Item) -> Result<impl Iterator<Item = Item>, ConvertOtelError> {
    let traces_data: TracesData = match item.content_type() {
        Some(&ContentType::Json) => {
            todo!()
        }
        Some(&ContentType::Protobuf) => TracesData::decode(item.payload())?,
        other => return Err(ConvertOtelError::InvalidContentType(other.cloned())),
    };
    // TODO: should write resource and scope into span?
    let items = traces_data
        .resource_spans
        .into_iter()
        .flat_map(|resource| resource.scope_spans.into_iter())
        .flat_map(|scope_spans| scope_spans.spans.into_iter())
        .flat_map(|span| Item::new(serde_json::to_vec(&span).ok()?)); // TODO: error handling

    Ok(items)
}

/// Creates a span from the transaction and applies tag extraction on it.
///
/// Returns `None` when [`tag_extraction::extract_span_tags`] clears the span, which it shouldn't.
pub fn extract_transaction_span(
    event: &Event,
    max_tag_value_size: usize,
    span_allowed_hosts: &[String],
    scrub_mongo_description: ScrubMongoDescription,
) -> Option<Span> {
    let mut spans = [Span::from(event).into()];

    tag_extraction::extract_span_tags(
        event,
        &mut spans,
        max_tag_value_size,
        span_allowed_hosts,
        scrub_mongo_description,
    );
    tag_extraction::extract_segment_span_tags(event, &mut spans);

    spans.into_iter().next().and_then(Annotated::into_value)
}
