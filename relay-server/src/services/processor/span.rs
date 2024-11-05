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
        convert_trace_data(item, |item: Item| envelope.add_item(item));
    }
}

#[derive(Debug, thiserror::Error)]
enum ConvertOtelError {
    #[error("invalid content type: {0:?}")]
    InvalidContentType(Option<ContentType>),

    #[error("parse error: {0}")]
    ParseError(#[from] prost::DecodeError),
}

fn convert_trace_data<F: Fn(Item)>(item: Item, yield_: F) -> Result<(), ConvertOtelError> {
    let traces_data: TracesData = match item.content_type() {
        Some(&ContentType::Json) => {
            todo!()
        }
        Some(&ContentType::Protobuf) => TracesData::decode(item.payload())?,
        other => return Err(ConvertOtelError::InvalidContentType(other.cloned())),
    };
    for resource_span in traces_data.resource_spans {
        for scope_span in resource_span.scope_spans {
            for span in scope_span.spans {
                let Ok(payload) = serde_json::to_vec(&span) else {
                    // TODO: emit outcome
                    continue;
                };
                let mut item = Item::new(ItemType::OtelSpan);
                item.set_payload(ContentType::Json, payload);
                yield_(item);
            }
        }
    }

    Ok(())
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
