//! Processor code related to standalone spans.

use std::collections::BTreeSet;

use prost::Message;
use relay_dynamic_config::Feature;
use relay_event_normalization::span::description::ScrubMongoDescription;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_spans::otel_trace::TracesData;

use crate::envelope::{ContentType, Item, ItemType};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::SpanGroup;
use crate::utils::TypedEnvelope;
use crate::Envelope;
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
        convert_trace_data(item, &mut state.managed_envelope);
    }
}

fn convert_trace_data(item: Item, managed_envelope: &mut TypedEnvelope<SpanGroup>) {
    let traces_data = match parse_traces_data(item) {
        Ok(traces_data) => traces_data,
        Err(reason) => {
            // NOTE: logging quantity=1 is semantically wrong, but we cannot know the real quantity
            // without parsing.
            track_invalid(managed_envelope, reason);
            return;
        }
    };
    for resource in traces_data.resource_spans {
        for scope in resource.scope_spans {
            for span in scope.spans {
                // TODO: resources and scopes contain attributes, should denormalize into spans?
                let Ok(payload) = serde_json::to_vec(&span) else {
                    track_invalid(managed_envelope, DiscardReason::Internal);
                    continue;
                };
                let mut item = Item::new(ItemType::OtelSpan);
                item.set_payload(ContentType::Json, payload);
                managed_envelope.envelope_mut().add_item(item);
            }
        }
    }
    managed_envelope.update(); // update envelope summary
}

fn track_invalid(managed_envelope: &mut TypedEnvelope<SpanGroup>, reason: DiscardReason) {
    managed_envelope.track_outcome(Outcome::Invalid(reason), DataCategory::Span, 1);
    managed_envelope.track_outcome(Outcome::Invalid(reason), DataCategory::SpanIndexed, 1);
}

fn parse_traces_data(item: Item) -> Result<TracesData, DiscardReason> {
    match item.content_type() {
        Some(&ContentType::Json) => {
            serde_json::from_slice(&item.payload()).map_err(|_| DiscardReason::InvalidJson)
        }
        Some(&ContentType::Protobuf) => {
            TracesData::decode(item.payload()).map_err(|_| DiscardReason::InvalidProtobuf)
        }
        _ => Err(DiscardReason::ContentType),
    }
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
