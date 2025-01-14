//! Processor code related to standalone spans.

use std::sync::Arc;

use prost::Message;
use relay_dynamic_config::Feature;
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_spans::otel_trace::TracesData;

use crate::envelope::{ContentType, Item, ItemType};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{payload, should_filter, SpanGroup};
use crate::utils::ItemAction;
use crate::utils::TypedEnvelope;

#[cfg(feature = "processing")]
mod processing;
use crate::services::projects::project::ProjectInfo;
#[cfg(feature = "processing")]
pub use processing::*;
use relay_config::Config;

pub fn filter(
    payload: &mut payload::NoEvent<SpanGroup>,
    config: Arc<Config>,
    project_info: Arc<ProjectInfo>,
) {
    let disabled = should_filter(&config, &project_info, Feature::StandaloneSpanIngestion);
    let otel_disabled = should_filter(&config, &project_info, Feature::OtelEndpoint);

    payload.managed_envelope.retain_items(|item| {
        if disabled && item.is_span() {
            relay_log::debug!("dropping span because feature is disabled");
            ItemAction::DropSilently
        } else if otel_disabled && item.ty() == &ItemType::OtelTracesData {
            relay_log::debug!("dropping otel trace because feature is disabled");
            ItemAction::DropSilently
        } else {
            ItemAction::Keep
        }
    });
}

pub fn convert_otel_traces_data(payload: &mut payload::NoEvent<SpanGroup>) {
    let envelope = payload.managed_envelope.envelope_mut();

    for item in envelope.take_items_by(|item| item.ty() == &ItemType::OtelTracesData) {
        convert_traces_data(payload, item);
    }
}

fn convert_traces_data(payload: &mut payload::NoEvent<SpanGroup>, item: Item) {
    let traces_data = match parse_traces_data(item) {
        Ok(traces_data) => traces_data,
        Err(reason) => {
            // NOTE: logging quantity=1 is semantically wrong, but we cannot know the real quantity
            // without parsing.
            track_invalid(payload, reason);
            return;
        }
    };
    for resource in traces_data.resource_spans {
        for scope in resource.scope_spans {
            for span in scope.spans {
                // TODO: resources and scopes contain attributes, should denormalize into spans?
                let Ok(serialized_payload) = serde_json::to_vec(&span) else {
                    track_invalid(payload, DiscardReason::Internal);
                    continue;
                };
                let mut item = Item::new(ItemType::OtelSpan);
                item.set_payload(ContentType::Json, serialized_payload);
                payload.managed_envelope.envelope_mut().add_item(item);
            }
        }
    }
    payload.managed_envelope.update(); // update envelope summary
}

fn track_invalid(payload: &mut payload::NoEvent<SpanGroup>, reason: DiscardReason) {
    payload
        .managed_envelope
        .track_outcome(Outcome::Invalid(reason), DataCategory::Span, 1);
    payload
        .managed_envelope
        .track_outcome(Outcome::Invalid(reason), DataCategory::SpanIndexed, 1);
}

fn parse_traces_data(item: Item) -> Result<TracesData, DiscardReason> {
    match item.content_type() {
        Some(&ContentType::Json) => serde_json::from_slice(&item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as JSON"
            );
            DiscardReason::InvalidJson
        }),
        Some(&ContentType::Protobuf) => TracesData::decode(item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as protobuf"
            );
            DiscardReason::InvalidProtobuf
        }),
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
) -> Option<Span> {
    let mut spans = [Span::from(event).into()];

    tag_extraction::extract_span_tags(event, &mut spans, max_tag_value_size, span_allowed_hosts);
    tag_extraction::extract_segment_span_tags(event, &mut spans);

    spans.into_iter().next().and_then(Annotated::into_value)
}
