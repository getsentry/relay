//! Processor code related to standalone spans.

use relay_dynamic_config::{Feature, GlobalConfig};
use relay_event_normalization::span::tag_extraction;
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::Annotated;
use std::net::IpAddr;

use crate::services::processor::SpanGroup;
use crate::{services::processor::ProcessEnvelopeState, utils::ItemAction};
use relay_spans::{otel_to_sentry_span, otel_trace::Span as OtelSpan};

#[cfg(feature = "processing")]
mod processing;
use crate::envelope::ItemType;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::statsd::RelayTimers;
#[cfg(feature = "processing")]
pub use processing::*;
use relay_filter::{GenericFiltersConfig, ProjectFiltersConfig};
use relay_statsd::metric;

pub fn filter(state: &mut ProcessEnvelopeState<SpanGroup>, global_config: &GlobalConfig) {
    let standalone_span_ingestion_disabled = !state
        .project_state
        .has_feature(Feature::StandaloneSpanIngestion);

    let client_ip = state.managed_envelope.envelope().meta().client_addr();
    let filter_settings = &state.project_state.config.filter_settings;

    state.managed_envelope.retain_items(|item| {
        if item.is_span() && standalone_span_ingestion_disabled {
            relay_log::warn!("dropping span because feature is disabled");
            return ItemAction::DropSilently;
        }

        // TODO: check how to reduce double-parsing.
        let mut annotated_span = match item.ty() {
            ItemType::OtelSpan => match serde_json::from_slice::<OtelSpan>(&item.payload()) {
                Ok(otel_span) => Annotated::new(otel_to_sentry_span(otel_span)),
                Err(err) => {
                    relay_log::debug!("failed to parse OTel span: {}", err);
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidJson));
                }
            },
            ItemType::Span => match Annotated::<Span>::from_json_bytes(&item.payload()) {
                Ok(span) => span,
                Err(err) => {
                    relay_log::debug!("failed to parse span: {}", err);
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidJson));
                }
            },
            _ => return ItemAction::Keep,
        };

        let Some(span) = annotated_span.value_mut() else {
            return ItemAction::Keep;
        };

        metric!(timer(RelayTimers::EventProcessingFiltering), {
            if let Err(filter_stat_key) = relay_filter::should_filter(
                span,
                client_ip,
                filter_settings,
                global_config.filters(),
            ) {
                return ItemAction::Drop(Outcome::Filtered(filter_stat_key));
            }
        });

        ItemAction::Keep
    });
}

/// Creates a span from the transaction and applies tag extraction on it.
///
/// Returns `None` when [`tag_extraction::extract_span_tags`] clears the span, which it shouldn't.
pub fn extract_transaction_span(event: &Event, max_tag_value_size: usize) -> Option<Span> {
    let mut spans = [Span::from(event).into()];

    tag_extraction::extract_span_tags(event, &mut spans, max_tag_value_size);

    spans.into_iter().next().and_then(Annotated::into_value)
}
