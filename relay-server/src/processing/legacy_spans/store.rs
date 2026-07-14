use relay_event_normalization::eap::{Ingress, Pipeline};
use relay_event_schema::protocol::Span;
use relay_protocol::Annotated;

use crate::processing::Retention;
use crate::processing::legacy_spans::{Error, Result};
use crate::services::outcome::DiscardReason;
use crate::services::store::StoreSpanV2;

macro_rules! required {
    ($value:expr) => {{
        match $value {
            Annotated(Some(value), _) => value,
            Annotated(None, meta) => {
                relay_log::debug!(
                    "dropping span because of missing required field {} with meta {meta:?}",
                    stringify!($value),
                );
                return Err(Error::Invalid(DiscardReason::InvalidSpan));
            }
        }
    }};
}

/// Converts a [`Span`] into a [`StoreSpanV2`] to be sent to Kafka.
pub fn convert(span: Annotated<Span>, retentions: Retention) -> Result<Box<StoreSpanV2>> {
    // It's ok to enable `infer_name` here—the span has gone through the legacy standalone
    // pipeline, so it has been PII scrubbed.
    let span = span.map_value(|span| relay_spans::span_v1_to_span_v2(span, true));
    let mut span = required!(span);

    let attributes = span.attributes.get_or_insert_with(Default::default);
    attributes.insert(
        relay_conventions::attributes::SENTRY__RELAY__INGRESS,
        Ingress::Legacy.to_string(),
    );
    attributes.insert(
        relay_conventions::attributes::SENTRY__RELAY__PIPELINE,
        Pipeline::SpanLegacy.to_string(),
    );

    Ok(Box::new(StoreSpanV2 {
        routing_key: span.trace_id.value().copied().map(Into::into),
        retention_days: retentions.standard,
        downsampled_retention_days: retentions.downsampled,
        event_id: None,
        item: span,
        performance_issues_spans: false,
    }))
}
