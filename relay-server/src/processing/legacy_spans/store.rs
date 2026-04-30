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
    let span = span.map_value(relay_spans::span_v1_to_span_v2);
    let span = required!(span);

    Ok(Box::new(StoreSpanV2 {
        routing_key: span.trace_id.value().copied().map(Into::into),
        retention_days: retentions.standard,
        downsampled_retention_days: retentions.downsampled,
        item: span,
    }))
}
