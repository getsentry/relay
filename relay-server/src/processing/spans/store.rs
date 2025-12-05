use std::ops::Deref;

use relay_event_schema::protocol::Attributes;
use relay_protocol::Annotated;
use relay_protocol::FiniteF64;

use crate::processing::Retention;
use crate::processing::spans::{Error, IndexedSpanOnly, Result};
use crate::services::outcome::DiscardReason;
use crate::services::store::StoreSpanV2;

pub mod attachment;

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

/// Context required for [`convert`].
pub struct Context {
    /// Server side applied sample rate.
    pub server_sample_rate: Option<f64>,
    /// Item retention.
    pub retention: Retention,
}

/// Converts a processed [`SpanV2`](super::SpanV2) into a [Kafka](crate::services::store::Store) compatible format.
pub fn convert(span: IndexedSpanOnly, ctx: &Context) -> Result<Box<StoreSpanV2>> {
    let mut span = required!(span.0.value);

    let routing_key = span.trace_id.value().map(|v| *v.deref());

    inject_server_sample_rate(&mut span.attributes, ctx.server_sample_rate);

    Ok(Box::new(StoreSpanV2 {
        routing_key,
        retention_days: ctx.retention.standard,
        downsampled_retention_days: ctx.retention.downsampled,
        item: span,
    }))
}

/// Injects a server sample rate into a span.
fn inject_server_sample_rate(
    attributes: &mut Annotated<Attributes>,
    server_sample_rate: Option<f64>,
) {
    let Some(server_sample_rate) = server_sample_rate.and_then(FiniteF64::new) else {
        return;
    };

    let attributes = attributes.get_or_insert_with(Default::default);
    attributes.insert("sentry.server_sample_rate", server_sample_rate.to_f64());
}
