use std::ops::Deref;

use relay_event_schema::protocol::SpanV2;
use relay_protocol::{Annotated, FiniteF64};

use crate::processing::Retention;
use crate::processing::spans::{Error, IndexedSpan, Result};
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

/// Context required for [`convert`].
pub struct Context {
    /// Server side applied sample rate.
    pub server_sample_rate: Option<f64>,
    /// Item retention.
    pub retention: Retention,
}

/// Converts a processed [`SpanV2`] into a [Kafka](crate::services::store::Store) compatible format.
pub fn convert(span: IndexedSpan, ctx: &Context) -> Result<Box<StoreSpanV2>> {
    // TODO: We are not doing anything with the attachment here.
    let mut span = required!(span.0.span.value);

    let routing_key = span.trace_id.value().map(|v| *v.deref());

    inject_server_sample_rate(&mut span, ctx.server_sample_rate);

    Ok(Box::new(StoreSpanV2 {
        routing_key,
        retention_days: ctx.retention.standard,
        downsampled_retention_days: ctx.retention.downsampled,
        item: span,
    }))
}

/// Injects a server sample rate into a span.
fn inject_server_sample_rate(span: &mut SpanV2, server_sample_rate: Option<f64>) {
    let Some(server_sample_rate) = server_sample_rate.and_then(FiniteF64::new) else {
        return;
    };

    let attributes = span.attributes.get_or_insert_with(Default::default);
    attributes.insert("sentry.server_sample_rate", server_sample_rate.to_f64());
}
