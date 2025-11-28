use std::ops::Deref;

use bytes::Bytes;
use relay_event_schema::protocol::{AttachmentV2Meta, SpanV2};
use relay_protocol::{Annotated, FiniteF64};
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::TraceItem;

use crate::managed::{Counted, Managed, Quantities, Rejected};
use crate::processing::Retention;
use crate::processing::spans::{Error, IndexedSpan, Result, ValidatedSpanAttachment};
use crate::processing::utils::store::proto_timestamp;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::store::StoreSpanV2;
use crate::services::upload::StoreAttachment;

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

fn convert_attachment(
    attachment: Managed<ValidatedSpanAttachment>,
) -> Result<Managed<StoreAttachment>, Rejected<Outcome>> {
    let scoping = attachment.scoping();
    let x = attachment.try_map(|attachment, record_keeper| {
        let ValidatedSpanAttachment { meta, body } = attachment;
        let trace_item = attachment_to_trace_item(scoping, meta)
            .ok_or(Outcome::Invalid(DiscardReason::InvalidSpanAttachment))?;

        Ok::<_, Outcome>(StoreAttachment { trace_item, body })
    });
    x
}

fn attachment_to_trace_item(
    scoping: Scoping,
    meta: Annotated<AttachmentV2Meta>,
) -> Option<TraceItem> {
    let AttachmentV2Meta {
        trace_id,
        attachment_id,
        timestamp,
        filename,
        content_type,
        attributes,
        other,
    } = meta.into_value()?;
    let trace_item = TraceItem {
        organization_id: scoping.organization_id.value(),
        project_id: scoping.project_id.value(),
        trace_id: trace_id.into_value()?.to_string(),
        item_id: attachment_id.into_value()?.into_bytes().to_vec(),
        item_type: 10, // TODO use enum from sentry-protos
        timestamp: timestamp.into_value().map(|ts| proto_timestamp(ts.0)),
        attributes: todo!(),
        client_sample_rate: todo!(),
        server_sample_rate: todo!(),
        retention_days: todo!(),
        received: todo!(),
        downsampled_retention_days: todo!(),
    };
    Some(trace_item)
}
