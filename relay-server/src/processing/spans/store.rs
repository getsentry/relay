use std::collections::HashMap;
use std::ops::Deref;

use bytes::Bytes;
use relay_event_schema::protocol::{AttachmentV2Meta, Attributes, SpanV2};
use relay_protocol::{Annotated, FiniteF64, IntoValue, Value};
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{AnyValue, TraceItem, any_value};

use crate::envelope::ContentType;
use crate::managed::{Counted, Managed, Quantities, Rejected};
use crate::processing::Retention;
use crate::processing::spans::{
    Error, ExpandedAttachment, ExpandedSpan, IndexedSpan, IndexedSpanOnly, Result,
};
use crate::processing::utils::store::{
    self, AttributeMeta, extract_client_sample_rate, extract_meta_attributes, proto_timestamp,
};
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
pub fn convert(span: IndexedSpanOnly, ctx: &Context) -> Result<Box<StoreSpanV2>> {
    let mut span = required!(span.0.value);

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

/// Converts an expanded attachment to a storable unit.
pub fn convert_attachment(
    attachment: Managed<ExpandedAttachment>,
    retention: Retention,
) -> Result<Managed<StoreAttachment>, Rejected<()>> {
    let scoping = attachment.scoping();
    let received_at = attachment.received_at();
    attachment.try_map(|attachment, _record_keeper| {
        let ExpandedAttachment { meta, body } = attachment;
        let ctx = store::Context {
            received_at,
            scoping,
            retention,
        };
        let trace_item = attachment_to_trace_item(meta, ctx)
            .ok_or(Outcome::Invalid(DiscardReason::InvalidSpanAttachment))?;

        Ok::<_, Outcome>(StoreAttachment { trace_item, body })
    })
}

fn attachment_to_trace_item(
    meta: Annotated<AttachmentV2Meta>,
    ctx: store::Context,
) -> Option<TraceItem> {
    let meta = meta.into_value()?;
    let annotated_meta = extract_meta_attributes(&meta, &meta.attributes);
    let AttachmentV2Meta {
        trace_id,
        attachment_id,
        timestamp,
        filename,
        content_type,
        attributes,
        other,
    } = meta;

    let fields = AttachmentMetaFields {
        content_type: content_type.into_value()?,
        filename: filename.into_value(),
    };

    let attributes = attributes.into_value()?;

    let client_sample_rate = extract_client_sample_rate(&attributes).unwrap_or(1.0);

    let trace_item = TraceItem {
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        trace_id: trace_id.into_value()?.to_string(),
        item_id: attachment_id.into_value()?.into_bytes().to_vec(),
        item_type: 10, // TODO use enum from sentry-protos
        timestamp: timestamp.into_value().map(|ts| proto_timestamp(ts.0)),
        attributes: attachment_attributes(annotated_meta, attributes, fields),
        client_sample_rate,
        server_sample_rate: 1.0, // FIXME: this should come from dynamic sampling
        retention_days: ctx.retention.standard as u32,
        received: Some(proto_timestamp(ctx.received_at)),
        downsampled_retention_days: ctx.retention.downsampled as u32,
    };
    Some(trace_item)
}

struct AttachmentMetaFields {
    content_type: String,
    filename: Option<String>,
}

// TODO: remove code-duplication between logs, trace metrics and attachments.
fn attachment_attributes(
    meta: HashMap<String, AnyValue>,
    attributes: Attributes,
    fields: AttachmentMetaFields,
) -> HashMap<String, AnyValue> {
    let mut result = meta;
    result.reserve(attributes.0.len() + 5);

    for (name, attribute) in attributes {
        let meta = AttributeMeta {
            meta: IntoValue::extract_meta_tree(&attribute),
        };
        if let Some(meta) = meta.to_any_value() {
            result.insert(format!("sentry._meta.fields.attributes.{name}"), meta);
        }

        let value = attribute
            .into_value()
            .and_then(|v| v.value.value.into_value());

        let Some(value) = value else {
            continue;
        };

        let Some(value) = (match value {
            Value::Bool(v) => Some(any_value::Value::BoolValue(v)),
            Value::I64(v) => Some(any_value::Value::IntValue(v)),
            Value::U64(v) => i64::try_from(v).ok().map(any_value::Value::IntValue),
            Value::F64(v) => Some(any_value::Value::DoubleValue(v)),
            Value::String(v) => Some(any_value::Value::StringValue(v)),
            Value::Array(_) | Value::Object(_) => {
                debug_assert!(false, "unsupported trace metric value");
                None
            }
        }) else {
            continue;
        };

        result.insert(name, AnyValue { value: Some(value) });
    }

    let AttachmentMetaFields {
        content_type,
        filename,
    } = fields;

    result.insert(
        "sentry.content-type".to_owned(),
        AnyValue {
            value: Some(any_value::Value::StringValue(
                content_type.as_str().to_owned(),
            )),
        },
    );

    // See https://opentelemetry.io/docs/specs/semconv/registry/attributes/file/#file-name.
    if let Some(filename) = filename {
        result.insert(
            "file.name".to_owned(),
            AnyValue {
                value: Some(any_value::Value::StringValue(filename)),
            },
        );
    }

    // TODO: get span ID from item header

    // if let Some(span_id) = span_id {
    //     result.insert(
    //         "sentry.span_id".to_owned(),
    //         AnyValue {
    //             value: Some(any_value::Value::StringValue(span_id.to_string())),
    //         },
    //     );
    // }

    result
}
