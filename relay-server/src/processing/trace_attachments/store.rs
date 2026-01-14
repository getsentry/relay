use std::collections::HashMap;

use chrono::{DateTime, Utc};
use relay_event_schema::protocol::{Attributes, SpanId, TraceAttachmentMeta};
use relay_protocol::{Annotated, IntoValue, Value};
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{AnyValue, TraceItem, TraceItemType, any_value};

use crate::managed::{Managed, Rejected};
use crate::processing::Retention;
use crate::processing::trace_attachments::types::ExpandedAttachment;
use crate::processing::utils::store::{
    AttributeMeta, extract_client_sample_rate, extract_meta_attributes, proto_timestamp,
    uuid_to_item_id,
};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::upload::StoreAttachment;

/// Converts an expanded attachment to a storable unit.
pub fn convert(
    attachment: Managed<ExpandedAttachment>,
    retention: Retention,
    server_sample_rate: Option<f64>,
) -> Result<Managed<StoreAttachment>, Rejected<()>> {
    let scoping = attachment.scoping();
    let received_at = attachment.received_at();
    attachment.try_map(|attachment, _record_keeper| {
        let ExpandedAttachment {
            parent_id,
            meta,
            body,
        } = attachment;
        let ctx = Context {
            span_id: parent_id.and_then(|p| p.as_span_id()),
            received_at,
            scoping,
            retention,
            server_sample_rate,
        };
        let trace_item = attachment_to_trace_item(meta, ctx)
            .ok_or(Outcome::Invalid(DiscardReason::InvalidTraceAttachment))?;

        Ok::<_, Outcome>(StoreAttachment { trace_item, body })
    })
}

/// Context for converting an attachment to a trace item.
#[derive(Debug, Clone, Copy)]
struct Context {
    /// Received time.
    received_at: DateTime<Utc>,
    /// Item scoping.
    scoping: Scoping,
    /// Item retention.
    #[cfg(feature = "processing")]
    retention: Retention,
    /// Server-side sample rate.
    server_sample_rate: Option<f64>,
    /// The ID of the span that owns the attachment.
    span_id: Option<SpanId>,
}

fn attachment_to_trace_item(
    meta: Annotated<TraceAttachmentMeta>,
    ctx: Context,
) -> Option<TraceItem> {
    let meta = meta.into_value()?;
    let annotated_meta = extract_meta_attributes(&meta, &meta.attributes);
    let TraceAttachmentMeta {
        trace_id,
        attachment_id,
        timestamp,
        filename,
        content_type,
        attributes,
        other: _,
    } = meta;

    let fields = Fields {
        content_type: content_type.into_value()?,
        filename: filename.into_value(),
        span_id: ctx.span_id,
    };

    let attributes = attributes.into_value().unwrap_or_default();

    let client_sample_rate = extract_client_sample_rate(&attributes).unwrap_or(1.0);

    let trace_item = TraceItem {
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        trace_id: trace_id.into_value()?.to_string(),
        item_id: uuid_to_item_id(*attachment_id.into_value()?),
        item_type: TraceItemType::Attachment.into(),
        timestamp: Some(proto_timestamp(timestamp.into_value()?.0)),
        attributes: convert_attributes(annotated_meta, attributes, fields),
        client_sample_rate,
        server_sample_rate: ctx.server_sample_rate.unwrap_or(1.0),
        retention_days: ctx.retention.standard as u32,
        received: Some(proto_timestamp(ctx.received_at)),
        downsampled_retention_days: ctx.retention.downsampled as u32,
    };
    Some(trace_item)
}

struct Fields {
    content_type: String,
    filename: Option<String>,
    span_id: Option<SpanId>,
}

// TODO: remove code-duplication between logs, trace metrics and attachments.
fn convert_attributes(
    meta: HashMap<String, AnyValue>,
    attributes: Attributes,
    fields: Fields,
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
                debug_assert!(false, "unsupported attachment attribute value");
                None
            }
        }) else {
            continue;
        };

        result.insert(name, AnyValue { value: Some(value) });
    }

    let Fields {
        content_type,
        filename,
        span_id,
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

    if let Some(span_id) = span_id {
        result.insert(
            "sentry.span_id".to_owned(),
            AnyValue {
                value: Some(any_value::Value::StringValue(span_id.to_string())),
            },
        );
    }

    result
}
