use chrono::{DateTime, Utc};
use relay_event_schema::protocol::{Attribute, Attributes, SpanId, TraceAttachmentMeta};
use relay_protocol::Annotated;
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{TraceItem, TraceItemType};

use crate::managed::{Counted, Managed, Quantities, Rejected};
use crate::processing::Retention;
use crate::processing::trace_attachments::types::ExpandedAttachment;
use crate::processing::utils::store::{
    attributes_with_meta, extract_client_sample_rate, proto_timestamp,
    quantities_to_trace_item_outcomes, uuid_to_item_id,
};
use crate::services::objectstore::StoreTraceAttachment;
use crate::services::outcome::{DiscardReason, Outcome};

/// The trace item attribute that carries the content type of the attachment.
pub const CONTENT_TYPE_ATTRIBUTE: &str = "sentry.content-type";

/// Converts an expanded attachment to a storable unit.
pub fn convert(
    attachment: Managed<ExpandedAttachment>,
    retention: Retention,
    server_sample_rate: Option<f64>,
) -> Result<Managed<StoreTraceAttachment>, Rejected<()>> {
    let scoping = attachment.scoping();
    let received_at = attachment.received_at();
    attachment.try_map(|attachment, _record_keeper| {
        let quantities = attachment.quantities();

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
        let content_type = meta.value().and_then(|m| m.content_type.value().cloned());
        let filename = meta.value().and_then(|m| m.filename.value().cloned());
        let trace_item = attachment_to_trace_item(meta, quantities, ctx)
            .ok_or(Outcome::Invalid(DiscardReason::InvalidTraceAttachment))?;

        Ok::<_, Outcome>(StoreTraceAttachment {
            trace_item,
            body,
            content_type,
            filename,
            retention: retention.standard,
        })
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
    quantities: Quantities,
    ctx: Context,
) -> Option<TraceItem> {
    let meta = meta.into_value()?;
    let client_sample_rate = meta
        .attributes
        .value()
        .and_then(extract_client_sample_rate)
        .unwrap_or(1.0);
    let trace_id = meta.trace_id.value()?;
    let attachment_id = meta.attachment_id.value()?;
    let timestamp = meta.timestamp.value()?;
    meta.content_type.value()?;
    let fields = Fields {
        content_type: meta.content_type,
        filename: meta.filename,
        span_id: ctx.span_id.into(),
    };

    let trace_item = TraceItem {
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        trace_id: trace_id.to_string(),
        item_id: uuid_to_item_id(**attachment_id),
        item_type: TraceItemType::Attachment.into(),
        timestamp: Some(proto_timestamp(timestamp.0)),
        attributes: attributes_with_meta(
            attachment_attributes(meta.attributes, fields),
            Some(&meta.timestamp),
            Some(&meta.trace_id),
            Some(&meta.attachment_id),
        ),
        client_sample_rate,
        server_sample_rate: ctx.server_sample_rate.unwrap_or(1.0),
        retention_days: ctx.retention.standard as u32,
        received: Some(proto_timestamp(ctx.received_at)),
        downsampled_retention_days: ctx.retention.downsampled as u32,
        outcomes: Some(quantities_to_trace_item_outcomes(quantities, ctx.scoping)),
    };

    Some(trace_item)
}

struct Fields {
    content_type: Annotated<String>,
    filename: Annotated<String>,
    span_id: Annotated<SpanId>,
}

fn attachment_attributes(
    mut result: Annotated<Attributes>,
    fields: Fields,
) -> Annotated<Attributes> {
    let Fields {
        content_type,
        filename,
        span_id,
    } = fields;

    let attributes = &mut result.get_or_insert_with(Attributes::default).0;
    attributes.insert(
        CONTENT_TYPE_ATTRIBUTE.to_owned(),
        content_type.map_value(Attribute::from),
    );
    attributes.insert("file.name".to_owned(), filename.map_value(Attribute::from));
    attributes.insert(
        "sentry.span_id".to_owned(),
        span_id.map_value(|span_id| Attribute::from(span_id.to_string())),
    );

    result
}

#[cfg(test)]
mod tests {
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::AttachmentId;
    use relay_protocol::Error as MetaError;

    use super::*;

    #[test]
    fn test_attachment_meta() {
        let mut meta = TraceAttachmentMeta {
            trace_id: Annotated::new("5B8EFFF798038103D269B633813FC60C".parse().unwrap()),
            attachment_id: Annotated::new(AttachmentId::random()),
            timestamp: Annotated::new(DateTime::from_timestamp(946684800, 0).unwrap().into()),
            filename: Annotated::new("attachment.txt".to_owned()),
            content_type: Annotated::new("text/plain".to_owned()),
            ..Default::default()
        };

        meta.trace_id
            .meta_mut()
            .add_error(MetaError::invalid("trace_id"));
        meta.attachment_id
            .meta_mut()
            .add_error(MetaError::invalid("attachment_id"));
        meta.timestamp
            .meta_mut()
            .add_error(MetaError::invalid("timestamp"));
        meta.filename
            .meta_mut()
            .add_error(MetaError::invalid("filename"));
        meta.content_type
            .meta_mut()
            .add_error(MetaError::invalid("content_type"));

        let trace_item = attachment_to_trace_item(
            Annotated::new(meta),
            Quantities::new(),
            Context {
                received_at: DateTime::from_timestamp(1, 0).unwrap(),
                scoping: Scoping {
                    organization_id: OrganizationId::new(1),
                    project_id: ProjectId::new(42),
                    project_key: "12333333333333333333333333333333".parse().unwrap(),
                    key_id: Some(3),
                },
                retention: Retention {
                    standard: 42,
                    downsampled: 43,
                },
                server_sample_rate: None,
                span_id: None,
            },
        )
        .unwrap();

        for key in [
            "sentry._meta.fields.attributes.sentry.trace_id",
            "sentry._meta.fields.item_id",
            "sentry._meta.fields.timestamp",
            "sentry._meta.fields.attributes.file.name",
            "sentry._meta.fields.attributes.sentry.content-type",
        ] {
            assert!(trace_item.attributes.contains_key(key), "missing {key}");
        }
    }
}
