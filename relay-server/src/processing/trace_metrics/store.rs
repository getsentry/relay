use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use relay_base_schema::metrics::MetricUnit;
use relay_event_schema::protocol::trace_metric;
use relay_event_schema::protocol::{Attribute, Attributes, MetricType, SpanId, TraceMetric};
use relay_protocol::{Annotated, Value};
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{TraceItem, TraceItemType};
use uuid::Uuid;

use crate::envelope::WithHeader;
use crate::managed::Managed;
use crate::processing::trace_metrics::{Error, Result};
use crate::processing::trace_metrics::{store, utils};
use crate::processing::utils::store::{
    attributes_with_meta, extract_client_sample_rate, quantities_to_trace_item_outcomes,
    uuid_to_item_id,
};
use crate::processing::{self, Counted, Retention};
use crate::services::outcome::DiscardReason;
use crate::services::store::StoreTraceItem;

macro_rules! required {
    ($value:expr) => {{
        match $value {
            value @ Annotated(Some(_), _) => value,
            Annotated(None, meta) => {
                relay_log::debug!(
                    "dropping trace metric because of missing required field {} with meta {meta:?}",
                    stringify!($value),
                );
                return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
            }
        }
    }};
}

#[derive(Debug, Clone, Copy)]
pub struct Context {
    /// Received time.
    pub received_at: DateTime<Utc>,
    /// Item scoping.
    pub scoping: Scoping,
    /// Item retention.
    pub retention: Retention,
}

pub fn convert(metric: WithHeader<TraceMetric>, ctx: &Context) -> Result<StoreTraceItem> {
    let quantities = metric.quantities();
    let payload_size_bytes = metric
        .header
        .as_ref()
        .and_then(|h| h.byte_size)
        .unwrap_or_default();

    let metric = required!(metric.value).into_value().unwrap();
    let timestamp = required!(metric.timestamp);
    let trace_id = required!(metric.trace_id);
    let client_sample_rate = metric
        .attributes
        .value()
        .and_then(extract_client_sample_rate)
        .unwrap_or(1.0);
    let value = required!(metric.value);
    let numeric_value = extract_numeric_value(value.value().unwrap().clone())?;
    let fields = FieldAttributes {
        metric_name: required!(metric.name),
        metric_type: required!(metric.ty),
        metric_unit: metric.unit,
        value: value.map_value(|_| numeric_value),
        timestamp: timestamp.clone(),
        span_id: metric.span_id,
        payload_size_bytes,
    };
    let timestamp_value = timestamp.value().unwrap();
    let attributes = attributes(metric.attributes, fields);

    let trace_item = TraceItem {
        item_type: TraceItemType::Metric.into(),
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        received: Some(ts(ctx.received_at)),
        retention_days: ctx.retention.standard.into(),
        downsampled_retention_days: ctx.retention.downsampled.into(),
        timestamp: Some(ts(timestamp_value.0)),
        trace_id: trace_id.value().unwrap().to_string(),
        item_id: uuid_to_item_id(Uuid::new_v7((*timestamp_value).into())),
        attributes: attributes_with_meta(attributes, Some(&timestamp), Some(&trace_id), None),
        client_sample_rate,
        server_sample_rate: 1.0,
        outcomes: Some(quantities_to_trace_item_outcomes(quantities, ctx.scoping)),
    };

    Ok(StoreTraceItem { trace_item })
}

fn ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

struct FieldAttributes {
    metric_name: Annotated<String>,
    metric_type: Annotated<MetricType>,
    metric_unit: Annotated<MetricUnit>,
    value: Annotated<f64>,
    timestamp: Annotated<relay_event_schema::protocol::Timestamp>,
    span_id: Annotated<SpanId>,
    payload_size_bytes: u64,
}

fn extract_numeric_value(value: Value) -> Result<f64> {
    match value {
        Value::F64(v) => Ok(v),
        Value::I64(v) => Ok(v as f64),
        Value::U64(v) => Ok(v as f64),
        _ => Err(Error::Invalid(DiscardReason::InvalidTraceMetric)),
    }
}

fn attributes(mut result: Annotated<Attributes>, fields: FieldAttributes) -> Annotated<Attributes> {
    let FieldAttributes {
        metric_name,
        metric_type,
        metric_unit,
        value,
        timestamp,
        span_id,
        payload_size_bytes,
    } = fields;

    let metric_name_value = metric_name.value().unwrap().clone();
    let metric_type_value = metric_type.value().unwrap().to_string();
    let metric_unit_value = metric_unit.value().map(ToString::to_string);

    let attributes = &mut result.get_or_insert_with(Attributes::default).0;
    attributes.insert(
        "sentry.metric_name".to_owned(),
        metric_name.map_value(Attribute::from),
    );
    attributes.insert(
        "sentry.metric_type".to_owned(),
        metric_type.map_value(|metric_type| Attribute::from(metric_type.to_string())),
    );
    attributes.insert(
        format!("sentry._internal.cooccuring.name.{metric_name_value}"),
        Annotated::new(Attribute::from(true)),
    );
    attributes.insert(
        format!("sentry._internal.cooccuring.type.{metric_type_value}"),
        Annotated::new(Attribute::from(true)),
    );
    if let Some(metric_unit_value) = metric_unit_value {
        attributes.insert(
            format!("sentry._internal.cooccuring.unit.{metric_unit_value}"),
            Annotated::new(Attribute::from(true)),
        );
    }
    attributes.insert(
        "sentry.metric_unit".to_owned(),
        metric_unit.map_value(|metric_unit| Attribute::from(metric_unit.to_string())),
    );
    attributes.insert("sentry.value".to_owned(), value.map_value(Attribute::from));
    attributes.insert(
        "sentry.timestamp_precise".to_owned(),
        timestamp.map_value(|timestamp| {
            Attribute::from(
                timestamp
                    .into_inner()
                    .timestamp_nanos_opt()
                    .unwrap_or_default(),
            )
        }),
    );
    attributes.insert(
        "sentry.span_id".to_owned(),
        span_id.map_value(|span_id| Attribute::from(span_id.to_string())),
    );
    attributes.insert(
        "sentry.payload_size_bytes".to_owned(),
        Annotated::new(Attribute::from(payload_size_bytes as i64)),
    );

    result
}

/// Produce the supplied webvital trace metrics to kafka.
/// This is required right now to double-write these webvitals as trace metrics, while we
/// still write the vitals as spans.  Eventually, the sdks will natively emit metrics and we
/// can remove this code.
pub fn produce_webvitals_metrics(
    s: processing::StoreHandle<'_>,
    span: &Managed<Box<crate::services::store::StoreSpanV2>>,
    metrics: Vec<TraceMetric>,
) {
    for metric in metrics {
        let trace_metric_headers = trace_metric::TraceMetricHeader {
            byte_size: Some(utils::calculate_size(&metric)),
            other: std::collections::BTreeMap::default(),
        };

        let wheader = crate::envelope::WithHeader {
            header: trace_metric_headers.into(),
            value: metric.into(),
        };

        if let Ok(mut item) = store::convert(
            wheader,
            &store::Context {
                received_at: span.received_at(),
                scoping: span.scoping(),
                retention: processing::Retention {
                    standard: span.retention_days,
                    downsampled: span.downsampled_retention_days,
                },
            },
        ) {
            // Clear outcomes for these metrics, as we don't want them billed.
            item.trace_item.outcomes = None;
            s.send_to_store(span.wrap(item));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::{Attribute, AttributeType, AttributeValue, Attributes};
    use relay_protocol::{Error as MetaError, FromValue, Object};
    use relay_quotas::Scoping;

    use crate::processing::Retention;

    use super::*;

    macro_rules! trace_metric {
        ($($tt:tt)*) => {{
           WithHeader {
               header: Some(relay_event_schema::protocol::TraceMetricHeader {
                   byte_size: Some(420),
                   other: Default::default(),
               }),
               value: TraceMetric::from_value(serde_json::json!($($tt)*).into())
           }
        }};
    }

    fn test_context() -> Context {
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
        }
    }

    #[test]
    fn test_trace_metric_conversion() {
        let metric = trace_metric!({
            "timestamp": 946684800.0,
            "trace_id": "5B8EFFF798038103D269B633813FC60C",
            "span_id": "EEE19B7EC3C1B174",
            "name": "http.request.duration",
            "type": "distribution",
            "value": 123.45,
            "attributes": {
                "http.method": {
                    "value": "GET",
                    "type": "string"
                },
                "http.status_code": {
                    "value": "200",
                    "type": "integer"
                }
            }
        });

        let result = convert(metric, &test_context()).unwrap();
        let attributes = result
            .trace_item
            .attributes
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        assert!(attributes.contains_key("sentry.metric_name"));
        assert!(attributes.contains_key("sentry.metric_type"));
        assert!(attributes.contains_key("sentry.value"));
        assert!(attributes.contains_key("http.method"));
        assert!(attributes.contains_key("http.status_code"));
    }

    #[test]
    fn test_trace_metric_meta() {
        let mut metric = trace_metric!({
            "timestamp": 946684800.0,
            "trace_id": "5B8EFFF798038103D269B633813FC60C",
            "span_id": "EEE19B7EC3C1B174",
            "name": "http.request.duration",
            "type": "distribution",
            "unit": "millisecond",
            "value": 123.45
        });

        let metric_value = metric.value.value_mut().as_mut().unwrap();
        metric_value
            .timestamp
            .meta_mut()
            .add_error(MetaError::invalid("timestamp"));
        metric_value
            .trace_id
            .meta_mut()
            .add_error(MetaError::invalid("trace_id"));
        metric_value
            .span_id
            .meta_mut()
            .add_error(MetaError::invalid("span_id"));
        metric_value
            .name
            .meta_mut()
            .add_error(MetaError::invalid("name"));
        metric_value
            .ty
            .meta_mut()
            .add_error(MetaError::invalid("type"));
        metric_value
            .unit
            .meta_mut()
            .add_error(MetaError::invalid("unit"));
        metric_value
            .value
            .meta_mut()
            .add_error(MetaError::invalid("value"));

        let result = convert(metric, &test_context()).unwrap();
        let attributes = result.trace_item.attributes;

        for key in [
            "sentry._meta.fields.timestamp",
            "sentry._meta.fields.attributes.sentry.trace_id",
            "sentry._meta.fields.attributes.sentry.timestamp_precise",
            "sentry._meta.fields.attributes.sentry.span_id",
            "sentry._meta.fields.attributes.sentry.metric_name",
            "sentry._meta.fields.attributes.sentry.metric_type",
            "sentry._meta.fields.attributes.sentry.metric_unit",
            "sentry._meta.fields.attributes.sentry.value",
        ] {
            assert!(attributes.contains_key(key), "missing {key}");
        }
    }

    #[test]
    fn test_extract_client_sample_rate_function() {
        let mut attrs_map = BTreeMap::new();
        let attr = Attribute {
            value: AttributeValue {
                ty: Annotated::new(AttributeType::Double),
                value: Annotated::new(Value::F64(0.5)),
            },
            other: Object::new(),
        };
        attrs_map.insert("sentry.client_sample_rate".to_owned(), Annotated::new(attr));
        let attrs = Attributes(attrs_map);

        let sample_rate = extract_client_sample_rate(&attrs);
        assert_eq!(sample_rate, Some(0.5));

        // Without sample rate attribute
        let empty_attrs = Attributes(BTreeMap::new());
        let default_rate = extract_client_sample_rate(&empty_attrs);
        assert_eq!(default_rate, None);

        // Invalid sample rate
        let invalid_attrs = Attributes(BTreeMap::from([(
            "sentry.client_sample_rate".to_owned(),
            Annotated::new(Attribute {
                value: AttributeValue {
                    ty: Annotated::new(AttributeType::Double),
                    value: Annotated::new(Value::F64(2.0)),
                },
                other: Object::new(),
            }),
        )]));
        let invalid_rate = extract_client_sample_rate(&invalid_attrs);
        assert_eq!(invalid_rate, None);
    }
}
