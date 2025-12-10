use std::collections::HashMap;

use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use relay_base_schema::metrics::MetricUnit;
use relay_event_schema::protocol::{Attributes, MetricType, SpanId, TraceMetric};
use relay_protocol::{Annotated, IntoValue, Value};
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{AnyValue, TraceItem, TraceItemType, any_value};
use uuid::Uuid;

use crate::envelope::WithHeader;
use crate::processing::trace_metrics::{Error, Result};
use crate::processing::utils::store::{
    AttributeMeta, extract_client_sample_rate, extract_meta_attributes,
};
use crate::processing::{Counted, Retention};
use crate::services::outcome::DiscardReason;
use crate::services::store::StoreTraceItem;

macro_rules! required {
    ($value:expr) => {{
        match $value {
            Annotated(Some(value), _) => value,
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

    let metric = required!(metric.value);
    let timestamp = required!(metric.timestamp);

    let meta = extract_meta_attributes(&metric, &metric.attributes);
    let attrs = metric.attributes.0.unwrap_or_default();
    let fields = FieldAttributes {
        metric_name: required!(metric.name),
        metric_type: required!(metric.ty),
        metric_unit: metric.unit.into_value(),
        value: extract_numeric_value(required!(metric.value))?,
        timestamp,
        span_id: metric.span_id.into_value(),
    };

    let client_sample_rate = extract_client_sample_rate(&attrs).unwrap_or(1.0);

    let trace_item = TraceItem {
        item_type: TraceItemType::Metric.into(),
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        received: Some(ts(ctx.received_at)),
        retention_days: ctx.retention.standard.into(),
        downsampled_retention_days: ctx.retention.downsampled.into(),
        timestamp: Some(ts(timestamp.0)),
        trace_id: required!(metric.trace_id).to_string(),
        item_id: Uuid::new_v7(timestamp.into()).as_bytes().to_vec(),
        attributes: attributes(meta, attrs, fields),
        client_sample_rate,
        server_sample_rate: 1.0,
    };

    Ok(StoreTraceItem {
        trace_item,
        quantities,
    })
}

fn ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

struct FieldAttributes {
    metric_name: String,
    metric_type: MetricType,
    metric_unit: Option<MetricUnit>,
    value: f64,
    timestamp: relay_event_schema::protocol::Timestamp,
    span_id: Option<SpanId>,
}

fn extract_numeric_value(value: Value) -> Result<f64> {
    match value {
        Value::F64(v) => Ok(v),
        Value::I64(v) => Ok(v as f64),
        Value::U64(v) => Ok(v as f64),
        _ => Err(Error::Invalid(DiscardReason::InvalidTraceMetric)),
    }
}

fn attributes(
    meta: HashMap<String, AnyValue>,
    attributes: Attributes,
    fields: FieldAttributes,
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

    let FieldAttributes {
        metric_name,
        metric_type,
        metric_unit,
        value,
        timestamp,
        span_id,
    } = fields;

    result.insert(
        "sentry.metric_name".to_owned(),
        AnyValue {
            value: Some(any_value::Value::StringValue(metric_name.clone())),
        },
    );

    result.insert(
        "sentry.metric_type".to_owned(),
        AnyValue {
            value: Some(any_value::Value::StringValue(metric_type.to_string())),
        },
    );

    // Add key names matching metric name and type to workaround current co-occuring attributes limitations.
    result.insert(
        format!("sentry._internal.cooccuring.name.{metric_name}"),
        AnyValue {
            value: Some(any_value::Value::BoolValue(true)),
        },
    );
    result.insert(
        format!("sentry._internal.cooccuring.type.{metric_type}"),
        AnyValue {
            value: Some(any_value::Value::BoolValue(true)),
        },
    );

    if let Some(metric_unit) = metric_unit {
        result.insert(
            "sentry.metric_unit".to_owned(),
            AnyValue {
                value: Some(any_value::Value::StringValue(metric_unit.to_string())),
            },
        );

        result.insert(
            format!("sentry._internal.cooccuring.unit.{metric_unit}"),
            AnyValue {
                value: Some(any_value::Value::BoolValue(true)),
            },
        );
    }

    result.insert(
        "sentry.value".to_owned(),
        AnyValue {
            value: Some(any_value::Value::DoubleValue(value)),
        },
    );

    let timestamp_nanos = timestamp
        .into_inner()
        .timestamp_nanos_opt()
        .unwrap_or_default();

    result.insert(
        "sentry.timestamp_precise".to_owned(),
        AnyValue {
            value: Some(any_value::Value::IntValue(timestamp_nanos)),
        },
    );

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::{Attribute, AttributeType, AttributeValue};
    use relay_protocol::FromValue;
    use relay_protocol::Object;
    use relay_quotas::Scoping;

    use crate::processing::Retention;

    use super::*;

    macro_rules! trace_metric {
        ($($tt:tt)*) => {{
           WithHeader {
               header: None,
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
