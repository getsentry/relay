use std::collections::HashMap;

use chrono::{DateTime, Utc};
use relay_event_schema::protocol::{Attributes, MetricType, SpanId, TraceMetric};
use relay_protocol::{Annotated, IntoValue, MetaTree, Value};
use relay_quotas::Scoping;
use serde::Serialize;
use uuid::Uuid;

#[cfg(feature = "processing")]
use prost_types::Timestamp;
#[cfg(feature = "processing")]
use sentry_protos::snuba::v1::{AnyValue, TraceItem, TraceItemType, any_value};

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::envelope::WithHeader;
use crate::processing::Counted;
use crate::processing::trace_metrics::{Error, Result};
use crate::services::outcome::DiscardReason;

#[cfg(feature = "processing")]
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
    /// Storage retention in days.
    pub retention: Option<u16>,
    /// Storage retention for downsampled data in days
    pub downsampled_retention: Option<u16>,
}

#[cfg(feature = "processing")]
pub fn convert(metric: WithHeader<TraceMetric>, ctx: &Context) -> Result<StoreTraceItem> {
    let quantities = metric.quantities();

    let metric = required!(metric.value);
    let timestamp = required!(metric.timestamp);

    let meta = extract_meta_attributes(&metric);
    let attrs = metric.attributes.0.unwrap_or_default();
    let fields = FieldAttributes {
        metric_name: required!(metric.name),
        metric_type: required!(metric.r#type),
        value: extract_numeric_value(required!(metric.value))?,
        timestamp,
        span_id: metric.span_id.into_value(),
    };
    let retention_days = ctx.retention.unwrap_or(DEFAULT_EVENT_RETENTION);

    let trace_item = TraceItem {
        item_type: TraceItemType::Metric.into(),
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        received: Some(ts(ctx.received_at)),
        retention_days: retention_days.into(),
        downsampled_retention_days: ctx.downsampled_retention.unwrap_or(retention_days).into(),
        timestamp: Some(ts(timestamp.0)),
        trace_id: required!(metric.trace_id).to_string(),
        item_id: Uuid::new_v7(timestamp.into()).as_bytes().to_vec(),
        attributes: attributes(meta, attrs, fields),
        client_sample_rate: 1.0,
        server_sample_rate: 1.0,
    };

    Ok(StoreTraceItem {
        trace_item,
        quantities,
    })
}

#[cfg(feature = "processing")]
fn ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

#[cfg(feature = "processing")]
#[derive(Debug, Serialize)]
struct AttributeMeta {
    /// Meta as it was extracted from Relay's annotated model.
    meta: MetaTree,
}

#[cfg(feature = "processing")]
impl AttributeMeta {
    fn to_any_value(&self) -> Option<AnyValue> {
        if self.meta.is_empty() {
            return None;
        }

        let s = serde_json::to_string(self)
            .inspect_err(|err| {
                relay_log::error!(
                    error = err as &dyn std::error::Error,
                    "attribute meta serialization failed"
                )
            })
            .ok()?;

        Some(AnyValue {
            value: Some(any_value::Value::StringValue(s)),
        })
    }
}

#[cfg(feature = "processing")]
fn extract_meta_attributes(metric: &TraceMetric) -> HashMap<String, AnyValue> {
    let mut meta = IntoValue::extract_child_meta(metric);
    let attributes = meta.remove("attributes");

    let mut result = HashMap::with_capacity(
        meta.len()
            + attributes.as_ref().map_or(0, size_of_meta_tree)
            + metric.attributes.value().map_or(0, |a| a.0.len()),
    );

    for (key, meta) in meta {
        let attr = AttributeMeta { meta };
        if let Some(value) = attr.to_any_value() {
            let key = format!("sentry._meta.fields.{key}");
            result.insert(key, value);
        }
    }

    let Some(mut attributes) = attributes else {
        return result;
    };

    for (key, meta) in std::mem::take(&mut attributes.children) {
        let attr = AttributeMeta { meta };
        if let Some(value) = attr.to_any_value() {
            let key = format!("sentry._meta.fields.attributes.{key}");
            result.insert(key, value);
        }
    }

    let meta = AttributeMeta { meta: attributes };
    if let Some(value) = meta.to_any_value() {
        result.insert("sentry._meta.fields.attributes".to_owned(), value);
    }

    result
}

#[cfg(feature = "processing")]
fn size_of_meta_tree(meta: &MetaTree) -> usize {
    let mut size = 0;

    if !meta.meta.is_empty() {
        size += 1;
    }
    for meta in meta.children.values() {
        if !meta.meta.is_empty() {
            size += 1;
        }
    }

    size
}

#[cfg(feature = "processing")]
struct FieldAttributes {
    metric_name: String,
    metric_type: MetricType,
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

#[cfg(feature = "processing")]
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
        value,
        timestamp,
        span_id,
    } = fields;

    result.insert(
        "sentry.metric_name".to_owned(),
        AnyValue {
            value: Some(any_value::Value::StringValue(metric_name)),
        },
    );

    result.insert(
        "sentry.metric_type".to_owned(),
        AnyValue {
            value: Some(any_value::Value::StringValue(metric_type.to_string())),
        },
    );

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
        "sentry.timestamp_nanos".to_owned(),
        AnyValue {
            value: Some(any_value::Value::StringValue(timestamp_nanos.to_string())),
        },
    );
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

#[cfg(all(test, feature = "processing"))]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_protocol::FromValue;

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
            retention: Some(42),
            downsampled_retention: Some(42),
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
}
