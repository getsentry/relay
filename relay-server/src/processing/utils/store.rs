use std::array::TryFromSliceError;
use std::collections::HashMap;

use chrono::Utc;
use relay_conventions::attributes::SENTRY__CLIENT_SAMPLE_RATE;
use relay_event_schema::protocol::{AttachmentId, Attributes, Timestamp, TraceId};
use relay_protocol::{Annotated, IntoValue, MetaTree, Value};

use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{AnyValue, ArrayValue, CategoryCount, Outcomes, any_value};
use serde::Serialize;
use uuid::Uuid;

use crate::managed::Quantities;

/// Represents metadata extracted from Relay's annotated model.
///
/// This struct holds metadata about processing errors, transformations, and other
/// information that occurred during processing of the original payload.
///
/// The attribute metadata itself is serialized as a JSON string.
#[derive(Debug, Serialize)]
pub struct AttributeMeta {
    /// Meta as it was extracted from Relay's annotated model.
    pub meta: MetaTree,
}

impl AttributeMeta {
    /// Converts the metadata to an AnyValue for TraceItem attributes.
    ///
    /// Returns None if the metadata is empty, otherwise serializes the metadata
    /// to JSON and wraps it in a StringValue.
    pub fn to_any_value(&self) -> Option<AnyValue> {
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

fn convert_attribute_value(value: Value) -> Option<any_value::Value> {
    match value {
        Value::Bool(v) => Some(any_value::Value::BoolValue(v)),
        Value::I64(v) => Some(any_value::Value::IntValue(v)),
        Value::U64(v) => i64::try_from(v).ok().map(any_value::Value::IntValue),
        Value::F64(v) => Some(any_value::Value::DoubleValue(v)),
        Value::String(v) => Some(any_value::Value::StringValue(v)),
        Value::Array(v) => Some(any_value::Value::ArrayValue(ArrayValue {
            values: v
                .into_iter()
                .filter_map(|v| {
                    let Some(v) = v.into_value() else {
                        return Some(AnyValue { value: None });
                    };

                    let v = match v {
                        Value::Bool(v) => any_value::Value::BoolValue(v),
                        Value::I64(v) => any_value::Value::IntValue(v),
                        Value::U64(v) => any_value::Value::IntValue(v as i64),
                        Value::F64(v) => any_value::Value::DoubleValue(v),
                        Value::String(v) => any_value::Value::StringValue(v),
                        Value::Array(_) | Value::Object(_) => {
                            debug_assert!(
                                false,
                                "arrays and objects nested in arrays is not yet supported"
                            );
                            return None;
                        }
                    };

                    Some(AnyValue { value: Some(v) })
                })
                .collect(),
        })),
        Value::Object(_) => {
            debug_assert!(false, "objects are not yet supported");
            None
        }
    }
}

pub fn attributes_with_meta(
    attributes: Annotated<Attributes>,
    timestamp: Option<&Annotated<Timestamp>>,
    trace_id: Option<&Annotated<TraceId>>,
    item_id: Option<&Annotated<AttachmentId>>,
) -> HashMap<String, AnyValue> {
    let Annotated(attributes, meta) = attributes;
    let attributes = attributes.unwrap_or_default();
    let mut result = HashMap::with_capacity(attributes.0.len() * 2 + 4);

    for (name, attribute) in attributes {
        let meta = IntoValue::extract_meta_tree(&attribute);
        let value = attribute
            .into_value()
            .and_then(|v| v.value.value.into_value())
            .and_then(convert_attribute_value);
        if let Some(value) = value {
            result.insert(name.clone(), AnyValue { value: Some(value) });
        }
        if let Some(meta) = (AttributeMeta { meta }).to_any_value() {
            result.insert(format!("sentry._meta.fields.attributes.{name}"), meta);
        }
    }

    let fields = [
        // `timestamp` and `item_id` are always returned as top-level fields by EAP RPC
        timestamp.map(|value| ("timestamp", IntoValue::extract_meta_tree(value))),
        item_id.map(|value| ("item_id", IntoValue::extract_meta_tree(value))),
        // `trace_id` is returned as the `sentry.trace_id` attribute by EAP RPC
        trace_id.map(|value| {
            (
                "attributes.sentry.trace_id",
                IntoValue::extract_meta_tree(value),
            )
        }),
    ];
    for (name, meta) in fields.into_iter().flatten() {
        if let Some(meta) = (AttributeMeta { meta }).to_any_value() {
            result.insert(format!("sentry._meta.fields.{name}"), meta);
        }
    }

    if let Some(meta) = (AttributeMeta {
        meta: MetaTree {
            meta,
            children: Default::default(),
        },
    })
    .to_any_value()
    {
        result.insert("sentry._meta.fields.attributes".to_owned(), meta);
    }

    result
}

/// Converts a [`chrono::DateTime`] into a [`prost_types::Timestamp`]
pub fn proto_timestamp(dt: chrono::DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

/// Extracts the client sample rate from trace attributes.
pub fn extract_client_sample_rate(attributes: &Attributes) -> Option<f64> {
    attributes
        .get_value(SENTRY__CLIENT_SAMPLE_RATE)
        .and_then(|value| value.as_f64())
        .filter(|v| *v > 0.0)
        .filter(|v| *v <= 1.0)
}

/// Massages a UUID into the format that EAP expects.
pub fn uuid_to_item_id(id: Uuid) -> Vec<u8> {
    // See https://github.com/getsentry/snuba/blob/a319040728d638841612cef117ec414d3e54d70f/rust_snuba/src/processors/eap_items.rs#L257
    id.as_u128().to_le_bytes().to_vec()
}

/// Reverse operation of [`uuid_to_item_id`].
pub fn item_id_to_uuid(item_id: &[u8]) -> Result<Uuid, TryFromSliceError> {
    let item_id: [u8; 16] = item_id.try_into()?;
    let item_id = u128::from_le_bytes(item_id);
    Ok(Uuid::from_u128(item_id))
}

/// Converts [`Quantities`] and [`Scoping`] into Trace Item [`Outcomes`].
pub fn quantities_to_trace_item_outcomes(q: Quantities, scoping: Scoping) -> Outcomes {
    let category_count = q
        .into_iter()
        .map(|(category, quantity)| CategoryCount {
            data_category: category as u32,
            quantity: quantity as u64,
        })
        .collect();

    Outcomes {
        category_count,
        key_id: scoping.key_id.unwrap_or(0),
    }
}
