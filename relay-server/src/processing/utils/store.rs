use std::array::TryFromSliceError;
use std::collections::HashMap;

use chrono::Utc;
use relay_conventions::CLIENT_SAMPLE_RATE;
use relay_event_schema::protocol::Attributes;
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

/// Extracts TraceItem meta attributes from any structure that implements IntoValue.
///
/// The implementation piggy backs on [`IntoValue::extract_child_meta`],
/// a lighter implementation using a [`relay_event_schema::processor::Processor`]
/// which removes the meta instead of cloning.
///
/// All extracted metadata is converted into [`Attributes`] compatible values,
/// by building a metadata representation for each top level field and attribute,
/// serializing the result into JSON and building an appropriate metadata key.
///
/// The schema for metadata keys follows the format `sentry._meta.fields.{key}`,
/// for attributes respectively `sentry._meta.fields.attributes.{key}`.
pub fn extract_meta_attributes<T: IntoValue>(
    item: &T,
    attributes: &Annotated<Attributes>,
) -> HashMap<String, AnyValue> {
    let mut meta = IntoValue::extract_child_meta(item);
    // Attributes are the only 'nested' meta we allow.
    let attributes_meta = meta.remove("attributes");

    let mut result = HashMap::with_capacity(
        meta.len()
            + attributes_meta.as_ref().map_or(0, size_of_meta_tree)
            + attributes.value().map_or(0, |a| a.0.len()),
    );

    for (key, meta) in meta {
        let attr = AttributeMeta { meta };
        if let Some(value) = attr.to_any_value() {
            let key = format!("sentry._meta.fields.{key}");
            result.insert(key, value);
        }
    }

    let Some(mut attributes_meta) = attributes_meta else {
        return result;
    };

    for (key, meta) in std::mem::take(&mut attributes_meta.children) {
        let attr = AttributeMeta { meta };
        if let Some(value) = attr.to_any_value() {
            let key = format!("sentry._meta.fields.attributes.{key}");
            result.insert(key, value);
        }
    }

    // The `attributes` field itself can have metadata attached,
    // we already took out all the metadata of the children, so now just emit
    // the remaining metadata on the `attributes`.
    let meta = AttributeMeta {
        meta: attributes_meta,
    };
    if let Some(value) = meta.to_any_value() {
        result.insert("sentry._meta.fields.attributes".to_owned(), value);
    }

    result
}

/// Calculates the immediate size of the meta tree passed in.
///
/// This only counts non empty meta elements of the passed in meta tree and its children,
/// it does not recursively traverse the children.
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

/// Converts [`Attributes`] into EAP compatible values.
pub fn convert_attributes_into(result: &mut HashMap<String, AnyValue>, attributes: Attributes) {
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
            // Meta has already been handled, no value -> skip.
            // There are also no current plans to handle `null` in EAP.
            continue;
        };

        // Assertions for invalid types should never happen as Relay filters and validates
        // attributes beforehand already.
        let Some(value) = (match value {
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
        }) else {
            continue;
        };

        result.insert(name, AnyValue { value: Some(value) });
    }
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
        .get_value(CLIENT_SAMPLE_RATE)
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
