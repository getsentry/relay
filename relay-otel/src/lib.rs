//! OpenTelemetry to Sentry transformation utilities.
//!
//! This crate provides common functionality for converting OpenTelemetry data structures
//! to Sentry format. It serves as a shared library for both span and log transformations.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;
use relay_event_schema::protocol::{Attribute, AttributeType};
use relay_protocol::Value;

/// Converts an OpenTelemetry AnyValue to a Sentry attribute.
///
/// This function handles the conversion of OpenTelemetry attribute values to Sentry attribute types.
/// Complex types like arrays and key-value lists are serialized to JSON strings for safety and
/// compatibility.
///
/// For array and key-value list values, this function filters out nested complex types
/// (nested arrays and key-value lists) before serialization to prevent issues with
/// the OTLP protocol and ensure safe handling.
pub fn otel_value_to_attribute(otel_value: OtelValue) -> Option<Attribute> {
    let (ty, value) = match otel_value {
        OtelValue::StringValue(s) => (AttributeType::String, Value::String(s)),
        OtelValue::BoolValue(b) => (AttributeType::Boolean, Value::Bool(b)),
        OtelValue::IntValue(i) => (AttributeType::Integer, Value::I64(i)),
        OtelValue::DoubleValue(d) => (AttributeType::Double, Value::F64(d)),
        OtelValue::BytesValue(bytes) => {
            let s = String::from_utf8(bytes).ok()?;
            (AttributeType::String, Value::String(s))
        }
        OtelValue::ArrayValue(array) => {
            // Filter out nested arrays and key-value lists for safety.
            // This is not usually allowed by the OTLP protocol, but we filter
            // these values out before serializing for robustness.
            let safe_values: Vec<serde_json::Value> = array
                .values
                .into_iter()
                .filter_map(|v| match v.value? {
                    OtelValue::StringValue(s) => Some(serde_json::Value::String(s)),
                    OtelValue::BoolValue(b) => Some(serde_json::Value::Bool(b)),
                    OtelValue::IntValue(i) => {
                        Some(serde_json::Value::Number(serde_json::Number::from(i)))
                    }
                    OtelValue::DoubleValue(d) => {
                        serde_json::Number::from_f64(d).map(serde_json::Value::Number)
                    }
                    OtelValue::BytesValue(bytes) => {
                        String::from_utf8(bytes).ok().map(serde_json::Value::String)
                    }
                    // Skip nested complex types for safety
                    OtelValue::ArrayValue(_) | OtelValue::KvlistValue(_) => None,
                })
                .collect();

            // Serialize the array values as a JSON string. Even though there is some nominal
            // support for array values in Sentry, it's not robust and not ready to be used.
            // Instead, serialize arrays to a JSON string, and have the UI decode the JSON if
            // possible.
            let json = serde_json::to_string(&safe_values).unwrap_or_default();
            (AttributeType::String, Value::String(json))
        }
        OtelValue::KvlistValue(kvlist) => {
            // Convert key-value list to JSON object and serialize as string.
            // Key-value pairs are supported by the type definition, but handling
            // varies between spans and logs, so we serialize to JSON for consistency.
            let mut json_obj = serde_json::Map::new();
            for kv in kvlist.values {
                if let Some(val) = kv.value.and_then(|v| match v.value? {
                    OtelValue::StringValue(s) => Some(serde_json::Value::String(s)),
                    OtelValue::BoolValue(b) => Some(serde_json::Value::Bool(b)),
                    OtelValue::IntValue(i) => {
                        Some(serde_json::Value::Number(serde_json::Number::from(i)))
                    }
                    OtelValue::DoubleValue(d) => {
                        serde_json::Number::from_f64(d).map(serde_json::Value::Number)
                    }
                    OtelValue::BytesValue(bytes) => {
                        String::from_utf8(bytes).ok().map(serde_json::Value::String)
                    }
                    // Skip nested complex types for safety
                    OtelValue::ArrayValue(_) | OtelValue::KvlistValue(_) => None,
                }) {
                    json_obj.insert(kv.key, val);
                }
            }
            let json = serde_json::to_string(&json_obj).unwrap_or_default();
            (AttributeType::String, Value::String(json))
        }
    };

    Some(Attribute::new(ty, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, KeyValue, KeyValueList, any_value,
    };

    #[test]
    fn test_string_value() {
        let otel_value = OtelValue::StringValue("test".to_owned());
        let attr = otel_value_to_attribute(otel_value).unwrap();

        assert_eq!(attr.value.ty.value().unwrap(), &AttributeType::String);
        if let Value::String(s) = attr.value.value.value().unwrap() {
            assert_eq!(s, "test");
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_bool_value() {
        let otel_value = OtelValue::BoolValue(true);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        assert_eq!(attr.value.ty.value().unwrap(), &AttributeType::Boolean);
        if let Value::Bool(b) = attr.value.value.value().unwrap() {
            assert!(b);
        } else {
            panic!("Expected boolean value");
        }
    }

    #[test]
    fn test_int_value() {
        let otel_value = OtelValue::IntValue(42);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        assert_eq!(attr.value.ty.value().unwrap(), &AttributeType::Integer);
        if let Value::I64(i) = attr.value.value.value().unwrap() {
            assert_eq!(*i, 42);
        } else {
            panic!("Expected integer value");
        }
    }

    #[test]
    fn test_double_value() {
        let otel_value = OtelValue::DoubleValue(3.5);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        assert_eq!(attr.value.ty.value().unwrap(), &AttributeType::Double);
        if let Value::F64(f) = attr.value.value.value().unwrap() {
            assert_eq!(*f, 3.5);
        } else {
            panic!("Expected double value");
        }
    }

    #[test]
    fn test_bytes_value() {
        let otel_value = OtelValue::BytesValue(b"hello".to_vec());
        let attr = otel_value_to_attribute(otel_value).unwrap();

        assert_eq!(attr.value.ty.value().unwrap(), &AttributeType::String);
        if let Value::String(s) = attr.value.value.value().unwrap() {
            assert_eq!(s, "hello");
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_array_value() {
        let array = ArrayValue {
            values: vec![
                AnyValue {
                    value: Some(any_value::Value::StringValue("item1".to_owned())),
                },
                AnyValue {
                    value: Some(any_value::Value::IntValue(42)),
                },
            ],
        };
        let otel_value = OtelValue::ArrayValue(array);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        assert_eq!(attr.value.ty.value().unwrap(), &AttributeType::String);
        if let Value::String(s) = attr.value.value.value().unwrap() {
            // Should be JSON array string
            assert!(s.contains("item1"));
            assert!(s.contains("42"));
        } else {
            panic!("Expected string value for array");
        }
    }

    #[test]
    fn test_kvlist_value() {
        let kvlist = KeyValueList {
            values: vec![KeyValue {
                key: "key1".to_owned(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("value1".to_owned())),
                }),
            }],
        };
        let otel_value = OtelValue::KvlistValue(kvlist);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        assert_eq!(attr.value.ty.value().unwrap(), &AttributeType::String);
        if let Value::String(s) = attr.value.value.value().unwrap() {
            // Should be JSON object string
            assert!(s.contains("key1"));
            assert!(s.contains("value1"));
        } else {
            panic!("Expected string value for kvlist");
        }
    }

    #[test]
    fn test_span_kvlist_value_works() {
        let kvlist = KeyValueList {
            values: vec![KeyValue {
                key: "key1".to_owned(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("value1".to_owned())),
                }),
            }],
        };
        let otel_value = OtelValue::KvlistValue(kvlist);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        // Should work and return JSON string
        assert_eq!(attr.value.ty.value().unwrap(), &AttributeType::String);
        if let Value::String(s) = attr.value.value.value().unwrap() {
            assert!(s.contains("key1"));
            assert!(s.contains("value1"));
        } else {
            panic!("Expected string value for kvlist");
        }
    }
}
