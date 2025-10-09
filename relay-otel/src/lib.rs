//! OpenTelemetry to Sentry transformation utilities.
//!
//! This crate provides common functionality for converting OpenTelemetry data structures
//! to Sentry format. It serves as a shared library for both span and log transformations.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use opentelemetry_proto::tonic::{
    common::v1::{InstrumentationScope, any_value::Value as OtelValue},
    resource::v1::Resource,
};
use relay_event_schema::protocol::{Attribute, AttributeType, Attributes};
use relay_protocol::{Annotated, Value};

/// Converts an OpenTelemetry AnyValue to a Sentry attribute.
///
/// This function handles the conversion of OpenTelemetry attribute values to Sentry attribute types.
/// Complex types like arrays and key-value lists are serialized to strings.
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

            let json = serde_json::to_string(&safe_values).ok()?;
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
            let json = serde_json::to_string(&json_obj).ok()?;
            (AttributeType::String, Value::String(json))
        }
    };

    Some(Attribute::new(ty, value))
}

/// Applies Otel scopes into Sentry [`Attributes`].
pub fn otel_scope_into_attributes(
    attributes: &mut Attributes,
    resource: Option<&Resource>,
    scope: Option<&InstrumentationScope>,
) {
    for attribute in resource.into_iter().flat_map(|s| &s.attributes) {
        if let Some(attr) = attribute
            .value
            .clone()
            .and_then(|v| v.value)
            .and_then(otel_value_to_attribute)
        {
            let key = format!("resource.{}", attribute.key);
            attributes.insert_raw(key, Annotated::new(attr));
        }
    }

    for attribute in scope.into_iter().flat_map(|s| &s.attributes) {
        if let Some(attr) = attribute
            .value
            .clone()
            .and_then(|v| v.value)
            .and_then(otel_value_to_attribute)
        {
            let key = format!("instrumentation.{}", attribute.key);
            attributes.insert_raw(key, Annotated::new(attr));
        }
    }

    if let Some(scope) = scope {
        attributes.insert("instrumentation.name".to_owned(), scope.name.clone());
        attributes.insert("instrumentation.version".to_owned(), scope.version.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, KeyValue, KeyValueList, any_value,
    };
    use relay_protocol::{SerializableAnnotated, get_value};

    #[test]
    fn test_string_value() {
        let otel_value = OtelValue::StringValue("test".to_owned());
        let attr = otel_value_to_attribute(otel_value).unwrap();

        let value = &attr.value.value;
        assert_eq!(get_value!(value!), &Value::String("test".to_owned()));
    }

    #[test]
    fn test_bool_value() {
        let otel_value = OtelValue::BoolValue(true);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        let value = &attr.value.value;
        assert_eq!(get_value!(value!), &Value::Bool(true));
    }

    #[test]
    fn test_int_value() {
        let otel_value = OtelValue::IntValue(42);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        let value = &attr.value.value;
        assert_eq!(get_value!(value!), &Value::I64(42));
    }

    #[test]
    fn test_double_value() {
        let otel_value = OtelValue::DoubleValue(3.5);
        let attr = otel_value_to_attribute(otel_value).unwrap();

        let value = &attr.value.value;
        assert_eq!(get_value!(value!), &Value::F64(3.5));
    }

    #[test]
    fn test_bytes_value() {
        let otel_value = OtelValue::BytesValue(b"hello".to_vec());
        let attr = otel_value_to_attribute(otel_value).unwrap();

        let value = &attr.value.value;
        assert_eq!(get_value!(value!), &Value::String("hello".to_owned()));
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

        let value = &attr.value.value;
        assert_eq!(
            get_value!(value!),
            &Value::String("[\"item1\",42]".to_owned())
        );
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

        let value = &attr.value.value;
        assert_eq!(
            get_value!(value!),
            &Value::String("{\"key1\":\"value1\"}".to_owned())
        );
    }

    #[test]
    fn test_scope_attributes() {
        let resource = serde_json::from_value(serde_json::json!({
            "attributes": [{
                "key": "service.name",
                "value": {"stringValue": "test-service"},
            },
            {
                "key": "the.answer",
                "value": {"stringValue": "foobar"},
            },
        ]}))
        .unwrap();

        let scope = InstrumentationScope {
            name: "Eins Name".to_owned(),
            version: "123.42".to_owned(),
            attributes: vec![
                KeyValue {
                    key: "the.answer".to_owned(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::IntValue(42)),
                    }),
                },
                // Clashes with `scope.name` and should be overwritten.
                KeyValue {
                    key: "name".to_owned(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("oops".to_owned())),
                    }),
                },
            ],
            dropped_attributes_count: 12,
        };

        let mut attributes = Attributes::new();

        otel_scope_into_attributes(&mut attributes, Some(&resource), Some(&scope));

        insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new(attributes)), @r#"
        {
          "instrumentation.name": {
            "type": "string",
            "value": "Eins Name"
          },
          "instrumentation.the.answer": {
            "type": "integer",
            "value": 42
          },
          "instrumentation.version": {
            "type": "string",
            "value": "123.42"
          },
          "resource.service.name": {
            "type": "string",
            "value": "test-service"
          },
          "resource.the.answer": {
            "type": "string",
            "value": "foobar"
          }
        }
        "#);
    }
}
