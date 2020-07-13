use crate::protocol::{JsonLenientString, Mechanism, RawStacktrace, Stacktrace, ThreadId};
use crate::types::{Annotated, Object, Value};

/// A single exception.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_exception", value_type = "Exception")]
pub struct Exception {
    /// Exception type. One of value or exception is required, checked in StoreNormalizeProcessor
    #[metastructure(field = "type", max_chars = "symbol")]
    pub ty: Annotated<String>,

    /// Human readable display value.
    #[metastructure(max_chars = "message", pii = "maybe")]
    pub value: Annotated<JsonLenientString>,

    /// Module name of this exception.
    #[metastructure(max_chars = "symbol")]
    pub module: Annotated<String>,

    /// Stack trace containing frames of this exception.
    #[metastructure(
        legacy_alias = "sentry.interfaces.Stacktrace",
        skip_serialization = "empty"
    )]
    pub stacktrace: Annotated<Stacktrace>,

    /// Optional unprocessed stack trace.
    #[metastructure(skip_serialization = "empty")]
    pub raw_stacktrace: Annotated<RawStacktrace>,

    /// Identifier of the thread this exception occurred in.
    #[metastructure(max_chars = "enumlike")]
    pub thread_id: Annotated<ThreadId>,

    /// Mechanism by which this exception was generated and handled.
    pub mechanism: Annotated<Mechanism>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[test]
fn test_exception_roundtrip() {
    use crate::types::Map;

    // stack traces and mechanism are tested separately
    let json = r#"{
  "type": "mytype",
  "value": "myvalue",
  "module": "mymodule",
  "thread_id": 42,
  "other": "value"
}"#;
    let exception = Annotated::new(Exception {
        ty: Annotated::new("mytype".to_string()),
        value: Annotated::new("myvalue".to_string().into()),
        module: Annotated::new("mymodule".to_string()),
        thread_id: Annotated::new(ThreadId::Int(42)),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
        ..Default::default()
    });

    assert_eq_dbg!(exception, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, exception.to_json_pretty().unwrap());
}

#[test]
fn test_exception_default_values() {
    let json = r#"{"type":"mytype"}"#;
    let exception = Annotated::new(Exception {
        ty: Annotated::new("mytype".to_string()),
        ..Default::default()
    });

    assert_eq_dbg!(exception, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, exception.to_json().unwrap());
}

#[test]
fn test_exception_empty_fields() {
    let json = r#"{"type":"","value":""}"#;
    let exception = Annotated::new(Exception {
        ty: Annotated::new("".to_string()),
        value: Annotated::new("".to_string().into()),
        ..Default::default()
    });

    assert_eq_dbg!(exception, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, exception.to_json().unwrap());
}

#[test]
fn test_coerces_object_value_to_string() {
    let input = r#"{"value":{"unauthorized":true}}"#;
    let output = r#"{"value":"{\"unauthorized\":true}"}"#;

    let exception = Annotated::new(Exception {
        value: Annotated::new(r#"{"unauthorized":true}"#.to_string().into()),
        ..Default::default()
    });

    assert_eq_dbg!(exception, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, exception.to_json().unwrap());
}

#[test]
fn test_explicit_none() {
    let json = r#"{
  "value": null,
  "type": "ZeroDivisionError"
}"#;

    let exception = Annotated::new(Exception {
        ty: Annotated::new("ZeroDivisionError".to_string()),
        ..Default::default()
    });

    assert_eq_dbg!(exception, Annotated::from_json(json).unwrap());
    assert_eq_str!(
        r#"{"type":"ZeroDivisionError"}"#,
        exception.to_json().unwrap()
    );
}
