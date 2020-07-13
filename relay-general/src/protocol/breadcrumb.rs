#[cfg(test)]
use chrono::{TimeZone, Utc};

use crate::protocol::{Level, Timestamp};
use crate::types::{Annotated, Object, Value};

/// A breadcrumb.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_breadcrumb", value_type = "Breadcrumb")]
pub struct Breadcrumb {
    /// The timestamp of the breadcrumb.
    pub timestamp: Annotated<Timestamp>,

    /// The type of the breadcrumb.
    #[metastructure(field = "type", max_chars = "enumlike")]
    pub ty: Annotated<String>,

    /// The optional category of the breadcrumb.
    #[metastructure(max_chars = "enumlike")]
    pub category: Annotated<String>,

    /// Severity level of the breadcrumb.
    pub level: Annotated<Level>,

    /// Human readable message for the breadcrumb.
    #[metastructure(pii = "true", max_chars = "message")]
    pub message: Annotated<String>,

    /// Custom user-defined data of this breadcrumb.
    #[metastructure(pii = "true", bag_size = "medium")]
    #[metastructure(skip_serialization = "empty")]
    pub data: Annotated<Object<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[test]
fn test_breadcrumb_roundtrip() {
    use crate::types::Map;

    let input = r#"{
  "timestamp": 946684800,
  "type": "mytype",
  "category": "mycategory",
  "level": "fatal",
  "message": "my message",
  "data": {
    "a": "b"
  },
  "c": "d"
}"#;

    let output = r#"{
  "timestamp": 946684800.0,
  "type": "mytype",
  "category": "mycategory",
  "level": "fatal",
  "message": "my message",
  "data": {
    "a": "b"
  },
  "c": "d"
}"#;

    let breadcrumb = Annotated::new(Breadcrumb {
        timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
        ty: Annotated::new("mytype".to_string()),
        category: Annotated::new("mycategory".to_string()),
        level: Annotated::new(Level::Fatal),
        message: Annotated::new("my message".to_string()),
        data: {
            let mut map = Map::new();
            map.insert(
                "a".to_string(),
                Annotated::new(Value::String("b".to_string())),
            );
            Annotated::new(map)
        },
        other: {
            let mut map = Map::new();
            map.insert(
                "c".to_string(),
                Annotated::new(Value::String("d".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(breadcrumb, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, breadcrumb.to_json_pretty().unwrap());
}

#[test]
fn test_breadcrumb_default_values() {
    let input = r#"{"timestamp":946684800}"#;
    let output = r#"{"timestamp":946684800.0}"#;

    let breadcrumb = Annotated::new(Breadcrumb {
        timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
        ..Default::default()
    });

    assert_eq_dbg!(breadcrumb, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, breadcrumb.to_json().unwrap());
}
