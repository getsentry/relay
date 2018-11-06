use general_derive::{FromValue, ProcessValue, ToValue};

#[cfg(test)]
use chrono::TimeZone;

use super::*;

/// A breadcrumb.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_breadcrumb")]
pub struct Breadcrumb {
    /// The timestamp of the breadcrumb (required).
    #[metastructure(required = "true")]
    pub timestamp: Annotated<DateTime<Utc>>,

    /// The type of the breadcrumb.
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,

    /// The optional category of the breadcrumb.
    #[metastructure(cap_size = "enumlike")]
    pub category: Annotated<String>,

    /// Severity level of the breadcrumb (required).
    pub level: Annotated<Level>,

    /// Human readable message for the breadcrumb.
    #[metastructure(pii_kind = "freeform", cap_size = "message")]
    pub message: Annotated<String>,

    /// Custom user-defined data of this breadcrumb.
    #[metastructure(pii_kind = "databag")]
    pub data: Annotated<Object<Value>>,

    /// Identifier of the event this breadcrumb belongs to.
    // TODO: Remove this?
    pub event_id: Annotated<EventId>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[test]
fn test_breadcrumb_roundtrip() {
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
        timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
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
        timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
        ..Default::default()
    });

    assert_eq_dbg!(breadcrumb, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, breadcrumb.to_json().unwrap());
}

#[test]
fn test_breadcrumb_invalid() {
    let breadcrumb = Annotated::new(Breadcrumb {
        timestamp: Annotated::from_error("value required", None),
        ..Default::default()
    });
    assert_eq_dbg!(breadcrumb, Annotated::from_json("{}").unwrap());
}
