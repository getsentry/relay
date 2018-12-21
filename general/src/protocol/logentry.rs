use crate::protocol::LenientString;
use crate::types::{Annotated, Array, FromValue, Object, Value};

/// A log entry message.
///
/// A log message is similar to the `message` attribute on the event itself but
/// can additionally hold optional parameters.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[metastructure(process_func = "process_logentry", value_type = "LogEntry")]
pub struct LogEntry {
    /// The log message with parameter placeholders (required).
    #[metastructure(pii = "true", max_chars = "message")]
    pub message: Annotated<String>,

    /// The formatted message
    #[metastructure(pii = "true", max_chars = "message")]
    pub formatted: Annotated<String>,

    /// Positional parameters to be interpolated into the log message.
    #[metastructure(pii = "true")]
    pub params: Annotated<Array<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "true")]
    pub other: Object<Value>,
}

impl FromValue for LogEntry {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        // raw 'message' is coerced to the Message interface, as its used for pure index of
        // searchable strings. If both a raw 'message' and a Message interface exist, try and
        // add the former as the 'formatted' attribute of the latter.
        // See GH-3248
        match value {
            x @ Annotated(Some(Value::Object(_)), _)
            | x @ Annotated(None, _)
            | x @ Annotated(Some(Value::Null), _) => {
                #[derive(Debug, FromValue)]
                struct Helper {
                    message: Annotated<String>,
                    formatted: Annotated<String>,
                    params: Annotated<Array<Value>>,
                    #[metastructure(additional_properties)]
                    other: Object<Value>,
                }

                Helper::from_value(x).map_value(
                    |Helper {
                         message,
                         formatted,
                         params,
                         other,
                     }| LogEntry {
                        message,
                        formatted,
                        params,
                        other,
                    },
                )
            }
            x => Annotated::new(LogEntry {
                formatted: LenientString::from_value(x).map_value(|x| x.into_inner()),
                ..Default::default()
            }),
        }
    }
}

#[test]
fn test_logentry_roundtrip() {
    let json = r#"{
  "message": "Hello, %s %s!",
  "params": [
    "World",
    1
  ],
  "other": "value"
}"#;

    let entry = Annotated::new(LogEntry {
        message: Annotated::new("Hello, %s %s!".to_string()),
        formatted: Annotated::empty(),
        params: Annotated::new(vec![
            Annotated::new(Value::String("World".to_string())),
            Annotated::new(Value::I64(1)),
        ]),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(entry, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, entry.to_json_pretty().unwrap());
}

#[test]
fn test_logentry_from_message() {
    let input = r#""hi""#;
    let output = r#"{
  "formatted": "hi"
}"#;

    let entry = Annotated::new(LogEntry {
        formatted: Annotated::new("hi".to_string()),
        ..Default::default()
    });

    assert_eq_dbg!(entry, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, entry.to_json_pretty().unwrap());
}

#[test]
fn test_logenty_empty_params() {
    let input = r#"{"params":[]}"#;
    let entry = Annotated::new(LogEntry {
        params: Annotated::new(vec![]),
        ..Default::default()
    });

    assert_eq_dbg!(entry, Annotated::from_json(input).unwrap());
    assert_eq_str!(input, entry.to_json().unwrap());
}
