use crate::protocol::JsonLenientString;
use crate::types::{Annotated, Error, FromValue, Meta, Object, Value};

/// A log entry message.
///
/// A log message is similar to the `message` attribute on the event itself but
/// can additionally hold optional parameters.
///
/// ```json
/// {
///   "message": {
///     "message": "My raw message with interpreted strings like %s",
///     "params": ["this"]
///   }
/// }
/// ```
///
/// ```json
/// {
///   "message": {
///     "message": "My raw message with interpreted strings like {foo}",
///     "params": {"foo": "this"}
///   }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_logentry", value_type = "LogEntry")]
pub struct LogEntry {
    /// The log message with parameter placeholders.
    ///
    /// This attribute is primarily used for grouping related events together into issues.
    /// Therefore this really should just be a string template, i.e. `Sending %d requests` instead
    /// of `Sending 9999 requests`. The latter is much better at home in `formatted`.
    ///
    /// It must not exceed 8192 characters. Longer messages will be truncated.
    #[metastructure(max_chars = "message")]
    pub message: Annotated<Message>,

    /// The formatted message. If `message` and `params` are given, Sentry
    /// will attempt to backfill `formatted` if empty.
    ///
    /// It must not exceed 8192 characters. Longer messages will be truncated.
    #[metastructure(max_chars = "message", pii = "true")]
    pub formatted: Annotated<Message>,

    /// Parameters to be interpolated into the log message. This can be an array of positional
    /// parameters as well as a mapping of named arguments to their values.
    #[metastructure(bag_size = "medium")]
    pub params: Annotated<Value>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "true")]
    pub other: Object<Value>,
}

impl From<String> for LogEntry {
    fn from(formatted_msg: String) -> Self {
        LogEntry {
            formatted: Annotated::new(formatted_msg.into()),
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(value_type = "Message", value_type = "String")]
pub struct Message(String);

impl From<String> for Message {
    fn from(msg: String) -> Message {
        Message(msg)
    }
}

impl AsRef<str> for Message {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl FromValue for LogEntry {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        // raw 'message' is coerced to the Message interface, as its used for pure index of
        // searchable strings. If both a raw 'message' and a Message interface exist, try and
        // add the former as the 'formatted' attribute of the latter.
        // See GH-3248
        match value {
            x @ Annotated(Some(Value::Object(_)), _) => {
                #[derive(Debug, FromValue)]
                struct Helper {
                    message: Annotated<String>,
                    formatted: Annotated<String>,
                    params: Annotated<Value>,
                    #[metastructure(additional_properties)]
                    other: Object<Value>,
                }

                Helper::from_value(x).map_value(|helper| {
                    let params = match helper.params {
                        a @ Annotated(Some(Value::Object(_)), _) => a,
                        a @ Annotated(Some(Value::Array(_)), _) => a,
                        a @ Annotated(None, _) => a,
                        Annotated(Some(value), _) => Annotated::from_error(
                            Error::expected("message parameters"),
                            Some(value),
                        ),
                    };

                    LogEntry {
                        message: helper.message.map_value(Message),
                        formatted: helper.formatted.map_value(Message),
                        params,
                        other: helper.other,
                    }
                })
            }
            Annotated(None, meta) => Annotated(None, meta),
            // The next two cases handle the legacy top-level `message` attribute, which was sent as
            // literal string, false (which should be ignored) or even as deep JSON object. Sentry
            // historically JSONified this field.
            Annotated(Some(Value::Bool(false)), _) => Annotated(None, Meta::default()),
            x => Annotated::new(LogEntry {
                formatted: JsonLenientString::from_value(x)
                    .map_value(JsonLenientString::into_inner)
                    .map_value(Message),
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
        message: Annotated::new("Hello, %s %s!".to_string().into()),
        formatted: Annotated::empty(),
        params: Annotated::new(Value::Array(vec![
            Annotated::new(Value::String("World".to_string())),
            Annotated::new(Value::I64(1)),
        ])),
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
        formatted: Annotated::new("hi".to_string().into()),
        ..Default::default()
    });

    assert_eq_dbg!(entry, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, entry.to_json_pretty().unwrap());
}

#[test]
fn test_logentry_empty_params() {
    let input = r#"{"params":[]}"#;
    let entry = Annotated::new(LogEntry {
        params: Annotated::new(Value::Array(vec![])),
        ..Default::default()
    });

    assert_eq_dbg!(entry, Annotated::from_json(input).unwrap());
    assert_eq_str!(input, entry.to_json().unwrap());
}

#[test]
fn test_logentry_named_params() {
    let json = r#"{
  "message": "Hello, %s!",
  "params": {
    "name": "World"
  }
}"#;

    let entry = Annotated::new(LogEntry {
        message: Annotated::new("Hello, %s!".to_string().into()),
        params: Annotated::new(Value::Object({
            let mut object = Object::new();
            object.insert(
                "name".to_string(),
                Annotated::new(Value::String("World".to_string())),
            );
            object
        })),
        ..LogEntry::default()
    });

    assert_eq_dbg!(entry, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, entry.to_json_pretty().unwrap());
}

#[test]
fn test_logentry_invalid_params() {
    let json = r#"{
  "message": "Hello, %s!",
  "params": 42
}"#;

    let entry = Annotated::new(LogEntry {
        message: Annotated::new("Hello, %s!".to_string().into()),
        params: Annotated::from_error(Error::expected("message parameters"), Some(Value::I64(42))),
        ..LogEntry::default()
    });

    assert_eq_dbg!(entry, Annotated::from_json(json).unwrap());
}
