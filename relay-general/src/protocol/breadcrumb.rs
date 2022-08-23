#[cfg(test)]
use chrono::{TimeZone, Utc};

use crate::protocol::{EventId, Level, Timestamp};
use crate::types::{Annotated, Object, Value};

/// The Breadcrumbs Interface specifies a series of application events, or "breadcrumbs", that
/// occurred before an event.
///
/// An event may contain one or more breadcrumbs in an attribute named `breadcrumbs`. The entries
/// are ordered from oldest to newest. Consequently, the last entry in the list should be the last
/// entry before the event occurred.
///
/// While breadcrumb attributes are not strictly validated in Sentry, a breadcrumb is most useful
/// when it includes at least a `timestamp` and `type`, `category` or `message`. The rendering of
/// breadcrumbs in Sentry depends on what is provided.
///
/// The following example illustrates the breadcrumbs part of the event payload and omits other
/// attributes for simplicity.
///
/// ```json
/// {
///   "breadcrumbs": {
///     "values": [
///       {
///         "timestamp": "2016-04-20T20:55:53.845Z",
///         "message": "Something happened",
///         "category": "log",
///         "data": {
///           "foo": "bar",
///           "blub": "blah"
///         }
///       },
///       {
///         "timestamp": "2016-04-20T20:55:53.847Z",
///         "type": "navigation",
///         "data": {
///           "from": "/login",
///           "to": "/dashboard"
///         }
///       }
///     ]
///   }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_breadcrumb", value_type = "Breadcrumb")]
pub struct Breadcrumb {
    /// The timestamp of the breadcrumb. Recommended.
    ///
    /// A timestamp representing when the breadcrumb occurred. The format is either a string as
    /// defined in [RFC 3339](https://tools.ietf.org/html/rfc3339) or a numeric (integer or float)
    /// value representing the number of seconds that have elapsed since the [Unix
    /// epoch](https://en.wikipedia.org/wiki/Unix_time).
    ///
    /// Breadcrumbs are most useful when they include a timestamp, as it creates a timeline leading
    /// up to an event.
    pub timestamp: Annotated<Timestamp>,

    /// The type of the breadcrumb. _Optional_, defaults to `default`.
    ///
    /// - `default`: Describes a generic breadcrumb. This is typically a log message or
    ///   user-generated breadcrumb. The `data` field is entirely undefined and as such, completely
    ///   rendered as a key/value table.
    ///
    /// - `navigation`: Describes a navigation breadcrumb. A navigation event can be a URL change
    ///   in a web application, or a UI transition in a mobile or desktop application, etc.
    ///
    ///   Such a breadcrumb's `data` object has the required fields `from` and `to`, which
    ///   represent an application route/url each.
    ///
    /// - `http`: Describes an HTTP request breadcrumb. This represents an HTTP request transmitted
    ///   from your application. This could be an AJAX request from a web application, or a
    ///   server-to-server HTTP request to an API service provider, etc.
    ///
    ///   Such a breadcrumb's `data` property has the fields `url`, `method`, `status_code`
    ///   (integer) and `reason` (string).
    #[metastructure(field = "type", legacy_alias = "ty", max_chars = "enumlike")]
    pub ty: Annotated<String>,

    /// A dotted string indicating what the crumb is or from where it comes. _Optional._
    ///
    /// Typically it is a module name or a descriptive string. For instance, _ui.click_ could be
    /// used to indicate that a click happened in the UI or _flask_ could be used to indicate that
    /// the event originated in the Flask framework.
    #[metastructure(max_chars = "enumlike")]
    pub category: Annotated<String>,

    /// Severity level of the breadcrumb. _Optional._
    ///
    /// Allowed values are, from highest to lowest: `fatal`, `error`, `warning`, `info`, and
    /// `debug`. Levels are used in the UI to emphasize and deemphasize the crumb. Defaults to
    /// `info`.
    pub level: Annotated<Level>,

    /// Human readable message for the breadcrumb.
    ///
    /// If a message is provided, it is rendered as text with all whitespace preserved. Very long
    /// text might be truncated in the UI.
    #[metastructure(pii = "true", max_chars = "message")]
    pub message: Annotated<String>,

    /// Arbitrary data associated with this breadcrumb.
    ///
    /// Contains a dictionary whose contents depend on the breadcrumb `type`. Additional parameters
    /// that are unsupported by the type are rendered as a key/value table.
    #[metastructure(pii = "true", bag_size = "medium")]
    #[metastructure(skip_serialization = "empty")]
    pub data: Annotated<Object<Value>>,

    /// Identifier of the event this breadcrumb belongs to.
    ///
    /// Sentry events can appear as breadcrumbs in other events as long as they have occurred in the
    /// same organization. This identifier links to the original event.
    #[metastructure(skip_serialization = "null")]
    pub event_id: Annotated<EventId>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

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
  "event_id": "52df9022835246eeb317dbd739ccd059",
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
  "event_id": "52df9022835246eeb317dbd739ccd059",
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
            event_id: Annotated::new("52df9022835246eeb317dbd739ccd059".parse().unwrap()),
            other: {
                let mut map = Map::new();
                map.insert(
                    "c".to_string(),
                    Annotated::new(Value::String("d".to_string())),
                );
                map
            },
        });

        assert_eq!(breadcrumb, Annotated::from_json(input).unwrap());
        assert_eq!(output, breadcrumb.to_json_pretty().unwrap());
    }

    #[test]
    fn test_breadcrumb_default_values() {
        let input = r#"{"timestamp":946684800}"#;
        let output = r#"{"timestamp":946684800.0}"#;

        let breadcrumb = Annotated::new(Breadcrumb {
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            ..Default::default()
        });

        assert_eq!(breadcrumb, Annotated::from_json(input).unwrap());
        assert_eq!(output, breadcrumb.to_json().unwrap());
    }

    #[test]
    fn test_python_ty_regression() {
        // The Python SDK used to send "ty" instead of "type". We're lenient to accept both.
        let input = r#"{"timestamp":946684800,"ty":"http"}"#;
        let output = r#"{"timestamp":946684800.0,"type":"http"}"#;

        let breadcrumb = Annotated::new(Breadcrumb {
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            ty: Annotated::new("http".into()),
            ..Default::default()
        });

        assert_eq!(breadcrumb, Annotated::from_json(input).unwrap());
        assert_eq!(output, breadcrumb.to_json().unwrap());
    }
}
