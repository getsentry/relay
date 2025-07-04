use relay_protocol::{Annotated, Array, Empty, Error, FromValue, IntoValue, Object, Value};

use std::fmt;
use std::str::FromStr;

use serde::Serialize;

use crate::processor::ProcessValue;
use crate::protocol::{Attributes, OperationType, SpanId, Timestamp, TraceId};

/// A version 2 (transactionless) span.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue)]
pub struct SpanV2 {
    /// The ID of the trace to which this span belongs.
    #[metastructure(required = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// The Span ID.
    #[metastructure(required = true, trim = false)]
    pub span_id: Annotated<SpanId>,

    /// Span type (see `OperationType` docs).
    #[metastructure(required = true)]
    pub name: Annotated<OperationType>,

    /// The span's status.
    #[metastructure(required = true)]
    pub status: Annotated<SpanV2Status>,

    /// Indicates whether a span's parent is remote.
    ///
    /// For OpenTelemetry spans, this is derived from span flags bits 8 and 9. See
    /// `SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK` and `SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK`.
    ///
    /// The states are:
    ///  - `false`: is not remote
    ///  - `true`: is remote
    #[metastructure(required = true)]
    pub is_remote: Annotated<bool>,

    /// Used to clarify the relationship between parents and children, or to distinguish between
    /// spans, e.g. a `server` and `client` span with the same name.
    ///
    /// See <https://opentelemetry.io/docs/specs/otel/trace/api/#spankind>
    #[metastructure(skip_serialization = "empty", trim = false)]
    pub kind: Annotated<SpanV2Kind>,

    /// Timestamp when the span started.
    #[metastructure(required = true)]
    pub start_timestamp: Annotated<Timestamp>,

    /// Timestamp when the span was ended.
    #[metastructure(required = true)]
    pub end_timestamp: Annotated<Timestamp>,

    /// Links from this span to other spans.
    #[metastructure(pii = "maybe")]
    pub links: Annotated<Array<SpanV2Link>>,

    /// Arbitrary attributes on a span.
    #[metastructure(pii = "true", trim = false)]
    pub attributes: Annotated<Attributes>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

/// Status of a V2 span.
///
/// This is a subset of OTEL's statuses (unset, ok, error), plus
/// a catchall variant for forward compatibility.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanV2Status {
    /// The span completed successfully.
    Ok,
    /// The span contains an error.
    Error,
    /// Catchall variant for forward compatibility.
    Other(String),
}

impl SpanV2Status {
    /// Returns the string representation of the status.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Other(s) => s,
        }
    }
}

impl Empty for SpanV2Status {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl AsRef<str> for SpanV2Status {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SpanV2Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for SpanV2Status {
    fn from(value: String) -> Self {
        match value.as_str() {
            "ok" => Self::Ok,
            "error" => Self::Error,
            _ => Self::Other(value),
        }
    }
}

impl FromValue for SpanV2Status {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        String::from_value(value).map_value(|s| s.into())
    }
}

impl IntoValue for SpanV2Status {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(match self {
            SpanV2Status::Other(s) => s,
            _ => self.to_string(),
        })
    }

    fn serialize_payload<S>(
        &self,
        s: S,
        _behavior: relay_protocol::SkipSerialization,
    ) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        s.serialize_str(self.as_str())
    }
}

/// The kind of a V2 span.
///
/// This corresponds to OTEL's kind enum, plus a
/// catchall variant for forward compatibility.
#[derive(Clone, Debug, PartialEq, ProcessValue)]
pub enum SpanV2Kind {
    /// An operation internal to an application.
    Internal,
    /// Server-side processing requested by a client.
    Server,
    /// A request from a client to a server.
    Client,
    /// Scheduling of an operation.
    Producer,
    /// Processing of a scheduled operation.
    Consumer,
}

impl SpanV2Kind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Internal => "internal",
            Self::Server => "server",
            Self::Client => "client",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }
}

impl Empty for SpanV2Kind {
    fn is_empty(&self) -> bool {
        false
    }
}

impl Default for SpanV2Kind {
    fn default() -> Self {
        Self::Internal
    }
}

impl fmt::Display for SpanV2Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParseSpanV2KindError;

impl fmt::Display for ParseSpanV2KindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid span kind")
    }
}

impl FromStr for SpanV2Kind {
    type Err = ParseSpanV2KindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s {
            "internal" => Self::Internal,
            "server" => Self::Server,
            "client" => Self::Client,
            "producer" => Self::Producer,
            "consumer" => Self::Consumer,
            _ => return Err(ParseSpanV2KindError),
        };
        Ok(kind)
    }
}

impl FromValue for SpanV2Kind {
    fn from_value(Annotated(value, meta): Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match &value {
            Some(Value::String(s)) => match s.parse() {
                Ok(kind) => Annotated(Some(kind), meta),
                Err(_) => Annotated::from_error(Error::expected("a span kind"), value),
            },
            Some(_) => Annotated::from_error(Error::expected("a span kind"), value),
            None => Annotated::empty(),
        }
    }
}

impl IntoValue for SpanV2Kind {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(
        &self,
        s: S,
        _behavior: relay_protocol::SkipSerialization,
    ) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        s.serialize_str(self.as_str())
    }
}

/// A link from a span to another span.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(trim = false)]
pub struct SpanV2Link {
    /// The trace id of the linked span.
    #[metastructure(required = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// The span id of the linked span.
    #[metastructure(required = true, trim = false)]
    pub span_id: Annotated<SpanId>,

    /// Whether the linked span was positively/negatively sampled.
    #[metastructure(trim = false)]
    pub sampled: Annotated<bool>,

    /// Span link attributes, similar to span attributes/data.
    #[metastructure(pii = "maybe", trim = false)]
    pub attributes: Annotated<Attributes>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "maybe", trim = false)]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_span_serialization() {
        let json = r#"{
  "trace_id": "6cf173d587eb48568a9b2e12dcfbea52",
  "span_id": "438f40bd3b4a41ee",
  "name": "GET http://app.test/",
  "status": "ok",
  "is_remote": true,
  "kind": "server",
  "start_timestamp": 1742921669.25,
  "end_timestamp": 1742921669.75,
  "links": [
    {
      "trace_id": "627a2885119dcc8184fae7eef09438cb",
      "span_id": "6c71fc6b09b8b716",
      "sampled": true,
      "attributes": {
        "sentry.link.type": {
          "type": "string",
          "value": "previous_trace"
        }
      }
    }
  ],
  "attributes": {
    "custom.error_rate": {
      "type": "double",
      "value": 0.5
    },
    "custom.is_green": {
      "type": "boolean",
      "value": true
    },
    "http.response.status_code": {
      "type": "integer",
      "value": 200
    },
    "sentry.environment": {
      "type": "string",
      "value": "local"
    },
    "sentry.origin": {
      "type": "string",
      "value": "manual"
    },
    "sentry.platform": {
      "type": "string",
      "value": "php"
    },
    "sentry.release": {
      "type": "string",
      "value": "1.0.0"
    },
    "sentry.sdk.name": {
      "type": "string",
      "value": "sentry.php"
    },
    "sentry.sdk.version": {
      "type": "string",
      "value": "4.10.0"
    },
    "sentry.transaction_info.source": {
      "type": "string",
      "value": "url"
    },
    "server.address": {
      "type": "string",
      "value": "DHWKN7KX6N.local"
    }
  }
}"#;

        let mut attributes = Attributes::new();

        attributes.insert("custom.error_rate".to_owned(), 0.5);
        attributes.insert("custom.is_green".to_owned(), true);
        attributes.insert("sentry.release".to_owned(), "1.0.0".to_owned());
        attributes.insert("sentry.environment".to_owned(), "local".to_owned());
        attributes.insert("sentry.platform".to_owned(), "php".to_owned());
        attributes.insert("sentry.sdk.name".to_owned(), "sentry.php".to_owned());
        attributes.insert("sentry.sdk.version".to_owned(), "4.10.0".to_owned());
        attributes.insert(
            "sentry.transaction_info.source".to_owned(),
            "url".to_owned(),
        );
        attributes.insert("sentry.origin".to_owned(), "manual".to_owned());
        attributes.insert("server.address".to_owned(), "DHWKN7KX6N.local".to_owned());
        attributes.insert("http.response.status_code".to_owned(), 200i64);

        let mut link_attributes = Attributes::new();
        link_attributes.insert("sentry.link.type".to_owned(), "previous_trace".to_owned());

        let links = vec![Annotated::new(SpanV2Link {
            trace_id: Annotated::new("627a2885119dcc8184fae7eef09438cb".parse().unwrap()),
            span_id: Annotated::new("6c71fc6b09b8b716".parse().unwrap()),
            sampled: Annotated::new(true),
            attributes: Annotated::new(link_attributes),
            ..Default::default()
        })];
        let span = Annotated::new(SpanV2 {
            start_timestamp: Annotated::new(
                Utc.timestamp_opt(1742921669, 250000000).unwrap().into(),
            ),
            end_timestamp: Annotated::new(Utc.timestamp_opt(1742921669, 750000000).unwrap().into()),
            name: Annotated::new("GET http://app.test/".to_owned()),
            trace_id: Annotated::new("6cf173d587eb48568a9b2e12dcfbea52".parse().unwrap()),
            span_id: Annotated::new("438f40bd3b4a41ee".parse().unwrap()),
            parent_span_id: Annotated::empty(),
            status: Annotated::new(SpanV2Status::Ok),
            kind: Annotated::new(SpanV2Kind::Server),
            is_remote: Annotated::new(true),
            links: Annotated::new(links),
            attributes: Annotated::new(attributes),
            ..Default::default()
        });
        assert_eq!(json, span.to_json_pretty().unwrap());

        let span_from_string = Annotated::from_json(json).unwrap();
        assert_eq!(span, span_from_string);
    }
}
