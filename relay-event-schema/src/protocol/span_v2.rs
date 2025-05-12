use opentelemetry_proto::tonic::trace::v1::{
    span::SpanKind as OtelSpanKind, status::StatusCode as OtelSpanStatus,
};
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use std::fmt;
use std::str::FromStr;

use serde::Serialize;

use crate::processor::ProcessValue;
use crate::protocol::{Attribute, SpanId, Timestamp, TraceId};

use super::OperationType;

/// A version 2 (transactionless) span.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue)]
pub struct SpanV2 {
    #[metastructure(required = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// The ID of the trace the span belongs to.
    /// The Span id.
    #[metastructure(required = true, trim = false)]
    pub span_id: Annotated<SpanId>,

    /// Span type (see `OperationType` docs).
    #[metastructure(required = true, max_chars = 128)]
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
    ///  - `empty`: unknown
    ///  - `false`: is not remote
    ///  - `true`: is remote
    #[metastructure(required = true)]
    pub is_remote: Annotated<bool>,

    // Used to clarify the relationship between parents and children, or to distinguish between
    // spans, e.g. a `server` and `client` span with the same name.
    //
    // See <https://opentelemetry.io/docs/specs/otel/trace/api/#spankind>
    #[metastructure(required = true, skip_serialization = "empty", trim = false)]
    pub kind: Annotated<SpanV2Kind>,

    /// Timestamp when the span started.
    #[metastructure(required = true)]
    pub start_timestamp: Annotated<Timestamp>,

    /// Timestamp when the span was ended.
    #[metastructure(required = true)]
    pub end_timestamp: Annotated<Timestamp>,

    /// Arbitrary attributes on a span.
    #[metastructure(pii = "true", trim = false)]
    pub attributes: Annotated<Object<Attribute>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl SpanV2 {
    pub fn attribute(&self, key: &str) -> Option<&Annotated<Value>> {
        Some(&self.attributes.value()?.get(key)?.value()?.value.value)
    }
}

/// Status of a V2 span.
///
/// This is a subset of OTEL's statuses (unset, ok, error).
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
pub enum SpanV2Status {
    Unset = OtelSpanStatus::Unset as i32,
    Ok = OtelSpanStatus::Ok as i32,
    Error = OtelSpanStatus::Error as i32,
}

impl SpanV2Status {
    /// Returns the string representation of the status.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Ok => "ok",
            Self::Error => "error",
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

/// Error parsing a `SpanV2Status`.
#[derive(Clone, Copy, Debug)]
pub struct ParseSpanV2StatusError;

impl fmt::Display for ParseSpanV2StatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid span status")
    }
}

impl std::error::Error for ParseSpanV2StatusError {}

impl FromStr for SpanV2Status {
    type Err = ParseSpanV2StatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "unset" => Ok(Self::Unset),
            "ok" => Ok(Self::Ok),
            "error" => Ok(Self::Error),
            _ => Err(ParseSpanV2StatusError),
        }
    }
}

impl FromValue for SpanV2Status {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match value {
            Annotated(Some(Value::String(s)), meta) => Annotated(Self::from_str(&s).ok(), meta),
            Annotated(_, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for SpanV2Status {
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

#[derive(Clone, Debug, PartialEq, ProcessValue)]
#[repr(i32)]
pub enum SpanV2Kind {
    Unspecified = OtelSpanKind::Unspecified as i32,
    Internal = OtelSpanKind::Internal as i32,
    Server = OtelSpanKind::Server as i32,
    Client = OtelSpanKind::Client as i32,
    Producer = OtelSpanKind::Producer as i32,
    Consumer = OtelSpanKind::Consumer as i32,
}

impl SpanV2Kind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
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

#[derive(Debug)]
pub struct ParseSpanV2KindError;

impl std::str::FromStr for SpanV2Kind {
    type Err = ParseSpanV2KindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "unspecified" => Self::Unspecified,
            "internal" => Self::Internal,
            "server" => Self::Server,
            "client" => Self::Client,
            "producer" => Self::Producer,
            "consumer" => Self::Consumer,
            _ => return Err(ParseSpanV2KindError),
        })
    }
}

impl Default for SpanV2Kind {
    fn default() -> Self {
        Self::Internal
    }
}

impl From<OtelSpanKind> for SpanV2Kind {
    fn from(otel_kind: OtelSpanKind) -> Self {
        match otel_kind {
            OtelSpanKind::Unspecified => Self::Unspecified,
            OtelSpanKind::Internal => Self::Internal,
            OtelSpanKind::Server => Self::Server,
            OtelSpanKind::Client => Self::Client,
            OtelSpanKind::Producer => Self::Producer,
            OtelSpanKind::Consumer => Self::Consumer,
        }
    }
}

impl fmt::Display for SpanV2Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromValue for SpanV2Kind {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match value {
            Annotated(Some(Value::String(s)), meta) => Annotated(Self::from_str(&s).ok(), meta),
            Annotated(_, meta) => Annotated(None, meta),
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

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use similar_asserts::assert_eq;

    use super::*;

    macro_rules! attrs {
        ($($name:expr => $val:expr , $ty:ident),* $(,)?) => {
            std::collections::BTreeMap::from([$((
                $name.to_owned(),
                relay_protocol::Annotated::new(
                    $crate::protocol::Attribute::new(
                        $crate::protocol::AttributeType::$ty,
                        $val.into()
                    )
                )
            ),)*])
        };
    }

    #[test]
    fn test_span_serialization() {
        let json = r#"{
  "trace_id": "6cf173d587eb48568a9b2e12dcfbea52",
  "span_id": "438f40bd3b4a41ee",
  "name": "GET http://app.test/",
  "status": "ok",
  "is_remote": true,
  "kind": "server",
  "start_timestamp": 1742921669.158209,
  "end_timestamp": 1742921669.180536,
  "attributes": {
    "http.response.status_code": {
      "type": "integer",
      "value": "200"
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

        let attributes = attrs!(
            "sentry.release" => "1.0.0" , String,
            "sentry.environment" => "local", String,
            "sentry.platform" => "php", String,
            "sentry.sdk.name" => "sentry.php", String,
            "sentry.sdk.version" => "4.10.0", String,
            "sentry.transaction_info.source" => "url", String,
            "sentry.origin" => "manual", String,
            "server.address" => "DHWKN7KX6N.local", String,
            "http.response.status_code" => "200", Integer,
        );
        let span = Annotated::new(SpanV2 {
            start_timestamp: Annotated::new(
                Utc.timestamp_opt(1742921669, 158209000).unwrap().into(),
            ),
            end_timestamp: Annotated::new(Utc.timestamp_opt(1742921669, 180536000).unwrap().into()),
            name: Annotated::new("GET http://app.test/".to_owned()),
            trace_id: Annotated::new("6cf173d587eb48568a9b2e12dcfbea52".parse().unwrap()),
            span_id: Annotated::new(SpanId("438f40bd3b4a41ee".into())),
            parent_span_id: Annotated::empty(),
            status: Annotated::new(SpanV2Status::Ok),
            kind: Annotated::new(SpanV2Kind::Server),
            is_remote: Annotated::new(true),
            attributes: Annotated::new(attributes),
            ..Default::default()
        });
        assert_eq!(json, span.to_json_pretty().unwrap());

        let span_from_string = Annotated::from_json(json).unwrap();
        assert_eq!(span, span_from_string);
    }
}
