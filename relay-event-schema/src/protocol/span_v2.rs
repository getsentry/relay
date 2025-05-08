use relay_protocol::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};
use std::fmt;
use std::str::FromStr;

use serde::Serialize;

use crate::protocol::{Attribute, SpanId, Timestamp, TraceId};

use super::OperationType;

/// A version 2 (transactionless) span.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue)]
pub struct SpanV2 {
    /// Timestamp when the span was ended.
    #[metastructure(required = true)]
    pub end_timestamp: Annotated<Timestamp>,

    /// Timestamp when the span started.
    #[metastructure(required = true)]
    pub start_timestamp: Annotated<Timestamp>,

    /// The ID of the trace the span belongs to.
    #[metastructure(required = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// The Span id.
    #[metastructure(required = false, trim = false)]
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// Span type (see `OperationType` docs).
    #[metastructure(max_chars = 128)]
    pub name: Annotated<OperationType>,

    /// The span's status.
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
    pub is_remote: Annotated<bool>,

    /// The span's kind.
    pub kind: Annotated<SpanV2Kind>,

    /// Arbitrary attributes on a span.
    #[metastructure(pii = "true", trim = false)]
    pub attributes: Annotated<Object<Attribute>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

/// Status of a V2 span.
///
/// This is a subset of OTEL's statuses (unset, ok, error).
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum SpanV2Status {
    Ok = 1,
    Error = 2,
}

impl SpanV2Status {
    /// Returns the string representation of the status.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
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
            "ok" => Ok(Self::Ok),
            "error" => Ok(Self::Error),
            _ => Err(ParseSpanV2StatusError),
        }
    }
}

impl Empty for SpanV2Status {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for SpanV2Status {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(status) => Annotated(Some(status), meta),
                Err(_) => {
                    meta.add_error(Error::expected("a span status"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::I64(value)), mut meta) => Annotated(
                Some(match value {
                    1 => Self::Ok,
                    2 => Self::Error,
                    _ => {
                        meta.add_error(Error::expected("a span status"));
                        meta.set_original_value(Some(value));
                        return Annotated(None, meta);
                    }
                }),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a string"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for SpanV2Status {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}

/// The "kind" of a span, as defined by OTEL.
///
/// See <https://opentelemetry.io/docs/concepts/signals/traces/#span-kind>.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum SpanV2Kind {
    Client = 1,
    Server = 2,
    Internal = 3,
    Producer = 4,
    Consumer = 5,
}

impl SpanV2Kind {
    /// Returns the string representation of the kind.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Server => "server",
            Self::Internal => "internal",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }
}

impl AsRef<str> for SpanV2Kind {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SpanV2Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error parsing a `SpanV2Status`.
#[derive(Clone, Copy, Debug)]
pub struct ParseSpanV2KindError;

impl fmt::Display for ParseSpanV2KindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid span kind")
    }
}

impl std::error::Error for ParseSpanV2KindError {}

impl FromStr for SpanV2Kind {
    type Err = ParseSpanV2KindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "client" => Ok(Self::Client),
            "server" => Ok(Self::Server),
            "internal" => Ok(Self::Internal),
            "producer" => Ok(Self::Producer),
            "consumer" => Ok(Self::Consumer),
            _ => Err(ParseSpanV2KindError),
        }
    }
}

impl Empty for SpanV2Kind {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for SpanV2Kind {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(status) => Annotated(Some(status), meta),
                Err(_) => {
                    meta.add_error(Error::expected("a span kind"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::I64(value)), mut meta) => Annotated(
                Some(match value {
                    1 => Self::Client,
                    2 => Self::Server,
                    3 => Self::Internal,
                    4 => Self::Producer,
                    5 => Self::Consumer,
                    _ => {
                        meta.add_error(Error::expected("a span kind"));
                        meta.set_original_value(Some(value));
                        return Annotated(None, meta);
                    }
                }),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a string"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for SpanV2Kind {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}
