use relay_protocol::{
    Annotated, Array, Empty, Error, FromValue, HexId, IntoValue, Object, SkipSerialization, Val,
    Value,
};
use serde::Serializer;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use uuid::Uuid;

use crate::processor::ProcessValue;
use crate::protocol::{OperationType, OriginType, SpanData, SpanLink, SpanStatus};

/// Represents a W3C Trace Context `trace-id`.
///
/// The `trace-id` is a globally unique identifier for a distributed trace,
/// used to correlate requests across service boundaries.
///
/// Format:
/// - 16-byte array (128 bits), represented as 32-character hexadecimal string
/// - Example: `"4bf92f3577b34da6a3ce929d0e0e4736"`
/// - MUST NOT be all zeros (`"00000000000000000000000000000000"`)
/// - MUST contain only hex digits (`0-9`, `a-f`, `A-F`)
///
/// Our implementation allows uppercase hexadecimal characters for backward compatibility, even
/// though the original spec only allows lowercase hexadecimal characters.
///
/// See: <https://www.w3.org/TR/trace-context/#trace-id>
#[derive(Clone, Copy, PartialEq, Empty, ProcessValue)]
pub struct TraceId(Uuid);

impl TraceId {
    /// Creates a new, random, trace id.
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

relay_common::impl_str_serde!(TraceId, "a trace identifier");

impl FromStr for TraceId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s)
            .map(Into::into)
            .map_err(|_| Error::invalid("the trace id is not valid"))
    }
}

impl TryFrom<&[u8]> for TraceId {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let uuid =
            Uuid::from_slice(value).map_err(|_| Error::invalid("the trace id is not valid"))?;
        Ok(Self(uuid))
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_simple())
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId(\"{}\")", self.0.as_simple())
    }
}

impl From<Uuid> for TraceId {
    fn from(uuid: Uuid) -> Self {
        TraceId(uuid)
    }
}

impl Deref for TraceId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromValue for TraceId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(trace_id) => Annotated(Some(trace_id), meta),
                Err(_) => {
                    meta.add_error(Error::invalid("not a valid trace id"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("trace id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for TraceId {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        s.collect_str(self)
    }
}

/// A 16-character hex string as described in the W3C trace context spec, stored
/// internally as an array of 8 bytes.
#[derive(Clone, Copy, Default, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct SpanId([u8; 8]);

relay_common::impl_str_serde!(SpanId, "a span identifier");

impl FromStr for SpanId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match u64::from_str_radix(s, 16) {
            Ok(id) if s.len() == 16 && id > 0 => Ok(Self(id.to_be_bytes())),
            _ => Err(Error::invalid("not a valid span id")),
        }
    }
}

impl TryFrom<&[u8]> for SpanId {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match <[u8; 8]>::try_from(value) {
            Ok(bytes) if !bytes.iter().all(|&x| x == 0) => Ok(Self(bytes)),
            _ => Err(Error::invalid("not a valid span id")),
        }
    }
}

impl fmt::Debug for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpanId(\"")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        write!(f, "\")")
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl FromValue for SpanId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(span_id) => Annotated::new(span_id),
                Err(e) => {
                    meta.add_error(e);
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("span id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl Empty for SpanId {
    fn is_empty(&self) -> bool {
        false
    }
}

impl IntoValue for SpanId {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        s.collect_str(self)
    }
}

impl ProcessValue for SpanId {}

impl std::ops::Deref for SpanId {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> From<&'a SpanId> for Val<'a> {
    fn from(value: &'a SpanId) -> Self {
        Val::HexId(HexId(&value.0))
    }
}

/// Trace context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_trace_context")]
pub struct TraceContext {
    /// The trace ID.
    #[metastructure(required = true)]
    pub trace_id: Annotated<TraceId>,

    /// The ID of the span.
    #[metastructure(required = true)]
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// Span type (see `OperationType` docs).
    #[metastructure(max_chars = 128)]
    pub op: Annotated<OperationType>,

    /// Whether the trace failed or succeeded. Currently only used to indicate status of individual
    /// transactions.
    pub status: Annotated<SpanStatus>,

    /// The amount of time in milliseconds spent in this transaction span,
    /// excluding its immediate child spans.
    pub exclusive_time: Annotated<f64>,

    /// The client-side sample rate as reported in the envelope's `trace.sample_rate` header.
    ///
    /// The server takes this field from envelope headers and writes it back into the event. Clients
    /// should not ever send this value.
    pub client_sample_rate: Annotated<f64>,

    /// The origin of the trace indicates what created the trace (see [OriginType] docs).
    #[metastructure(max_chars = 128, allow_chars = "a-zA-Z0-9_.")]
    pub origin: Annotated<OriginType>,

    /// Track whether the trace connected to this event has been sampled entirely.
    ///
    /// This flag only applies to events with [`Error`] type that have an associated dynamic sampling context.
    pub sampled: Annotated<bool>,

    /// Data of the trace's root span.
    #[metastructure(pii = "maybe", skip_serialization = "null")]
    pub data: Annotated<SpanData>,

    /// Links to other spans from the trace's root span.
    #[metastructure(pii = "maybe", skip_serialization = "null")]
    pub links: Annotated<Array<SpanLink>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for TraceContext {
    fn default_key() -> &'static str {
        "trace"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Trace(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Trace(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Trace(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Trace(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Context, Route};

    #[test]
    fn test_trace_id_as_u128() {
        // Test valid hex string
        let trace_id: TraceId = "4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap();
        assert_eq!(trace_id.as_u128(), 0x4c79f60c11214eb38604f4ae0781bfb2);

        // Test empty string (should return 0)
        let empty_trace_id: Result<TraceId, Error> = "".parse();
        assert!(empty_trace_id.is_err());

        // Test string with invalid length (should return 0)
        let short_trace_id: Result<TraceId, Error> = "4c79f60c11214eb38604f4ae0781bfb".parse(); // 31 chars
        assert!(short_trace_id.is_err());

        let long_trace_id: Result<TraceId, Error> = "4c79f60c11214eb38604f4ae0781bfb2a".parse(); // 33 chars
        assert!(long_trace_id.is_err());

        // Test string with invalid hex characters (should return 0)
        let invalid_trace_id: Result<TraceId, Error> = "4c79f60c11214eb38604f4ae0781bfbg".parse(); // 'g' is not a hex char
        assert!(invalid_trace_id.is_err());
    }

    #[test]
    fn test_trace_context_roundtrip() {
        let json = r#"{
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "span_id": "fa90fdead5f74052",
  "parent_span_id": "fa90fdead5f74053",
  "op": "http",
  "status": "ok",
  "exclusive_time": 0.0,
  "client_sample_rate": 0.5,
  "origin": "auto.http",
  "data": {
    "route": {
      "name": "/users",
      "params": {
        "tok": "test"
      },
      "custom_field": "something"
    },
    "custom_field_empty": ""
  },
  "links": [
    {
      "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
      "span_id": "ea90fdead5f74052",
      "sampled": true,
      "attributes": {
        "sentry.link.type": "previous_trace"
      }
    }
  ],
  "other": "value",
  "type": "trace"
}"#;
        let context = Annotated::new(Context::Trace(Box::new(TraceContext {
            trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
            span_id: Annotated::new("fa90fdead5f74052".parse().unwrap()),
            parent_span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
            op: Annotated::new("http".into()),
            status: Annotated::new(SpanStatus::Ok),
            exclusive_time: Annotated::new(0.0),
            client_sample_rate: Annotated::new(0.5),
            origin: Annotated::new("auto.http".to_owned()),
            data: Annotated::new(SpanData {
                route: Annotated::new(Route {
                    name: Annotated::new("/users".into()),
                    params: Annotated::new({
                        let mut map = Object::new();
                        map.insert(
                            "tok".to_owned(),
                            Annotated::new(Value::String("test".into())),
                        );
                        map
                    }),
                    other: Object::from([(
                        "custom_field".into(),
                        Annotated::new(Value::String("something".into())),
                    )]),
                }),
                other: Object::from([(
                    "custom_field_empty".into(),
                    Annotated::new(Value::String("".into())),
                )]),
                ..Default::default()
            }),
            links: Annotated::new(Array::from(vec![Annotated::new(SpanLink {
                trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                span_id: Annotated::new("ea90fdead5f74052".parse().unwrap()),
                sampled: Annotated::new(true),
                attributes: Annotated::new({
                    let mut map: std::collections::BTreeMap<String, Annotated<Value>> =
                        Object::new();
                    map.insert(
                        "sentry.link.type".into(),
                        Annotated::new(Value::String("previous_trace".into())),
                    );
                    map
                }),
                ..Default::default()
            })])),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_owned(),
                    Annotated::new(Value::String("value".to_owned())),
                );
                map
            },
            sampled: Annotated::empty(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }

    #[test]
    fn test_trace_context_normalization() {
        let json = r#"{
  "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
  "span_id": "FA90FDEAD5F74052",
  "type": "trace"
}"#;
        let context = Annotated::new(Context::Trace(Box::new(TraceContext {
            trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
            span_id: Annotated::new("fa90fdead5f74052".parse().unwrap()),
            ..Default::default()
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_trace_id_formatting() {
        let test_cases = [
            // Test case 1: Formatting with hyphens in input
            (
                r#"{
  "trace_id": "b1e2a9dc9b8e4cd0af0e80e6b83b56e6",
  "type": "trace"
}"#,
                "b1e2a9dc-9b8e-4cd0-af0e-80e6b83b56e6",
                true,
            ),
            // Test case 2: Parsing with hyphens in JSON
            (
                r#"{
  "trace_id": "b1e2a9dc-9b8e-4cd0-af0e-80e6b83b56e6",
  "type": "trace"
}"#,
                "b1e2a9dc9b8e4cd0af0e80e6b83b56e6",
                false,
            ),
            // Test case 3: Uppercase in input
            (
                r#"{
  "trace_id": "b1e2a9dc9b8e4cd0af0e80e6b83b56e6",
  "type": "trace"
}"#,
                "B1E2A9DC9B8E4CD0AF0E80E6B83B56E6",
                true,
            ),
            // Test case 4: Uppercase in JSON
            (
                r#"{
  "trace_id": "B1E2A9DC9B8E4CD0AF0E80E6B83B56E6",
  "type": "trace"
}"#,
                "b1e2a9dc9b8e4cd0af0e80e6b83b56e6",
                false,
            ),
        ];

        for (json, trace_id_str, is_to_json) in test_cases {
            let context = Annotated::new(Context::Trace(Box::new(TraceContext {
                trace_id: Annotated::new(trace_id_str.parse().unwrap()),
                ..Default::default()
            })));

            if is_to_json {
                assert_eq!(json, context.to_json_pretty().unwrap());
            } else {
                assert_eq!(context, Annotated::from_json(json).unwrap());
            }
        }
    }

    #[test]
    fn test_trace_context_with_routes() {
        let json = r#"{
  "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
  "span_id": "FA90FDEAD5F74052",
  "type": "trace",
  "data": {
    "route": "HomeRoute"
  }
}"#;
        let context = Annotated::new(Context::Trace(Box::new(TraceContext {
            trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
            span_id: Annotated::new("fa90fdead5f74052".parse().unwrap()),
            data: Annotated::new(SpanData {
                route: Annotated::new(Route {
                    name: Annotated::new("HomeRoute".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
    }
}
