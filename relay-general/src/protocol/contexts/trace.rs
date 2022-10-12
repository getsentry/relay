use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::types::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};

/// A 32-character hex string as described in the W3C trace context spec.
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct TraceId(pub String);

impl FromValue for TraceId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                static TRACE_ID: OnceCell<Regex> = OnceCell::new();
                let regex = TRACE_ID.get_or_init(|| Regex::new("^[a-fA-F0-9]{32}$").unwrap());

                if !regex.is_match(&value) || value.bytes().all(|x| x == b'0') {
                    meta.add_error(Error::invalid("not a valid trace id"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                } else {
                    Annotated(Some(TraceId(value.to_ascii_lowercase())), meta)
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("trace id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// A 16-character hex string as described in the W3C trace context spec.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Empty, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct SpanId(pub String);

impl FromValue for SpanId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                static SPAN_ID: OnceCell<Regex> = OnceCell::new();
                let regex = SPAN_ID.get_or_init(|| Regex::new("^[a-fA-F0-9]{16}$").unwrap());

                if !regex.is_match(&value) || value.bytes().all(|x| x == b'0') {
                    meta.add_error(Error::invalid("not a valid span id"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                } else {
                    Annotated(Some(SpanId(value.to_ascii_lowercase())), meta)
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("span id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// Trace context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_trace_context")]
pub struct TraceContext {
    /// The trace ID.
    #[metastructure(required = "true")]
    pub trace_id: Annotated<TraceId>,

    /// The ID of the span.
    #[metastructure(required = "true")]
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// Span type (see `OperationType` docs).
    #[metastructure(max_chars = "enumlike")]
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

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

#[doc(inline)]
pub use relay_common::{ParseSpanStatusError, SpanStatus};

impl ProcessValue for SpanStatus {}

impl Empty for SpanStatus {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for SpanStatus {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(status) => Annotated(Some(status), meta),
                Err(_) => {
                    meta.add_error(Error::expected("a trace status"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::I64(value)), mut meta) => Annotated(
                Some(match value {
                    0 => SpanStatus::Ok,
                    1 => SpanStatus::Cancelled,
                    2 => SpanStatus::Unknown,
                    3 => SpanStatus::InvalidArgument,
                    4 => SpanStatus::DeadlineExceeded,
                    5 => SpanStatus::NotFound,
                    6 => SpanStatus::AlreadyExists,
                    7 => SpanStatus::PermissionDenied,
                    8 => SpanStatus::ResourceExhausted,
                    9 => SpanStatus::FailedPrecondition,
                    10 => SpanStatus::Aborted,
                    11 => SpanStatus::OutOfRange,
                    12 => SpanStatus::Unimplemented,
                    13 => SpanStatus::InternalError,
                    14 => SpanStatus::Unavailable,
                    15 => SpanStatus::DataLoss,
                    16 => SpanStatus::Unauthenticated,
                    _ => {
                        meta.add_error(Error::expected("a trace status"));
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

impl IntoValue for SpanStatus {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&self.to_string(), s)
    }
}

impl TraceContext {
    /// The key under which a trace context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "trace"
    }
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
  "other": "value",
  "type": "trace"
}"#;
    let context = Annotated::new(Context::Trace(Box::new(TraceContext {
        trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
        span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
        parent_span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
        op: Annotated::new("http".into()),
        status: Annotated::new(SpanStatus::Ok),
        exclusive_time: Annotated::new(0.0),
        client_sample_rate: Annotated::new(0.5),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
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
        trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
        span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
        ..Default::default()
    })));

    assert_eq!(context, Annotated::from_json(json).unwrap());
}
