#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, Error, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::{OperationType, OriginType, SpanStatus};

/// A 32-character hex string as described in the W3C trace context spec.
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct TraceId(pub String);

impl FromValue for TraceId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                if !is_hex_string(&value, 32) || value.bytes().all(|x| x == b'0') {
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

impl AsRef<str> for TraceId {
    fn as_ref(&self) -> &str {
        &self.0
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
                if !is_hex_string(&value, 16) || value.bytes().all(|x| x == b'0') {
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

impl AsRef<str> for SpanId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

fn is_hex_string(string: &str, len: usize) -> bool {
    string.len() == len && string.bytes().all(|b| b.is_ascii_hexdigit())
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

    /// The origin of the trace indicates what created the trace (see [OriginType] docs).
    #[metastructure(max_chars = "enumlike", allow_chars = "a-zA-Z0-9_.")]
    pub origin: Annotated<OriginType>,

    /// Track whether the trace connected to this event has been sampled entirely.
    ///
    /// This flag only applies to events with [`Error`] type that have an associated dynamic sampling context.
    pub sampled: Annotated<bool>,

    /// Arbitrary additional data on a trace.
    #[metastructure(pii = "maybe", skip_serialization = "empty")]
    pub data: Annotated<Data>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

/// The arbitrary data on the trace.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Data {
    /// The current route in the application.
    ///
    /// Set by React Native SDK.
    #[metastructure(pii = "maybe", skip_serialization = "empty")]
    pub route: Annotated<Route>,
    /// The previous route in the application
    ///
    /// Set by React Native SDK.
    #[metastructure(field = "previousRoute", pii = "maybe", skip_serialization = "empty")]
    pub previous_route: Annotated<Route>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(
        additional_properties,
        retain = "true",
        pii = "maybe",
        skip_serialization = "empty"
    )]
    pub other: Object<Value>,
}

/// The route in the application, set by React Native SDK.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Route {
    /// Parameters assigned to this route.
    #[metastructure(pii = "true", skip_serialization = "empty", bag_size = "medium")]
    params: Annotated<Object<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(
        additional_properties,
        retain = "true",
        pii = "maybe",
        skip_serialization = "empty"
    )]
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
    use crate::protocol::Context;

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
      "params": {
        "tok": "test"
      },
      "path": "/path"
    }
  },
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
            origin: Annotated::new("auto.http".to_owned()),
            data: Annotated::new(Data {
                route: Annotated::new(Route {
                    params: Annotated::new({
                        let mut map = Object::new();
                        map.insert(
                            "tok".to_string(),
                            Annotated::new(Value::String("test".into())),
                        );
                        map
                    }),
                    other: {
                        let mut map = Object::new();
                        map.insert(
                            "path".to_string(),
                            Annotated::new(Value::String("/path".into())),
                        );
                        map
                    },
                }),
                ..Default::default()
            }),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
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
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
            ..Default::default()
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
    }
}
