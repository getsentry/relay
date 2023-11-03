#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, Getter, IntoValue, Object, Val, Value};

use crate::processor::ProcessValue;
use crate::protocol::{
    Event, EventId, JsonLenientString, OperationType, OriginType, ProfileContext, SpanId,
    SpanStatus, Timestamp, TraceContext, TraceId,
};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_span", value_type = "Span")]
pub struct Span {
    /// Timestamp when the span was ended.
    #[metastructure(required = "true")]
    pub timestamp: Annotated<Timestamp>,

    /// Timestamp when the span started.
    #[metastructure(required = "true")]
    pub start_timestamp: Annotated<Timestamp>,

    /// The amount of time in milliseconds spent in this span,
    /// excluding its immediate child spans.
    pub exclusive_time: Annotated<f64>,

    /// Human readable description of a span (e.g. method URL).
    #[metastructure(pii = "maybe")]
    pub description: Annotated<String>,

    /// Span type (see `OperationType` docs).
    #[metastructure(max_chars = "enumlike")]
    pub op: Annotated<OperationType>,

    /// The Span id.
    #[metastructure(required = "true")]
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// The ID of the trace the span belongs to.
    #[metastructure(required = "true")]
    pub trace_id: Annotated<TraceId>,

    /// A unique identifier for a segment within a trace (8 byte hexadecimal string).
    ///
    /// For spans embedded in transactions, the `segment_id` is the `span_id` of the containing
    /// transaction.
    pub segment_id: Annotated<SpanId>,

    /// Whether or not the current span is the root of the segment.
    pub is_segment: Annotated<bool>,

    /// The status of a span.
    pub status: Annotated<SpanStatus>,

    /// Arbitrary tags on a span, like on the top-level event.
    #[metastructure(pii = "maybe")]
    pub tags: Annotated<Object<JsonLenientString>>,

    /// The origin of the span indicates what created the span (see [OriginType] docs).
    #[metastructure(max_chars = "enumlike", allow_chars = "a-zA-Z0-9_.")]
    pub origin: Annotated<OriginType>,

    /// ID of a profile that can be associated with the span.
    pub profile_id: Annotated<EventId>,

    /// Arbitrary additional data on a span, like `extra` on the top-level event.
    #[metastructure(pii = "true")]
    pub data: Annotated<Object<Value>>,

    /// Tags generated by Relay. These tags are a superset of the tags set on span metrics.
    pub sentry_tags: Annotated<Object<String>>,

    /// Timestamp when the event has been received by Sentry.
    pub received: Annotated<Timestamp>,

    // TODO remove retain when the api stabilizes
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl From<&Event> for Span {
    fn from(event: &Event) -> Self {
        let mut span = Self {
            description: event.transaction.clone(),
            is_segment: Some(true).into(),
            received: event.received.clone(),
            start_timestamp: event.start_timestamp.clone(),
            timestamp: event.timestamp.clone(),
            ..Default::default()
        };

        if let Some(trace_context) = event.context::<TraceContext>().cloned() {
            span.exclusive_time = trace_context.exclusive_time;
            span.op = trace_context.op;
            span.span_id = trace_context.span_id;
            span.parent_span_id = trace_context.parent_span_id;
            span.trace_id = trace_context.trace_id;
            span.segment_id = span.span_id.clone(); // a transaction is a segment
            span.status = trace_context.status;
        }

        if let Some(profile_context) = event.context::<ProfileContext>() {
            span.profile_id = profile_context.profile_id.clone();
        }

        span
    }
}

impl Getter for Span {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        Some(match path.strip_prefix("span.")? {
            "exclusive_time" => self.exclusive_time.value()?.into(),
            "description" => self.description.as_str()?.into(),
            "op" => self.op.as_str()?.into(),
            "span_id" => self.span_id.as_str()?.into(),
            "parent_span_id" => self.parent_span_id.as_str()?.into(),
            "trace_id" => self.trace_id.as_str()?.into(),
            "status" => self.status.as_str()?.into(),
            "origin" => self.origin.as_str()?.into(),
            path => {
                if let Some(key) = path.strip_prefix("tags.") {
                    self.tags.value()?.get(key)?.as_str()?.into()
                } else if let Some(key) = path.strip_prefix("data.") {
                    let escaped = key.replace("\\.", "\0");
                    let mut path = escaped.split('.').map(|s| s.replace('\0', "."));
                    let root = path.next()?;

                    let mut val = self.data.value()?.get(&root)?.value()?;
                    for part in path {
                        // While there is path segments left, `val` has to be an Object.
                        let relay_protocol::Value::Object(map) = val else {
                            return None;
                        };
                        val = map.get(&part)?.value()?;
                    }
                    val.into()
                } else if let Some(key) = path.strip_prefix("sentry_tags.") {
                    self.sentry_tags.value()?.get(key)?.as_str()?.into()
                } else {
                    return None;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use insta::assert_debug_snapshot;
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_span_serialization() {
        let json = r#"{
  "timestamp": 0.0,
  "start_timestamp": -63158400.0,
  "exclusive_time": 1.23,
  "description": "desc",
  "op": "operation",
  "span_id": "fa90fdead5f74052",
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "status": "ok",
  "origin": "auto.http"
}"#;

        let span = Annotated::new(Span {
            timestamp: Annotated::new(Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(1968, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            exclusive_time: Annotated::new(1.23),
            description: Annotated::new("desc".to_owned()),
            op: Annotated::new("operation".to_owned()),
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
            status: Annotated::new(SpanStatus::Ok),
            origin: Annotated::new("auto.http".to_owned()),
            ..Default::default()
        });
        assert_eq!(json, span.to_json_pretty().unwrap());

        let span_from_string = Annotated::from_json(json).unwrap();
        assert_eq!(span, span_from_string);
    }

    #[test]
    fn test_getter_span_data() {
        let span = Annotated::<Span>::from_json(
            r#"{
                "data": {
                    "foo": {"bar": 1},
                    "foo.bar": 2
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        assert_eq!(span.get_value("span.data.foo.bar"), Some(Val::I64(1)));
        assert_eq!(span.get_value(r"span.data.foo\.bar"), Some(Val::I64(2)));

        assert_eq!(span.get_value("span.data"), None);
        assert_eq!(span.get_value("span.data."), None);
        assert_eq!(span.get_value("span.data.x"), None);
    }

    #[test]
    fn span_from_event() {
        let event = Annotated::<Event>::from_json(
            r#"{
                "contexts": {
                    "profile": {"profile_id": "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaab"},
                    "trace": {
                        "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                        "span_id": "FA90FDEAD5F74052",
                        "type": "trace"
                    }
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        assert_debug_snapshot!(Span::from(&event), @r###"
        Span {
            timestamp: ~,
            start_timestamp: ~,
            exclusive_time: ~,
            description: ~,
            op: ~,
            span_id: SpanId(
                "fa90fdead5f74052",
            ),
            parent_span_id: ~,
            trace_id: TraceId(
                "4c79f60c11214eb38604f4ae0781bfb2",
            ),
            segment_id: SpanId(
                "fa90fdead5f74052",
            ),
            is_segment: true,
            status: ~,
            tags: ~,
            origin: ~,
            profile_id: EventId(
                a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab,
            ),
            data: ~,
            sentry_tags: ~,
            received: ~,
            other: {},
        }
        "###);
    }
}
