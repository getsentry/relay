#![allow(dead_code, unused_variables)]
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use relay_event_schema::protocol::{Span as EventSpan, SpanId, Timestamp, TraceId};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue)]
#[metastructure(process_func = "process_span", value_type = "Span")]
pub struct Span {
    #[metastructure(required = "true")]
    pub end_time: Annotated<Timestamp>,
    #[metastructure(required = "true")]
    pub start_time: Annotated<Timestamp>,
    #[metastructure(pii = "maybe")]
    pub name: Annotated<String>,
    #[metastructure(required = "true")]
    pub span_id: Annotated<SpanId>,
    #[metastructure(required = "true")]
    pub trace_id: Annotated<TraceId>,
    #[metastructure(pii = "maybe")]
    pub attributes: Annotated<Object<Value>>,
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl From<&Span> for EventSpan {
    fn from(from: &Span) -> Self {
        let mut span = EventSpan {
            description: from.name.clone(),
            span_id: from.span_id.clone(),
            trace_id: from.trace_id.clone(),
            ..Default::default()
        };
        let mut sentry_tags: Object<String> = Default::default();
        let mut remaining_attributes: Object<Value> = Default::default();

        if let Some(attributes) = from.attributes.clone().value_mut() {
            if let Some(environment) = attributes.remove::<str>("sentry.environment") {
                if let Some(env) = environment.value() {
                    sentry_tags.insert(
                        "environment".into(),
                        Annotated::new(env.as_str().unwrap().to_string()),
                    );
                }
            }

            for (key, value) in attributes {
                remaining_attributes.insert(key.into(), value.clone());
            }
        }

        span.data = Annotated::new(remaining_attributes);
        span.sentry_tags = Annotated::new(sentry_tags);

        span
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_span() {
        let json = r#"{
            "trace_id": "75E054B31042428F874AAB5188E3D666",
            "span_id": "deadbeefdeadbeef",
            "span_kind": "somekind",
            "name": "GET https://google.com/api",
            "start_time": 1697490227.0,
            "end_time": 1697490237.0,
            "attributes": {
                "op": "http.client",
                "sentry.environment": "test"
            }
        }"#;
        let otel_span: Annotated<Span> = Annotated::from_json(json).unwrap();
        let event_span: Annotated<EventSpan> = Annotated::new(otel_span.value().unwrap().into());
        assert_eq!(
            event_span
                .value()
                .unwrap()
                .sentry_tags
                .value()
                .unwrap()
                .get("environment")
                .unwrap(),
            &Annotated::new("test".to_string())
        );
    }
}
