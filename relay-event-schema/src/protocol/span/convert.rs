//! TODO: docs
use crate::protocol::{Event, Span, TraceContext};

macro_rules! map_fields {
    (
        common:
            $($field:ident), *
        ;
        mapped:
            $(span.$span_field:ident <=> event.$event_field:ident), *
            trace:
                $(span.$span_field_from_trace_context:ident <=> event.contexts.trace.$trace_context_field:ident), *

        ;
        fixed_for_span:
            $(span.$fixed_span_field:ident <= $fixed_span_value:expr), *
        ;
        fixed_for_event:
            $($fixed_event_value:expr => span.$fixed_event_field:ident), *
    ) => {
        impl From<&Event> for Span {
            fn from(event: &Event) -> Self {
                let trace_context = event.context::<TraceContext>();
                Self {
                    $(
                        $field: event.$field.clone(),
                    )*
                    $(
                        $span_field: event.$event_field.clone(),
                    )*
                    $(
                        $span_field_from_trace_context: trace_context.map_or(None, |ctx|ctx.$trace_context_field.value().cloned()).into(),
                    )*
                    $(
                        $fixed_span_field: $fixed_span_value.into(),
                    )*
                    ..Default::default()
                }
            }
        }

        impl From<&Span> for Event {
            fn from(span: &Span) -> Self {
                Self {
                    $(
                        $field: span.$field.clone(),
                    )*
                    $(
                        $event_field: span.$span_field.clone(),
                    )*
                    $(
                        $fixed_event_field: $fixed_event_value.into(),
                    )*
                    // contexts: Annotated::new(
                    //     Contexts(

                    //     )
                    // )
                    ..Default::default()
                }
            }
        }
    };
}

map_fields!(
    common:
        _metrics_summary,
        measurements,
        platform,
        received,
        start_timestamp,
        timestamp
    ;
    mapped:
        span.description <=> event.transaction
        trace:
            span.exclusive_time <=> event.contexts.trace.exclusive_time,
            span.op <=> event.contexts.trace.op,
            span.parent_span_id <=> event.contexts.trace.parent_span_id,
            span.segment_id <=> event.contexts.trace.span_id,
            span.span_id <=> event.contexts.trace.span_id,
            span.status <=> event.contexts.trace.status,
            span.trace_id <=> event.contexts.trace.trace_id
    ;
    fixed_for_span:
        span.is_segment <= Some(true),
        span.was_transaction <= true
    ;
    fixed_for_event:
        // nothing yet
);
