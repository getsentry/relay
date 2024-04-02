//! TODO: docs
use crate::protocol::{Event, ProfileContext, Span, TraceContext};

macro_rules! map_fields {
    (
        common:
            $($field:ident), *
        ;
        mapped:
            $(span.$span_field:ident <=> event.$event_field:ident), *
            contexts:
            $(
                #$context_type:ty:
                    $(span.$span_field_from_context:ident <=> context.$context_field:ident), *
            )*

        ;
        fixed_for_span:
            $(span.$fixed_span_field:ident <= $fixed_span_value:expr), *
        ;
        fixed_for_event:
            $($fixed_event_value:expr => span.$fixed_event_field:ident), *
    ) => {
        impl From<&Event> for Span {
            fn from(event: &Event) -> Self {
                Self {
                    $(
                        $field: event.$field.clone(),
                    )*
                    $(
                        $span_field: event.$event_field.clone(),
                    )*
                    $(
                        $(
                            $span_field_from_context: event.context::<$context_type>()
                                .map_or(None, |ctx|ctx.$context_field.value().cloned()).into(),
                        )*
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
        contexts:
            #TraceContext:
                span.exclusive_time <=> context.exclusive_time,
                span.op <=> context.op,
                span.parent_span_id <=> context.parent_span_id,
                span.segment_id <=> context.span_id,
                span.span_id <=> context.span_id,
                span.status <=> context.status,
                span.trace_id <=> context.trace_id
            #ProfileContext:
                span.profile_id <=> context.profile_id
    ;
    fixed_for_span:
        span.is_segment <= Some(true),
        span.was_transaction <= true
    ;
    fixed_for_event:
        // nothing yet
);
