//! TODO: docs
use crate::protocol::{Event, Span};

macro_rules! map_fields {
    (
        common:
            $($field:ident), *
        ;
        mapped:
            $(span.$span_field:ident <=> event.$event_field:ident), *
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
    ;
    fixed_for_span:
        span.is_segment <= Some(true),
        span.was_transaction <= true
    ;
    fixed_for_event:
        // nothing yet
);
