//! This module defines bidirectional field mappings between spans and transactions.
use crate::protocol::{
    ContextInner, Contexts, DefaultContext, Event, ProfileContext, Span, TraceContext,
};

use relay_base_schema::events::EventType;
use relay_protocol::Annotated;
use std::collections::BTreeMap;

/// Implements the conversion between transaction events and segment spans.
///
/// Invoking this macro implements both `From<&Event> for Span` and `From<&Span> for Event`.
///
/// Example:
///
/// ```ignore
/// map_fields!(
///     top_level {
///         span.received <=> event.received
///     }
///     contexts {
///         TraceContext {
///             span.trace_id <=> context.trace_id
///         }
///     }
///     fixed_for_span {
///         // ...
///     }
///     fixed_for_event {
///         // ...
///     }
/// );
/// ```
#[macro_export]
macro_rules! map_fields {
    (
        top_level {
            $(span.$span_field:ident <=> event.$event_field:ident), *
        }
        contexts {
        $(
            $ContextType:ident {
                $(span.$primary_span_field:ident $(, $(span.$additional_span_field:ident),+)? <=> context.$context_field:ident), *
            }
        )*
        }
        fixed_for_span {
            $(span.$fixed_span_field:ident <= $fixed_span_value:expr), *
        }
        fixed_for_event {
            $($fixed_event_value:expr => event.$fixed_event_field:ident), *
        }
    ) => {
        #[allow(clippy::needless_update)]
        impl From<&Event> for Span {
            fn from(event: &Event) -> Self {
                Self {
                    $(
                        $span_field: event.$event_field.clone().map_value(Into::into),
                    )*
                    $(
                        $(
                            $primary_span_field: event.context::<$ContextType>()
                                .map_or(None, |ctx|ctx.$context_field.value().cloned()).into(),
                            $(
                                $(
                                    $additional_span_field: event.context::<$ContextType>()
                                    .map_or(None, |ctx|ctx.$context_field.value().cloned()).into(),
                                )+
                            )?
                        )*
                    )*
                    $(
                        $fixed_span_field: $fixed_span_value.into(),
                    )*
                    ..Default::default()
                }
            }
        }

        #[allow(clippy::needless_update)]
        impl TryFrom<&Span> for Event {
            type Error = ();

            fn try_from(span: &Span) -> Result<Self, ()> {
                use relay_protocol::Empty;

                if !span.is_segment.value().unwrap_or(&false) {
                    // Only segment spans can become transactions.
                    return Err(());
                }
                let event = Self {
                    $(
                        $event_field: span.$span_field.clone().map_value(Into::into),
                    )*
                    $(
                        $fixed_event_field: $fixed_event_value.into(),
                    )*
                    contexts: Annotated::new(
                        Contexts({
                            let mut contexts = BTreeMap::new();
                            $(
                                let mut context = $ContextType::default();
                                let mut has_fields = false;
                                $(
                                    if !span.$primary_span_field.is_empty() {
                                        context.$context_field = span.$primary_span_field.clone();
                                        has_fields = true;
                                    }
                                )*
                                if has_fields {
                                    let context_key = <$ContextType as DefaultContext>::default_key().into();
                                    contexts.insert(context_key, ContextInner(context.into_context()).into());
                                }
                            )*
                            contexts
                        })
                    ),
                    ..Default::default()
                };


                Ok(event)
            }
        }
    };
}

// This macro call implements a bidirectional mapping between transaction event and segment spans,
// allowing users to call both `Event::from(&span)` and `Span::from(&event)`.
map_fields!(
    top_level {
        span._metrics_summary <=> event._metrics_summary,
        span.description <=> event.transaction,
        span.measurements <=> event.measurements,
        span.platform <=> event.platform,
        span.received <=> event.received,
        span.start_timestamp <=> event.start_timestamp,
        span.tags <=> event.tags,
        span.timestamp <=> event.timestamp
    }
    contexts {
        TraceContext {
            span.exclusive_time <=> context.exclusive_time,
            span.op <=> context.op,
            span.parent_span_id <=> context.parent_span_id,
            // A transaction corresponds to a segment span, so span_id and segment_id have the same value:
            span.span_id, span.segment_id <=> context.span_id,
            span.status <=> context.status,
            span.trace_id <=> context.trace_id
        }
        ProfileContext {
            span.profile_id <=> context.profile_id
        }
    }
    fixed_for_span {
        // A transaction event corresponds to a segment span.
        span.is_segment <= true,
        span.was_transaction <= true
    }
    fixed_for_event {
        EventType::Transaction => event.ty
    }
);

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use super::*;

    #[test]
    fn roundtrip() {
        let event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "contexts": {
                    "profile": {"profile_id": "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaab"},
                    "trace": {
                        "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                        "span_id": "FA90FDEAD5F74052",
                        "type": "trace",
                        "op": "myop",
                        "status": "ok",
                        "exclusive_time": 123.4,
                        "parent_span_id": "FA90FDEAD5F74051"
                    }
                },
                "_metrics_summary": {
                    "some_metric": [
                        {
                            "min": 1.0,
                            "max": 2.0,
                            "sum": 3.0,
                            "count": 2,
                            "tags": {
                                "environment": "test"
                            }
                        }
                    ]
                },
                "measurements": {
                    "memory": {
                        "value": 9001.0,
                        "unit": "byte"
                    }
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let span_from_event = Span::from(&event);
        insta::assert_debug_snapshot!(span_from_event, @r###"
        Span {
            timestamp: ~,
            start_timestamp: ~,
            exclusive_time: 123.4,
            description: ~,
            op: "myop",
            span_id: SpanId(
                "fa90fdead5f74052",
            ),
            parent_span_id: SpanId(
                "fa90fdead5f74051",
            ),
            trace_id: TraceId(
                "4c79f60c11214eb38604f4ae0781bfb2",
            ),
            segment_id: SpanId(
                "fa90fdead5f74052",
            ),
            is_segment: true,
            status: Ok,
            tags: ~,
            origin: ~,
            profile_id: EventId(
                a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab,
            ),
            data: ~,
            sentry_tags: ~,
            received: ~,
            measurements: Measurements(
                {
                    "memory": Measurement {
                        value: 9001.0,
                        unit: Information(
                            Byte,
                        ),
                    },
                },
            ),
            _metrics_summary: MetricsSummary(
                {
                    "some_metric": [
                        MetricSummary {
                            min: 1.0,
                            max: 2.0,
                            sum: 3.0,
                            count: 2,
                            tags: {
                                "environment": "test",
                            },
                        },
                    ],
                },
            ),
            platform: ~,
            was_transaction: true,
            other: {},
        }
        "###);

        let roundtripped = Event::try_from(&span_from_event).unwrap();
        assert_eq!(event, roundtripped);
    }

    #[test]
    fn no_empty_profile_context() {
        let span = Span {
            is_segment: true.into(),
            ..Default::default()
        };
        let event = Event::try_from(&span).unwrap();

        // No profile context is set.
        // profile_id is required on ProfileContext so we should not create an empty one.
        assert!(event.context::<ProfileContext>().is_none());
    }
}
