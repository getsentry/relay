//! This module defines bidirectional field mappings between spans and transactions.

use crate::protocol::{BrowserContext, Contexts, Event, ProfileContext, Span, TraceContext};

use relay_base_schema::events::EventType;
use relay_protocol::Annotated;

macro_rules! write_path(
    ($root:expr, $path_root:ident $($path_segment:ident)*) => {
        {
            let annotated = &mut ($root).$path_root;
            $(
                let value = annotated.get_or_insert_with(Default::default);
                let annotated = &mut value.$path_segment;
            )*
            annotated
        }
    };
);

macro_rules! read_value(
    ($span:expr, $path_root:ident $($path_segment:ident)*) => {
        {
            let value = ($span).$path_root.value();
            $(
                let value = value.and_then(|value| value.$path_segment.value());
            )*
            value
        }
    };
);

macro_rules! context_write_path (
    ($event:expr, $ContextType:ty, $context_field:ident) => {
        {
            let contexts = $event.contexts.get_or_insert_with(Contexts::default);
            let context = contexts.get_or_default::<$ContextType>();
            write_path!(context, $context_field)
        }
    };
);

macro_rules! event_write_path(
    ($event:expr, contexts browser $context_field:ident) => {
        context_write_path!($event, BrowserContext, $context_field)
    };
    ($event:expr, contexts trace $context_field:ident) => {
        context_write_path!($event, TraceContext, $context_field)
    };
    ($event:expr, contexts profile $context_field:ident) => {
        context_write_path!($event, ProfileContext, $context_field)
    };
    ($event:expr, $path_root:ident $($path_segment:ident)*) => {
        {
            write_path!($event, $path_root $($path_segment)*)
        }
    };
);

macro_rules! context_value (
    ($event:expr, $ContextType:ty, $context_field:ident) => {
        {
            let context = &($event).context::<$ContextType>();
            context.map_or(None, |ctx|ctx.$context_field.value())
        }
    };
);

macro_rules! event_value(
    ($event:expr, contexts browser $context_field:ident) => {
        context_value!($event, BrowserContext, $context_field)
    };
    ($event:expr, contexts trace $context_field:ident) => {
        context_value!($event, TraceContext, $context_field)
    };
    ($event:expr, contexts profile $context_field:ident) => {
        context_value!($event, ProfileContext, $context_field)
    };
    ($event:expr, $path_root:ident $($path_segment:ident)*) => {
        {
            let value = ($event).$path_root.value();
            $(
                let value = value.and_then(|value|value.$path_segment.value());
            )*
            value
        }
    };
);

#[derive(Debug, thiserror::Error)]
pub enum TryFromSpanError {
    #[error("span is not a segment")]
    NotASegment,
    #[error("span has no span ID")]
    MissingSpanId,
    #[error("failed to parse event ID")]
    InvalidSpanId(#[from] uuid::Error),
}

/// Implements the conversion between transaction events and segment spans.
///
/// Invoking this macro implements both `From<&Event> for Span` and `From<&Span> for Event`.
macro_rules! map_fields {
    (
        $(span $(. $span_path:ident)+ <=> event $(. $event_path:ident)+),+
        ;
        $(span . $fixed_span_path:tt <= $fixed_span_field:expr),+
        ;
        $($fixed_event_field:expr => event . $fixed_event_path:tt),+
    ) => {
        impl From<&Event> for Span {
            fn from(event: &Event) -> Self {
                let mut span = Span::default();
                $(
                    if let Some(value) = event_value!(event, $($event_path)+) {
                        *write_path!(&mut span, $($span_path)+) = Annotated::new(value.clone()).map_value(Into::into);
                    }
                )+
                $(
                    *write_path!(&mut span, $fixed_span_path) = Annotated::new($fixed_span_field);
                )+
                span
            }
        }

        impl<'a> TryFrom<&'a Span> for Event {
            type Error = TryFromSpanError;

            fn try_from(span: &Span) -> Result<Self, Self::Error> {
                let mut event = Event::default();
                let span_id = span.span_id.value().ok_or(TryFromSpanError::MissingSpanId)?;
                event.id = Annotated::new(span_id.try_into()?);

                if !span.is_segment.value().unwrap_or(&false) {
                    // Only segment spans can become transactions.
                    return Err(TryFromSpanError::NotASegment);
                }

                $(
                    if let Some(value) = read_value!(span, $($span_path)+) {
                        *event_write_path!(&mut event, $($event_path)+) = Annotated::new(value.clone()).map_value(Into::into)
                    }
                )+
                $(
                    *event_write_path!(&mut event, $fixed_event_path) = Annotated::new($fixed_event_field);
                )+

                Ok(event)
            }
        }
    };
}

// This macro call implements a bidirectional mapping between transaction event and segment spans,
// allowing users to call both `Event::from(&span)` and `Span::from(&event)`.
map_fields!(
    span._metrics_summary <=> event._metrics_summary,
    span.description <=> event.transaction,
    span.data.segment_name <=> event.transaction,
    span.measurements <=> event.measurements,
    span.platform <=> event.platform,
    span.received <=> event.received,
    span.start_timestamp <=> event.start_timestamp,
    span.tags <=> event.tags,
    span.timestamp <=> event.timestamp,
    span.exclusive_time <=> event.contexts.trace.exclusive_time,
    span.op <=> event.contexts.trace.op,
    span.parent_span_id <=> event.contexts.trace.parent_span_id,
    // A transaction corresponds to a segment span, so span_id and segment_id have the same value:
    span.span_id <=> event.contexts.trace.span_id,
    span.segment_id <=> event.contexts.trace.span_id,
    span.status <=> event.contexts.trace.status,
    span.trace_id <=> event.contexts.trace.trace_id,
    span.profile_id <=> event.contexts.profile.profile_id,
    span.data.release <=> event.release,
    span.data.environment <=> event.environment,
    span.data.browser_name <=> event.contexts.browser.name,
    span.data.sdk_name <=> event.client_sdk.name,
    span.data.sdk_version <=> event.client_sdk.version,
    span.origin <=> event.contexts.trace.origin
    ;
    span.is_segment <= true,
    span.was_transaction <= true
    ;
    EventType::Transaction => event.ty
);

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use crate::protocol::{SpanData, SpanId};

    use super::*;

    #[test]
    fn roundtrip() {
        let event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "platform": "php",
                "sdk": {"name": "sentry.php", "version": "1.2.3"},
                "release": "myapp@1.0.0",
                "environment": "prod",
                "transaction": "my 1st transaction",
                "contexts": {
                    "browser": {"name": "Chrome"},
                    "profile": {"profile_id": "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaab"},
                    "trace": {
                        "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                        "span_id": "FA90FDEAD5F74052",
                        "type": "trace",
                        "origin": "manual",
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
            description: "my 1st transaction",
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
            origin: "manual",
            profile_id: EventId(
                a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab,
            ),
            data: SpanData {
                app_start_type: ~,
                ai_total_tokens_used: ~,
                ai_prompt_tokens_used: ~,
                ai_completion_tokens_used: ~,
                browser_name: "Chrome",
                code_filepath: ~,
                code_lineno: ~,
                code_function: ~,
                code_namespace: ~,
                db_operation: ~,
                db_system: ~,
                environment: "prod",
                release: LenientString(
                    "myapp@1.0.0",
                ),
                http_decoded_response_content_length: ~,
                http_request_method: ~,
                http_response_content_length: ~,
                http_response_transfer_size: ~,
                resource_render_blocking_status: ~,
                server_address: ~,
                cache_hit: ~,
                cache_key: ~,
                cache_item_size: ~,
                http_response_status_code: ~,
                ai_pipeline_name: ~,
                ai_model_id: ~,
                ai_input_messages: ~,
                ai_responses: ~,
                thread_name: ~,
                segment_name: "my 1st transaction",
                ui_component_name: ~,
                url_scheme: ~,
                user: ~,
                replay_id: ~,
                sdk_name: "sentry.php",
                sdk_version: "1.2.3",
                frames_slow: ~,
                frames_frozen: ~,
                frames_total: ~,
                frames_delay: ~,
                messaging_destination_name: ~,
                messaging_message_retry_count: ~,
                messaging_message_receive_latency: ~,
                messaging_message_body_size: ~,
                messaging_message_id: ~,
                user_agent_original: ~,
                url_full: ~,
                client_address: ~,
                other: {},
            },
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
            platform: "php",
            was_transaction: true,
            other: {},
        }
        "###);

        let mut event_from_span = Event::try_from(&span_from_event).unwrap();

        let event_id = event_from_span.id.value_mut().take().unwrap();
        assert_eq!(&event_id.to_string(), "0000000000000000fa90fdead5f74052");

        assert_eq!(event, event_from_span);
    }

    #[test]
    fn segment_name_takes_precedence_over_description() {
        let span = Span {
            span_id: SpanId("fa90fdead5f74052".to_owned()).into(),
            is_segment: true.into(),
            description: "This is the description".to_owned().into(),
            data: SpanData {
                segment_name: "This is the segment name".to_owned().into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let event = Event::try_from(&span).unwrap();

        assert_eq!(event.transaction.as_str(), Some("This is the segment name"));
    }

    #[test]
    fn no_empty_profile_context() {
        let span = Span {
            span_id: SpanId("fa90fdead5f74052".to_owned()).into(),
            is_segment: true.into(),
            ..Default::default()
        };
        let event = Event::try_from(&span).unwrap();

        // No profile context is set.
        // profile_id is required on ProfileContext so we should not create an empty one.
        assert!(event.context::<ProfileContext>().is_none());
    }
}
