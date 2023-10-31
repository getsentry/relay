//! Span validation.

use relay_event_schema::protocol::Span;

use relay_event_schema::processor::ProcessingAction;

/// Returns a [`ProcessingAction`] error if a span's timestamps are invalid.
///
/// For a span's timestamps to be valid, the following should happen:
/// - Start and end timestamps must exist.
/// - Start timestamp must be no greater than end timestamp.
pub(crate) fn validate_span_timestamps(span: &Span) -> Result<(), ProcessingAction> {
    match (span.start_timestamp.value(), span.timestamp.value()) {
        (Some(start), Some(end)) => {
            if end < start {
                return Err(ProcessingAction::InvalidTransaction(
                    "end timestamp in span is smaller than start timestamp",
                ));
            }
        }
        (_, None) => {
            // XXX: Maybe do the same as event.timestamp?
            return Err(ProcessingAction::InvalidTransaction(
                "span is missing timestamp",
            ));
        }
        (None, _) => {
            // XXX: Maybe copy timestamp over?
            return Err(ProcessingAction::InvalidTransaction(
                "span is missing start_timestamp",
            ));
        }
    }

    Ok(())
}

/// Returns a [`ProcessingAction`] error if a span's span id or trace id are missing.
pub(crate) fn validate_span_ids(span: &Span) -> Result<(), ProcessingAction> {
    if span.trace_id.value().is_none() {
        return Err(ProcessingAction::InvalidTransaction(
            "span is missing trace_id",
        ));
    }
    if span.span_id.value().is_none() {
        return Err(ProcessingAction::InvalidTransaction(
            "span is missing span_id",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::normalize::processor::NormalizeProcessor;
    use chrono::{TimeZone, Utc};
    use relay_event_schema::processor::{process_value, ProcessingAction, ProcessingState};
    use relay_event_schema::protocol::{
        Contexts, Event, EventType, Span, SpanId, TraceContext, TraceId,
    };
    use relay_protocol::{get_value, Annotated};
    use similar_asserts::assert_eq;

    #[test]
    fn test_defaults_transaction_event_with_span_with_missing_op() {
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();

        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            timestamp: Annotated::new(end.into()),
            start_timestamp: Annotated::new(start.into()),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),

                ..Default::default()
            })]),
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        let span = get_value!(event.spans).unwrap().first().unwrap();
        assert_eq!(get_value!(span.op).unwrap(), &"default".to_owned());
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_span_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing span_id"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing start_timestamp"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_trace_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing trace_id"
            ))
        );
    }
}
