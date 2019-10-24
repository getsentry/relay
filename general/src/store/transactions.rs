use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{Context, ContextInner, Contexts, Event, EventType, Span};
use crate::types::{Annotated, Meta, ProcessingAction, ProcessingResult};

pub struct TransactionsProcessor;

impl Processor for TransactionsProcessor {
    fn process_event(
        &mut self,
        event: &mut Event,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if event.ty.value() != Some(&EventType::Transaction) {
            return Ok(());
        }

        if event.timestamp.value().is_none() {
            // This invariant should be already guaranteed for regular error events.
            return Err(ProcessingAction::InvalidEvent(
                "timestamp hard-required for transaction events",
            ));
        }

        // XXX: Maybe copy timestamp over?
        if event.start_timestamp.value().is_none() {
            return Err(ProcessingAction::InvalidEvent(
                "start_timestamp hard-required for transaction events",
            ));
        }

        if let Some(Contexts(ref contexts)) = event.contexts.value() {
            match contexts.get("trace").and_then(Annotated::value) {
                Some(ContextInner(Context::Trace(ref trace_context))) => {
                    if trace_context.trace_id.value().is_none() {
                        return Err(ProcessingAction::InvalidEvent(
                            "trace context is missing trace_id",
                        ));
                    }

                    if trace_context.span_id.value().is_none() {
                        return Err(ProcessingAction::InvalidEvent(
                            "trace context is missing span_id",
                        ));
                    }

                    if trace_context.op.value().is_none() {
                        return Err(ProcessingAction::InvalidEvent(
                            "trace context is missing op",
                        ));
                    }
                }
                Some(_) => {
                    return Err(ProcessingAction::InvalidEvent(
                        "context at event.contexts.trace must be of type trace.",
                    ));
                }
                None => {
                    return Err(ProcessingAction::InvalidEvent(
                        "trace context hard-required for transaction events",
                    ));
                }
            }
        } else {
            return Err(ProcessingAction::InvalidEvent(
                "trace context hard-required for transaction events",
            ));
        }

        if let Some(spans) = event.spans.value() {
            for span in spans {
                if span.value().is_none() {
                    return Err(ProcessingAction::InvalidEvent(
                        "spans must be valid in transaction event",
                    ));
                }
            }
        }

        event.process_child_values(self, state)?;

        Ok(())
    }

    fn process_span(
        &mut self,
        span: &mut Span,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // XXX: Maybe do the same as event.timestamp?
        if span.timestamp.value().is_none() {
            return Err(ProcessingAction::InvalidEvent("span is missing timestamp"));
        }

        // XXX: Maybe copy timestamp over?
        if span.start_timestamp.value().is_none() {
            return Err(ProcessingAction::InvalidEvent(
                "span is missing start_timestamp",
            ));
        }

        if span.trace_id.value().is_none() {
            return Err(ProcessingAction::InvalidEvent("span is missing trace_id"));
        }

        if span.span_id.value().is_none() {
            return Err(ProcessingAction::InvalidEvent("span is missing span_id"));
        }

        if span.op.value().is_none() {
            return Err(ProcessingAction::InvalidEvent("span is missing op"));
        }

        span.process_child_values(self, state)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::process_value;
    use crate::protocol::{SpanId, TraceContext, TraceId};
    use crate::types::Object;
    use chrono::offset::TimeZone;
    use chrono::Utc;

    #[test]
    fn test_skips_non_transaction_events() {
        let mut event = Annotated::new(Event::default());
        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();
        assert!(event.value().is_some());
    }

    #[test]
    fn test_discards_when_missing_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "timestamp hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_when_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "start_timestamp hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_contexts_map() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "trace context hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts(Object::new())),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "trace context hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_on_null_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert("trace".to_owned(), Annotated::empty());
                contexts
            })),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "trace context hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_trace_id_in_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "trace context is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_span_id_in_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "trace context is missing span_id"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_op_in_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "trace context is missing op"
            ))
        );
    }

    #[test]
    fn test_allows_transaction_event_without_span_list() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();
        assert!(event.value().is_some());
    }

    #[test]
    fn test_allows_transaction_event_with_empty_span_list() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![]),
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();
        assert!(event.value().is_some());
    }

    #[test]
    fn test_discards_transaction_event_with_nulled_out_span() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![Annotated::empty()]),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "spans must be valid in transaction event"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![Annotated::new(Span {
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent("span is missing timestamp"))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent(
                "span is missing start_timestamp"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_trace_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent("span is missing trace_id"))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_span_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent("span is missing span_id"))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_op() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),

                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidEvent("span is missing op"))
        );
    }

    #[test]
    fn test_allows_valid_transaction_event_with_spans() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0)),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                op: Annotated::new("db.statement".to_owned()),
                ..Default::default()
            })]),
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert!(event.value().is_some());
    }
}
