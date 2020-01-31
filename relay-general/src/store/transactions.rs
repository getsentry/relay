use chrono::{DateTime, Duration, Utc};

use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{Context, ContextInner, Contexts, Event, EventType, Span};
use crate::types::{Annotated, Meta, ProcessingAction, ProcessingResult, Timestamp};

pub struct TransactionsProcessor {
    /// Timestamp when the client thinks it sent the event. None means that we default to
    /// event.timestamp.
    sent_at: Option<DateTime<Utc>>,
    client_clock_drift: Option<Duration>,

    // This is an attribute so we can mock it in testing.
    now: DateTime<Utc>,
}

impl TransactionsProcessor {
    pub fn new(sent_at: Option<DateTime<Utc>>) -> Self {
        TransactionsProcessor {
            sent_at,
            client_clock_drift: None,
            now: Utc::now(),
        }
    }
}

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

        match (event.start_timestamp.value(), event.timestamp.value_mut()) {
            (Some(start), Some(end)) => {
                if *end < *start {
                    return Err(ProcessingAction::InvalidTransaction(
                        "end timestamp is smaller than start timestamp",
                    ));
                }

                let sent_at = self.sent_at.unwrap_or(*end);
                self.client_clock_drift = Some(sent_at.signed_duration_since(self.now));
            }
            (_, None) => {
                // This invariant should be already guaranteed for regular error events.
                return Err(ProcessingAction::InvalidTransaction(
                    "timestamp hard-required for transaction events",
                ));
            }
            (None, _) => {
                // XXX: Maybe copy timestamp over?
                return Err(ProcessingAction::InvalidTransaction(
                    "start_timestamp hard-required for transaction events",
                ));
            }
        }

        if let Some(Contexts(ref contexts)) = event.contexts.value() {
            match contexts.get("trace").and_then(Annotated::value) {
                Some(ContextInner(Context::Trace(ref trace_context))) => {
                    if trace_context.trace_id.value().is_none() {
                        return Err(ProcessingAction::InvalidTransaction(
                            "trace context is missing trace_id",
                        ));
                    }

                    if trace_context.span_id.value().is_none() {
                        return Err(ProcessingAction::InvalidTransaction(
                            "trace context is missing span_id",
                        ));
                    }

                    if trace_context.op.value().is_none() {
                        return Err(ProcessingAction::InvalidTransaction(
                            "trace context is missing op",
                        ));
                    }
                }
                Some(_) => {
                    return Err(ProcessingAction::InvalidTransaction(
                        "context at event.contexts.trace must be of type trace.",
                    ));
                }
                None => {
                    return Err(ProcessingAction::InvalidTransaction(
                        "trace context hard-required for transaction events",
                    ));
                }
            }
        } else {
            return Err(ProcessingAction::InvalidTransaction(
                "trace context hard-required for transaction events",
            ));
        }

        if let Some(spans) = event.spans.value() {
            for span in spans {
                if span.value().is_none() {
                    return Err(ProcessingAction::InvalidTransaction(
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

        if span.op.value().is_none() {
            return Err(ProcessingAction::InvalidTransaction("span is missing op"));
        }

        span.process_child_values(self, state)?;

        Ok(())
    }

    fn process_timestamp(
        &mut self,
        timestamp: &mut Timestamp,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        let drift = self.client_clock_drift.expect(
            "Expected client_clock_drift to be set in process_timestamp. \
             Processing order is messed up.",
        );
        *timestamp = timestamp.checked_sub_signed(drift).unwrap_or(*timestamp);

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
            &mut TransactionsProcessor::new(None),
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
            &mut TransactionsProcessor::new(None),
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
            &mut TransactionsProcessor::new(None),
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing timestamp"
            ))
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
                &mut TransactionsProcessor::new(None),
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing trace_id"
            ))
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing span_id"
            ))
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
                &mut TransactionsProcessor::new(None),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction("span is missing op"))
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
            &mut TransactionsProcessor::new(None),
            ProcessingState::root(),
        )
        .unwrap();

        assert!(event.value().is_some());
    }

    #[test]
    fn test_no_clock_drift() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);

        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(end),
            start_timestamp: Annotated::new(start),
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

        let mut processor = TransactionsProcessor::new(None);
        processor.now = end;

        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(*event.value().unwrap().timestamp.value().unwrap(), end);
        assert_eq!(
            *event.value().unwrap().start_timestamp.value().unwrap(),
            start
        );
    }

    #[test]
    fn test_some_clock_drift() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);
        let now = Utc.ymd(2000, 1, 3).and_hms(0, 0, 0);

        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(end),
            start_timestamp: Annotated::new(start),
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

        let mut processor = TransactionsProcessor::new(None);
        processor.now = now;

        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(*event.value().unwrap().timestamp.value().unwrap(), now);
        assert_eq!(
            *event.value().unwrap().start_timestamp.value().unwrap(),
            end
        ); // shift by 1 day == end
    }
}
