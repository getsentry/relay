use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{Context, ContextInner, Event, EventType, Span, Timestamp};
use crate::types::{Annotated, Meta, ProcessingAction, ProcessingResult};
/// Rejects transactions based on required fields.
pub struct TransactionsProcessor;

/// Get the value for a measurement, e.g. lcp -> event.measurements.lcp
pub fn get_measurement(transaction: &Event, name: &str) -> Option<f64> {
    let measurements = transaction.measurements.value()?;
    let annotated = measurements.get(name)?;
    let value = annotated.value().and_then(|m| m.value.value())?;
    Some(*value)
}

pub fn get_transaction_op(transaction: &Event) -> Option<&str> {
    let context = transaction.contexts.value()?.get("trace")?.value()?;
    match **context {
        Context::Trace(ref trace_context) => Some(trace_context.op.value()?),
        _ => None,
    }
}

/// Returns start and end timestamps if they are both set and start <= end.
pub fn validate_timestamps(
    transaction_event: &Event,
) -> Result<(Timestamp, Timestamp), ProcessingAction> {
    match (
        transaction_event.start_timestamp.value(),
        transaction_event.timestamp.value(),
    ) {
        (Some(&start), Some(&end)) => {
            if end < start {
                return Err(ProcessingAction::InvalidTransaction(
                    "end timestamp is smaller than start timestamp",
                ));
            }
            Ok((start, end))
        }
        (_, None) => {
            // This invariant should be already guaranteed for regular error events.
            Err(ProcessingAction::InvalidTransaction(
                "timestamp hard-required for transaction events",
            ))
        }
        (None, _) => {
            // XXX: Maybe copy timestamp over?
            Err(ProcessingAction::InvalidTransaction(
                "start_timestamp hard-required for transaction events",
            ))
        }
    }
}

pub fn validate_annotated_transaction(event: &mut Annotated<Event>) -> ProcessingResult {
    event.apply(|event, _meta| validate_transaction(event))
}

pub fn validate_transaction(event: &mut Event) -> ProcessingResult {
    if event.ty.value() != Some(&EventType::Transaction) {
        return Ok(());
    }

    validate_timestamps(event)?;

    let err_trace_context_required = Err(ProcessingAction::InvalidTransaction(
        "trace context hard-required for transaction events",
    ));

    let contexts = match event.contexts.value_mut() {
        Some(contexts) => contexts,
        None => return err_trace_context_required,
    };

    let trace_context = match contexts.get_mut("trace").map(Annotated::value_mut) {
        Some(Some(trace_context)) => trace_context,
        _ => return err_trace_context_required,
    };

    match trace_context {
        ContextInner(Context::Trace(trace_context)) => {
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

            trace_context.op.get_or_insert_with(|| "default".to_owned());
            Ok(())
        }
        _ => Err(ProcessingAction::InvalidTransaction(
            "context at event.contexts.trace must be of type trace.",
        )),
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

        // The transaction name is expected to be non-empty by downstream services (e.g. Snuba), but
        // Relay doesn't reject events missing the transaction name. Instead, a default transaction
        // name is given, similar to how Sentry gives an "<unlabeled event>" title to error events.
        // SDKs should avoid sending empty transaction names, setting a more contextual default
        // value when possible.
        if event.transaction.value().map_or(true, |s| s.is_empty()) {
            event
                .transaction
                .set_value(Some("<unlabeled transaction>".to_owned()))
        }

        validate_transaction(event)?;

        let spans = event.spans.value_mut().get_or_insert_with(|| Vec::new());

        for span in spans {
            if span.value().is_none() {
                return Err(ProcessingAction::InvalidTransaction(
                    "spans must be valid in transaction event",
                ));
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

        span.op.get_or_insert_with(|| "default".to_owned());

        span.process_child_values(self, state)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::offset::TimeZone;
    use chrono::Utc;

    use crate::processor::process_value;
    use crate::protocol::{Contexts, SpanId, TraceContext, TraceId};
    use crate::testutils::{assert_annotated_snapshot, assert_eq_dbg};
    use crate::types::Object;

    fn new_test_event() -> Annotated<Event> {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 1).and_hms(0, 0, 10);
        Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            start_timestamp: Annotated::new(start.into()),
            timestamp: Annotated::new(end.into()),
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
                start_timestamp: Annotated::new(start.into()),
                timestamp: Annotated::new(end.into()),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                op: Annotated::new("db.statement".to_owned()),
                ..Default::default()
            })]),
            ..Default::default()
        })
    }

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
            Err(ProcessingAction::InvalidTransaction(
                "timestamp hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_when_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
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
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
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
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            contexts: Annotated::new(Contexts(Object::new())),
            ..Default::default()
        });

        assert_eq_dbg!(
            process_value(
                &mut event,
                &mut TransactionsProcessor,
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
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            Err(ProcessingAction::InvalidTransaction(
                "trace context hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_trace_id_in_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            Err(ProcessingAction::InvalidTransaction(
                "trace context is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_span_id_in_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            Err(ProcessingAction::InvalidTransaction(
                "trace context is missing span_id"
            ))
        );
    }

    #[test]
    fn test_defaults_missing_op_in_context() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 1).and_hms(0, 0, 10);

        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            timestamp: Annotated::new(end.into()),
            start_timestamp: Annotated::new(start.into()),
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

        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/",
          "timestamp": 946684810.0,
          "start_timestamp": 946684800.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74053",
              "op": "default",
              "type": "trace"
            }
          },
          "spans": []
        }
        "###);
    }

    #[test]
    fn test_allows_transaction_event_without_span_list() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
    fn test_allows_transaction_event_with_null_span_list() {
        let mut event = new_test_event();

        event
            .apply(|event, _| {
                event.spans.set_value(None);
                Ok(())
            })
            .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/",
          "timestamp": 946684810.0,
          "start_timestamp": 946684800.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74053",
              "op": "http.server",
              "type": "trace"
            }
          },
          "spans": []
        }
        "###);
    }

    #[test]
    fn test_discards_transaction_event_with_nulled_out_span() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            Err(ProcessingAction::InvalidTransaction(
                "spans must be valid in transaction event"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            Err(ProcessingAction::InvalidTransaction(
                "span is missing timestamp"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            Err(ProcessingAction::InvalidTransaction(
                "span is missing start_timestamp"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_trace_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
                start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            Err(ProcessingAction::InvalidTransaction(
                "span is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_span_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
            start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
                start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
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
            Err(ProcessingAction::InvalidTransaction(
                "span is missing span_id"
            ))
        );
    }

    #[test]
    fn test_defaults_transaction_event_with_span_with_missing_op() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 1).and_hms(0, 0, 10);

        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            timestamp: Annotated::new(end.into()),
            start_timestamp: Annotated::new(start.into()),
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
                timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 10).into()),
                start_timestamp: Annotated::new(Utc.ymd(2000, 1, 1).and_hms(0, 0, 0).into()),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),

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

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/",
          "timestamp": 946684810.0,
          "start_timestamp": 946684800.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74053",
              "op": "http.server",
              "type": "trace"
            }
          },
          "spans": [
            {
              "timestamp": 946684810.0,
              "start_timestamp": 946684800.0,
              "op": "default",
              "span_id": "fa90fdead5f74053",
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }
          ]
        }
        "###);
    }

    #[test]
    fn test_allows_valid_transaction_event_with_spans() {
        let mut event = new_test_event();

        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/",
          "timestamp": 946684810.0,
          "start_timestamp": 946684800.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74053",
              "op": "http.server",
              "type": "trace"
            }
          },
          "spans": [
            {
              "timestamp": 946684810.0,
              "start_timestamp": 946684800.0,
              "op": "db.statement",
              "span_id": "fa90fdead5f74053",
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }
          ]
        }
        "###);
    }

    #[test]
    fn test_defaults_transaction_name_when_missing() {
        let mut event = new_test_event();

        event
            .apply(|event, _| {
                event.transaction.set_value(None);
                Ok(())
            })
            .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "<unlabeled transaction>",
          "timestamp": 946684810.0,
          "start_timestamp": 946684800.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74053",
              "op": "http.server",
              "type": "trace"
            }
          },
          "spans": [
            {
              "timestamp": 946684810.0,
              "start_timestamp": 946684800.0,
              "op": "db.statement",
              "span_id": "fa90fdead5f74053",
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }
          ]
        }
        "###);
    }

    #[test]
    fn test_defaults_transaction_name_when_empty() {
        let mut event = new_test_event();

        event
            .apply(|event, _| {
                event.transaction.set_value(Some("".to_owned()));
                Ok(())
            })
            .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "<unlabeled transaction>",
          "timestamp": 946684810.0,
          "start_timestamp": 946684800.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74053",
              "op": "http.server",
              "type": "trace"
            }
          },
          "spans": [
            {
              "timestamp": 946684810.0,
              "start_timestamp": 946684800.0,
              "op": "db.statement",
              "span_id": "fa90fdead5f74053",
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }
          ]
        }
        "###);
    }
}
