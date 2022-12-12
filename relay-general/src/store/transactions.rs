use relay_common::SpanStatus;

use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{
    Context, ContextInner, Event, EventType, Span, Timestamp, TransactionSource,
};
use crate::store::regexes::TRANSACTION_NAME_NORMALIZER_REGEX;
use crate::types::{Annotated, Meta, ProcessingAction, ProcessingResult, Remark, RemarkType};

/// Rejects transactions based on required fields.
#[derive(Default)]
pub struct TransactionsProcessor {
    normalize_names: bool,
}

impl TransactionsProcessor {
    pub fn new(normalize_names: bool) -> Self {
        Self { normalize_names }
    }
}

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

/// List of SDKs which we assume to produce high cardinality transaction names, such as
/// "/user/123134/login".
/// Newer SDK send the [`TransactionSource`] attribute, which we can rely on to determine cardinality,
/// but for old SDKs, we fall back to this list.
pub fn is_high_cardinality_sdk(event: &Event) -> bool {
    let client_sdk = match event.client_sdk.value() {
        Some(info) => info,
        None => {
            return false;
        }
    };

    let sdk_name = client_sdk
        .name
        .value()
        .map(|s| s.as_str())
        .unwrap_or_default();

    if [
        "sentry.javascript.angular",
        "sentry.javascript.browser",
        "sentry.javascript.ember",
        "sentry.javascript.gatsby",
        "sentry.javascript.react",
        "sentry.javascript.remix",
        "sentry.javascript.vue",
        "sentry.javascript.nextjs",
        "sentry.php.laravel",
        "sentry.php.symfony",
    ]
    .contains(&sdk_name)
    {
        return true;
    }

    let is_http_status_404 = event.get_tag_value("http.status_code") == Some("404");
    if sdk_name == "sentry.python" && is_http_status_404 && client_sdk.has_integration("django") {
        return true;
    }

    let http_method = event
        .request
        .value()
        .and_then(|r| r.method.as_str())
        .unwrap_or_default();
    if sdk_name == "sentry.javascript.node"
        && http_method.eq_ignore_ascii_case("options")
        && client_sdk.has_integration("Express")
    {
        return true;
    }

    if sdk_name == "sentry.ruby" && event.has_module("rack") {
        let trace = event
            .contexts
            .value()
            .and_then(|c| c.get("trace"))
            .and_then(Annotated::value);
        if let Some(ContextInner(Context::Trace(trace_context))) = trace {
            let status = trace_context.status.value().unwrap_or(&SpanStatus::Unknown);
            if [
                // See https://github.com/getsentry/sentry-ruby/blob/ad4828f6d8d60e98217b2edb1ab003fb627d6bdb/sentry-ruby/lib/sentry/span.rb#L7-L19
                SpanStatus::InvalidArgument,
                SpanStatus::Unauthenticated,
                SpanStatus::PermissionDenied,
                SpanStatus::NotFound,
                SpanStatus::AlreadyExists,
                SpanStatus::ResourceExhausted,
                SpanStatus::Cancelled,
                SpanStatus::InternalError,
                SpanStatus::Unimplemented,
                SpanStatus::Unavailable,
                SpanStatus::DeadlineExceeded,
            ]
            .contains(status)
            {
                return true;
            }
        }
    }

    false
}

/// Set a default transaction source if it is missing, but only if the transaction name was
/// extracted as a metrics tag.
/// This behavior makes it possible to identify transactions for which the transaction name was
/// not extracted as a tag on the corresponding metrics, because
///     source == null <=> transaction name == null
/// See `relay_server::metrics_extraction::transactions::get_transaction_name`.
fn set_default_transaction_source(event: &mut Event) {
    let source = event
        .transaction_info
        .value()
        .and_then(|info| info.source.value());

    if source.is_none() && !is_high_cardinality_sdk(event) {
        let transaction_info = event.transaction_info.get_or_insert_with(Default::default);
        transaction_info
            .source
            .set_value(Some(TransactionSource::Unknown));
    }
}

/// Normalize the transaction name.
///
/// Replaces UUIDs, SHAs and numerical IDs in transaction names by placeholders.
fn normalize_transaction_name(transaction: &mut Annotated<String>) -> ProcessingResult {
    let capture_names = TRANSACTION_NAME_NORMALIZER_REGEX
        .capture_names()
        .flatten()
        .collect::<Vec<_>>();

    transaction.apply(|trans, meta| {
        // Collect all the remarks if anything matches.
        for matches in TRANSACTION_NAME_NORMALIZER_REGEX.captures_iter(trans) {
            for name in &capture_names {
                if let Some(m) = matches.name(name) {
                    let remark =
                        Remark::with_range(RemarkType::Substituted, *name, (m.start(), m.end()));
                    meta.add_remark(remark);
                    break;
                }
            }
        }

        let changed = TRANSACTION_NAME_NORMALIZER_REGEX
            .replace_all(trans, "*")
            .to_string();
        if *trans != changed && changed != "*" {
            meta.set_original_value(Some(trans.to_string()));
            *trans = changed
        } else {
            meta.clear_remarks();
        }
        Ok(())
    })
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

        // Normalize transaction names for URLs transaction sources only.
        if event.get_transaction_source() == &TransactionSource::Url && self.normalize_names {
            normalize_transaction_name(&mut event.transaction)?;
        }

        validate_transaction(event)?;

        let spans = event.spans.value_mut().get_or_insert_with(|| Vec::new());

        for span in spans {
            if let Some(val) = span.value_mut() {
                if val.timestamp.value().is_none() {
                    val.timestamp.set_value(event.timestamp.value().cloned());
                    val.status = Annotated::new(SpanStatus::DeadlineExceeded);
                }
            } else {
                return Err(ProcessingAction::InvalidTransaction(
                    "spans must be valid in transaction event",
                ));
            }
        }

        set_default_transaction_source(event);

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
    use chrono::offset::TimeZone;
    use chrono::Utc;
    use similar_asserts::assert_eq;

    use crate::processor::process_value;
    use crate::protocol::{Contexts, SpanId, TraceContext, TraceId, TransactionSource};
    use crate::testutils::assert_annotated_snapshot;
    use crate::types::Object;

    use super::*;

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
            &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/",
          "transaction_info": {
            "source": "unknown"
          },
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
            &mut TransactionsProcessor::default(),
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
            &mut TransactionsProcessor::default(),
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
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/",
          "transaction_info": {
            "source": "unknown"
          },
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
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
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/",
          "transaction_info": {
            "source": "unknown"
          },
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
    fn test_default_transaction_source_unknown() {
        let mut event = Annotated::<Event>::from_json(
            r###"
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
                "sdk": {
                    "name": "sentry.dart.flutter"
                },
                "spans": []
            }
            "###,
        )
        .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        let source = event
            .value()
            .unwrap()
            .transaction_info
            .value()
            .and_then(|info| info.source.value())
            .unwrap();

        assert_eq!(source, &TransactionSource::Unknown);
    }

    #[test]
    fn test_default_transaction_source_none() {
        let mut event = Annotated::<Event>::from_json(
            r###"
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
                "sdk": {
                    "name": "sentry.javascript.browser"
                },
                "spans": []
            }
        "###,
        )
        .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        let transaction_info = &event.value().unwrap().transaction_info;

        assert!(transaction_info.value().is_none());
    }

    #[test]
    fn test_allows_valid_transaction_event_with_spans() {
        let mut event = new_test_event();

        process_value(
            &mut event,
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/",
          "transaction_info": {
            "source": "unknown"
          },
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
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "<unlabeled transaction>",
          "transaction_info": {
            "source": "unknown"
          },
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
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "<unlabeled transaction>",
          "transaction_info": {
            "source": "unknown"
          },
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
    fn test_is_high_cardinality_sdk_ruby_ok() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "op": "rails.request",
                    "status": "ok"
                }
            },
            "sdk": {"name": "sentry.ruby"},
            "modules": {"rack": "1.2.3"}

        }
        "#;
        let event = Annotated::<Event>::from_json(json).unwrap();

        assert!(!is_high_cardinality_sdk(&event.0.unwrap()));
    }

    #[test]
    fn test_is_high_cardinality_sdk_ruby_error() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "foo",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "op": "rails.request",
                    "status": "internal_error"
                }
            },
            "sdk": {"name": "sentry.ruby"},
            "modules": {"rack": "1.2.3"}

        }
        "#;
        let event = Annotated::<Event>::from_json(json).unwrap();
        assert!(!event.meta().has_errors());

        assert!(is_high_cardinality_sdk(&event.0.unwrap()));
    }

    #[test]
    fn test_transaction_name_dont_normalize() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            }
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        // This must not normalize transaction name, since it's disabled.
        process_value(
            &mut event,
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0",
          "transaction_info": {
            "source": "url"
          },
          "timestamp": 1619420400.0,
          "start_timestamp": 1619420341.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74053",
              "op": "rails.request",
              "status": "ok",
              "type": "trace"
            }
          },
          "spans": []
        }
        "###);
    }

    #[test]
    fn test_transaction_name_normalize() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            },
            "sdk": {"name": "sentry.ruby"},
            "modules": {"rack": "1.2.3"}

        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/foo/*/user/*/0",
          "transaction_info": {
            "source": "url"
          },
          "modules": {
            "rack": "1.2.3"
          },
          "timestamp": 1619420400.0,
          "start_timestamp": 1619420341.0,
          "contexts": {
            "trace": {
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
              "span_id": "fa90fdead5f74053",
              "op": "rails.request",
              "status": "ok",
              "type": "trace"
            }
          },
          "sdk": {
            "name": "sentry.ruby"
          },
          "spans": [],
          "_meta": {
            "transaction": {
              "": {
                "rem": [
                  [
                    "sha1",
                    "s",
                    5,
                    45
                  ],
                  [
                    "int",
                    "s",
                    51,
                    54
                  ]
                ],
                "val": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0"
              }
            }
          }
        }
        "###);
    }

    macro_rules! transaction_name_test {
        ($name:ident, $input:literal, $output:literal) => {
            #[test]
            fn $name() {
                let json = format!(
                    r#"
                    {{
                        "type": "transaction",
                        "transaction": "{}",
                        "transaction_info": {{
                          "source": "url"
                        }},
                        "timestamp": "2021-04-26T08:00:00+0100",
                        "start_timestamp": "2021-04-26T07:59:01+0100",
                        "contexts": {{
                            "trace": {{
                                "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                                "span_id": "fa90fdead5f74053",
                                "op": "rails.request",
                                "status": "ok"
                            }}
                        }}
                    }}
                "#,
                    $input
                );

                let mut event = Annotated::<Event>::from_json(&json).unwrap();

                process_value(
                    &mut event,
                    &mut TransactionsProcessor::new(true),
                    ProcessingState::root(),
                )
                .unwrap();

                assert_eq!($output, event.value().unwrap().transaction.value().unwrap());
            }
        };
    }

    transaction_name_test!(test_transaction_name_normalize_id, "/1234", "/*");
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_1,
        "/user/path-with-1234/",
        "/user/*/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_2,
        "/testing/open-19-close/1",
        "/testing/*/1"
    );
    transaction_name_test!(
        test_transaction_name_normalize_sha,
        "/hash/4c79f60c11214eb38604f4ae0781bfb2/diff",
        "/hash/*/diff"
    );
    transaction_name_test!(
        test_transaction_name_normalize_uuid,
        "/u/7b25feea-ed2d-4132-bcbd-6232b7922add/edit",
        "/u/*/edit"
    );
    transaction_name_test!(
        test_transaction_name_normalize_hex,
        "/u/0x3707344A4093822299F31D008/profile/123123213",
        "/u/*/profile/*"
    );
    transaction_name_test!(
        test_transaction_name_normalize_windows_path,
        r#"C:\\\\Program Files\\1234\\Files"#,
        r#"C:\\Program Files\*\Files"#
    );
    transaction_name_test!(test_transaction_name_skip_replace_all, "12345", "12345");
    transaction_name_test!(
        test_transaction_name_skip_replace_all2,
        "open-12345-close",
        "open-12345-close"
    );
}
