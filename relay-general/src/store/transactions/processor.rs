use std::borrow::Cow;
use std::collections::BTreeMap;

use once_cell::sync::Lazy;
use regex::Regex;
use relay_common::SpanStatus;

use super::TransactionNameRule;
use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{
    Context, ContextInner, Event, EventType, Span, Timestamp, TransactionInfo, TransactionSource,
};
use crate::store::regexes::{SQL_NORMALIZER_REGEX, TRANSACTION_NAME_NORMALIZER_REGEX};
use crate::types::{
    Annotated, Meta, ProcessingAction, ProcessingResult, Remark, RemarkType, Value,
};

/// Configuration around removing high-cardinality parts of URL transactions.
#[derive(Clone, Debug, Default)]
pub struct TransactionNameConfig<'r> {
    /// Rules for identifier replacement that were discovered by Sentry's transaction clusterer.
    pub rules: &'r [TransactionNameRule],
}

/// Rejects transactions based on required fields.
#[derive(Default)]
pub struct TransactionsProcessor<'r> {
    name_config: TransactionNameConfig<'r>,
    scrub_span_descriptions: bool,
}

impl<'r> TransactionsProcessor<'r> {
    pub fn new(name_config: TransactionNameConfig<'r>, scrub_span_descriptions: bool) -> Self {
        Self {
            name_config,
            scrub_span_descriptions,
        }
    }

    /// Applies the rule if any found to the transaction name.
    ///
    /// It find the first rule matching the criteria:
    /// - source matchining the one provided in the rule sorce
    /// - rule hasn't epired yet
    /// - glob pattern matches the transaction name
    ///
    /// Note: we add `/` at the end of the transaction name if there isn't one, to make sure that
    /// patterns like `/<something>/*/**` where we have `**` at the end are a match.
    pub fn apply_transaction_rename_rule(
        &self,
        transaction: &mut Annotated<String>,
        info: &mut std::option::Option<TransactionInfo>,
    ) -> ProcessingResult {
        if let Some(info) = info.as_mut() {
            transaction.apply(|transaction, meta| {
                let result = self.name_config.rules.iter().find_map(|rule| {
                    rule.match_and_apply(Cow::Borrowed(transaction), info)
                        .map(|applied_result| (rule.pattern.compiled().pattern(), applied_result))
                });

                if let Some((rule, result)) = result {
                    if *transaction != result {
                        // If another rule was applied before, we don't want to
                        // rename the transaction name to keep the original one.
                        // We do want to continue adding remarks though, in
                        // order to keep track of all rules applied.
                        if meta.original_value().is_none() {
                            meta.set_original_value(Some(transaction.clone()));
                        }
                        // add also the rule which was applied to the transaction name
                        meta.add_remark(Remark::new(RemarkType::Substituted, rule));
                        *transaction = result;
                        info.source
                            .value_mut()
                            .replace(TransactionSource::Sanitized);
                    }
                }

                Ok(())
            })?;
        }
        Ok(())
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

fn validate_transaction(event: &mut Event) -> ProcessingResult {
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

/// Normalize the given string.
///
/// Replaces UUIDs, SHAs and numerical IDs in transaction names by placeholders.
/// Returns `Ok(true)` if the name was changed.
fn scrub_identifiers(string: &mut Annotated<String>) -> Result<bool, ProcessingAction> {
    scrub_identifiers_with_regex(string, &TRANSACTION_NAME_NORMALIZER_REGEX)
}

/// Normalize the given SQL-query-like string.
fn scrub_sql_queries(string: &mut Annotated<String>) -> Result<bool, ProcessingAction> {
    scrub_identifiers_with_regex(string, &SQL_NORMALIZER_REGEX)
}

fn scrub_identifiers_with_regex(
    string: &mut Annotated<String>,
    pattern: &Lazy<Regex>,
) -> Result<bool, ProcessingAction> {
    let capture_names = pattern.capture_names().flatten().collect::<Vec<_>>();

    let mut did_change = false;
    string.apply(|trans, meta| {
        let mut caps = Vec::new();
        // Collect all the remarks if anything matches.
        for captures in pattern.captures_iter(trans) {
            for name in &capture_names {
                if let Some(capture) = captures.name(name) {
                    let remark = Remark::with_range(
                        RemarkType::Substituted,
                        *name,
                        (capture.start(), capture.end()),
                    );
                    caps.push((capture, remark));
                    break;
                }
            }
        }

        if caps.is_empty() {
            // Nothing to do for this transaction.
            return Ok(());
        }

        // Sort by the capture end position.
        caps.sort_by_key(|(capture, _)| capture.end());
        let mut changed = String::with_capacity(trans.len());
        let mut last_end = 0usize;
        for (capture, remark) in caps {
            changed.push_str(&trans[last_end..capture.start()]);
            changed.push('*');
            last_end = capture.end();
            meta.add_remark(remark);
        }
        changed.push_str(&trans[last_end..]);

        if !changed.is_empty() && changed != "*" {
            meta.set_original_value(Some(trans.to_string()));
            *trans = changed;
            did_change = true;
        }
        Ok(())
    })?;
    Ok(did_change)
}

impl Processor for TransactionsProcessor<'_> {
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

        // Normalize transaction names for URLs and Sanitized transaction sources.
        // This in addition to renaming rules can catch some high cardinality parts.
        let mut sanitized = false;

        if matches!(
            event.get_transaction_source(),
            &TransactionSource::Url | &TransactionSource::Sanitized
        ) {
            scrub_identifiers(&mut event.transaction)?.then(|| {
                sanitized = true;
            });
        }

        if !self.name_config.rules.is_empty() {
            self.apply_transaction_rename_rule(
                &mut event.transaction,
                event.transaction_info.value_mut(),
            )?;

            sanitized = true;
        }

        if sanitized && matches!(event.get_transaction_source(), &TransactionSource::Url) {
            event
                .transaction_info
                .get_or_insert_with(Default::default)
                .source
                .set_value(Some(TransactionSource::Sanitized));
        }

        validate_transaction(event)?;

        let spans = event.spans.value_mut().get_or_insert_with(|| Vec::new());

        for span in spans {
            if let Some(span) = span.value_mut() {
                if span.timestamp.value().is_none() {
                    // event timestamp guaranteed to be `Some` due to validate_transaction call
                    span.timestamp.set_value(event.timestamp.value().cloned());
                    span.status = Annotated::new(SpanStatus::DeadlineExceeded);
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

        if self.scrub_span_descriptions {
            scrub_span_description(span)?;
        }

        span.process_child_values(self, state)?;

        Ok(())
    }
}

fn scrub_span_description(span: &mut Span) -> Result<(), ProcessingAction> {
    if span.description.value().is_none() {
        return Ok(());
    }

    let mut scrubbed = span.description.clone();
    let mut did_scrub = false;

    if let Some(is_url_like) = span.op.value().map(|op| op.starts_with("http")) {
        if is_url_like && scrub_identifiers(&mut scrubbed)? {
            did_scrub = true;
        }
    }

    if let Some(is_db_like) = span.op.value().map(|op| op.starts_with("db")) {
        if is_db_like && scrub_sql_queries(&mut scrubbed)? {
            did_scrub = true;
        }
    }

    if did_scrub {
        if let Some(new_desc) = scrubbed.into_value() {
            span.data
                .get_or_insert_with(BTreeMap::new)
                // We don't care what the cause of scrubbing was, since we assume
                // that after scrubbing the value is sanitized.
                .insert(
                    "description.scrubbed".to_owned(),
                    Annotated::new(Value::String(new_desc)),
                );
        };
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use chrono::offset::TimeZone;
    use chrono::{Duration, Utc};
    use insta::assert_debug_snapshot;
    use similar_asserts::assert_eq;

    use super::*;
    use crate::processor::process_value;
    use crate::protocol::{Contexts, SpanId, TraceContext, TraceId, TransactionSource};
    use crate::store::{LazyGlob, RedactionRule, RuleScope};
    use crate::testutils::assert_annotated_snapshot;
    use crate::types::Object;

    fn new_test_event() -> Annotated<Event> {
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();
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
    fn test_replace_missing_timestamp() {
        let span = Span {
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(1968, 1, 1, 0, 0, 1).unwrap().into(),
            ),
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
            ..Default::default()
        };

        let mut event = new_test_event().0.unwrap();
        event.spans = Annotated::new(vec![Annotated::new(span)]);

        TransactionsProcessor::default()
            .process_event(
                &mut event,
                &mut Meta::default(),
                &ProcessingState::default(),
            )
            .unwrap();

        assert_eq!(
            event.spans.value().unwrap()[0].value().unwrap().timestamp,
            event.timestamp
        );

        assert_eq!(
            event.spans.value().unwrap()[0]
                .value()
                .unwrap()
                .status
                .value()
                .unwrap(),
            &SpanStatus::DeadlineExceeded
        );
    }

    #[test]
    fn test_discards_when_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();

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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
    fn test_discards_transaction_event_with_span_with_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
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
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();

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
            &mut TransactionsProcessor::new(TransactionNameConfig::default(), false),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/foo/*/user/*/0",
          "transaction_info": {
            "source": "sanitized"
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
                    "int",
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

    /// When no identifiers are scrubbed, we should not set an original value in _meta.
    #[test]
    fn test_transaction_name_skip_original_value() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/static/page",
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
            &mut TransactionsProcessor::new(TransactionNameConfig::default(), false),
            ProcessingState::root(),
        )
        .unwrap();

        assert!(event.meta().is_empty());
    }

    #[test]
    fn test_transaction_name_normalize_mark_as_sanitized() {
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

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(TransactionNameConfig::default(), false),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/foo/*/user/*/0",
          "transaction_info": {
            "source": "sanitized"
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
          "spans": [],
          "_meta": {
            "transaction": {
              "": {
                "rem": [
                  [
                    "int",
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

    #[test]
    fn test_transaction_name_rename_with_rules() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/rule-target/user/123/0/",
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

        let rule1 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/user/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            scope: Default::default(),
            redaction: Default::default(),
        };
        let rule2 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            scope: Default::default(),
            redaction: Default::default(),
        };
        // This should not happend, such rules shouldn't be sent to relay at all.
        let rule3 = TransactionNameRule {
            pattern: LazyGlob::new("/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            scope: Default::default(),
            redaction: Default::default(),
        };
        let mut rules = vec![rule1, rule2, rule3];

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(
                TransactionNameConfig {
                    rules: rules.as_ref(),
                },
                false,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
         {
           "type": "transaction",
           "transaction": "/foo/*/user/*/0/",
           "transaction_info": {
             "source": "sanitized"
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
                     "int",
                     "s",
                     22,
                     25
                   ],
                   [
                     "/foo/*/user/*/**",
                     "s"
                   ]
                 ],
                 "val": "/foo/rule-target/user/123/0/"
               }
             }
           }
         }
         "###);

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        // Make the first rule expire, the second rule must be applied.
        rules[0].expiry = Utc::now() - Duration::hours(1);

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(
                TransactionNameConfig {
                    rules: rules.as_ref(),
                },
                false,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/foo/*/user/*/0/",
          "transaction_info": {
            "source": "sanitized"
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
                    "int",
                    "s",
                    22,
                    25
                  ],
                  [
                    "/foo/*/**",
                    "s"
                  ]
                ],
                "val": "/foo/rule-target/user/123/0/"
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_transaction_name_unsupported_source() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0",
            "transaction_info": {
              "source": "foobar"
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
        let rule1 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            scope: Default::default(),
            redaction: Default::default(),
        };
        // This should not happend, such rules shouldn't be sent to relay at all.
        let rule2 = TransactionNameRule {
            pattern: LazyGlob::new("/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            scope: Default::default(),
            redaction: Default::default(),
        };
        let rules = vec![rule1, rule2];

        // This must not normalize transaction name, since it's disabled.
        process_value(
            &mut event,
            &mut TransactionsProcessor::new(
                TransactionNameConfig {
                    rules: rules.as_ref(),
                },
                false,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
        {
          "type": "transaction",
          "transaction": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0",
          "transaction_info": {
            "source": "foobar"
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
    fn test_transaction_name_rename_end_slash() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/rule-target/user",
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

        let rule = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            scope: Default::default(),
            redaction: Default::default(),
        };

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(TransactionNameConfig { rules: &[rule] }, false),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event, @r###"
         {
           "type": "transaction",
           "transaction": "/foo/*/user",
           "transaction_info": {
             "source": "sanitized"
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
                     "/foo/*/**",
                     "s"
                   ]
                 ],
                 "val": "/foo/rule-target/user"
               }
             }
           }
         }
         "###);
    }

    #[test]
    fn test_normalize_transaction_names() {
        let should_be_replaced = [
            "/aaa11111-aa11-11a1-a11a-1aaa1111a111",
            "/1aa111aa-11a1-11aa-a111-a1a11111aa11",
            "/00a00000-0000-0000-0000-000000000001",
            "/test/b25feeaa-ed2d-4132-bcbd-6232b7922add/url",
        ];
        let replaced = should_be_replaced.map(|s| {
            let mut s = Annotated::new(s.to_owned());
            scrub_identifiers(&mut s).unwrap();
            s.0.unwrap()
        });
        assert_debug_snapshot!(replaced);
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
                    &mut TransactionsProcessor::new(TransactionNameConfig::default(), false),
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
        test_transaction_name_normalize_in_segments_3,
        "/testing/open19close/1",
        "/testing/*/1"
    );
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_4,
        "/testing/asdf012/asdf034/asdf056",
        "/testing/*/*/*"
    );
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_5,
        "/foo/test%A33/1234",
        "/foo/test%A33/*"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_1,
        "/%2Ftest%2Fopen%20and%20help%2F1%0A",
        "/%2Ftest%2Fopen%20and%20help%2F1%0A"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_2,
        "/this/1234/%E2%9C%85/foo/bar/098123908213",
        "/this/*/%E2%9C%85/foo/bar/*"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_3,
        "/foo/hello%20world-4711/",
        "/foo/*/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_4,
        "/foo/hello%20world-0xdeadbeef/",
        "/foo/*/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_5,
        "/foo/hello%20world-4711/",
        "/foo/*/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_6,
        "/foo/hello%2Fworld/",
        "/foo/hello%2Fworld/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_7,
        "/foo/hello%201/",
        "/foo/hello%201/"
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

    #[test]
    fn test_scrub_identifiers_before_rules() {
        // There's a rule matching the transaction name. However, the UUID
        // should be scrubbed first. Scrubbing the UUID makes the rule to not
        // match the transformed transaction name anymore.

        let mut event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "transaction": "/remains/rule-target/1234567890",
                "transaction_info": {
                    "source": "url"
                },
                "timestamp": "2021-04-26T08:00:00+0100",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053"
                    }
                }
            }"#,
        )
        .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(
                TransactionNameConfig {
                    rules: &[TransactionNameRule {
                        pattern: LazyGlob::new("/remains/*/1234567890/".to_owned()),
                        expiry: Utc.with_ymd_and_hms(3000, 1, 1, 1, 1, 1).unwrap(),
                        scope: RuleScope::default(),
                        redaction: RedactionRule::default(),
                    }],
                },
                false,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        // Annotate the snapshot instead of comparing transaction names, to also
        // make sure the event's _meta is correct.
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_scrub_identifiers_and_apply_rules() {
        // Ensure rules are applied after scrubbing identifiers. Rules are only
        // applied when `transaction.source="url"`, so this test ensures this
        // value isn't set as part of identifier scrubbing.
        let mut event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "transaction": "/remains/rule-target/1234567890",
                "transaction_info": {
                    "source": "url"
                },
                "timestamp": "2021-04-26T08:00:00+0100",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053"
                    }
                }
            }"#,
        )
        .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(
                TransactionNameConfig {
                    rules: &[TransactionNameRule {
                        pattern: LazyGlob::new("/remains/*/**".to_owned()),
                        expiry: Utc.with_ymd_and_hms(3000, 1, 1, 1, 1, 1).unwrap(),
                        scope: RuleScope::default(),
                        redaction: RedactionRule::default(),
                    }],
                },
                false,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_no_sanitized_if_no_rules() {
        let mut event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "transaction": "/remains/rule-target/whatever",
                "transaction_info": {
                    "source": "url"
                },
                "timestamp": "2021-04-26T08:00:00+0100",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053"
                    }
                }
            }"#,
        )
        .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(TransactionNameConfig::default(), false),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(event);
    }

    macro_rules! span_description_test {
        // Tests the scrubbed span description for the given op.
        // An empty output `""` means the span description was not scrubbed at all.
        ($name:ident, $description_in:literal, $op_in:literal, $output:literal) => {
            #[test]
            fn $name() {
                let json = format!(
                    r#"
                    {{
                        "description": "{}",
                        "span_id": "bd2eb23da2beb459",
                        "start_timestamp": 1597976393.4619668,
                        "timestamp": 1597976393.4718769,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "op": "{}"
                    }}
                "#,
                    $description_in, $op_in
                );

                let mut span = Annotated::<Span>::from_json(&json).unwrap();

                process_value(
                    &mut span,
                    &mut TransactionsProcessor::new(TransactionNameConfig::default(), true),
                    ProcessingState::root(),
                )
                .unwrap();

                // The input description may contain escaped characters, and the
                // default formatter (when taking the value from the span
                // description) automatically escapes them. The goal is to
                // compute raw values, so we want to get rid of character
                // escaping, and the debug formatter does that. The debug
                // formatter doesn't remove the leading and trailing `"`s, so we
                // manually add them to the input literal.
                assert_eq!(
                    format!("\"{}\"", $description_in),
                    format!("{:?}", span.value().unwrap().description.value().unwrap())
                );

                if $output == "" {
                    assert!(span
                        .value()
                        .and_then(|span| span.data.value())
                        .and_then(|data| data.get("description.scrubbed"))
                        .is_none());
                } else {
                    assert_eq!(
                        $output,
                        span.value()
                            .and_then(|span| span.data.value())
                            .and_then(|data| data.get("description.scrubbed"))
                            .and_then(|an_value| an_value.as_str())
                            .unwrap()
                    );
                }
            }
        };
    }

    span_description_test!(span_description_scrub_empty, "", "http.client", "");

    span_description_test!(
        span_description_scrub_only_domain,
        "GET http://service.io",
        "http.client",
        ""
    );

    span_description_test!(
        span_description_scrub_only_urllike_on_http_ops,
        "GET https://www.service.io/resources/01234",
        "db.sql.query",
        ""
    );

    span_description_test!(
        span_description_scrub_path_ids_end,
        "GET https://www.service.io/resources/01234",
        "http.client",
        "GET https://www.service.io/resources/*"
    );

    span_description_test!(
        span_description_scrub_path_ids_middle,
        "GET https://www.service.io/resources/01234/details",
        "http.client",
        "GET https://www.service.io/resources/*/details"
    );

    span_description_test!(
        span_description_scrub_path_multiple_ids,
        "GET https://www.service.io/users/01234-qwerty/settings/98765-adfghj",
        "http.client",
        "GET https://www.service.io/users/*/settings/*"
    );

    span_description_test!(
        span_description_scrub_path_md5_hashes,
        "GET /clients/563712f9722fb0996ac8f3905b40786f/project/01234",
        "http.client",
        "GET /clients/*/project/*"
    );

    span_description_test!(
        span_description_scrub_path_sha_hashes,
        "GET /clients/403926033d001b5279df37cbbe5287b7c7c267fa/project/01234",
        "http.client",
        "GET /clients/*/project/*"
    );

    span_description_test!(
        span_description_scrub_path_uuids,
        "GET /clients/8ff81d74-606d-4c75-ac5e-cee65cbbc866/project/01234",
        "http.client",
        "GET /clients/*/project/*"
    );

    // TODO(iker): Add span description test for URLs with paths

    span_description_test!(
        span_description_scrub_only_dblike_on_db_ops,
        "SELECT count() FROM table WHERE id IN (%s, %s)",
        "http.client",
        ""
    );

    span_description_test!(
        span_description_scrub_various_parameterized_ins_percentage,
        "SELECT count() FROM table WHERE id IN (%s, %s) AND id IN (%s, %s, %s)",
        "db.sql.query",
        "SELECT count() FROM table WHERE id IN (*) AND id IN (*)"
    );

    span_description_test!(
        span_description_scrub_various_parameterized_ins_dollar,
        "SELECT count() FROM table WHERE id IN ($1, $2, $3)",
        "db.sql.query",
        "SELECT count() FROM table WHERE id IN (*)"
    );

    span_description_test!(
        span_description_scrub_various_parameterized_questionmarks,
        "SELECT count() FROM table WHERE id IN (?, ?, ?)",
        "db.sql.query",
        "SELECT count() FROM table WHERE id IN (*)"
    );

    span_description_test!(
        span_description_scrub_unparameterized_ins_uppercase,
        "SELECT count() FROM table WHERE id IN (100, 101, 102)",
        "db.sql.query",
        "SELECT count() FROM table WHERE id IN (*)"
    );

    span_description_test!(
        span_description_scrub_various_parameterized_ins_lowercase,
        "select count() from table where id in (100, 101, 102)",
        "db.sql.query",
        "select count() from table where id in (*)"
    );

    span_description_test!(
        span_description_scrub_savepoint_uppercase,
        "SAVEPOINT unquoted_identifier",
        "db.sql.query",
        "SAVEPOINT *"
    );

    span_description_test!(
        span_description_scrub_savepoint_uppercase_semicolon,
        "SAVEPOINT unquoted_identifier;",
        "db.sql.query",
        "SAVEPOINT *;"
    );

    span_description_test!(
        span_description_scrub_savepoint_lowercase,
        "savepoint unquoted_identifier",
        "db.sql.query",
        "savepoint *"
    );

    span_description_test!(
        span_description_scrub_savepoint_quoted,
        "SAVEPOINT 'single_quoted_identifier'",
        "db.sql.query",
        "SAVEPOINT *"
    );

    span_description_test!(
        span_description_scrub_savepoint_quoted_backtick,
        "SAVEPOINT `backtick_quoted_identifier`",
        "db.sql.query",
        "SAVEPOINT *"
    );

    span_description_test!(
        span_description_scrub_single_quoted_string,
        "SELECT * FROM table WHERE sku = 'foo'",
        "db.sql.query",
        "SELECT * FROM table WHERE sku = *"
    );

    span_description_test!(
        span_description_scrub_single_quoted_string_unfinished,
        r#"SELECT * FROM table WHERE quote = 'it\\'s a string"#,
        "db.sql.query",
        "SELECT * FROM table WHERE quote = *"
    );

    span_description_test!(
        span_description_dont_scrub_double_quoted_strings_format_postgres,
        r#"SELECT * from \"table\" WHERE sku = *"#,
        "db.sql.query",
        ""
    );

    span_description_test!(
        span_description_dont_scrub_double_quoted_strings_format_mysql,
        r#"SELECT * from table WHERE sku = \"foo\""#,
        "db.sql.query",
        ""
    );
}
