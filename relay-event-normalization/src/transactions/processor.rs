use std::borrow::Cow;
use std::ops::Range;

use once_cell::sync::Lazy;
use regex::Regex;
use relay_base_schema::events::EventType;
use relay_common::time::UnixTimestamp;
use relay_event_schema::processor::{
    self, ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::{Event, SpanStatus, TraceContext, TransactionSource};
use relay_protocol::{Annotated, Meta, Remark, RemarkType};

use crate::regexes::TRANSACTION_NAME_NORMALIZER_REGEX;
use crate::TransactionNameRule;

/// Configuration for sanitizing unparameterized transaction names.
#[derive(Clone, Debug, Default)]
pub struct TransactionNameConfig<'r> {
    /// Rules for identifier replacement that were discovered by Sentry's transaction clusterer.
    pub rules: &'r [TransactionNameRule],
}

/// Rejects transactions based on required fields.
#[derive(Debug, Default)]
pub struct TransactionsProcessor<'r> {
    name_config: TransactionNameConfig<'r>,
    timestamp_range: Option<Range<UnixTimestamp>>,
}

impl<'r> TransactionsProcessor<'r> {
    /// Creates a new `TransactionsProcessor` instance.
    pub fn new(
        name_config: TransactionNameConfig<'r>,
        timestamp_range: Option<Range<UnixTimestamp>>,
    ) -> Self {
        Self {
            name_config,
            timestamp_range,
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
    ) -> ProcessingResult {
        processor::apply(transaction, |transaction, meta| {
            let result = self.name_config.rules.iter().find_map(|rule| {
                rule.match_and_apply(Cow::Borrowed(transaction))
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
                }
            }

            Ok(())
        })?;

        Ok(())
    }

    /// Returns `true` if the given transaction name should be treated as a URL.
    ///
    /// We treat a transaction as URL if one of the following conditions apply:
    ///
    /// 1. It is marked with `source:url`
    /// 2. It is marked with `source:sanitized`, in which case we run normalization again.
    /// 3. It has no source attribute because it's from an old SDK version,
    ///    but it contains slashes and we expect it to be high-cardinality
    ///    based on the SDK information (see [`set_default_transaction_source`]).
    fn treat_transaction_as_url(&self, event: &Event) -> bool {
        let source = event
            .transaction_info
            .value()
            .and_then(|i| i.source.value());

        matches!(
            source,
            Some(&TransactionSource::Url | &TransactionSource::Sanitized)
        ) || (source.is_none() && event.transaction.value().map_or(false, |t| t.contains('/')))
    }

    fn normalize_transaction_name(&self, event: &mut Event) -> ProcessingResult {
        if self.treat_transaction_as_url(event) {
            // Normalize transaction names for URLs and Sanitized transaction sources.
            // This in addition to renaming rules can catch some high cardinality parts.
            scrub_identifiers(&mut event.transaction)?;

            // Apply rules discovered by the transaction clusterer in sentry.
            if !self.name_config.rules.is_empty() {
                self.apply_transaction_rename_rule(&mut event.transaction)?;
            }

            // Always mark URL transactions as sanitized, even if no modification were made by
            // clusterer rules or regex matchers. This has the consequence that the transaction name
            // is always extracted as a tag on transaction metrics.
            // Instead of changing the source to "sanitized", we could have changed metrics extraction
            // to also extract the transaction name for URL transactions. But this is the safer way,
            // because the product currently uses queries that assume that `source:url` is equivalent
            // to `transaction:<< unparameterized >>`.
            event
                .transaction_info
                .get_or_insert_with(Default::default)
                .source
                .set_value(Some(TransactionSource::Sanitized));
        }

        Ok(())
    }

    fn validate_transaction(&self, event: &mut Event) -> ProcessingResult {
        self.validate_timestamps(event)?;

        let Some(trace_context) = event.context_mut::<TraceContext>() else {
            return Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context",
            ));
        };

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

    /// Returns start and end timestamps if they are both set and start <= end.
    fn validate_timestamps(&self, transaction_event: &Event) -> ProcessingResult {
        match (
            transaction_event.start_timestamp.value(),
            transaction_event.timestamp.value(),
        ) {
            (Some(start), Some(end)) => {
                if end < start {
                    return Err(ProcessingAction::InvalidTransaction(
                        "end timestamp is smaller than start timestamp",
                    ));
                }

                if let Some(ref range) = self.timestamp_range {
                    let Some(timestamp) = UnixTimestamp::from_datetime(end.into_inner()) else {
                        return Err(ProcessingAction::InvalidTransaction(
                            "invalid unix timestamp",
                        ));
                    };
                    if !range.contains(&timestamp) {
                        return Err(ProcessingAction::InvalidTransaction(
                            "timestamp is out of the valid range for metrics",
                        ));
                    }
                }

                Ok(())
            }
            (_, None) => Err(ProcessingAction::InvalidTransaction(
                "timestamp hard-required for transaction events",
            )),
            // XXX: Maybe copy timestamp over?
            (None, _) => Err(ProcessingAction::InvalidTransaction(
                "start_timestamp hard-required for transaction events",
            )),
        }
    }
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

        set_default_transaction_source(event);
        self.normalize_transaction_name(event)?;
        self.validate_transaction(event)?;
        end_all_spans(event)?;

        event.process_child_values(self, state)?;
        Ok(())
    }
}

/// Span status codes for the Ruby Rack integration that indicate raw URLs being sent as
/// transaction names. These cases are considered as high-cardinality.
///
/// See <https://github.com/getsentry/sentry-ruby/blob/ad4828f6d8d60e98217b2edb1ab003fb627d6bdb/sentry-ruby/lib/sentry/span.rb#L7-L19>
const RUBY_URL_STATUSES: &[SpanStatus] = &[
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
];

/// List of SDKs which we assume to produce high cardinality transaction names, such as
/// "/user/123134/login".
const RAW_URL_SDKS: &[&str] = &[
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
];

/// Returns `true` if the event's transaction name is known to contain unsanitized values.
///
/// Newer SDK send the [`TransactionSource`] attribute, which we can rely on to determine
/// cardinality. If the source is missing, this function gives an indication whether the transaction
/// name should be sanitized.
pub fn is_high_cardinality_sdk(event: &Event) -> bool {
    let Some(client_sdk) = event.client_sdk.value() else {
        return false;
    };

    let sdk_name = event.sdk_name();
    if RAW_URL_SDKS.contains(&sdk_name) {
        return true;
    }

    let is_http_status_404 = event.tag_value("http.status_code") == Some("404");
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
        if let Some(trace) = event.context::<TraceContext>() {
            if RUBY_URL_STATUSES.contains(trace.status.value().unwrap_or(&SpanStatus::Unknown)) {
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
pub fn set_default_transaction_source(event: &mut Event) {
    let source = event
        .transaction_info
        .value()
        .and_then(|info| info.source.value());

    if source.is_none() && !is_high_cardinality_transaction(event) {
        // Assume low cardinality, set transaction source "Unknown" to signal that the transaction
        // tag can be safely added to transaction metrics.
        let transaction_info = event.transaction_info.get_or_insert_with(Default::default);
        transaction_info
            .source
            .set_value(Some(TransactionSource::Unknown));
    }
}

fn is_high_cardinality_transaction(event: &Event) -> bool {
    let transaction = event.transaction.as_str().unwrap_or_default();
    // We treat transactions from legacy SDKs as URLs if they contain slashes.
    // Otherwise, we assume low cardinality.
    transaction.contains('/') && is_high_cardinality_sdk(event)
}

/// Normalize the given string.
///
/// Replaces UUIDs, SHAs and numerical IDs in transaction names by placeholders.
/// Returns `Ok(true)` if the name was changed.
pub(crate) fn scrub_identifiers(string: &mut Annotated<String>) -> Result<bool, ProcessingAction> {
    scrub_identifiers_with_regex(string, &TRANSACTION_NAME_NORMALIZER_REGEX, "*")
}

fn scrub_identifiers_with_regex(
    string: &mut Annotated<String>,
    pattern: &Lazy<Regex>,
    replacer: &str,
) -> Result<bool, ProcessingAction> {
    let capture_names = pattern.capture_names().flatten().collect::<Vec<_>>();

    let mut did_change = false;
    processor::apply(string, |trans, meta| {
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
        let mut changed = String::with_capacity(trans.len() + caps.len() * replacer.len());
        let mut last_end = 0usize;
        for (capture, remark) in caps {
            changed.push_str(&trans[last_end..capture.start()]);
            changed.push_str(replacer);
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

/// Copies the event's end timestamp into the spans that don't have one.
fn end_all_spans(event: &mut Event) -> ProcessingResult {
    let spans = event.spans.value_mut().get_or_insert_with(Vec::new);
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
    Ok(())
}

#[cfg(test)]
mod tests {

    use chrono::{Duration, TimeZone, Utc};
    use insta::assert_debug_snapshot;
    use itertools::Itertools;
    use relay_common::glob2::LazyGlob;
    use relay_event_schema::processor::process_value;
    use relay_event_schema::protocol::{ClientSdkInfo, Contexts, Span, SpanId, TraceId};
    use relay_protocol::{get_value, Object};

    use crate::RedactionRule;

    use super::*;

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

    fn new_test_event() -> Annotated<Event> {
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();
        Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            start_timestamp: Annotated::new(start.into()),
            timestamp: Annotated::new(end.into()),
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
    fn test_discards_when_timestamp_out_of_range() {
        let mut event = new_test_event();

        let processor = &mut TransactionsProcessor::new(
            TransactionNameConfig::default(),
            Some(UnixTimestamp::now()..UnixTimestamp::now()),
        );

        assert!(matches!(
            process_value(&mut event, processor, ProcessingState::root()),
            Err(ProcessingAction::InvalidTransaction(
                "timestamp is out of the valid range for metrics"
            ))
        ));
    }

    #[test]
    fn test_replace_missing_timestamp() {
        let span = Span {
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 1).unwrap().into(),
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

        let spans = event.spans;
        let span = get_value!(spans[0]!);

        assert_eq!(span.timestamp, event.timestamp);
        assert_eq!(span.status.value().unwrap(), &SpanStatus::DeadlineExceeded);
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
                "missing valid trace context"
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
            contexts: Annotated::new(Contexts::new()),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut TransactionsProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
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
                "missing valid trace context"
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
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext::default());
                Annotated::new(contexts)
            },
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
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
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
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        let trace_context = get_value!(event.contexts)
            .unwrap()
            .get::<TraceContext>()
            .unwrap();
        let trace_op = trace_context.op.value().unwrap();
        assert_eq!(trace_op, "default");
    }

    #[test]
    fn test_allows_transaction_event_without_span_list() {
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

        processor::apply(&mut event, |event, _| {
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
        assert!(get_value!(event.spans).unwrap().is_empty());
    }

    #[test]
    fn test_discards_transaction_event_with_nulled_out_span() {
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
    fn test_default_transaction_source_unknown() {
        let mut event = Annotated::<Event>::from_json(
            r#"
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
            "#,
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
    fn test_allows_valid_transaction_event_with_spans() {
        let mut event = new_test_event();

        assert!(process_value(
            &mut event,
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .is_ok());
    }

    #[test]
    fn test_defaults_transaction_name_when_missing() {
        let mut event = new_test_event();

        processor::apply(&mut event, |event, _| {
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

        assert_eq!(get_value!(event.transaction!), "<unlabeled transaction>");
    }

    #[test]
    fn test_defaults_transaction_name_when_empty() {
        let mut event = new_test_event();

        processor::apply(&mut event, |event, _| {
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

        assert_eq!(get_value!(event.transaction!), "<unlabeled transaction>");
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
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                5,
                45,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                51,
                54,
            ),
        ),
    },
]"#);
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
            &mut TransactionsProcessor::default(),
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
            &mut TransactionsProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
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
            redaction: Default::default(),
        };
        let rule2 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };
        // This should not happend, such rules shouldn't be sent to relay at all.
        let rule3 = TransactionNameRule {
            pattern: LazyGlob::new("/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(
                TransactionNameConfig {
                    rules: &[rule1, rule2, rule3],
                },
                None,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                22,
                25,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/user/*/**",
        range: None,
    },
]"#);
    }

    #[test]
    fn test_transaction_name_rules_skip_expired() {
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
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        let rule1 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/user/*/**".to_string()),
            expiry: Utc::now() - Duration::hours(1), // Expired rule
            redaction: Default::default(),
        };
        let rule2 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };
        // This should not happend, such rules shouldn't be sent to relay at all.
        let rule3 = TransactionNameRule {
            pattern: LazyGlob::new("/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(
                TransactionNameConfig {
                    rules: &[rule1, rule2, rule3],
                },
                None,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                22,
                25,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/**",
        range: None,
    },
]"#);
    }

    #[test]
    fn test_normalize_twice() {
        // Simulate going through a chain of relays.
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
                    "op": "rails.request"
                }
            }
        }
        "#;

        let rules = vec![TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/user/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        }];

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        let mut processor = TransactionsProcessor::new(
            TransactionNameConfig {
                rules: rules.as_ref(),
            },
            None,
        );
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                22,
                25,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/user/*/**",
        range: None,
    },
]"#);

        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        // Process again:
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                22,
                25,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/user/*/**",
        range: None,
    },
]"#);

        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
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
            redaction: Default::default(),
        };
        // This should not happend, such rules shouldn't be sent to relay at all.
        let rule2 = TransactionNameRule {
            pattern: LazyGlob::new("/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
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
                None,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(
            get_value!(event.transaction!),
            "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0"
        );
        assert!(get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .next()
            .is_none());
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "foobar"
        );
    }

    fn run_with_unknown_source(sdk: &str) -> Annotated<Event> {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/user/jane/blog/",
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
        event
            .value_mut()
            .as_mut()
            .unwrap()
            .client_sdk
            .set_value(Some(ClientSdkInfo {
                name: sdk.to_owned().into(),
                ..Default::default()
            }));
        let rules: Vec<TransactionNameRule> = serde_json::from_value(serde_json::json!([
            {"pattern": "/user/*/**", "expiry": "3021-04-26T07:59:01+0100", "redaction": {"method": "replace"}}
        ]))
        .unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(
                TransactionNameConfig {
                    rules: rules.as_ref(),
                },
                None,
            ),
            ProcessingState::root(),
        )
        .unwrap();
        event
    }

    #[test]
    fn test_normalize_legacy_javascript() {
        // Javascript without source annotation gets sanitized.
        let event = run_with_unknown_source("sentry.javascript.browser");

        assert_eq!(get_value!(event.transaction!), "/user/*/blog/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "/user/*/**",
        range: None,
    },
]"#);

        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
    }

    #[test]
    fn test_normalize_legacy_python() {
        // Python without source annotation does not get sanitized, because we assume it to be
        // low cardinality.
        let event = run_with_unknown_source("sentry.python");
        assert_eq!(get_value!(event.transaction!), "/user/jane/blog/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "unknown"
        );
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
            redaction: Default::default(),
        };

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut TransactionsProcessor::new(TransactionNameConfig { rules: &[rule] }, None),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/**",
        range: None,
    },
]"#);

        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
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
        assert_eq!(
            replaced,
            ["/*", "/*", "/*", "/test/*/url",].map(str::to_owned)
        )
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
                    &mut TransactionsProcessor::default(),
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
        r"C:\\\\Program Files\\1234\\Files",
        r"C:\\Program Files\*\Files"
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
                        redaction: RedactionRule::default(),
                    }],
                },
                None,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/remains/rule-target/*");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                21,
                31,
            ),
        ),
    },
]"#);
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
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
                        redaction: RedactionRule::default(),
                    }],
                },
                None,
            ),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/remains/*/*");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                21,
                31,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/remains/*/**",
        range: None,
    },
]"#);
    }
}
