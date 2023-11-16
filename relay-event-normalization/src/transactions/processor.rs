use std::borrow::Cow;
use std::ops::Range;

use once_cell::sync::Lazy;
use regex::Regex;
use relay_common::time::UnixTimestamp;
use relay_event_schema::processor::{self, ProcessingAction, ProcessingResult};
use relay_event_schema::protocol::{Event, SpanStatus, TraceContext, TransactionSource};
use relay_protocol::{Annotated, Remark, RemarkType};

use crate::regexes::TRANSACTION_NAME_NORMALIZER_REGEX;
use crate::TransactionNameRule;

/// Configuration for sanitizing unparameterized transaction names.
#[derive(Clone, Debug, Default)]
pub struct TransactionNameConfig<'r> {
    /// Rules for identifier replacement that were discovered by Sentry's transaction clusterer.
    pub rules: &'r [TransactionNameRule],
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
pub(crate) fn end_all_spans(event: &mut Event) -> ProcessingResult {
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

/// Returns `true` if the given transaction name should be treated as a URL.
///
/// We treat a transaction as URL if one of the following conditions apply:
///
/// 1. It is marked with `source:url`
/// 2. It is marked with `source:sanitized`, in which case we run normalization again.
/// 3. It has no source attribute because it's from an old SDK version,
///    but it contains slashes and we expect it to be high-cardinality
///    based on the SDK information (see [`set_default_transaction_source`]).
fn treat_transaction_as_url(event: &Event) -> bool {
    let source = event
        .transaction_info
        .value()
        .and_then(|i| i.source.value());

    matches!(
        source,
        Some(&TransactionSource::Url | &TransactionSource::Sanitized)
    ) || (source.is_none() && event.transaction.value().map_or(false, |t| t.contains('/')))
}

/// Returns a [`ProcessingResult`] error if the transaction isn't valid.
///
/// A transaction is valid in the following cases:
/// - The transaction has a start and end timestamp.
/// - The start timestamp is no greater than the end timestamp.
/// - The transaction has a trace and span ids in the trace context.
pub(crate) fn validate_transaction(
    event: &Event,
    transaction_range: Option<&Range<UnixTimestamp>>,
) -> ProcessingResult {
    validate_transaction_timestamps(event, transaction_range)?;

    let Some(trace_context) = event.context::<TraceContext>() else {
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

    Ok(())
}

/// Returns a [`ProcessingResult`] error if start > end or either is missing.
fn validate_transaction_timestamps(
    transaction_event: &Event,
    transaction_range: Option<&Range<UnixTimestamp>>,
) -> ProcessingResult {
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

            if let Some(range) = transaction_range {
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

/// Applies scrubbing and transaction rename rules to URL transaction names.
///
/// If there's no transaction name, it sets `<unlabeled transaction>`.
/// Additionally, for URL transaction names:
/// - Applies static scrubbing on low value tokens such as UUIDs, SHAs and IDs.
/// - Applies dynamic transaction name rules, pushed from upstream.
/// - Sets the transaction source to sanitized.
pub(crate) fn normalize_transaction_name(
    event: &mut Event,
    transaction_name_config: &TransactionNameConfig,
) -> ProcessingResult {
    if treat_transaction_as_url(event) {
        // Normalize transaction names for URLs and Sanitized transaction sources.
        // This in addition to renaming rules can catch some high cardinality parts.
        scrub_identifiers(&mut event.transaction)?;

        // Apply rules discovered by the transaction clusterer in sentry.
        if !transaction_name_config.rules.is_empty() {
            apply_transaction_rename_rule(&mut event.transaction, transaction_name_config)?;
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

/// Applies the rule if any found to the transaction name.
///
/// It find the first rule matching the criteria:
/// - source matchining the one provided in the rule sorce
/// - rule hasn't epired yet
/// - glob pattern matches the transaction name
///
/// Note: we add `/` at the end of the transaction name if there isn't one, to make sure that
/// patterns like `/<something>/*/**` where we have `**` at the end are a match.
fn apply_transaction_rename_rule(
    transaction: &mut Annotated<String>,
    config: &TransactionNameConfig,
) -> ProcessingResult {
    processor::apply(transaction, |transaction, meta| {
        let result = config.rules.iter().find_map(|rule| {
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

#[cfg(test)]
mod tests {

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
}
