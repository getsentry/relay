//! Span normalization logic.

use regex::Regex;
use relay_conventions::attributes::{
    SENTRY__DSC__PROJECT_ID, SENTRY__DSC__TRACE_ID, SENTRY__DSC__TRANSACTION,
};
use relay_event_schema::protocol::{Event, SpanData, TraceContext};
use relay_protocol::Annotated;
use relay_sampling::DynamicSamplingContext;
use std::sync::LazyLock;

pub mod ai;
pub mod country_subregion;
pub mod description;
pub mod exclusive_time;
pub mod tag_extraction;

/// Regex used to scrub hex IDs and multi-digit numbers from table names and other identifiers.
pub static TABLE_NAME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?ix)
        [0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12} |
        [0-9a-f]{8,} |
        \d\d+
        ",
    )
    .unwrap()
});

/// Applies [`relay_conventions`] configured normalizations to transaction spans.
pub fn normalize_conventions(event: &mut Event) {
    if let Some(data) = event
        .contexts
        .value_mut()
        .as_mut()
        .and_then(|c| c.get_mut::<TraceContext>())
        .map(|c| &mut c.data)
    {
        crate::eap::normalize_attribute_names(data);
    }

    if let Some(spans) = event.spans.value_mut() {
        for data in spans
            .iter_mut()
            .filter_map(|span| span.value_mut().as_mut())
            .map(|span| &mut span.data)
        {
            crate::eap::normalize_attribute_names(data);
        }
    }
}

/// Replaces snake_case app start spans op with dot.case op.
///
/// This is done for the affected React Native SDK versions (from 3 to 4.4).
pub fn normalize_app_start_spans(event: &mut Event) {
    if !event.sdk_name().eq("sentry.javascript.react-native")
        || !(event.sdk_version().starts_with("4.4")
            || event.sdk_version().starts_with("4.3")
            || event.sdk_version().starts_with("4.2")
            || event.sdk_version().starts_with("4.1")
            || event.sdk_version().starts_with("4.0")
            || event.sdk_version().starts_with('3'))
    {
        return;
    }

    if let Some(spans) = event.spans.value_mut() {
        for span in spans {
            if let Some(span) = span.value_mut()
                && let Some(op) = span.op.value()
            {
                if op == "app_start_cold" {
                    span.op.set_value(Some("app.start.cold".to_owned()));
                    break;
                } else if op == "app_start_warm" {
                    span.op.set_value(Some("app.start.warm".to_owned()));
                    break;
                }
            }
        }
    }
}

/// Writes DSC attributes needed for dynamic sampling into the spans' `data`.
///
/// If `sentry.dsc.trace_id` is already present in a span's `data`, the function does nothing for
/// that span.
pub fn normalize_dsc_for_event_spans(event: &mut Event, dsc: Option<&DynamicSamplingContext>) {
    if let Some(ctx) = event.context_mut::<TraceContext>() {
        normalize_dsc_for_span_data(&mut ctx.data, dsc);
    }
    if let Some(spans) = event.spans.value_mut() {
        for span in spans {
            if let Some(span) = span.value_mut() {
                normalize_dsc_for_span_data(&mut span.data, dsc);
            }
        }
    }
}

/// Writes DSC attributes needed for dynamic sampling into `span_data`.
///
/// If `sentry.dsc.trace_id` is already present in `span_data`, the function does nothing.
pub fn normalize_dsc_for_span_data(
    span_data: &mut Annotated<SpanData>,
    dsc: Option<&DynamicSamplingContext>,
) {
    let Some(dsc) = dsc else {
        return;
    };

    let data = span_data.get_or_insert_with(SpanData::default);
    if data.get_value(SENTRY__DSC__TRACE_ID).is_some() {
        return;
    }
    data.insert_value(SENTRY__DSC__TRACE_ID, dsc.trace_id.to_string());
    if let Some(project_id) = &dsc.project_id {
        data.insert_value(SENTRY__DSC__PROJECT_ID, project_id.to_string());
    }
    if let Some(transaction) = &dsc.transaction {
        data.insert_value(SENTRY__DSC__TRANSACTION, transaction.to_string());
    }
}
