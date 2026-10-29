//! Apply cached rate limits and zero quotas before processing envelope payloads.
//! Summarize item headers, resolve limits and event dependencies, then filter items in place.

use crate::envelope::{
    AttachmentParentType, ContentType, Envelope, Item, Items, RejectionQuantities,
};
#[cfg(test)]
use crate::envelope::{AttachmentType, ItemType};
use crate::integrations::Integration;
use crate::managed::{Managed, RecordKeeper};
use crate::services::outcome::Outcome;
use crate::utils::{EnvelopeSummary, EventGroup};
use relay_profiling::ProfileType;
use relay_quotas::{DataCategory, ItemScoping, MetricNamespaceScoping, Quota, RateLimits, Scoping};

/// Applies cached limits and zero quotas before payload processing, without consuming quota.
///
/// Records rejected quantities directly with managed accounting while moving the original
/// item buffer in and out without reallocating it.
pub fn enforce_rate_limits(
    envelope: &mut Managed<Box<Envelope>>,
    cached: &RateLimits,
    quotas: &[Quota],
) -> RateLimits {
    if cached.is_ok() && quotas.iter().all(|quota| quota.limit != Some(0)) {
        return RateLimits::new();
    }
    let scoping = envelope.scoping();
    let mut client_rate_limits = RateLimits::new();
    envelope.modify(|envelope, records| {
        // Rejected transactions can reveal span quantities absent from the serialized input.
        // This is only debug bookkeeping; the filter calculates each rejected quantity once.
        #[cfg(debug_assertions)]
        if envelope
            .items()
            .any(|item| matches!(item, Item::Transaction(transaction) if transaction.span_count().is_none()))
        {
            records.lenient(DataCategory::Span);
            records.lenient(DataCategory::SpanIndexed);
        }
        let (items, rate_limits) = filter(
            envelope.replace_items(Items::new()),
            &scoping,
            cached,
            quotas,
            records,
        );
        envelope.replace_items(items);
        client_rate_limits = rate_limits;
    });
    client_rate_limits
}

// Final item retention decision.
enum RetentionAction<'a> {
    Keep,
    Drop(Option<&'a Outcome>),
    KeepRateLimited(&'a Outcome),
}

impl<'a> RetentionAction<'a> {
    fn drop_on_rejection(outcome: Option<&'a Outcome>) -> Self {
        match outcome {
            None => Self::Keep,
            Some(outcome) => Self::Drop(Some(outcome)),
        }
    }
}

// Resolved rejection reasons shared by the item handlers. Quantities stay with the items.
struct Enforcement {
    event: Option<(DataCategory, Outcome)>,
    spans: Option<Outcome>,
    profiles: Option<Outcome>,
    event_attachments: Option<Outcome>,
    span_attachments: Option<Outcome>,
    trace_attachments: Option<Outcome>,
    backend_profiles: Option<Outcome>,
    ui_profiles: Option<Outcome>,
    sessions: Option<Outcome>,
    feedback: Option<Outcome>,
    replays: Option<Outcome>,
    monitors: Option<Outcome>,
    backend_chunks: Option<Outcome>,
    ui_chunks: Option<Outcome>,
    logs: Option<Outcome>,
    trace_metrics: Option<Outcome>,
}

impl Enforcement {
    fn has_rejections(&self) -> bool {
        // List every field so adding a new enforcement also requires considering it here.
        !matches!(
            self,
            Self {
                event: None,
                spans: None,
                profiles: None,
                event_attachments: None,
                span_attachments: None,
                trace_attachments: None,
                backend_profiles: None,
                ui_profiles: None,
                sessions: None,
                feedback: None,
                replays: None,
                monitors: None,
                backend_chunks: None,
                ui_chunks: None,
                logs: None,
                trace_metrics: None,
            }
        )
    }
}

fn resolve_limits(
    summary: EnvelopeSummary,
    scoping: &Scoping,
    cached: &RateLimits,
    quotas: &[Quota],
) -> (Enforcement, RateLimits) {
    let mut client_rate_limits = RateLimits::new();
    let mut check = |has_relevant_items: bool, category, include_in_client_limits: bool| {
        if !has_relevant_items {
            return None;
        }
        let limits = cached.check_with_quotas(quotas, &scoping.item(category));
        let outcome = Outcome::RateLimited(limits.longest()?.reason_code.clone());
        if include_in_client_limits {
            client_rate_limits.merge(limits);
        }
        Some(outcome)
    };
    let event = summary
        .event
        .category()
        .and_then(|category| check(true, category, true).map(|outcome| (category, outcome)));
    let spans = match &event {
        Some((DataCategory::Transaction, outcome)) => Some(outcome.clone()),
        _ => check(
            summary.span_quantity > 0 || summary.has_span_attachments,
            DataCategory::Span,
            true,
        ),
    };
    let profiles_present = summary.profile_quantity.total > 0;
    let profiles = match (&event, summary.event) {
        (Some((_, outcome)), _) => Some(outcome.clone()),
        (None, EventGroup::Absent) => check(profiles_present, DataCategory::Profile, true)
            .or_else(|| check(profiles_present, DataCategory::Transaction, true)),
        _ => check(profiles_present, DataCategory::Profile, true),
    };
    let event_attachments = match &event {
        Some((_, outcome)) => Some(outcome.clone()),
        None => check(
            summary.has_event_attachments,
            DataCategory::Attachment,
            summary.has_plain_attachments,
        )
        .or_else(|| {
            check(
                summary.has_event_attachments,
                DataCategory::AttachmentItem,
                summary.has_plain_attachments,
            )
        }),
    };
    let span_attachments = spans
        .clone()
        .or_else(|| {
            check(
                summary.has_span_attachments,
                DataCategory::Attachment,
                false,
            )
        })
        .or_else(|| {
            check(
                summary.has_span_attachments,
                DataCategory::AttachmentItem,
                false,
            )
        });
    let trace_attachments = check(
        summary.has_trace_attachments,
        DataCategory::Attachment,
        false,
    )
    .or_else(|| {
        check(
            summary.has_trace_attachments,
            DataCategory::AttachmentItem,
            false,
        )
    });
    let backend_profiles = profiles.clone().or_else(|| {
        check(
            summary.profile_quantity.backend > 0,
            DataCategory::ProfileBackend,
            true,
        )
    });
    let ui_profiles = profiles.clone().or_else(|| {
        check(
            summary.profile_quantity.ui > 0,
            DataCategory::ProfileUi,
            true,
        )
    });
    let sessions = check(summary.session_quantity > 0, DataCategory::Session, true);
    let feedback = check(summary.has_user_reports, DataCategory::UserReportV2, true);
    let replays = check(summary.replay_quantity > 0, DataCategory::Replay, true);
    let monitors = check(summary.monitor_quantity > 0, DataCategory::Monitor, true);
    let backend_chunks = check(
        summary.profile_chunk_quantity > 0,
        DataCategory::ProfileChunk,
        true,
    );
    let ui_chunks = check(
        summary.profile_chunk_ui_quantity > 0,
        DataCategory::ProfileChunkUi,
        true,
    );
    let logs = check(summary.log_item_quantity > 0, DataCategory::LogItem, true)
        .or_else(|| check(summary.log_byte_quantity > 0, DataCategory::LogByte, true));
    let trace_metrics = check(
        summary.trace_metric_quantity > 0,
        DataCategory::TraceMetric,
        true,
    )
    .or_else(|| {
        check(
            summary.trace_metric_byte_quantity > 0,
            DataCategory::TraceMetricByte,
            true,
        )
    });
    // Metrics stay in the envelope; advertise client rate limits across namespaces without parsing them.
    if summary.has_metrics {
        let scope = ItemScoping {
            namespace: MetricNamespaceScoping::Any,
            ..scoping.item(DataCategory::MetricBucket)
        };
        client_rate_limits.merge(cached.check_with_quotas(quotas, &scope));
    }
    // Resolve the logical event rejection and use its reason for dependent items.
    let event = match (event, summary.event, &span_attachments, &trace_attachments) {
        (Some(event), _, _, _) => Some(event),
        (None, EventGroup::SpanEventCreators, Some(outcome), _)
        | (None, EventGroup::TraceEventCreators, _, Some(outcome))
        | (None, EventGroup::BothSpanFirst, Some(outcome), Some(_))
        | (None, EventGroup::BothTraceFirst, Some(_), Some(outcome)) => {
            Some((DataCategory::Error, outcome.clone()))
        }
        (
            None,
            EventGroup::Absent
            | EventGroup::SurvivesAttachmentLimits(_)
            | EventGroup::SpanEventCreators
            | EventGroup::TraceEventCreators
            | EventGroup::BothSpanFirst
            | EventGroup::BothTraceFirst,
            _,
            _,
        ) => None,
    };
    let event_outcome = event.as_ref().map(|(_, outcome)| outcome);
    let enforcement = Enforcement {
        // Attachment limits may have just removed the last event-creating item. Apply
        // that event rejection to profiles before retention.
        profiles: event_outcome.cloned().or(profiles),
        backend_profiles: event_outcome.cloned().or(backend_profiles),
        ui_profiles: event_outcome.cloned().or(ui_profiles),
        event,
        spans,
        event_attachments,
        span_attachments,
        trace_attachments,
        sessions,
        feedback,
        replays,
        monitors,
        backend_chunks,
        ui_chunks,
        logs,
        trace_metrics,
    };
    (enforcement, client_rate_limits)
}

fn filter(
    mut items: Items,
    scoping: &Scoping,
    cached: &RateLimits,
    quotas: &[Quota],
    records: &mut RecordKeeper<'_>,
) -> (Items, RateLimits) {
    let summary = EnvelopeSummary::compute_items(&items);
    let (enforcement, client_rate_limits) = resolve_limits(summary, scoping, cached, quotas);
    if !enforcement.has_rejections() {
        return (items, client_rate_limits);
    }
    // A logical event may have several creators, or no explicit event item. Record it once,
    // before its dependent items, using the final decision (including loss of all creators).
    if let Some((category, outcome)) = &enforcement.event {
        for category in std::iter::once(*category).chain(category.index_category()) {
            records.reject_err(outcome.clone(), (category, 1));
        }
    }
    items.retain(|item| {
        let event = enforcement.event.as_ref().map(|(_, outcome)| outcome);
        match item {
            Item::Event(item) => {
                apply_retention(records, RetentionAction::drop_on_rejection(event), item)
            }
            Item::UnrealReport(item) => {
                apply_retention(records, RetentionAction::drop_on_rejection(event), item)
            }
            Item::Security(item) => {
                apply_retention(records, RetentionAction::drop_on_rejection(event), item)
            }
            Item::RawSecurity(item) => {
                apply_retention(records, RetentionAction::drop_on_rejection(event), item)
            }
            Item::UserReportV2(item) => {
                apply_retention(records, RetentionAction::drop_on_rejection(event), item)
            }
            Item::FormData(item) => {
                apply_retention(records, RetentionAction::drop_on_rejection(event), item)
            }
            Item::MetricBuckets(_) | Item::ClientReport(_) | Item::Unknown(_) => true,
            Item::UserReport(item) => apply_retention(
                records,
                RetentionAction::drop_on_rejection(event.or(enforcement.feedback.as_ref())),
                item,
            ),
            Item::ReplayEvent(item) => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.replays.as_ref()),
                item,
            ),
            Item::ReplayRecording(item) => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.replays.as_ref()),
                item,
            ),
            Item::ReplayVideo(item) => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.replays.as_ref()),
                item,
            ),
            Item::CheckIn(item) => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.monitors.as_ref()),
                item,
            ),
            Item::Span(item) => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.spans.as_ref()),
                item,
            ),
            Item::Session(item) => apply_retention(
                records,
                enforcement
                    .sessions
                    .as_ref()
                    .map_or(RetentionAction::Keep, |_| RetentionAction::Drop(None)),
                item,
            ),
            Item::Sessions(item) => apply_retention(
                records,
                enforcement
                    .sessions
                    .as_ref()
                    .map_or(RetentionAction::Keep, |_| RetentionAction::Drop(None)),
                item,
            ),
            Item::Log(item) => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.logs.as_ref()),
                item,
            ),
            Item::TraceMetric(item) => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.trace_metrics.as_ref()),
                item,
            ),
            Item::ProfileChunk(item) => {
                let outcome = match item.profile_type() {
                    None => return true,
                    Some(ProfileType::Backend) => enforcement.backend_chunks.as_ref(),
                    Some(ProfileType::Ui) => enforcement.ui_chunks.as_ref(),
                };
                apply_retention(records, RetentionAction::drop_on_rejection(outcome), item)
            }
            Item::Integration(item) => match item.content_type() {
                Some(ContentType::Integration(Integration::Logs(_))) => apply_retention(
                    records,
                    RetentionAction::drop_on_rejection(enforcement.logs.as_ref()),
                    item,
                ),
                Some(ContentType::Integration(Integration::Spans(_))) => apply_retention(
                    records,
                    RetentionAction::drop_on_rejection(enforcement.spans.as_ref()),
                    item,
                ),
                _ => true,
            },
            Item::Transaction(item) => {
                apply_retention(records, RetentionAction::drop_on_rejection(event), item)
            }
            Item::Profile(item) => {
                let outcome = match item.profile_type() {
                    None => enforcement.profiles.as_ref(),
                    Some(ProfileType::Backend) => enforcement.backend_profiles.as_ref(),
                    Some(ProfileType::Ui) => enforcement.ui_profiles.as_ref(),
                };
                apply_retention(records, RetentionAction::drop_on_rejection(outcome), item)
            }
            Item::Attachment(item) => {
                let action = if let Some(outcome) = event {
                    RetentionAction::Drop(Some(outcome))
                } else {
                    match item.attachment_parent_type() {
                        AttachmentParentType::Span => RetentionAction::drop_on_rejection(
                            enforcement.span_attachments.as_ref(),
                        ),
                        AttachmentParentType::Trace => RetentionAction::drop_on_rejection(
                            enforcement.trace_attachments.as_ref(),
                        ),
                        AttachmentParentType::Event if item.carries_event_payload() => enforcement
                            .event_attachments
                            .as_ref()
                            .map_or(RetentionAction::Keep, RetentionAction::KeepRateLimited),
                        AttachmentParentType::Event => RetentionAction::drop_on_rejection(
                            enforcement.event_attachments.as_ref(),
                        ),
                    }
                };
                apply_retention(records, action, item)
            }
        }
    });
    (items, client_rate_limits)
}

// Compute quantities only after deciding to reject an item that was not already accounted for.
// Empty quantities need no special case; source quantities still transfer independently.
fn apply_retention<T: RejectionQuantities>(
    records: &mut RecordKeeper<'_>,
    action: RetentionAction<'_>,
    item: &mut T,
) -> bool {
    let (outcome, keep) = match action {
        RetentionAction::Keep => return true,
        RetentionAction::Drop(outcome) => (outcome, false),
        RetentionAction::KeepRateLimited(outcome) => (Some(outcome), true),
    };
    if item.rate_limited() {
        return keep;
    }
    for quantity in item
        .rejected_quantities()
        .into_iter()
        .filter(|(_, quantity)| *quantity > 0)
    {
        records.reject_err(outcome.cloned(), quantity);
    }
    // Source quantities transfer total accounting only; no indexed categories are added.
    if let Some(source) = item.source_quantities() {
        for quantity in [
            (DataCategory::Transaction, source.transactions),
            (DataCategory::Span, source.spans),
        ]
        .into_iter()
        .filter(|(_, quantity)| *quantity > 0)
        {
            records.reject_err(outcome.cloned(), quantity);
        }
    }
    if keep {
        item.set_rate_limited(true);
    }
    keep
}

#[cfg(test)]
mod tests;
