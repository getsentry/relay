//! Apply cached rate limits and zero quotas before processing envelope payloads.
//! Summarize item headers, resolve limits and event dependencies, then filter items in place.

use crate::envelope::{
    AttachmentParentType, AttachmentType, ContentType, Envelope, Item, ItemHeaderKey, ItemType,
    Items,
};
use crate::integrations::Integration;
use crate::managed::{Managed, RecordKeeper};
use crate::services::outcome::Outcome;
use crate::utils::{AttachmentRole, EnvelopeSummary, EventGroup};
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
        if envelope.items().any(|item| {
            *item.ty() == ItemType::Transaction
                && item.header::<u32>(ItemHeaderKey::SpanCount).is_none()
        }) {
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
    event: Option<Outcome>,
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
    records: &mut RecordKeeper<'_>,
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
    // Record the event rejection once and use its reason for dependent items.
    let event = match (event, summary.event, &span_attachments, &trace_attachments) {
        (Some((category, outcome)), _, _, _) => {
            for category in std::iter::once(category).chain(category.index_category()) {
                records.reject_err(outcome.clone(), (category, 1));
            }
            Some(outcome)
        }
        (None, EventGroup::SpanEventCreators, Some(outcome), _)
        | (None, EventGroup::TraceEventCreators, _, Some(outcome))
        | (None, EventGroup::BothSpanFirst, Some(outcome), Some(_))
        | (None, EventGroup::BothTraceFirst, Some(_), Some(outcome)) => {
            records.reject_err(outcome.clone(), (DataCategory::Error, 1));
            Some(outcome.clone())
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
    let enforcement = Enforcement {
        // Attachment limits may have just removed the last event-creating item. Apply
        // that event rejection to profiles before retention.
        profiles: event.clone().or(profiles),
        backend_profiles: event.clone().or(backend_profiles),
        ui_profiles: event.clone().or(ui_profiles),
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
    let (enforcement, client_rate_limits) =
        resolve_limits(summary, scoping, cached, quotas, records);
    if !enforcement.has_rejections() {
        return (items, client_rate_limits);
    }
    items.retain(|item| {
        let event = enforcement.event.as_ref();
        match item.ty() {
            ItemType::Event
            | ItemType::UnrealReport
            | ItemType::Security
            | ItemType::RawSecurity
            | ItemType::UserReportV2
            | ItemType::FormData => {
                apply_retention(records, RetentionAction::drop_on_rejection(event), [], item)
            }
            ItemType::MetricBuckets | ItemType::ClientReport | ItemType::Unknown(_) => true,
            ItemType::UserReport => apply_retention(
                records,
                RetentionAction::drop_on_rejection(event.or(enforcement.feedback.as_ref())),
                [(DataCategory::UserReportV2, 1)],
                item,
            ),
            ItemType::ReplayEvent | ItemType::ReplayRecording | ItemType::ReplayVideo => {
                apply_retention(
                    records,
                    RetentionAction::drop_on_rejection(enforcement.replays.as_ref()),
                    [(DataCategory::Replay, 1)],
                    item,
                )
            }
            ItemType::Session | ItemType::Sessions => apply_retention(
                records,
                enforcement
                    .sessions
                    .as_ref()
                    .map_or(RetentionAction::Keep, |_| RetentionAction::Drop(None)),
                [(
                    DataCategory::Session,
                    item.item_count().unwrap_or(1) as usize,
                )],
                item,
            ),
            ItemType::CheckIn => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.monitors.as_ref()),
                [(
                    DataCategory::Monitor,
                    item.item_count().unwrap_or(1) as usize,
                )],
                item,
            ),
            ItemType::Log => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.logs.as_ref()),
                [
                    (
                        DataCategory::LogItem,
                        item.item_count().unwrap_or(1) as usize,
                    ),
                    (DataCategory::LogByte, item.len().max(1)),
                ],
                item,
            ),
            ItemType::TraceMetric => apply_retention(
                records,
                RetentionAction::drop_on_rejection(enforcement.trace_metrics.as_ref()),
                [
                    (
                        DataCategory::TraceMetric,
                        item.item_count().unwrap_or(1) as usize,
                    ),
                    (DataCategory::TraceMetricByte, item.len().max(1)),
                ],
                item,
            ),
            ItemType::Span => {
                let count = item.item_count().unwrap_or(1) as usize;
                apply_retention(
                    records,
                    RetentionAction::drop_on_rejection(enforcement.spans.as_ref()),
                    [
                        (DataCategory::Span, count),
                        (DataCategory::SpanIndexed, count),
                    ],
                    item,
                )
            }
            ItemType::ProfileChunk => match item.profile_type() {
                None => true,
                Some(ProfileType::Backend) => apply_retention(
                    records,
                    RetentionAction::drop_on_rejection(enforcement.backend_chunks.as_ref()),
                    [(
                        DataCategory::ProfileChunk,
                        item.item_count().unwrap_or(1) as usize,
                    )],
                    item,
                ),
                Some(ProfileType::Ui) => apply_retention(
                    records,
                    RetentionAction::drop_on_rejection(enforcement.ui_chunks.as_ref()),
                    [(
                        DataCategory::ProfileChunkUi,
                        item.item_count().unwrap_or(1) as usize,
                    )],
                    item,
                ),
            },
            ItemType::Integration => match item.content_type() {
                Some(ContentType::Integration(Integration::Logs(_))) => apply_retention(
                    records,
                    RetentionAction::drop_on_rejection(enforcement.logs.as_ref()),
                    [
                        (
                            DataCategory::LogItem,
                            item.item_count().unwrap_or(1) as usize,
                        ),
                        (DataCategory::LogByte, item.len().max(1)),
                    ],
                    item,
                ),
                Some(ContentType::Integration(Integration::Spans(_))) => {
                    let count = item.item_count().unwrap_or(1) as usize;
                    apply_retention(
                        records,
                        RetentionAction::drop_on_rejection(enforcement.spans.as_ref()),
                        [
                            (DataCategory::Span, count),
                            (DataCategory::SpanIndexed, count),
                        ],
                        item,
                    )
                }
                _ => true,
            },
            ItemType::Transaction => match event {
                None => true,
                Some(outcome) => {
                    let spans = 1 + calc_span_count(item);
                    apply_retention(
                        records,
                        RetentionAction::Drop(Some(outcome)),
                        [
                            (DataCategory::Span, spans),
                            (DataCategory::SpanIndexed, spans),
                        ],
                        item,
                    )
                }
            },
            ItemType::Profile => {
                let count = item.item_count().unwrap_or(1) as usize;
                match item.profile_type() {
                    None => apply_retention(
                        records,
                        RetentionAction::drop_on_rejection(enforcement.profiles.as_ref()),
                        [
                            (DataCategory::Profile, count),
                            (DataCategory::ProfileIndexed, count),
                        ],
                        item,
                    ),
                    Some(ProfileType::Backend) => apply_retention(
                        records,
                        RetentionAction::drop_on_rejection(enforcement.backend_profiles.as_ref()),
                        [
                            (DataCategory::Profile, count),
                            (DataCategory::ProfileIndexed, count),
                            (DataCategory::ProfileBackend, count),
                        ],
                        item,
                    ),
                    Some(ProfileType::Ui) => apply_retention(
                        records,
                        RetentionAction::drop_on_rejection(enforcement.ui_profiles.as_ref()),
                        [
                            (DataCategory::Profile, count),
                            (DataCategory::ProfileIndexed, count),
                            (DataCategory::ProfileUi, count),
                        ],
                        item,
                    ),
                }
            }
            ItemType::Attachment => {
                let action = if let Some(outcome) = event {
                    RetentionAction::Drop(Some(outcome))
                } else {
                    match (
                        item.attachment_parent_type(),
                        AttachmentRole::from_type(item.attachment_type()),
                    ) {
                        (AttachmentParentType::Span, _) => RetentionAction::drop_on_rejection(
                            enforcement.span_attachments.as_ref(),
                        ),
                        (AttachmentParentType::Trace, _) => RetentionAction::drop_on_rejection(
                            enforcement.trace_attachments.as_ref(),
                        ),
                        (AttachmentParentType::Event, AttachmentRole::CreatesEvent) => enforcement
                            .event_attachments
                            .as_ref()
                            .map_or(RetentionAction::Keep, RetentionAction::KeepRateLimited),
                        (AttachmentParentType::Event, AttachmentRole::Plain) => {
                            RetentionAction::drop_on_rejection(
                                enforcement.event_attachments.as_ref(),
                            )
                        }
                    }
                };
                apply_retention(
                    records,
                    action,
                    [
                        (DataCategory::Attachment, item.attachment_body_size()),
                        (DataCategory::AttachmentItem, 1),
                    ],
                    item,
                )
            }
        }
    });
    (items, client_rate_limits)
}

// Called only for rejected transactions. Read supplied counts as u32, matching Item::span_count;
// otherwise count the JSON array without constructing spans or caching anything on the discarded item.
fn calc_span_count(item: &Item) -> usize {
    #[derive(serde::Deserialize)]
    struct PartialEvent {
        spans: crate::utils::SeqCount,
    }

    item.header::<u32>(ItemHeaderKey::SpanCount)
        .map(|count| count as usize)
        .unwrap_or_else(|| {
            relay_statsd::metric!(timer(crate::statsd::RelayTimers::CheckNestedSpans), {
                serde_json::from_slice::<PartialEvent>(&item.payload())
                    .map_or(0, |event| event.spans.0)
            })
        })
}

// Applies the final retention decision and records its rejected quantities once.
fn apply_retention(
    records: &mut RecordKeeper<'_>,
    action: RetentionAction<'_>,
    quantities: impl IntoIterator<Item = (DataCategory, usize)>,
    item: &mut Item,
) -> bool {
    let (outcome, keep) = match action {
        RetentionAction::Keep => return true,
        RetentionAction::Drop(outcome) => (outcome, false),
        RetentionAction::KeepRateLimited(outcome) => (Some(outcome), true),
    };
    if item.rate_limited() {
        return keep;
    }
    if *item.ty() != ItemType::Attachment
        || item.attachment_type() != Some(AttachmentType::NintendoSwitchDyingMessage)
    {
        for quantity in quantities.into_iter().filter(|(_, quantity)| *quantity > 0) {
            records.reject_err(outcome.cloned(), quantity);
        }
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
