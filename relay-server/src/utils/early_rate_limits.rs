//! Resolve envelope-wide decisions, then dispatch original items into direct handlers.
//! The filter traverses items twice: headers first, followed by one in-place applying pass.
//! No per-item projections, deferred items, category registry, or payload reordering.

use crate::envelope::{
    AttachmentParentType, AttachmentType, ContentType, Envelope, Item, ItemHeaderKey, ItemType,
    Items,
};
use crate::integrations::Integration;
use crate::managed::{Managed, RecordKeeper};
use crate::services::outcome::Outcome;
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
                && item.header::<usize>(ItemHeaderKey::SpanCount).is_none()
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

// This is a final per-item decision, consumed immediately without caching an item plan.
enum RetentionAction<'a> {
    Keep,
    Drop(Option<&'a Outcome>),
    KeepRateLimited(&'a Outcome),
}

impl<'a> RetentionAction<'a> {
    fn drop_if_limited(outcome: Option<&'a Outcome>) -> Self {
        match outcome {
            None => Self::Keep,
            Some(outcome) => Self::Drop(Some(outcome)),
        }
    }
}

// Items that create an event share one event per envelope. Event-creating attachments with
// the same parent type share a retention decision, so only the present parent types and
// their first occurrence order matter. There is no need to store or rescan individual items.
// This tracks event-creating items only: ordinary trace attachments can coexist with
// SpanEventCreators, and ordinary span attachments with TraceEventCreators.
#[derive(Default)]
enum EventGroup {
    // No item creates an event. Items that require an event can still arrive separately.
    #[default]
    Absent,
    // At least one event-creating item survives attachment limits (a direct event or a required
    // event-parent attachment). The first event-creating item in wire order selects the category.
    SurvivesAttachmentLimits(DataCategory),
    // Only span-parent crash attachments create the error event.
    SpanEventCreators,
    // Only trace-parent crash attachments create the error event.
    TraceEventCreators,
    // Both parent types have event-creating attachments. Either surviving type preserves the
    // event; if both are rejected, use the reason from the type that appeared first.
    BothSpanFirst,
    BothTraceFirst,
    // The shared event quota rejected the group. Its reason takes precedence over storage
    // limits, and its category determines whether standalone spans inherit that rejection.
    Rejected(DataCategory, Outcome),
}

impl EventGroup {
    fn add_event_creator(&mut self, category: DataCategory, parent: AttachmentParentType) {
        *self = match (&*self, parent) {
            (Self::SurvivesAttachmentLimits(_) | Self::Rejected(..), _) => return,
            (Self::Absent, AttachmentParentType::Event) => Self::SurvivesAttachmentLimits(category),
            (_, AttachmentParentType::Event) => Self::SurvivesAttachmentLimits(DataCategory::Error),
            (Self::Absent, AttachmentParentType::Span) => Self::SpanEventCreators,
            (Self::Absent, AttachmentParentType::Trace) => Self::TraceEventCreators,
            (Self::SpanEventCreators, AttachmentParentType::Trace) => Self::BothSpanFirst,
            (Self::TraceEventCreators, AttachmentParentType::Span) => Self::BothTraceFirst,
            _ => return,
        };
    }
}

// Positive presence is enough: the early pass does not consume quota.
#[derive(Default)]
struct Summary {
    event: EventGroup,
    plain_attachments: bool,
    event_attachments: bool,
    span_attachments: bool,
    trace_attachments: bool,
    sessions: bool,
    untyped_profiles: bool,
    backend_profiles: bool,
    ui_profiles: bool,
    backend_chunks: bool,
    ui_chunks: bool,
    feedback: bool,
    replays: bool,
    monitors: bool,
    log_items: bool,
    log_bytes: bool,
    spans: bool,
    trace_metrics: bool,
    trace_metric_bytes: bool,
    metrics: bool,
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

fn summarize(items: &[Item]) -> Summary {
    let mut summary = Summary::default();
    for item in items {
        // Previously limited items and Nintendo input have no individual quantities here.
        // They still participate in the shared event, and metrics still advertise client limits.
        let count_individual = !item.rate_limited()
            && item.attachment_type() != Some(AttachmentType::NintendoSwitchDyingMessage);
        match item.ty() {
            ItemType::Event | ItemType::UnrealReport => summary
                .event
                .add_event_creator(DataCategory::Error, AttachmentParentType::Event),
            ItemType::Security | ItemType::RawSecurity => summary
                .event
                .add_event_creator(DataCategory::Security, AttachmentParentType::Event),
            ItemType::UserReportV2 => summary
                .event
                .add_event_creator(DataCategory::UserReportV2, AttachmentParentType::Event),
            ItemType::UserReport => summary.feedback |= count_individual,
            ItemType::Session | ItemType::Sessions => {
                summary.sessions |= count_individual && item.item_count() != Some(0)
            }
            ItemType::ReplayEvent | ItemType::ReplayRecording | ItemType::ReplayVideo => {
                summary.replays |= count_individual
            }
            ItemType::CheckIn => {
                summary.monitors |= count_individual && item.item_count() != Some(0)
            }
            ItemType::Span => summary.spans |= count_individual && item.item_count() != Some(0),
            ItemType::MetricBuckets => summary.metrics = true,
            ItemType::FormData | ItemType::ClientReport | ItemType::Unknown(_) => {}
            ItemType::Log => {
                summary.log_items |= count_individual && item.item_count() != Some(0);
                summary.log_bytes |= count_individual;
            }
            ItemType::Transaction => {
                summary
                    .event
                    .add_event_creator(DataCategory::Transaction, AttachmentParentType::Event);
                summary.spans |= count_individual;
            }
            ItemType::TraceMetric => {
                summary.trace_metrics |= count_individual && item.item_count() != Some(0);
                summary.trace_metric_bytes |= count_individual;
            }
            ItemType::Profile => match item.profile_type() {
                Some(ProfileType::Backend) => {
                    summary.backend_profiles |= count_individual && item.item_count() != Some(0)
                }
                Some(ProfileType::Ui) => {
                    summary.ui_profiles |= count_individual && item.item_count() != Some(0)
                }
                None => {
                    summary.untyped_profiles |= count_individual && item.item_count() != Some(0)
                }
            },
            ItemType::ProfileChunk => match item.profile_type() {
                None => {}
                Some(ProfileType::Backend) => {
                    summary.backend_chunks |= count_individual && item.item_count() != Some(0)
                }
                Some(ProfileType::Ui) => {
                    summary.ui_chunks |= count_individual && item.item_count() != Some(0)
                }
            },
            ItemType::Integration => match item.content_type() {
                Some(ContentType::Integration(Integration::Spans(_))) => {
                    summary.spans |= count_individual && item.item_count() != Some(0)
                }
                Some(ContentType::Integration(Integration::Logs(_))) => {
                    summary.log_items |= count_individual && item.item_count() != Some(0);
                    summary.log_bytes |= count_individual;
                }
                _ => {}
            },
            ItemType::Attachment => {
                let parent = item.attachment_parent_type();
                match parent {
                    AttachmentParentType::Event => summary.event_attachments |= count_individual,
                    AttachmentParentType::Span => summary.span_attachments |= count_individual,
                    AttachmentParentType::Trace => summary.trace_attachments |= count_individual,
                }
                match item.attachment_type() {
                    Some(
                        AttachmentType::AppleCrashReport
                        | AttachmentType::Minidump
                        | AttachmentType::EventPayload
                        | AttachmentType::Prosperodump
                        | AttachmentType::Breadcrumbs
                        | AttachmentType::NvGpuDump
                        | AttachmentType::NintendoSwitchDyingMessage,
                    ) => summary.event.add_event_creator(DataCategory::Error, parent),
                    Some(
                        AttachmentType::Attachment
                        | AttachmentType::UnrealContext
                        | AttachmentType::UnrealLogs
                        | AttachmentType::ViewHierarchy
                        | AttachmentType::Flamegraph
                        | AttachmentType::NvShaderDebug,
                    )
                    | None => summary.plain_attachments = true,
                }
            }
        }
    }
    summary
}

fn resolve_limits(
    summary: Summary,
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
    let event = match summary.event {
        EventGroup::Absent => EventGroup::Absent,
        state @ EventGroup::Rejected(..) => state,
        state => {
            let category = match state {
                EventGroup::SurvivesAttachmentLimits(category) => category,
                _ => DataCategory::Error,
            };
            match check(true, category, true) {
                None => state,
                Some(outcome) => EventGroup::Rejected(category, outcome),
            }
        }
    };
    let spans = match &event {
        EventGroup::Rejected(DataCategory::Transaction, outcome) => Some(outcome.clone()),
        EventGroup::Rejected(..) => None,
        _ => check(
            summary.spans || summary.span_attachments,
            DataCategory::Span,
            true,
        ),
    };
    let profiles_present =
        summary.untyped_profiles || summary.backend_profiles || summary.ui_profiles;
    let profiles = match &event {
        EventGroup::Rejected(_, outcome) => Some(outcome.clone()),
        EventGroup::Absent => check(profiles_present, DataCategory::Profile, true)
            .or_else(|| check(profiles_present, DataCategory::Transaction, true)),
        _ => check(profiles_present, DataCategory::Profile, true),
    };
    let event_attachments = match &event {
        EventGroup::Rejected(_, outcome) => Some(outcome.clone()),
        _ => check(
            summary.event_attachments,
            DataCategory::Attachment,
            summary.plain_attachments,
        )
        .or_else(|| {
            check(
                summary.event_attachments,
                DataCategory::AttachmentItem,
                summary.plain_attachments,
            )
        }),
    };
    let span_attachments = spans
        .clone()
        .or_else(|| check(summary.span_attachments, DataCategory::Attachment, false))
        .or_else(|| {
            check(
                summary.span_attachments,
                DataCategory::AttachmentItem,
                false,
            )
        });
    let trace_attachments = check(summary.trace_attachments, DataCategory::Attachment, false)
        .or_else(|| {
            check(
                summary.trace_attachments,
                DataCategory::AttachmentItem,
                false,
            )
        });
    let backend_profiles = profiles
        .clone()
        .or_else(|| check(summary.backend_profiles, DataCategory::ProfileBackend, true));
    let ui_profiles = profiles
        .clone()
        .or_else(|| check(summary.ui_profiles, DataCategory::ProfileUi, true));
    let sessions = check(summary.sessions, DataCategory::Session, true);
    let feedback = check(summary.feedback, DataCategory::UserReportV2, true);
    let replays = check(summary.replays, DataCategory::Replay, true);
    let monitors = check(summary.monitors, DataCategory::Monitor, true);
    let backend_chunks = check(summary.backend_chunks, DataCategory::ProfileChunk, true);
    let ui_chunks = check(summary.ui_chunks, DataCategory::ProfileChunkUi, true);
    let logs = check(summary.log_items, DataCategory::LogItem, true)
        .or_else(|| check(summary.log_bytes, DataCategory::LogByte, true));
    let trace_metrics =
        check(summary.trace_metrics, DataCategory::TraceMetric, true).or_else(|| {
            check(
                summary.trace_metric_bytes,
                DataCategory::TraceMetricByte,
                true,
            )
        });
    // Metrics stay in the envelope; advertise client rate limits across namespaces without parsing them.
    if summary.metrics {
        let scope = ItemScoping {
            namespace: MetricNamespaceScoping::Any,
            ..scoping.item(DataCategory::MetricBucket)
        };
        client_rate_limits.merge(cached.check_with_quotas(quotas, &scope));
    }
    // Settle the shared event once and retain its rejection for dependent items.
    let event = match (event, &span_attachments, &trace_attachments) {
        (EventGroup::Rejected(category, outcome), _, _) => {
            for category in std::iter::once(category).chain(category.index_category()) {
                records.reject_err(outcome.clone(), (category, 1));
            }
            Some(outcome)
        }
        (EventGroup::SpanEventCreators, Some(outcome), _)
        | (EventGroup::TraceEventCreators, _, Some(outcome))
        | (EventGroup::BothSpanFirst, Some(outcome), Some(_))
        | (EventGroup::BothTraceFirst, Some(_), Some(outcome)) => {
            records.reject_err(outcome.clone(), (DataCategory::Error, 1));
            Some(outcome.clone())
        }
        (
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
        event,
        spans,
        profiles,
        event_attachments,
        span_attachments,
        trace_attachments,
        backend_profiles,
        ui_profiles,
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
    let summary = summarize(&items);
    let (enforcement, client_rate_limits) =
        resolve_limits(summary, scoping, cached, quotas, records);
    items.retain(|item| {
        let event = enforcement.event.as_ref();
        match item.ty() {
            ItemType::Event
            | ItemType::UnrealReport
            | ItemType::Security
            | ItemType::RawSecurity
            | ItemType::UserReportV2
            | ItemType::FormData => {
                apply_retention(records, RetentionAction::drop_if_limited(event), [], item)
            }
            ItemType::Sessions
            | ItemType::MetricBuckets
            | ItemType::ClientReport
            | ItemType::Unknown(_) => true,
            ItemType::UserReport => apply_retention(
                records,
                RetentionAction::drop_if_limited(event.or(enforcement.feedback.as_ref())),
                [(DataCategory::UserReportV2, 1)],
                item,
            ),
            ItemType::ReplayEvent => apply_retention(
                records,
                RetentionAction::drop_if_limited(event.or(enforcement.replays.as_ref())),
                [(DataCategory::Replay, 1)],
                item,
            ),
            ItemType::ReplayRecording | ItemType::ReplayVideo => apply_retention(
                records,
                RetentionAction::drop_if_limited(enforcement.replays.as_ref()),
                [(DataCategory::Replay, 1)],
                item,
            ),
            ItemType::Session => apply_retention(
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
                RetentionAction::drop_if_limited(enforcement.monitors.as_ref()),
                [(
                    DataCategory::Monitor,
                    item.item_count().unwrap_or(1) as usize,
                )],
                item,
            ),
            ItemType::Log => apply_retention(
                records,
                RetentionAction::drop_if_limited(enforcement.logs.as_ref()),
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
                RetentionAction::drop_if_limited(enforcement.trace_metrics.as_ref()),
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
                    RetentionAction::drop_if_limited(enforcement.spans.as_ref()),
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
                    RetentionAction::drop_if_limited(enforcement.backend_chunks.as_ref()),
                    [(
                        DataCategory::ProfileChunk,
                        item.item_count().unwrap_or(1) as usize,
                    )],
                    item,
                ),
                Some(ProfileType::Ui) => apply_retention(
                    records,
                    RetentionAction::drop_if_limited(enforcement.ui_chunks.as_ref()),
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
                    RetentionAction::drop_if_limited(enforcement.logs.as_ref()),
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
                        RetentionAction::drop_if_limited(enforcement.spans.as_ref()),
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
                        RetentionAction::drop_if_limited(event.or(enforcement.profiles.as_ref())),
                        [
                            (DataCategory::Profile, count),
                            (DataCategory::ProfileIndexed, count),
                        ],
                        item,
                    ),
                    Some(ProfileType::Backend) => apply_retention(
                        records,
                        RetentionAction::drop_if_limited(
                            event.or(enforcement.backend_profiles.as_ref()),
                        ),
                        [
                            (DataCategory::Profile, count),
                            (DataCategory::ProfileIndexed, count),
                            (DataCategory::ProfileBackend, count),
                        ],
                        item,
                    ),
                    Some(ProfileType::Ui) => apply_retention(
                        records,
                        RetentionAction::drop_if_limited(
                            event.or(enforcement.ui_profiles.as_ref()),
                        ),
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
                let action = match (event, item.attachment_parent_type(), item.attachment_type()) {
                    (Some(outcome), _, _) => RetentionAction::Drop(Some(outcome)),
                    (None, AttachmentParentType::Span, _) => {
                        RetentionAction::drop_if_limited(enforcement.span_attachments.as_ref())
                    }
                    (None, AttachmentParentType::Trace, _) => {
                        RetentionAction::drop_if_limited(enforcement.trace_attachments.as_ref())
                    }
                    (
                        None,
                        AttachmentParentType::Event,
                        Some(
                            AttachmentType::AppleCrashReport
                            | AttachmentType::Minidump
                            | AttachmentType::EventPayload
                            | AttachmentType::Prosperodump
                            | AttachmentType::Breadcrumbs
                            | AttachmentType::NvGpuDump
                            | AttachmentType::NintendoSwitchDyingMessage,
                        ),
                    ) => enforcement
                        .event_attachments
                        .as_ref()
                        .map_or(RetentionAction::Keep, RetentionAction::KeepRateLimited),
                    (None, AttachmentParentType::Event, _) => {
                        RetentionAction::drop_if_limited(enforcement.event_attachments.as_ref())
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

// Called only for rejected transactions. Keep a supplied count (including zero); otherwise
// count the JSON array without constructing spans or caching anything on the discarded item.
fn calc_span_count(item: &Item) -> usize {
    #[derive(serde::Deserialize)]
    struct PartialEvent {
        spans: crate::utils::SeqCount,
    }

    item.header(ItemHeaderKey::SpanCount).unwrap_or_else(|| {
        relay_statsd::metric!(timer(crate::statsd::RelayTimers::CheckNestedSpans), {
            serde_json::from_slice::<PartialEvent>(&item.payload()).map_or(0, |event| event.spans.0)
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
    match item.attachment_type() {
        Some(AttachmentType::NintendoSwitchDyingMessage) => {}
        _ => {
            for quantity in quantities.into_iter().filter(|(_, quantity)| *quantity > 0) {
                records.reject_err(outcome.cloned(), quantity);
            }
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
