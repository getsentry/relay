//! Apply cached rate limits and zero quotas before processing envelope payloads.

use crate::envelope::{
    AttachmentParentType, ContentType, Envelope, Item, ItemStorage, RejectionQuantities,
};
#[cfg(test)]
use crate::envelope::{AttachmentType, ItemType};
use crate::integrations::Integration;
use crate::managed::{Managed, RecordKeeper};
use crate::services::outcome::Outcome;
use crate::utils::EnvelopeSummary;
use enum_map::EnumMap;
use relay_profiling::ProfileType;
use relay_quotas::{DataCategory, ItemScoping, MetricNamespaceScoping, Quota, RateLimits, Scoping};

/// Applies cached limits and zero quotas before payload processing, without consuming quota.
///
/// Returns the rate limits to advertise to the client.
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
        // The header summary omits embedded spans when span_count is absent. Rejection
        // may count them from the payload, so the debug quantities can differ.
        #[cfg(debug_assertions)]
        if envelope
            .items()
            .any(|item| matches!(item, Item::Transaction(transaction) if transaction.span_count().is_none()))
        {
            records.lenient(DataCategory::Span);
            records.lenient(DataCategory::SpanIndexed);
        }
        client_rate_limits = filter(envelope, &scoping, cached, quotas, records);
    });
    client_rate_limits
}

/// One resolved rejection and its accumulated quantities.
///
/// Keep counts local to avoid creating and processing an outcome message per item.
/// Flush totals into RecordKeeper after retention.
struct Rejection {
    outcome: Option<Outcome>,
    quantities: EnumMap<DataCategory, usize>,
}

impl Rejection {
    fn new(outcome: Outcome) -> Self {
        Self {
            outcome: Some(outcome),
            quantities: EnumMap::default(),
        }
    }

    /// Silent removals still participate in managed accounting.
    fn silence(&mut self) {
        self.outcome = None;
    }

    fn add(&mut self, (category, quantity): (DataCategory, usize)) {
        self.quantities[category] += quantity;
    }

    fn flush(self, records: &mut RecordKeeper<'_>) {
        // Scan all data categories per rejection group; envelopes typically activate only a few groups.
        for (category, quantity) in self.quantities {
            if quantity > 0 {
                records.reject_err(self.outcome.clone(), (category, quantity));
            }
        }
    }
}

enum RetentionAction<'a> {
    Keep,
    Drop(&'a mut Rejection),
    KeepRateLimited(&'a mut Rejection),
}

impl<'a> RetentionAction<'a> {
    fn drop_on_rejection(rejection: Option<&'a mut Rejection>) -> Self {
        match rejection {
            None => Self::Keep,
            Some(rejection) => Self::Drop(rejection),
        }
    }
}

struct Enforcement {
    event: Option<Rejection>,
    spans: Option<Rejection>,
    profiles: Option<Rejection>,
    event_attachments: Option<Rejection>,
    span_attachments: Option<Rejection>,
    trace_attachments: Option<Rejection>,
    backend_profiles: Option<Rejection>,
    ui_profiles: Option<Rejection>,
    sessions: Option<Rejection>,
    feedback: Option<Rejection>,
    replays: Option<Rejection>,
    monitors: Option<Rejection>,
    backend_chunks: Option<Rejection>,
    ui_chunks: Option<Rejection>,
    logs: Option<Rejection>,
    trace_metrics: Option<Rejection>,
}

impl Enforcement {
    fn has_rejections(&self) -> bool {
        // Keep this exhaustive so new decisions must be handled here.
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

    fn flush(self, records: &mut RecordKeeper<'_>) {
        let Self {
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
        } = self;
        for rejection in [
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
        ]
        .into_iter()
        .flatten()
        {
            rejection.flush(records);
        }
    }
}

fn filter(
    envelope: &mut Envelope,
    scoping: &Scoping,
    cached: &RateLimits,
    quotas: &[Quota],
    records: &mut RecordKeeper<'_>,
) -> RateLimits {
    let summary = EnvelopeSummary::compute(envelope);
    let (mut enforcement, client_rate_limits) = resolve_limits(summary, scoping, cached, quotas);
    if !enforcement.has_rejections() {
        return client_rate_limits;
    }
    if let Some(sessions) = enforcement.sessions.as_mut() {
        sessions.silence();
    }
    envelope.retain_items(|item| {
        let event = enforcement.event.as_mut();
        match item {
            Item::Event(item) => apply_retention(RetentionAction::drop_on_rejection(event), item),
            Item::UnrealReport(item) => {
                apply_retention(RetentionAction::drop_on_rejection(event), item)
            }
            Item::Security(item) => {
                apply_retention(RetentionAction::drop_on_rejection(event), item)
            }
            Item::RawSecurity(item) => {
                apply_retention(RetentionAction::drop_on_rejection(event), item)
            }
            Item::UserReportV2(item) => {
                apply_retention(RetentionAction::drop_on_rejection(event), item)
            }
            Item::FormData(item) => {
                apply_retention(RetentionAction::drop_on_rejection(event), item)
            }
            Item::MetricBuckets(_) | Item::ClientReport(_) | Item::Unknown(_) => true,
            Item::UserReport(item) => apply_retention(
                RetentionAction::drop_on_rejection(event.or(enforcement.feedback.as_mut())),
                item,
            ),
            Item::ReplayEvent(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.replays.as_mut()),
                item,
            ),
            Item::ReplayRecording(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.replays.as_mut()),
                item,
            ),
            Item::ReplayVideo(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.replays.as_mut()),
                item,
            ),
            Item::CheckIn(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.monitors.as_mut()),
                item,
            ),
            Item::Span(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.spans.as_mut()),
                item,
            ),
            Item::Session(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.sessions.as_mut()),
                item,
            ),
            Item::Sessions(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.sessions.as_mut()),
                item,
            ),
            Item::Log(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.logs.as_mut()),
                item,
            ),
            Item::TraceMetric(item) => apply_retention(
                RetentionAction::drop_on_rejection(enforcement.trace_metrics.as_mut()),
                item,
            ),
            Item::ProfileChunk(item) => {
                let outcome = match item.profile_type() {
                    None => return true,
                    Some(ProfileType::Backend) => enforcement.backend_chunks.as_mut(),
                    Some(ProfileType::Ui) => enforcement.ui_chunks.as_mut(),
                };
                apply_retention(RetentionAction::drop_on_rejection(outcome), item)
            }
            Item::Integration(item) => match item.content_type() {
                Some(ContentType::Integration(Integration::Errors(_))) => {
                    apply_retention(RetentionAction::drop_on_rejection(event), item)
                }
                Some(ContentType::Integration(Integration::Logs(_))) => apply_retention(
                    RetentionAction::drop_on_rejection(enforcement.logs.as_mut()),
                    item,
                ),
                Some(ContentType::Integration(Integration::Spans(_))) => apply_retention(
                    RetentionAction::drop_on_rejection(enforcement.spans.as_mut()),
                    item,
                ),
                _ => true,
            },
            Item::Transaction(item) => {
                apply_retention(RetentionAction::drop_on_rejection(event), item)
            }
            Item::Profile(item) => {
                let outcome = match item.profile_type() {
                    None => enforcement.profiles.as_mut(),
                    Some(ProfileType::Backend) => enforcement.backend_profiles.as_mut(),
                    Some(ProfileType::Ui) => enforcement.ui_profiles.as_mut(),
                };
                apply_retention(RetentionAction::drop_on_rejection(outcome), item)
            }
            Item::Attachment(item) => {
                let action = match item.attachment_parent_type() {
                    AttachmentParentType::Span => {
                        RetentionAction::drop_on_rejection(enforcement.span_attachments.as_mut())
                    }
                    AttachmentParentType::Trace => RetentionAction::drop_on_rejection(
                        event.or(enforcement.trace_attachments.as_mut()),
                    ),
                    AttachmentParentType::Event => match event {
                        Some(rejection) => RetentionAction::Drop(rejection),
                        None if item.carries_event_payload() => enforcement
                            .event_attachments
                            .as_mut()
                            .map_or(RetentionAction::Keep, RetentionAction::KeepRateLimited),
                        None => RetentionAction::drop_on_rejection(
                            enforcement.event_attachments.as_mut(),
                        ),
                    },
                };
                apply_retention(action, item)
            }
        }
    });
    enforcement.flush(records);
    client_rate_limits
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
        .event_category
        .and_then(|category| check(true, category, true).map(|outcome| (category, outcome)));
    let spans = check(
        summary.span_quantity > 0 || summary.has_span_attachments,
        DataCategory::Span,
        true,
    );
    let profiles = match &event {
        Some((_, outcome)) => Some(outcome.clone()),
        None => check(
            summary.profile_quantity.total > 0,
            DataCategory::Profile,
            true,
        ),
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
    let event_outcome = event.as_ref().map(|(_, outcome)| outcome);
    let enforcement = Enforcement {
        backend_profiles: event_outcome
            .cloned()
            .or(profiles.clone().or_else(|| {
                check(
                    summary.profile_quantity.backend > 0,
                    DataCategory::ProfileBackend,
                    true,
                )
            }))
            .map(Rejection::new),
        ui_profiles: event_outcome
            .cloned()
            .or(profiles.clone().or_else(|| {
                check(
                    summary.profile_quantity.ui > 0,
                    DataCategory::ProfileUi,
                    true,
                )
            }))
            .map(Rejection::new),
        profiles: event_outcome.cloned().or(profiles).map(Rejection::new),
        event: event.map(|(category, outcome)| {
            // An envelope can have several event creators but accounts for a single event.
            let mut rejection = Rejection::new(outcome);
            for category in std::iter::once(category).chain(category.index_category()) {
                rejection.add((category, 1));
            }
            rejection
        }),
        spans: spans.map(Rejection::new),
        event_attachments: event_attachments.map(Rejection::new),
        span_attachments: span_attachments.map(Rejection::new),
        trace_attachments: trace_attachments.map(Rejection::new),
        sessions: check(summary.session_quantity > 0, DataCategory::Session, true)
            .map(Rejection::new),
        feedback: check(summary.has_user_reports, DataCategory::UserReportV2, true)
            .map(Rejection::new),
        replays: check(summary.replay_quantity > 0, DataCategory::Replay, true).map(Rejection::new),
        monitors: check(summary.monitor_quantity > 0, DataCategory::Monitor, true)
            .map(Rejection::new),
        backend_chunks: check(
            summary.profile_chunk_quantity > 0,
            DataCategory::ProfileChunk,
            true,
        )
        .map(Rejection::new),
        ui_chunks: check(
            summary.profile_chunk_ui_quantity > 0,
            DataCategory::ProfileChunkUi,
            true,
        )
        .map(Rejection::new),
        logs: check(summary.log_item_quantity > 0, DataCategory::LogItem, true)
            .or_else(|| check(summary.log_byte_quantity > 0, DataCategory::LogByte, true))
            .map(Rejection::new),
        trace_metrics: check(
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
        })
        .map(Rejection::new),
    };
    // Metric namespaces require payload parsing. Return backoff here and leave
    // filtering to later processing.
    if summary.has_metrics {
        client_rate_limits.merge(cached.check_with_quotas(
            quotas,
            &ItemScoping {
                namespace: MetricNamespaceScoping::Any,
                ..scoping.item(DataCategory::MetricBucket)
            },
        ));
    }
    (enforcement, client_rate_limits)
}

// Compute quantities only after deciding to reject an item that was not already accounted for.
// Empty quantities need no special case; source quantities still transfer independently.
fn apply_retention<T>(action: RetentionAction<'_>, item: &mut T) -> bool
where
    T: RejectionQuantities + std::ops::DerefMut<Target = ItemStorage>,
{
    let (rejection, keep) = match action {
        RetentionAction::Keep => return true,
        RetentionAction::Drop(rejection) => (rejection, false),
        RetentionAction::KeepRateLimited(rejection) => (rejection, true),
    };
    if item.rate_limited() {
        return keep;
    }
    for quantity in item
        .rejected_quantities()
        .into_iter()
        .filter(|(_, quantity)| *quantity > 0)
    {
        rejection.add(quantity);
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
            rejection.add(quantity);
        }
    }
    if keep {
        item.set_rate_limited(true);
    }
    keep
}

#[cfg(test)]
mod tests;
