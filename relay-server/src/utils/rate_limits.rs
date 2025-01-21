use std::fmt::{self, Write};

use relay_quotas::{
    DataCategories, DataCategory, ItemScoping, QuotaScope, RateLimit, RateLimitScope, RateLimits,
    ReasonCode, Scoping,
};

use crate::envelope::{CountFor, Envelope, Item, ItemType};
use crate::services::outcome::Outcome;
use crate::utils::ManagedEnvelope;

/// Name of the rate limits header.
pub const RATE_LIMITS_HEADER: &str = "X-Sentry-Rate-Limits";

/// Formats the `X-Sentry-Rate-Limits` header.
pub fn format_rate_limits(rate_limits: &RateLimits) -> String {
    let mut header = String::new();

    for rate_limit in rate_limits {
        if !header.is_empty() {
            header.push_str(", ");
        }

        write!(header, "{}:", rate_limit.retry_after.remaining_seconds()).ok();

        for (index, category) in rate_limit.categories.iter().enumerate() {
            if index > 0 {
                header.push(';');
            }
            write!(header, "{category}").ok();
        }

        write!(header, ":{}", rate_limit.scope.name()).ok();

        if let Some(ref reason_code) = rate_limit.reason_code {
            write!(header, ":{reason_code}").ok();
        } else if !rate_limit.namespaces.is_empty() {
            write!(header, ":").ok(); // delimits the empty reason code for namespaces
        }

        for (index, namespace) in rate_limit.namespaces.iter().enumerate() {
            header.push(if index == 0 { ':' } else { ';' });
            write!(header, "{namespace}").ok();
        }
    }

    header
}

/// Parses the `X-Sentry-Rate-Limits` header.
pub fn parse_rate_limits(scoping: &Scoping, string: &str) -> RateLimits {
    let mut rate_limits = RateLimits::new();

    for limit in string.split(',') {
        let limit = limit.trim();
        if limit.is_empty() {
            continue;
        }

        let mut components = limit.split(':');

        let retry_after = match components.next().and_then(|s| s.parse().ok()) {
            Some(retry_after) => retry_after,
            None => continue,
        };

        let mut categories = DataCategories::new();
        for category in components.next().unwrap_or("").split(';') {
            if !category.is_empty() {
                categories.push(DataCategory::from_name(category));
            }
        }

        let quota_scope = QuotaScope::from_name(components.next().unwrap_or(""));
        let scope = RateLimitScope::for_quota(scoping, quota_scope);

        let reason_code = components
            .next()
            .filter(|s| !s.is_empty())
            .map(ReasonCode::new);

        let namespace = components
            .next()
            .unwrap_or("")
            .split(';')
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.parse().ok())
            .collect();

        rate_limits.add(RateLimit {
            categories,
            scope,
            reason_code,
            retry_after,
            namespaces: namespace,
        });
    }

    rate_limits
}

/// Infer the data category from an item.
///
/// Categories depend mostly on the item type, with a few special cases:
/// - `Event`: the category is inferred from the event type. This requires the `event_type` header
///   to be set on the event item.
/// - `Attachment`: If the attachment creates an event (e.g. for minidumps), the category is assumed
///   to be `Error`.
fn infer_event_category(item: &Item) -> Option<DataCategory> {
    match item.ty() {
        ItemType::Event => Some(DataCategory::Error),
        ItemType::Transaction => Some(DataCategory::Transaction),
        ItemType::Security | ItemType::RawSecurity => Some(DataCategory::Security),
        ItemType::Nel => Some(DataCategory::Error),
        ItemType::UnrealReport => Some(DataCategory::Error),
        ItemType::UserReportV2 => Some(DataCategory::UserReportV2),
        ItemType::Attachment if item.creates_event() => Some(DataCategory::Error),
        ItemType::Attachment => None,
        ItemType::Session => None,
        ItemType::Sessions => None,
        ItemType::Statsd => None,
        ItemType::MetricBuckets => None,
        ItemType::FormData => None,
        ItemType::UserReport => None,
        ItemType::Profile => None,
        ItemType::ReplayEvent => None,
        ItemType::ReplayRecording => None,
        ItemType::ReplayVideo => None,
        ItemType::ClientReport => None,
        ItemType::CheckIn => None,
        ItemType::Span => None,
        ItemType::OtelSpan => None,
        ItemType::OtelTracesData => None,
        ItemType::ProfileChunk => Some(DataCategory::ProfileChunk),
        ItemType::Unknown(_) => None,
    }
}

/// A summary of `Envelope` contents.
///
/// Summarizes the contained event, size of attachments, session updates, and whether there are
/// plain attachments. This is used for efficient rate limiting or outcome handling.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvelopeSummary {
    /// The data category of the event in the envelope. `None` if there is no event.
    pub event_category: Option<DataCategory>,

    /// The quantity of all attachments combined in bytes.
    pub attachment_quantity: usize,

    /// The number of attachments.
    pub attachment_item_quantity: usize,

    /// The number of all session updates.
    pub session_quantity: usize,

    /// The number of profiles.
    pub profile_quantity: usize,

    /// The number of replays.
    pub replay_quantity: usize,

    /// The number of monitor check-ins.
    pub monitor_quantity: usize,

    /// Secondary number of transactions.
    ///
    /// This is 0 for envelopes which contain a transaction,
    /// only secondary transaction quantity should be tracked here,
    /// these are for example transaction counts extracted from metrics.
    ///
    /// A "primary" transaction is contained within the envelope,
    /// marking the envelope data category a [`DataCategory::Transaction`].
    pub secondary_transaction_quantity: usize,

    /// See `secondary_transaction_quantity`.
    pub secondary_span_quantity: usize,

    /// The number of standalone spans.
    pub span_quantity: usize,

    /// Indicates that the envelope contains regular attachments that do not create event payloads.
    pub has_plain_attachments: bool,

    /// The payload size of this envelope.
    pub payload_size: usize,

    /// The number of profile chunks in this envelope.
    pub profile_chunk_quantity: usize,
}

impl EnvelopeSummary {
    /// Creates an empty summary.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Creates an envelope summary and aggregates the given envelope.
    pub fn compute(envelope: &Envelope) -> Self {
        let mut summary = Self::empty();

        for item in envelope.items() {
            if item.creates_event() {
                summary.infer_category(item);
            } else if item.ty() == &ItemType::Attachment {
                // Plain attachments do not create events.
                summary.has_plain_attachments = true;
            }

            // If the item has been rate limited before, the quota has been consumed and outcomes
            // emitted. We can skip it here.
            if item.rate_limited() {
                continue;
            }

            if let Some(source_quantities) = item.source_quantities() {
                summary.secondary_transaction_quantity += source_quantities.transactions;
                summary.secondary_span_quantity += source_quantities.spans;
                summary.profile_quantity += source_quantities.profiles;
            }

            summary.payload_size += item.len();
            for (category, quantity) in item.quantities(CountFor::RateLimits) {
                summary.add_quantity(category, quantity);
            }
        }

        summary
    }

    fn add_quantity(&mut self, category: DataCategory, quantity: usize) {
        let target_quantity = match category {
            DataCategory::Attachment => &mut self.attachment_quantity,
            DataCategory::AttachmentItem => &mut self.attachment_item_quantity,
            DataCategory::Session => &mut self.session_quantity,
            DataCategory::Profile => &mut self.profile_quantity,
            DataCategory::Replay => &mut self.replay_quantity,
            DataCategory::ReplayVideo => &mut self.replay_quantity,
            DataCategory::Monitor => &mut self.monitor_quantity,
            DataCategory::Span => &mut self.span_quantity,
            DataCategory::ProfileChunk => &mut self.profile_chunk_quantity,
            // TODO: This catch-all return looks dangerous
            _ => return,
        };
        *target_quantity += quantity;
    }

    /// Infers the appropriate [`DataCategory`] for the envelope [`Item`].
    ///
    /// The inferred category is only applied to the [`EnvelopeSummary`] if there is not yet
    /// a category set.
    fn infer_category(&mut self, item: &Item) {
        if matches!(self.event_category, None | Some(DataCategory::Default)) {
            if let Some(category) = infer_event_category(item) {
                self.event_category = Some(category);
            }
        }
    }
}

/// Rate limiting information for a data category.
#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
pub struct CategoryLimit {
    /// The limited data category.
    category: DataCategory,
    /// The total rate limited quantity across all items.
    ///
    /// This will be `0` if nothing was rate limited.
    quantity: usize,
    /// The reason code of the applied rate limit.
    ///
    /// Defaults to `None` if the quota does not declare a reason code.
    reason_code: Option<ReasonCode>,
}

impl CategoryLimit {
    /// Creates a new `CategoryLimit`.
    ///
    /// Returns an inactive limit if `quantity` is `0` or `rate_limit` is `None`.
    fn new(category: DataCategory, quantity: usize, rate_limit: Option<&RateLimit>) -> Self {
        match rate_limit {
            Some(limit) => Self {
                category,
                quantity,
                reason_code: limit.reason_code.clone(),
            },
            None => Self::default(),
        }
    }

    /// Recreates the category limit, if active, for a new category with the same reason.
    pub fn clone_for(&self, category: DataCategory, quantity: usize) -> CategoryLimit {
        if !self.is_active() {
            return Self::default();
        }

        Self {
            category,
            quantity,
            reason_code: self.reason_code.clone(),
        }
    }

    /// Returns `true` if this is an active limit.
    ///
    /// This indicates that the category is limited and a certain quantity is removed from the
    /// Envelope. If the limit is inactive, there is no change.
    pub fn is_active(&self) -> bool {
        self.quantity > 0
    }
}

impl Default for CategoryLimit {
    fn default() -> Self {
        Self {
            category: DataCategory::Default,
            quantity: 0,
            reason_code: None,
        }
    }
}

/// Information on the limited quantities returned by [`EnvelopeLimiter::compute`].
#[derive(Default, Debug)]
#[cfg_attr(test, derive(Clone))]
pub struct Enforcement {
    /// The event item rate limit.
    pub event: CategoryLimit,
    /// The rate limit for the indexed category of the event.
    pub event_indexed: CategoryLimit,
    /// The combined attachment bytes rate limit.
    pub attachments: CategoryLimit,
    /// The combined attachment item rate limit.
    pub attachment_items: CategoryLimit,
    /// The combined session item rate limit.
    pub sessions: CategoryLimit,
    /// The combined profile item rate limit.
    pub profiles: CategoryLimit,
    /// The rate limit for the indexed profiles category.
    pub profiles_indexed: CategoryLimit,
    /// The combined replay item rate limit.
    pub replays: CategoryLimit,
    /// The combined check-in item rate limit.
    pub check_ins: CategoryLimit,
    /// The combined spans rate limit.
    pub spans: CategoryLimit,
    /// The rate limit for the indexed span category.
    pub spans_indexed: CategoryLimit,
    /// The combined rate limit for user-reports.
    pub user_reports_v2: CategoryLimit,
    /// The combined profile chunk item rate limit.
    pub profile_chunks: CategoryLimit,
}

impl Enforcement {
    /// Returns the `CategoryLimit` for the event.
    ///
    /// `None` if the event is not rate limited.
    pub fn active_event(&self) -> Option<&CategoryLimit> {
        if self.event.is_active() {
            Some(&self.event)
        } else if self.event_indexed.is_active() {
            Some(&self.event_indexed)
        } else {
            None
        }
    }

    /// Returns `true` if the event is rate limited.
    pub fn is_event_active(&self) -> bool {
        self.active_event().is_some()
    }

    /// Helper for `track_outcomes`.
    fn get_outcomes(self) -> impl Iterator<Item = (Outcome, DataCategory, usize)> {
        let Self {
            event,
            event_indexed,
            attachments,
            attachment_items,
            sessions: _, // Do not report outcomes for sessions.
            profiles,
            profiles_indexed,
            replays,
            check_ins,
            spans,
            spans_indexed,
            user_reports_v2,
            profile_chunks,
        } = self;

        let limits = [
            event,
            event_indexed,
            attachments,
            attachment_items,
            profiles,
            profiles_indexed,
            replays,
            check_ins,
            spans,
            spans_indexed,
            user_reports_v2,
            profile_chunks,
        ];

        limits
            .into_iter()
            .filter(move |limit| limit.is_active())
            .map(move |limit| {
                (
                    Outcome::RateLimited(limit.reason_code),
                    limit.category,
                    limit.quantity,
                )
            })
    }

    /// Applies the [`Enforcement`] on the [`Envelope`] by removing all items that were rate limited
    /// and emits outcomes for each rate limited category.
    ///
    /// # Example
    ///
    /// ## Interaction between Events and Attachments
    ///
    /// An envelope with an `Error` event and an `Attachment`. Two quotas specify to drop all
    /// attachments (reason `"a"`) and all errors (reason `"e"`). The result of enforcement will be:
    ///
    /// 1. All items are removed from the envelope.
    /// 2. Enforcements report both the event and the attachment dropped with reason `"e"`, since
    ///    dropping an event automatically drops all attachments with the same reason.
    /// 3. Rate limits report the single event limit `"e"`, since attachment limits do not need to
    ///    be checked in this case.
    ///
    /// ## Required Attachments
    ///
    /// An envelope with a single Minidump `Attachment`, and a single quota specifying to drop all
    /// attachments with reason `"a"`:
    ///
    /// 1. Since the minidump creates an event and is required for processing, it remains in the
    ///    envelope and is marked as `rate_limited`.
    /// 2. Enforcements report the attachment dropped with reason `"a"`.
    /// 3. Rate limits are empty since it is allowed to send required attachments even when rate
    ///    limited.
    ///
    /// ## Previously Rate Limited Attachments
    ///
    /// An envelope with a single item marked as `rate_limited`, and a quota specifying to drop
    /// everything with reason `"d"`:
    ///
    /// 1. The item remains in the envelope.
    /// 2. Enforcements are empty. Rate limiting has occurred at an earlier stage in the pipeline.
    /// 3. Rate limits are empty.
    pub fn apply_with_outcomes(self, envelope: &mut ManagedEnvelope) {
        envelope
            .envelope_mut()
            .retain_items(|item| self.retain_item(item));
        self.track_outcomes(envelope);
    }

    /// Returns `true` when an [`Item`] can be retained, `false` otherwise.
    fn retain_item(&self, item: &mut Item) -> bool {
        // Remove event items and all items that depend on this event
        if self.event.is_active() && item.requires_event() {
            return false;
        }

        // When checking limits for categories that have an indexed variant,
        // we only have to check the more specific, the indexed, variant
        // to determine whether an item is limited.
        match item.ty() {
            ItemType::Attachment => {
                if !(self.attachments.is_active() || self.attachment_items.is_active()) {
                    return true;
                }
                if item.creates_event() {
                    item.set_rate_limited(true);
                    true
                } else {
                    false
                }
            }
            ItemType::Session => !self.sessions.is_active(),
            ItemType::Profile => !self.profiles_indexed.is_active(),
            ItemType::ReplayEvent => !self.replays.is_active(),
            ItemType::ReplayVideo => !self.replays.is_active(),
            ItemType::ReplayRecording => !self.replays.is_active(),
            ItemType::CheckIn => !self.check_ins.is_active(),
            ItemType::Span | ItemType::OtelSpan | ItemType::OtelTracesData => {
                !self.spans_indexed.is_active()
            }
            ItemType::ProfileChunk => !self.profile_chunks.is_active(),
            ItemType::Event
            | ItemType::Transaction
            | ItemType::Security
            | ItemType::FormData
            | ItemType::RawSecurity
            | ItemType::Nel
            | ItemType::UnrealReport
            | ItemType::UserReport
            | ItemType::Sessions
            | ItemType::Statsd
            | ItemType::MetricBuckets
            | ItemType::ClientReport
            | ItemType::UserReportV2
            | ItemType::Unknown(_) => true,
        }
    }

    /// Invokes track outcome on all enforcements reported by the [`EnvelopeLimiter`].
    ///
    /// Relay generally does not emit outcomes for sessions, so those are skipped.
    fn track_outcomes(self, envelope: &mut ManagedEnvelope) {
        for (outcome, category, quantity) in self.get_outcomes() {
            envelope.track_outcome(outcome, category, quantity)
        }
    }
}

/// Which limits to check with the [`EnvelopeLimiter`].
#[derive(Debug, Copy, Clone)]
pub enum CheckLimits {
    /// Checks all limits except indexed categories.
    ///
    /// In the fast path it is necessary to apply cached rate limits but to not enforce indexed rate limits.
    /// Because at the time of the check the decision whether an envelope is sampled or not is not yet known.
    /// Additionally even if the item is later dropped by dynamic sampling, it must still be around to extract metrics
    /// and cannot be dropped too early.
    NonIndexed,
    /// Checks all limits against the envelope.
    #[cfg_attr(not(any(feature = "processing", test)), expect(dead_code))]
    All,
}

struct Check<F> {
    limits: CheckLimits,
    check: F,
}

impl<F, E> Check<F>
where
    F: FnMut(ItemScoping<'_>, usize) -> Result<RateLimits, E>,
{
    fn apply(&mut self, scoping: ItemScoping<'_>, quantity: usize) -> Result<RateLimits, E> {
        if matches!(self.limits, CheckLimits::NonIndexed) && scoping.category.is_indexed() {
            return Ok(RateLimits::default());
        }

        (self.check)(scoping, quantity)
    }
}

/// Enforces rate limits with the given `check` function on items in the envelope.
///
/// The `check` function is called with the following rules:
///  - Once for a single event, if present in the envelope.
///  - Once for all comprised attachments, unless the event was rate limited.
///  - Once for all comprised sessions.
///
/// Items violating the rate limit are removed from the envelope. This follows a set of rules:
///  - If the event is removed, all items depending on the event are removed (e.g. attachments).
///  - Attachments are not removed if they create events (e.g. minidumps).
///  - Sessions are handled separate to all of the above.
pub struct EnvelopeLimiter<F> {
    check: Check<F>,
    event_category: Option<DataCategory>,
}

impl<E, F> EnvelopeLimiter<F>
where
    F: FnMut(ItemScoping<'_>, usize) -> Result<RateLimits, E>,
{
    /// Create a new `EnvelopeLimiter` with the given `check` function.
    pub fn new(limits: CheckLimits, check: F) -> Self {
        Self {
            check: Check { check, limits },
            event_category: None,
        }
    }

    /// Assume an event with the given category, even if no item is present in the envelope.
    ///
    /// This ensures that rate limits for the given data category are checked even if there is no
    /// matching item in the envelope. Other items are handled according to the rules as if the
    /// event item were present.
    #[cfg(feature = "processing")]
    pub fn assume_event(&mut self, category: DataCategory) {
        self.event_category = Some(category);
    }

    /// Process rate limits for the envelope, returning applied limits.
    ///
    /// Returns a tuple of `Enforcement` and `RateLimits`:
    ///
    /// - Enforcements declare the quantities of categories that have been rate limited with the
    ///   individual reason codes that caused rate limiting. If multiple rate limits applied to a
    ///   category, then the longest limit is reported.
    /// - Rate limits declare all active rate limits, regardless of whether they have been applied
    ///   to items in the envelope. This excludes rate limits applied to required attachments, since
    ///   clients are allowed to continue sending them.
    pub fn compute(
        mut self,
        envelope: &mut Envelope,
        scoping: &Scoping,
    ) -> Result<(Enforcement, RateLimits), E> {
        let mut summary = EnvelopeSummary::compute(envelope);
        summary.event_category = self.event_category.or(summary.event_category);

        let (enforcement, rate_limits) = self.execute(&summary, scoping)?;
        Ok((enforcement, rate_limits))
    }

    fn execute(
        &mut self,
        summary: &EnvelopeSummary,
        scoping: &Scoping,
    ) -> Result<(Enforcement, RateLimits), E> {
        let mut rate_limits = RateLimits::new();
        let mut enforcement = Enforcement::default();

        // Handle event.
        if let Some(category) = summary.event_category {
            // Check the broad category for limits.
            let mut event_limits = self.check.apply(scoping.item(category), 1)?;
            enforcement.event = CategoryLimit::new(category, 1, event_limits.longest());

            if let Some(index_category) = category.index_category() {
                // Check the specific/indexed category for limits only if the specific one has not already
                // an enforced limit.
                if event_limits.is_empty() {
                    event_limits.merge(self.check.apply(scoping.item(index_category), 1)?);
                }

                enforcement.event_indexed =
                    CategoryLimit::new(index_category, 1, event_limits.longest());
            };

            rate_limits.merge(event_limits);
        }

        // Handle attachments.
        if let Some(limit) = enforcement.active_event() {
            let limit1 = limit.clone_for(DataCategory::Attachment, summary.attachment_quantity);
            let limit2 = limit.clone_for(
                DataCategory::AttachmentItem,
                summary.attachment_item_quantity,
            );
            enforcement.attachments = limit1;
            enforcement.attachment_items = limit2;
        } else {
            let mut attachment_limits = RateLimits::new();
            if summary.attachment_quantity > 0 {
                let item_scoping = scoping.item(DataCategory::Attachment);

                let attachment_byte_limits = self
                    .check
                    .apply(item_scoping, summary.attachment_quantity)?;

                enforcement.attachments = CategoryLimit::new(
                    DataCategory::Attachment,
                    summary.attachment_quantity,
                    attachment_byte_limits.longest(),
                );
                attachment_limits.merge(attachment_byte_limits);
            }
            if !attachment_limits.is_limited() && summary.attachment_item_quantity > 0 {
                let item_scoping = scoping.item(DataCategory::AttachmentItem);

                let attachment_item_limits = self
                    .check
                    .apply(item_scoping, summary.attachment_item_quantity)?;

                enforcement.attachment_items = CategoryLimit::new(
                    DataCategory::AttachmentItem,
                    summary.attachment_item_quantity,
                    attachment_item_limits.longest(),
                );
                attachment_limits.merge(attachment_item_limits);
            }

            // Only record rate limits for plain attachments. For all other attachments, it's
            // perfectly "legal" to send them. They will still be discarded in Sentry, but clients
            // can continue to send them.
            if summary.has_plain_attachments {
                rate_limits.merge(attachment_limits);
            }
        }

        // Handle sessions.
        if summary.session_quantity > 0 {
            let item_scoping = scoping.item(DataCategory::Session);
            let session_limits = self.check.apply(item_scoping, summary.session_quantity)?;
            enforcement.sessions = CategoryLimit::new(
                DataCategory::Session,
                summary.session_quantity,
                session_limits.longest(),
            );
            rate_limits.merge(session_limits);
        }

        // Handle profiles.
        if enforcement.is_event_active() {
            enforcement.profiles = enforcement
                .event
                .clone_for(DataCategory::Profile, summary.profile_quantity);

            enforcement.profiles_indexed = enforcement
                .event_indexed
                .clone_for(DataCategory::ProfileIndexed, summary.profile_quantity)
        } else if summary.profile_quantity > 0 {
            let mut profile_limits = self.check.apply(
                scoping.item(DataCategory::Profile),
                summary.profile_quantity,
            )?;

            // Profiles can persist in envelopes without transaction if the transaction item
            // was dropped by dynamic sampling.
            if profile_limits.is_empty() && summary.event_category.is_none() {
                profile_limits = self
                    .check
                    .apply(scoping.item(DataCategory::Transaction), 0)?;
            }

            enforcement.profiles = CategoryLimit::new(
                DataCategory::Profile,
                summary.profile_quantity,
                profile_limits.longest(),
            );

            if profile_limits.is_empty() {
                profile_limits.merge(self.check.apply(
                    scoping.item(DataCategory::ProfileIndexed),
                    summary.profile_quantity,
                )?);
            }

            enforcement.profiles_indexed = CategoryLimit::new(
                DataCategory::ProfileIndexed,
                summary.profile_quantity,
                profile_limits.longest(),
            );

            rate_limits.merge(profile_limits);
        }

        // Handle replays.
        if summary.replay_quantity > 0 {
            let item_scoping = scoping.item(DataCategory::Replay);
            let replay_limits = self.check.apply(item_scoping, summary.replay_quantity)?;
            enforcement.replays = CategoryLimit::new(
                DataCategory::Replay,
                summary.replay_quantity,
                replay_limits.longest(),
            );
            rate_limits.merge(replay_limits);
        }

        // Handle monitor checkins.
        if summary.monitor_quantity > 0 {
            let item_scoping = scoping.item(DataCategory::Monitor);
            let checkin_limits = self.check.apply(item_scoping, summary.monitor_quantity)?;
            enforcement.check_ins = CategoryLimit::new(
                DataCategory::Monitor,
                summary.monitor_quantity,
                checkin_limits.longest(),
            );
            rate_limits.merge(checkin_limits);
        }

        // Handle spans.
        if enforcement.is_event_active() {
            enforcement.spans = enforcement
                .event
                .clone_for(DataCategory::Span, summary.span_quantity);

            enforcement.spans_indexed = enforcement
                .event_indexed
                .clone_for(DataCategory::SpanIndexed, summary.span_quantity);
        } else if summary.span_quantity > 0 {
            let mut span_limits = self
                .check
                .apply(scoping.item(DataCategory::Span), summary.span_quantity)?;
            enforcement.spans = CategoryLimit::new(
                DataCategory::Span,
                summary.span_quantity,
                span_limits.longest(),
            );

            if span_limits.is_empty() {
                span_limits.merge(self.check.apply(
                    scoping.item(DataCategory::SpanIndexed),
                    summary.span_quantity,
                )?);
            }

            enforcement.spans_indexed = CategoryLimit::new(
                DataCategory::SpanIndexed,
                summary.span_quantity,
                span_limits.longest(),
            );

            rate_limits.merge(span_limits);
        }

        // Handle profile chunks.
        if summary.profile_chunk_quantity > 0 {
            let item_scoping = scoping.item(DataCategory::ProfileChunk);
            let profile_chunk_limits = self
                .check
                .apply(item_scoping, summary.profile_chunk_quantity)?;
            enforcement.profile_chunks = CategoryLimit::new(
                DataCategory::ProfileChunk,
                summary.profile_chunk_quantity,
                profile_chunk_limits.longest(),
            );
            rate_limits.merge(profile_chunk_limits);
        }

        Ok((enforcement, rate_limits))
    }
}

impl<F> fmt::Debug for EnvelopeLimiter<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvelopeLimiter")
            .field("event_category", &self.event_category)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_metrics::MetricNamespace;
    use relay_quotas::RetryAfter;
    use relay_system::Addr;
    use smallvec::smallvec;

    use super::*;
    use crate::services::processor::ProcessingGroup;
    use crate::{
        envelope::{AttachmentType, ContentType, SourceQuantities},
        extractors::RequestMeta,
    };

    #[test]
    fn test_format_rate_limits() {
        let mut rate_limits = RateLimits::new();

        // Add a generic rate limit for all categories.
        rate_limits.add(RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("my_limit")),
            retry_after: RetryAfter::from_secs(42),
            namespaces: smallvec![],
        });

        // Add a more specific rate limit for just one category.
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction, DataCategory::Security],
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(4711),
            namespaces: smallvec![],
        });

        let formatted = format_rate_limits(&rate_limits);
        let expected = "42::organization:my_limit, 4711:transaction;security:project";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn test_format_rate_limits_namespace() {
        let mut rate_limits = RateLimits::new();

        // Rate limit with reason code and namespace.
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::MetricBucket],
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("my_limit")),
            retry_after: RetryAfter::from_secs(42),
            namespaces: smallvec![MetricNamespace::Custom, MetricNamespace::Spans],
        });

        // Rate limit without reason code.
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::MetricBucket],
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(42),
            namespaces: smallvec![MetricNamespace::Spans],
        });

        let formatted = format_rate_limits(&rate_limits);
        let expected =
            "42:metric_bucket:organization:my_limit:custom;spans, 42:metric_bucket:organization::spans";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn test_parse_invalid_rate_limits() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        assert!(parse_rate_limits(&scoping, "").is_ok());
        assert!(parse_rate_limits(&scoping, "invalid").is_ok());
        assert!(parse_rate_limits(&scoping, ",,,").is_ok());
    }

    #[test]
    fn test_parse_rate_limits() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        // contains "foobar", an unknown scope that should be mapped to Unknown
        let formatted =
            "42::organization:my_limit, invalid, 4711:foobar;transaction;security:project";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![
                RateLimit {
                    categories: DataCategories::new(),
                    scope: RateLimitScope::Organization(OrganizationId::new(42)),
                    reason_code: Some(ReasonCode::new("my_limit")),
                    retry_after: rate_limits[0].retry_after,
                    namespaces: smallvec![],
                },
                RateLimit {
                    categories: smallvec![
                        DataCategory::Unknown,
                        DataCategory::Transaction,
                        DataCategory::Security,
                    ],
                    scope: RateLimitScope::Project(ProjectId::new(21)),
                    reason_code: None,
                    retry_after: rate_limits[1].retry_after,
                    namespaces: smallvec![],
                }
            ]
        );

        assert_eq!(42, rate_limits[0].retry_after.remaining_seconds());
        assert_eq!(4711, rate_limits[1].retry_after.remaining_seconds());
    }

    #[test]
    fn test_parse_rate_limits_namespace() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        let formatted = "42:metric_bucket:organization::custom;spans";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: smallvec![DataCategory::MetricBucket],
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: None,
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![MetricNamespace::Custom, MetricNamespace::Spans],
            }]
        );
    }

    #[test]
    fn test_parse_rate_limits_empty_namespace() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        // notice the trailing colon
        let formatted = "42:metric_bucket:organization:some_reason:";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: smallvec![DataCategory::MetricBucket],
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: Some(ReasonCode::new("some_reason")),
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![],
            }]
        );
    }

    #[test]
    fn test_parse_rate_limits_only_unknown() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        let formatted = "42:foo;bar:organization";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: smallvec![DataCategory::Unknown, DataCategory::Unknown],
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: None,
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![],
            },]
        );
    }

    macro_rules! envelope {
        ($( $item_type:ident $( :: $attachment_type:ident )? ),*) => {{
            let bytes = "{\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}";
            #[allow(unused_mut)]
            let mut envelope = Envelope::parse_bytes(bytes.into()).unwrap();
            $(
                let mut item = Item::new(ItemType::$item_type);
                item.set_payload(ContentType::OctetStream, "0123456789");
                $( item.set_attachment_type(AttachmentType::$attachment_type); )?
                envelope.add_item(item);
            )*

            let (outcome_aggregator, _) = Addr::custom();
            let (test_store, _) = Addr::custom();

            ManagedEnvelope::new(
                envelope,
                outcome_aggregator,
                test_store,
                ProcessingGroup::Ungrouped,
            )
        }}
    }

    fn set_extracted(envelope: &mut Envelope, ty: ItemType) {
        envelope
            .get_item_by_mut(|item| *item.ty() == ty)
            .unwrap()
            .set_metrics_extracted(true);
    }

    fn rate_limit(category: DataCategory) -> RateLimit {
        RateLimit {
            categories: vec![category].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(60),
            namespaces: smallvec![],
        }
    }

    #[derive(Debug, Default)]
    struct MockLimiter {
        denied: Vec<DataCategory>,
        called: BTreeMap<DataCategory, usize>,
        checked: BTreeSet<DataCategory>,
    }

    impl MockLimiter {
        pub fn deny(mut self, category: DataCategory) -> Self {
            self.denied.push(category);
            self
        }

        pub fn check(
            &mut self,
            scoping: ItemScoping<'_>,
            quantity: usize,
        ) -> Result<RateLimits, ()> {
            let cat = scoping.category;
            let previous = self.called.insert(cat, quantity);
            assert!(previous.is_none(), "rate limiter invoked twice for {cat}");

            let mut limits = RateLimits::new();
            if self.denied.contains(&cat) {
                limits.add(rate_limit(cat));
            }
            Ok(limits)
        }

        #[track_caller]
        pub fn assert_call(&mut self, category: DataCategory, expected: usize) {
            self.checked.insert(category);

            let quantity = self.called.get(&category).copied();
            assert_eq!(
                quantity,
                Some(expected),
                "Expected quantity `{expected}` for data category `{category}`, got {quantity:?}."
            );
        }
    }

    impl Drop for MockLimiter {
        fn drop(&mut self) {
            if std::thread::panicking() {
                return;
            }

            for checked in &self.checked {
                self.called.remove(checked);
            }

            if self.called.is_empty() {
                return;
            }

            let not_asserted = self
                .called
                .iter()
                .map(|(k, v)| format!("- {k}: {v}"))
                .collect::<Vec<_>>()
                .join("\n");

            panic!("Following calls to the limiter were not asserted:\n{not_asserted}");
        }
    }

    fn enforce_and_apply(
        mock: &mut MockLimiter,
        envelope: &mut ManagedEnvelope,
        #[allow(unused_variables)] assume_event: Option<DataCategory>,
    ) -> (Enforcement, RateLimits) {
        let scoping = envelope.scoping();

        #[allow(unused_mut)]
        let mut limiter = EnvelopeLimiter::new(CheckLimits::All, |s, q| mock.check(s, q));
        #[cfg(feature = "processing")]
        if let Some(assume_event) = assume_event {
            limiter.assume_event(assume_event);
        }

        let (enforcement, limits) = limiter.compute(envelope.envelope_mut(), &scoping).unwrap();

        // We implemented `clone` only for tests because we don't want to make `apply_with_outcomes`
        // &self because we want move semantics to prevent double tracking.
        enforcement.clone().apply_with_outcomes(envelope);

        (enforcement, limits)
    }

    #[test]
    fn test_enforce_pass_empty() {
        let mut envelope = envelope![];

        let mut mock = MockLimiter::default();
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(!limits.is_limited());
        assert!(envelope.envelope().is_empty());
    }

    #[test]
    fn test_enforce_limit_error_event() {
        let mut envelope = envelope![Event];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert!(envelope.envelope().is_empty());
        mock.assert_call(DataCategory::Error, 1);
    }

    #[test]
    fn test_enforce_limit_error_with_attachments() {
        let mut envelope = envelope![Event, Attachment];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert!(envelope.envelope().is_empty());
        mock.assert_call(DataCategory::Error, 1);
    }

    #[test]
    fn test_enforce_limit_minidump() {
        let mut envelope = envelope![Attachment::Minidump];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert!(envelope.envelope().is_empty());
        mock.assert_call(DataCategory::Error, 1);
    }

    #[test]
    fn test_enforce_limit_attachments() {
        let mut envelope = envelope![Attachment::Minidump, Attachment];

        let mut mock = MockLimiter::default().deny(DataCategory::Attachment);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        // Attachments would be limited, but crash reports create events and are thus allowed.
        assert!(limits.is_limited());
        assert_eq!(envelope.envelope().len(), 1);
        mock.assert_call(DataCategory::Error, 1);
        mock.assert_call(DataCategory::Attachment, 20);
    }

    /// Limit stand-alone profiles.
    #[test]
    fn test_enforce_limit_profiles() {
        let mut envelope = envelope![Profile, Profile];

        let mut mock = MockLimiter::default().deny(DataCategory::Profile);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert_eq!(envelope.envelope().len(), 0);
        mock.assert_call(DataCategory::Profile, 2);

        assert_eq!(
            get_outcomes(enforcement),
            vec![
                (DataCategory::Profile, 2),
                (DataCategory::ProfileIndexed, 2)
            ]
        );
    }

    /// Limit profile chunks.
    #[test]
    fn test_enforce_limit_profile_chunks() {
        let mut envelope = envelope![ProfileChunk, ProfileChunk];

        let mut mock = MockLimiter::default().deny(DataCategory::ProfileChunk);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert_eq!(envelope.envelope().len(), 0);
        mock.assert_call(DataCategory::ProfileChunk, 2);

        assert_eq!(
            get_outcomes(enforcement),
            vec![(DataCategory::ProfileChunk, 2),]
        );
    }

    /// Limit replays.
    #[test]
    fn test_enforce_limit_replays() {
        let mut envelope = envelope![ReplayEvent, ReplayRecording, ReplayVideo];

        let mut mock = MockLimiter::default().deny(DataCategory::Replay);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert_eq!(envelope.envelope().len(), 0);
        mock.assert_call(DataCategory::Replay, 3);

        assert_eq!(get_outcomes(enforcement), vec![(DataCategory::Replay, 3),]);
    }

    /// Limit monitor checkins.
    #[test]
    fn test_enforce_limit_monitor_checkins() {
        let mut envelope = envelope![CheckIn];

        let mut mock = MockLimiter::default().deny(DataCategory::Monitor);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert_eq!(envelope.envelope().len(), 0);
        mock.assert_call(DataCategory::Monitor, 1);

        assert_eq!(get_outcomes(enforcement), vec![(DataCategory::Monitor, 1)])
    }

    #[test]
    fn test_enforce_pass_minidump() {
        let mut envelope = envelope![Attachment::Minidump];

        let mut mock = MockLimiter::default().deny(DataCategory::Attachment);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        // If only crash report attachments are present, we don't emit a rate limit.
        assert!(!limits.is_limited());
        assert_eq!(envelope.envelope().len(), 1);
        mock.assert_call(DataCategory::Error, 1);
        mock.assert_call(DataCategory::Attachment, 10);
    }

    #[test]
    fn test_enforce_skip_rate_limited() {
        let mut envelope = envelope![];

        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::OctetStream, "0123456789");
        item.set_rate_limited(true);
        envelope.envelope_mut().add_item(item);

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(!limits.is_limited()); // No new rate limits applied.
        assert_eq!(envelope.envelope().len(), 1); // The item was retained
    }

    #[test]
    fn test_enforce_pass_sessions() {
        let mut envelope = envelope![Session, Session, Session];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        // If only crash report attachments are present, we don't emit a rate limit.
        assert!(!limits.is_limited());
        assert_eq!(envelope.envelope().len(), 3);
        mock.assert_call(DataCategory::Session, 3);
    }

    #[test]
    fn test_enforce_limit_sessions() {
        let mut envelope = envelope![Session, Session, Event];

        let mut mock = MockLimiter::default().deny(DataCategory::Session);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        // If only crash report attachments are present, we don't emit a rate limit.
        assert!(limits.is_limited());
        assert_eq!(envelope.envelope().len(), 1);
        mock.assert_call(DataCategory::Error, 1);
        mock.assert_call(DataCategory::Session, 2);
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_enforce_limit_assumed_event() {
        let mut envelope = envelope![];

        let mut mock = MockLimiter::default().deny(DataCategory::Transaction);
        let (_, limits) =
            enforce_and_apply(&mut mock, &mut envelope, Some(DataCategory::Transaction));

        assert!(limits.is_limited());
        assert!(envelope.envelope().is_empty()); // obviously
        mock.assert_call(DataCategory::Transaction, 1);
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_enforce_limit_assumed_attachments() {
        let mut envelope = envelope![Attachment, Attachment];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = enforce_and_apply(&mut mock, &mut envelope, Some(DataCategory::Error));

        assert!(limits.is_limited());
        assert!(envelope.envelope().is_empty());
        mock.assert_call(DataCategory::Error, 1);
    }

    #[test]
    fn test_enforce_transaction() {
        let mut envelope = envelope![Transaction];

        let mut mock = MockLimiter::default().deny(DataCategory::Transaction);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert!(enforcement.event_indexed.is_active());
        assert!(enforcement.event.is_active());
        mock.assert_call(DataCategory::Transaction, 1);

        assert_eq!(
            get_outcomes(enforcement),
            vec![
                (DataCategory::Transaction, 1),
                (DataCategory::TransactionIndexed, 1),
            ]
        );
    }

    #[test]
    fn test_enforce_transaction_non_indexed() {
        let mut envelope = envelope![Transaction, Profile];
        let scoping = envelope.scoping();

        let mut mock = MockLimiter::default().deny(DataCategory::TransactionIndexed);

        let limiter = EnvelopeLimiter::new(CheckLimits::NonIndexed, |s, q| mock.check(s, q));
        let (enforcement, limits) = limiter.compute(envelope.envelope_mut(), &scoping).unwrap();
        enforcement.clone().apply_with_outcomes(&mut envelope);

        assert!(!limits.is_limited());
        assert!(!enforcement.event_indexed.is_active());
        assert!(!enforcement.event.is_active());
        assert!(!enforcement.profiles_indexed.is_active());
        assert!(!enforcement.profiles.is_active());
        mock.assert_call(DataCategory::Transaction, 1);
        mock.assert_call(DataCategory::Profile, 1);
    }

    #[test]
    fn test_enforce_transaction_no_indexing_quota() {
        let mut envelope = envelope![Transaction];

        let mut mock = MockLimiter::default().deny(DataCategory::TransactionIndexed);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert!(enforcement.event_indexed.is_active());
        assert!(!enforcement.event.is_active());
        mock.assert_call(DataCategory::Transaction, 1);
        mock.assert_call(DataCategory::TransactionIndexed, 1);
    }

    #[test]
    fn test_enforce_transaction_attachment_enforced() {
        let mut envelope = envelope![Transaction, Attachment];

        let mut mock = MockLimiter::default().deny(DataCategory::Transaction);
        let (enforcement, _) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(enforcement.event.is_active());
        assert!(enforcement.attachments.is_active());
        mock.assert_call(DataCategory::Transaction, 1);
    }

    fn get_outcomes(enforcement: Enforcement) -> Vec<(DataCategory, usize)> {
        enforcement
            .get_outcomes()
            .map(|(_, data_category, quantity)| (data_category, quantity))
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_enforce_transaction_profile_enforced() {
        let mut envelope = envelope![Transaction, Profile];

        let mut mock = MockLimiter::default().deny(DataCategory::Transaction);
        let (enforcement, _) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(enforcement.event.is_active());
        assert!(enforcement.profiles.is_active());
        mock.assert_call(DataCategory::Transaction, 1);

        assert_eq!(
            get_outcomes(enforcement),
            vec![
                (DataCategory::Transaction, 1),
                (DataCategory::TransactionIndexed, 1),
                (DataCategory::Profile, 1),
                (DataCategory::ProfileIndexed, 1),
            ]
        );
    }

    #[test]
    fn test_enforce_transaction_standalone_profile_enforced() {
        // When the transaction is sampled, the profile survives as standalone.
        let mut envelope = envelope![Profile];

        let mut mock = MockLimiter::default().deny(DataCategory::Transaction);
        let (enforcement, _) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(enforcement.profiles.is_active());
        mock.assert_call(DataCategory::Profile, 1);
        mock.assert_call(DataCategory::Transaction, 0);

        assert_eq!(
            get_outcomes(enforcement),
            vec![
                (DataCategory::Profile, 1),
                (DataCategory::ProfileIndexed, 1),
            ]
        );
    }

    #[test]
    fn test_enforce_transaction_attachment_enforced_indexing_quota() {
        let mut envelope = envelope![Transaction, Attachment];
        set_extracted(envelope.envelope_mut(), ItemType::Transaction);

        let mut mock = MockLimiter::default().deny(DataCategory::TransactionIndexed);
        let (enforcement, _) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(!enforcement.event.is_active());
        assert!(enforcement.event_indexed.is_active());
        assert!(enforcement.attachments.is_active());
        assert!(enforcement.attachment_items.is_active());
        mock.assert_call(DataCategory::Transaction, 1);
        mock.assert_call(DataCategory::TransactionIndexed, 1);

        assert_eq!(
            get_outcomes(enforcement),
            vec![
                (DataCategory::TransactionIndexed, 1),
                (DataCategory::Attachment, 10),
                (DataCategory::AttachmentItem, 1)
            ]
        );
    }

    #[test]
    fn test_enforce_span() {
        let mut envelope = envelope![Span, OtelSpan];

        let mut mock = MockLimiter::default().deny(DataCategory::Span);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert!(enforcement.spans_indexed.is_active());
        assert!(enforcement.spans.is_active());
        mock.assert_call(DataCategory::Span, 2);

        assert_eq!(
            get_outcomes(enforcement),
            vec![(DataCategory::Span, 2), (DataCategory::SpanIndexed, 2)]
        );
    }

    #[test]
    fn test_enforce_span_no_indexing_quota() {
        let mut envelope = envelope![OtelSpan, Span];

        let mut mock = MockLimiter::default().deny(DataCategory::SpanIndexed);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert!(enforcement.spans_indexed.is_active());
        assert!(!enforcement.spans.is_active());
        mock.assert_call(DataCategory::Span, 2);
        mock.assert_call(DataCategory::SpanIndexed, 2);

        assert_eq!(
            get_outcomes(enforcement),
            vec![(DataCategory::SpanIndexed, 2)]
        );
    }

    #[test]
    fn test_enforce_span_metrics_extracted_no_indexing_quota() {
        let mut envelope = envelope![Span, OtelSpan];
        set_extracted(envelope.envelope_mut(), ItemType::Span);

        let mut mock = MockLimiter::default().deny(DataCategory::SpanIndexed);
        let (enforcement, limits) = enforce_and_apply(&mut mock, &mut envelope, None);

        assert!(limits.is_limited());
        assert!(enforcement.spans_indexed.is_active());
        assert!(!enforcement.spans.is_active());
        mock.assert_call(DataCategory::Span, 2);
        mock.assert_call(DataCategory::SpanIndexed, 2);

        assert_eq!(
            get_outcomes(enforcement),
            vec![(DataCategory::SpanIndexed, 2)]
        );
    }

    #[test]
    fn test_source_quantity_for_total_quantity() {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);

        let mut envelope = Envelope::from_request(None, request_meta);

        let mut item = Item::new(ItemType::MetricBuckets);
        item.set_source_quantities(SourceQuantities {
            transactions: 5,
            spans: 0,
            profiles: 2,
            buckets: 5,
        });
        envelope.add_item(item);

        let mut item = Item::new(ItemType::MetricBuckets);
        item.set_source_quantities(SourceQuantities {
            transactions: 2,
            spans: 0,
            profiles: 0,
            buckets: 3,
        });
        envelope.add_item(item);

        let summary = EnvelopeSummary::compute(&envelope);

        assert_eq!(summary.profile_quantity, 2);
        assert_eq!(summary.secondary_transaction_quantity, 7);
    }
}
