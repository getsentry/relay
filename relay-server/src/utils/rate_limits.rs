use std::fmt::{self, Write};

use relay_quotas::{
    DataCategories, DataCategory, ItemScoping, QuotaScope, RateLimit, RateLimitScope, RateLimits,
    ReasonCode, Scoping,
};

use crate::actors::outcome::{Outcome, TrackOutcome};
use crate::envelope::{Envelope, Item, ItemType};

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
            write!(header, "{}", category).ok();
        }

        write!(header, ":{}", rate_limit.scope.name()).ok();

        if let Some(ref reason_code) = rate_limit.reason_code {
            write!(header, ":{}", reason_code).ok();
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

        let reason_code = components.next().map(ReasonCode::new);

        rate_limits.add(RateLimit {
            categories,
            scope,
            reason_code,
            retry_after,
        });
    }

    rate_limits
}

/// Infer the data categories from an item.
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
        ItemType::UnrealReport => Some(DataCategory::Error),
        ItemType::Attachment if item.creates_event() => Some(DataCategory::Error),
        ItemType::Attachment => None,
        ItemType::Session => None,
        ItemType::Sessions => None,
        ItemType::Metrics => None,
        ItemType::MetricBuckets => None,
        ItemType::FormData => None,
        ItemType::UserReport => None,
        ItemType::Profile => None,
        ItemType::ReplayEvent => None,
        ItemType::ReplayRecording => None,
        ItemType::ClientReport => None,
        ItemType::Unknown(_) => None,
    }
}

/// A summary of `Envelope` contents.
///
/// Summarizes the contained event, size of attachments, session updates, and whether there are
/// plain attachments. This is used for efficient rate limiting or outcome handling.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct EnvelopeSummary {
    /// The data category of the event in the envelope. `None` if there is no event.
    ///
    /// This is the category used for quotas of this event.
    pub event_category: Option<DataCategory>,

    /// Whether the event is a transaction and thus can have metrics extracted from it.
    pub transaction_processing: bool,

    /// The quantity of all attachments combined in bytes.
    pub attachment_quantity: usize,

    /// The number of all session updates.
    pub session_quantity: usize,

    /// The number of profiles.
    pub profile_quantity: usize,

    /// The number of replays.
    pub replay_quantity: usize,

    /// Indicates that the envelope contains regular attachments that do not create event payloads.
    pub has_plain_attachments: bool,
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

            match item.ty() {
                ItemType::Attachment => summary.attachment_quantity += item.len().max(1),
                ItemType::Session => summary.session_quantity += 1,
                ItemType::Profile => summary.profile_quantity += 1,
                ItemType::ReplayEvent => summary.replay_quantity += 1,
                ItemType::ReplayRecording => summary.replay_quantity += 1,
                _ => (),
            }
        }

        summary
    }

    /// Infers the appropriate [`DataCategories`] from the envelope [`Item`].
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
#[derive(Debug, Clone)]
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

    /// Returns `true` if this is an active limit.
    ///
    /// This indicates that the category is limited and a certain quantity is removed from the
    /// Envelope. If the limit is inactive, there is no change.
    pub fn is_active(&self) -> bool {
        self.quantity > 0
    }

    /// The [`DataCategory`] this enforcement was created for and checked against.
    pub fn category(&self) -> DataCategory {
        self.category
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

/// Information on the limited quantities returned by [`EnvelopeLimiter::enforce`].
#[derive(Default, Debug, Clone)]
pub struct Enforcement {
    /// The event item rate limits.
    ///
    /// Some payloads have multiple rate-limits that may apply, each that is enforced will
    /// be in this list.
    pub event: CategoryLimit,
    /// Rate limits for transaction processing.
    ///
    /// Transaction processing is special since a normal transaction event has the primary
    /// rate limit using indexing quota ([`DataCategory::Transaction`]).  However extracting
    /// metrics from it has the [`DataCategory::TransactionProcessed`] rate limit attached
    /// to it.
    pub transaction_processed: CategoryLimit,
    /// The combined attachment item rate limit.
    pub attachments: CategoryLimit,
    /// The combined session item rate limit.
    pub sessions: CategoryLimit,
    /// The combined profile item rate limit.
    pub profiles: CategoryLimit,
    /// The combined replay item rate limit.
    pub replays: CategoryLimit,
}

impl Enforcement {
    /// Invokes [`TrackOutcome`] on all enforcements reported by the [`EnvelopeLimiter`].
    ///
    /// Relay generally does not emit outcomes for sessions, so those are skipped.
    pub fn track_outcomes(self, envelope: &Envelope, scoping: &Scoping) {
        // Do not report outcomes for sessions.
        for limit in [self.event, self.attachments, self.profiles, self.replays] {
            if limit.is_active() {
                let timestamp = relay_common::instant_to_date_time(envelope.meta().start_time());
                TrackOutcome::from_registry().send(TrackOutcome {
                    timestamp,
                    scoping: *scoping,
                    outcome: Outcome::RateLimited(limit.reason_code),
                    event_id: envelope.event_id(),
                    remote_addr: envelope.meta().remote_addr(),
                    category: limit.category,
                    // XXX: on the limiter we have quantity of usize, but in the protocol
                    // and data store we're limited to u32.
                    quantity: limit.quantity as u32,
                });
            }
        }
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
    check: F,
    event_category: Option<DataCategory>,
}

impl<E, F> EnvelopeLimiter<F>
where
    F: FnMut(ItemScoping<'_>, usize) -> Result<RateLimits, E>,
{
    /// Create a new `EnvelopeLimiter` with the given `check` function.
    pub fn new(check: F) -> Self {
        Self {
            check,
            event_category: Default::default(),
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

    /// Process rate limits for the envelope, removing offending items and returning applied limits.
    ///
    /// Returns a tuple of `Enforcement` and `RateLimits`:
    ///
    /// - Enforcements declare the quantities of categories that have been rate limited with the
    ///   individual reason codes that caused rate limiting. If multiple rate limits applied to a
    ///   category, then the longest limit is reported.
    /// - Rate limits declare all active rate limits, regardless of whether they have been applied
    ///   to items in the envelope. This excludes rate limits applied to required attachments, since
    ///   clients are allowed to continue sending them.
    ///
    /// The `early_enforcement` parameter is used to pass through the [`Enforcement`] from
    /// an earlier call to the rate limiter.  This is because while the request is still
    /// open we get called to see if we have any cached quotas which are exceeded to already
    /// reject a payload.  However e.g. transactions have two quota data categories and only
    /// one might be exceeded.  In this case the envelope is not reject and still processed.
    /// The earlier enformcenemts are then passed back in here so we can avoid duplicate
    /// and inconsistent checks etc.
    ///
    /// # Example
    ///
    /// **Interaction between Events and Attachments**
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
    /// **Required Attachments**
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
    /// **Previously Rate Limited Attachments**
    ///
    /// An envelope with a single item marked as `rate_limited`, and a quota specifying to drop
    /// everything with reason `"d"`:
    ///
    /// 1. The item remains in the envelope.
    /// 2. Enforcements are empty. Rate limiting has occurred at an earlier stage in the pipeline.
    /// 3. Rate limits are empty.
    pub fn enforce(
        mut self,
        envelope: &mut Envelope,
        scoping: &Scoping,
        early_enforcement: &Enforcement,
    ) -> Result<(Enforcement, RateLimits), E> {
        let mut summary = EnvelopeSummary::compute(envelope);
        if let Some(event_category) = self.event_category {
            summary.event_category = Some(event_category);
        }

        let (enforcement, rate_limits) = self.execute(&summary, scoping, early_enforcement)?;
        envelope.retain_items(|item| self.should_retain_item(item, &enforcement));
        envelope.set_early_enforcement(enforcement.clone());

        Ok((enforcement, rate_limits))
    }

    fn execute(
        &mut self,
        summary: &EnvelopeSummary,
        scoping: &Scoping,
        early_enforcement: &Enforcement,
    ) -> Result<(Enforcement, RateLimits), E> {
        let mut rate_limits = RateLimits::new();
        let mut enforcement = Enforcement::default();

        if let Some(category) = summary.event_category {
            if early_enforcement.event.is_active() {
                enforcement.event = early_enforcement.event.clone();
                // No need to set a RateLimit for this, we only need to report new rate
                // limits as those are the ones that need to be sent to the ProjectCache.
            } else {
                let event_limits = (self.check)(scoping.item(category), 1)?;
                let longest = event_limits.longest();
                enforcement.event = CategoryLimit::new(category, 1, longest);

                // Record the same reason for attachments, if there are any.
                enforcement.attachments = CategoryLimit::new(
                    DataCategory::Attachment,
                    summary.attachment_quantity,
                    longest,
                );

                rate_limits.merge(event_limits);
            }

            // Handle transactions specially, they have processing quota too.
            if category == DataCategory::Transaction {
                if early_enforcement.transaction_processed.is_active() {
                    enforcement.transaction_processed =
                        early_enforcement.transaction_processed.clone()
                } else {
                    let item_scoping = scoping.item(DataCategory::TransactionProcessed);
                    let limits = (self.check)(item_scoping, 1)?;
                    enforcement.transaction_processed =
                        CategoryLimit::new(DataCategory::TransactionProcessed, 1, limits.longest());
                    rate_limits.merge(limits);

                    // If only one of the rate limits applied we omit both of them.  If there is
                    // a rate limit the endpoint will return 429 but we need the client to keep
                    // sending transactions unless both quotas limits were exceeded.
                    if rate_limits.iter().count() == 1 {
                        rate_limits = RateLimits::new();
                    }
                }
            }
        }

        if !enforcement.event.is_active() && summary.attachment_quantity > 0 {
            let item_scoping = scoping.item(DataCategory::Attachment);
            let attachment_limits = (self.check)(item_scoping, summary.attachment_quantity)?;
            enforcement.attachments = CategoryLimit::new(
                DataCategory::Attachment,
                summary.attachment_quantity,
                attachment_limits.longest(),
            );

            // Only record rate limits for plain attachments. For all other attachments, it's
            // perfectly "legal" to send them. They will still be discarded in Sentry, but clients
            // can continue to send them.
            if summary.has_plain_attachments {
                rate_limits.merge(attachment_limits);
            }
        }

        if summary.session_quantity > 0 {
            let item_scoping = scoping.item(DataCategory::Session);
            let session_limits = (self.check)(item_scoping, summary.session_quantity)?;
            enforcement.sessions = CategoryLimit::new(
                DataCategory::Session,
                summary.session_quantity,
                session_limits.longest(),
            );
            rate_limits.merge(session_limits);
        }

        if summary.profile_quantity > 0 {
            let item_scoping = scoping.item(DataCategory::Profile);
            let profile_limits = (self.check)(item_scoping, summary.profile_quantity)?;
            enforcement.profiles = CategoryLimit::new(
                DataCategory::Profile,
                summary.profile_quantity,
                profile_limits.longest(),
            );
            rate_limits.merge(profile_limits);
        }

        if summary.replay_quantity > 0 {
            let item_scoping = scoping.item(DataCategory::Replay);
            let replay_limits = (self.check)(item_scoping, summary.replay_quantity)?;
            enforcement.replays = CategoryLimit::new(
                DataCategory::Replay,
                summary.replay_quantity,
                replay_limits.longest(),
            );
            rate_limits.merge(replay_limits);
        }

        Ok((enforcement, rate_limits))
    }

    /// Whether an envelope [`Item`] should be retained in the envelope.
    ///
    /// Items which were subject to rate limits should not be retained, which this function
    /// figures out.
    fn should_retain_item(&self, item: &mut Item, enforcement: &Enforcement) -> bool {
        // Remove event items and all items that depend on this event.
        if enforcement.event.is_active() && item.requires_event() {
            match item.ty() {
                ItemType::Transaction if !enforcement.transaction_processed.is_active() => {
                    // Only if both Transaction and TransactionProcessed quota is out should
                    // the transaction event be removed.
                    return true;
                }
                _ => return false,
            }
        }

        // Remove attachments, except those required for processing
        if enforcement.attachments.is_active() && item.ty() == &ItemType::Attachment {
            if item.creates_event() {
                item.set_rate_limited(true);
                return true;
            }

            return false;
        }

        // Remove sessions independently of events
        if enforcement.sessions.is_active() && item.ty() == &ItemType::Session {
            return false;
        }

        true
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
    use super::*;

    use std::collections::BTreeMap;

    use smallvec::smallvec;

    use relay_common::{ProjectId, ProjectKey};
    use relay_quotas::RetryAfter;

    use crate::envelope::{AttachmentType, ContentType};

    #[test]
    fn test_format_rate_limits() {
        let mut rate_limits = RateLimits::new();

        // Add a generic rate limit for all categories.
        rate_limits.add(RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("my_limit")),
            retry_after: RetryAfter::from_secs(42),
        });

        // Add a more specific rate limit for just one category.
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction, DataCategory::Security],
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(4711),
        });

        let formatted = format_rate_limits(&rate_limits);
        let expected = "42::organization:my_limit, 4711:transaction;security:project";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn test_parse_invalid_rate_limits() {
        let scoping = Scoping {
            organization_id: 42,
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
            organization_id: 42,
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
                    scope: RateLimitScope::Organization(42),
                    reason_code: Some(ReasonCode::new("my_limit")),
                    retry_after: rate_limits[0].retry_after,
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
                }
            ]
        );

        assert_eq!(42, rate_limits[0].retry_after.remaining_seconds());
        assert_eq!(4711, rate_limits[1].retry_after.remaining_seconds());
    }

    #[test]
    fn test_parse_rate_limits_only_unknown() {
        let scoping = Scoping {
            organization_id: 42,
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        // contains "foobar", an unknown scope that should be mapped to Unknown
        let formatted = "42:foo;bar:organization";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: smallvec![DataCategory::Unknown, DataCategory::Unknown],
                scope: RateLimitScope::Organization(42),
                reason_code: None,
                retry_after: rate_limits[0].retry_after,
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
            envelope
        }}
    }

    fn scoping() -> Scoping {
        Scoping {
            organization_id: 42,
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
            key_id: Some(17),
        }
    }

    fn rate_limit(category: DataCategory) -> RateLimit {
        RateLimit {
            categories: vec![category].into(),
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(60),
        }
    }

    #[derive(Debug, Default)]
    struct MockLimiter {
        denied: Vec<DataCategory>,
        called: BTreeMap<DataCategory, usize>,
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
            assert!(previous.is_none(), "rate limiter invoked twice for {}", cat);

            let mut limits = RateLimits::new();
            if self.denied.contains(&cat) {
                limits.add(rate_limit(cat));
            }
            Ok(limits)
        }

        pub fn assert_call(&self, category: DataCategory, quantity: Option<usize>) {
            assert_eq!(self.called.get(&category), quantity.as_ref());
        }
    }

    #[test]
    fn test_enforce_pass_empty() {
        let mut envelope = envelope![];

        let mut mock = MockLimiter::default();
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        assert!(!limits.is_limited());
        assert!(envelope.is_empty());
        mock.assert_call(DataCategory::Error, None);
        mock.assert_call(DataCategory::Attachment, None);
        mock.assert_call(DataCategory::Session, None);
    }

    #[test]
    fn test_enforce_limit_error_event() {
        let mut envelope = envelope![Event];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        assert!(limits.is_limited());
        assert!(envelope.is_empty());
        mock.assert_call(DataCategory::Error, Some(1));
        mock.assert_call(DataCategory::Attachment, None);
        mock.assert_call(DataCategory::Session, None);
    }

    #[test]
    fn test_enforce_limit_error_with_attachments() {
        let mut envelope = envelope![Event, Attachment];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        assert!(limits.is_limited());
        assert!(envelope.is_empty());
        mock.assert_call(DataCategory::Error, Some(1));
        // Error is limited, so no need to call the attachment quota
        mock.assert_call(DataCategory::Attachment, None);
        mock.assert_call(DataCategory::Session, None);
    }

    #[test]
    fn test_enforce_limit_minidump() {
        let mut envelope = envelope![Attachment::Minidump];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        assert!(limits.is_limited());
        assert!(envelope.is_empty());
        mock.assert_call(DataCategory::Error, Some(1));
        // Error is limited, so no need to call the attachment quota
        mock.assert_call(DataCategory::Attachment, None);
        mock.assert_call(DataCategory::Session, None);
    }

    #[test]
    fn test_enforce_limit_attachments() {
        let mut envelope = envelope![Attachment::Minidump, Attachment];

        let mut mock = MockLimiter::default().deny(DataCategory::Attachment);
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        // Attachments would be limited, but crash reports create events and are thus allowed.
        assert!(limits.is_limited());
        assert_eq!(envelope.len(), 1);
        mock.assert_call(DataCategory::Error, Some(1));
        mock.assert_call(DataCategory::Attachment, Some(20));
        mock.assert_call(DataCategory::Session, None);
    }

    #[test]
    fn test_enforce_pass_minidump() {
        let mut envelope = envelope![Attachment::Minidump];

        let mut mock = MockLimiter::default().deny(DataCategory::Attachment);
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        // If only crash report attachments are present, we don't emit a rate limit.
        assert!(!limits.is_limited());
        assert_eq!(envelope.len(), 1);
        mock.assert_call(DataCategory::Error, Some(1));
        mock.assert_call(DataCategory::Attachment, Some(10));
        mock.assert_call(DataCategory::Session, None);
    }

    #[test]
    fn test_enforce_skip_rate_limited() {
        let mut envelope = envelope![];

        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::OctetStream, "0123456789");
        item.set_rate_limited(true);
        envelope.add_item(item);

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        assert!(!limits.is_limited()); // No new rate limits applied.
        assert_eq!(envelope.len(), 1); // The item was retained
        mock.assert_call(DataCategory::Error, None);
        mock.assert_call(DataCategory::Attachment, None); // Limiter not invoked
        mock.assert_call(DataCategory::Session, None);
    }

    #[test]
    fn test_enforce_pass_sessions() {
        let mut envelope = envelope![Session, Session, Session];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        // If only crash report attachments are present, we don't emit a rate limit.
        assert!(!limits.is_limited());
        assert_eq!(envelope.len(), 3);
        mock.assert_call(DataCategory::Error, None);
        mock.assert_call(DataCategory::Attachment, None);
        mock.assert_call(DataCategory::Session, Some(3));
    }

    #[test]
    fn test_enforce_limit_sessions() {
        let mut envelope = envelope![Session, Session, Event];

        let mut mock = MockLimiter::default().deny(DataCategory::Session);
        let (_, limits) = EnvelopeLimiter::new(|s, q| mock.check(s, q))
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        // If only crash report attachments are present, we don't emit a rate limit.
        assert!(limits.is_limited());
        assert_eq!(envelope.len(), 1);
        mock.assert_call(DataCategory::Error, Some(1));
        mock.assert_call(DataCategory::Attachment, None);
        mock.assert_call(DataCategory::Session, Some(2));
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_enforce_limit_assumed_event() {
        let mut envelope = envelope![];

        let mut mock = MockLimiter::default().deny(DataCategory::Transaction);
        let mut limiter = EnvelopeLimiter::new(|s, q| mock.check(s, q));
        limiter.assume_event(DataCategory::Transaction);
        let (_, limits) = limiter
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        assert!(limits.is_limited());
        assert!(envelope.is_empty()); // obviously
        mock.assert_call(DataCategory::Transaction, Some(1));
        mock.assert_call(DataCategory::Attachment, None);
        mock.assert_call(DataCategory::Session, None);
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_enforce_limit_assumed_attachments() {
        let mut envelope = envelope![Attachment, Attachment];

        let mut mock = MockLimiter::default().deny(DataCategory::Error);
        let mut limiter = EnvelopeLimiter::new(|s, q| mock.check(s, q));
        limiter.assume_event(DataCategory::Error);
        let (_, limits) = limiter
            .enforce(&mut envelope, &scoping(), &Enforcement::default())
            .unwrap();

        assert!(limits.is_limited());
        assert!(envelope.is_empty());
        mock.assert_call(DataCategory::Error, Some(1));
        mock.assert_call(DataCategory::Attachment, None);
        mock.assert_call(DataCategory::Session, None);
    }
}
