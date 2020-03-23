use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use relay_common::ProjectId;

/// Data scoping information.
///
/// This structure holds information of all scopes required for attributing an item to quotas.
#[derive(Clone, Debug)]
pub struct Scoping {
    /// The organization id.
    pub organization_id: u64,

    /// The project id.
    pub project_id: ProjectId,

    /// The DSN public key.
    pub public_key: String,

    /// The public key's internal id.
    pub key_id: Option<u64>,
}

impl Scoping {
    /// Returns an `ItemScoping` for this scope.
    ///
    /// The item scoping will contain a reference to this scope and the information passed to this
    /// function. This is a cheap operation to allow rate limiting for an individual item.
    pub fn item(&self, category: DataCategory) -> ItemScoping<'_> {
        ItemScoping {
            category,
            scoping: self,
        }
    }
}

/// Data categorization and scoping information.
///
/// `ItemScoping` is always attached to a `Scope` and references it internally. It is a cheap,
/// copyable type intended for the use with `RateLimits` and `RateLimiter`. It implements
/// `Deref<Target = Scoping>` and `AsRef<Scoping>` for ease of use.
#[derive(Clone, Copy, Debug)]
pub struct ItemScoping<'a> {
    /// The data category of the item.
    pub category: DataCategory,

    /// Scoping of the data.
    pub scoping: &'a Scoping,
}

impl AsRef<Scoping> for ItemScoping<'_> {
    fn as_ref(&self) -> &Scoping {
        &self.scoping
    }
}

impl std::ops::Deref for ItemScoping<'_> {
    type Target = Scoping;

    fn deref(&self) -> &Self::Target {
        &self.scoping
    }
}

impl ItemScoping<'_> {
    /// Returns the identifier of the given scope.
    pub fn scope_id(&self, scope: QuotaScope) -> Option<u64> {
        match scope {
            QuotaScope::Organization => Some(self.organization_id),
            QuotaScope::Project => Some(self.project_id.value()),
            QuotaScope::Key => self.key_id,
            QuotaScope::Unknown => None,
        }
    }

    /// Checks whether the category matches any of the quota's categories.
    fn matches_categories(&self, categories: &DataCategories) -> bool {
        // An empty list of categories means that this quota matches all categories. Note that we
        // skip `Unknown` categories silently. If the list of categories only contains `Unknown`s,
        // we do **not** match, since apparently the quota is meant for some data this Relay does
        // not support yet.
        categories.is_empty() || categories.iter().any(|cat| *cat == self.category)
    }
}

/// Classifies the type of data that is being ingested.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DataCategory {
    /// Events with an `event_type` not explicitly listed below.
    Default,
    /// Error events.
    Error,
    /// Transaction events.
    Transaction,
    /// Events with an event type of `csp`, `hpkp`, `expectct` and `expectstaple`.
    Security,
    /// An attachment. Quantity is the size of the attachment in bytes.
    Attachment,
    /// Session updates. Quantity is the number of updates in the batch.
    Session,
    /// Any other data category not known by this Relay.
    #[serde(other)]
    Unknown,
}

impl DataCategory {
    /// Returns the data category corresponding to the given name.
    pub fn from_name(string: &str) -> Self {
        match string {
            "default" => Self::Default,
            "error" => Self::Error,
            "transaction" => Self::Transaction,
            "security" => Self::Security,
            "attachment" => Self::Attachment,
            "session" => Self::Session,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical name of this data category.
    pub fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Error => "error",
            Self::Transaction => "transaction",
            Self::Security => "security",
            Self::Attachment => "attachment",
            Self::Session => "session",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for DataCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for DataCategory {
    type Err = ();

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_name(string))
    }
}

/// An efficient container for data categories that avoids allocations.
///
/// `DataCategories` is to be treated like a set.
pub type DataCategories = SmallVec<[DataCategory; 8]>;

/// The scope that a quota applies to.
///
/// Except for the `Unknown` variant, this type directly translates to the variants of
/// `RateLimitScope` which are used by rate limits.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QuotaScope {
    /// The organization that this project belongs to.
    ///
    /// This is the top-level scope.
    Organization,

    /// The project.
    ///
    /// This is a sub-scope of `Organization`.
    Project,

    /// A project key, which corresponds to a DSN entry.
    ///
    /// This is a sub-scope of `Project`.
    Key,

    /// Any other scope that is not known by this Relay.
    #[serde(other)]
    Unknown,
}

impl QuotaScope {
    /// Returns the quota scope corresponding to the given name.
    pub fn from_name(string: &str) -> Self {
        match string {
            "organization" => Self::Organization,
            "project" => Self::Project,
            "key" => Self::Key,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical name of this scope.
    pub fn name(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::Project => "project",
            Self::Organization => "organization",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for QuotaScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for QuotaScope {
    type Err = ();

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_name(string))
    }
}

fn default_scope() -> QuotaScope {
    QuotaScope::Organization
}

/// A machine readable, freeform reason code for rate limits.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ReasonCode(String);

impl ReasonCode {
    /// Creates a new reason code from a string.
    ///
    /// This method is only to be used by tests. Reason codes should only be deserialized from
    /// quotas, but never constructed manually.
    #[cfg(test)]
    pub fn new<S: Into<String>>(code: S) -> Self {
        Self(code.into())
    }

    /// Returns the string representation of this reason code.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Configuration for a data ingestion quota (rate limiting).
///
/// Sentry applies multiple quotas to incoming data before accepting it, some of which can be
/// configured by the customer. Each piece of data (such as event, attachment) will be counted
/// against all quotas that it matches with based on the `category`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Quota {
    /// The unique identifier for counting this quota. Required, except for quotas with a `limit` of
    /// `0`, since they are statically enforced.
    #[serde(default)]
    pub id: Option<String>,

    /// A set of data categories that this quota applies to. If missing or empty, this quota
    /// applies to all data.
    #[serde(default = "DataCategories::new")]
    pub categories: DataCategories,

    /// A scope for this quota. This quota is enforced separately within each instance of this scope
    /// (e.g. for each project key separately). Defaults to `QuotaScope::Organization`.
    #[serde(default = "default_scope")]
    pub scope: QuotaScope,

    /// Identifier of the scope to apply to. If set, then this quota will only apply to the
    /// specified scope instance (e.g. a project key). Requires `scope` to be set explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_id: Option<String>,

    /// Maxmimum number of matching events allowed. Can be `0` to reject all events, `None` for an
    /// unlimited counted quota, or a positive number for enforcement. Requires `window` if the
    /// limit is not `0`.
    #[serde(default)]
    pub limit: Option<u32>,

    /// The time window in seconds to enforce this quota in. Required in all cases except `limit=0`,
    /// since those quotas are not measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<u64>,

    /// A machine readable reason returned when this quota is exceeded. Required in all cases except
    /// `limit=None`, since unlimited quotas can never be exceeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<ReasonCode>,
}

impl Quota {
    /// Checks whether this quota's scope matches the given item scoping.
    ///
    /// This quota matches, if:
    ///  - there is no `scope_id` constraint
    ///  - the `scope_id` constraint is not numeric
    ///  - the scope identifier matches the one from ascoping and the scope is known
    fn matches_scope(&self, scoping: ItemScoping<'_>) -> bool {
        // Check for a scope identifier constraint. If there is no constraint, this means that the
        // quota matches any scope. In case the scope is unknown, it will be coerced to the most
        // specific scope later.
        let scope_id = match self.scope_id {
            Some(ref scope_id) => scope_id,
            None => return true,
        };

        // Check if the scope identifier in the quota is parseable. If not, this means we cannot
        // fulfill the constraint, so the quota does not match.
        let parsed = match scope_id.parse::<u64>() {
            Ok(parsed) => parsed,
            Err(_) => return false,
        };

        // At this stage, require that the scope is known since we have to fulfill the constraint.
        scoping.scope_id(self.scope) == Some(parsed)
    }

    /// Checks whether the quota's constraints match the current item.
    pub fn matches(&self, scoping: ItemScoping<'_>) -> bool {
        self.matches_scope(scoping) && scoping.matches_categories(&self.categories)
    }
}

/// A monotonic expiration marker for `RateLimit`s.
///
/// `RetryAfter` marks an instant at which a rate limit expires, which is indicated by `expired`. It
/// can convert into the remaining time until expiration.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RetryAfter {
    when: Instant,
}

impl RetryAfter {
    /// Creates a retry after instance.
    #[inline]
    pub fn from_secs(seconds: u64) -> Self {
        let when = Instant::now() + Duration::from_secs(seconds);
        Self { when }
    }

    /// Returns the remaining duration until the rate limit expires.
    #[inline]
    pub fn remaining(self) -> Option<Duration> {
        let now = Instant::now();
        if now >= self.when {
            None
        } else {
            Some(self.when - now)
        }
    }

    /// Returns the remaining seconds until the rate limit expires.
    ///
    /// This is a shortcut to `retry_after.remaining().as_secs()` whith one exception: If the rate
    /// limit has expired, this function returns `0`.
    #[inline]
    pub fn remaining_seconds(self) -> u64 {
        match self.remaining() {
            // Compensate for the missing subsec part by adding 1s
            Some(duration) => duration.as_secs() + 1,
            None => 0,
        }
    }

    /// Returns whether this rate limit has expired.
    #[inline]
    pub fn expired(self) -> bool {
        self.remaining().is_none()
    }
}

impl fmt::Debug for RetryAfter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.remaining_seconds() {
            0 => write!(f, "RetryAfter(expired)"),
            remaining => write!(f, "RetryAfter({}s)", remaining),
        }
    }
}

#[cfg(test)]
impl serde::Serialize for RetryAfter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTupleStruct;
        let mut tup = serializer.serialize_tuple_struct("RetryAfter", 1)?;
        tup.serialize_field(&self.remaining_seconds())?;
        tup.end()
    }
}

#[derive(Debug)]
/// Error parsing a `RetryAfter`.
pub enum InvalidRetryAfter {
    /// The supplied delay in seconds was not valid.
    InvalidDelay(std::num::ParseFloatError),
}

impl FromStr for RetryAfter {
    type Err = InvalidRetryAfter;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let float = s.parse::<f64>().map_err(InvalidRetryAfter::InvalidDelay)?;
        let seconds = float.max(0.0).ceil() as u64;
        Ok(RetryAfter::from_secs(seconds))
    }
}

/// The scope that a rate limit applied to.
///
/// As opposed to `QuotaScope`, which only declared the class of the scope, this also carries
/// information about the scope instance. That is, the specific identifiers of the individual scopes
/// that a rate limit applied to.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(Serialize))]
pub enum RateLimitScope {
    /// An organization with identifier.
    Organization(u64),
    /// A project with identifier.
    Project(ProjectId),
    /// A DSN public key.
    Key(String),
}

impl RateLimitScope {
    /// Extracts a rate limiting scope from the given item scoping for a specific quota.
    pub fn for_quota(scoping: &Scoping, scope: QuotaScope) -> Self {
        match scope {
            QuotaScope::Organization => RateLimitScope::Organization(scoping.organization_id),
            QuotaScope::Project => RateLimitScope::Project(scoping.project_id),
            QuotaScope::Key => RateLimitScope::Key(scoping.public_key.clone()),
            // For unknown scopes, assume the most specific scope:
            QuotaScope::Unknown => RateLimitScope::Key(scoping.public_key.clone()),
        }
    }

    /// Returns the canonical name of this scope.
    pub fn name(&self) -> &'static str {
        match *self {
            Self::Key(_) => QuotaScope::Key.name(),
            Self::Project(_) => QuotaScope::Project.name(),
            Self::Organization(_) => QuotaScope::Organization.name(),
        }
    }
}

/// A bounded rate limit.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(test, derive(Serialize))]
pub struct RateLimit {
    /// A set of data categories that this quota applies to. If missing or empty, this rate limit
    /// applies to all data.
    pub categories: DataCategories,

    /// The scope of this rate limit.
    pub scope: RateLimitScope,

    /// A machine readable reason indicating which quota caused it.
    pub reason_code: Option<ReasonCode>,

    /// A marker when this rate limit expires.
    pub retry_after: RetryAfter,
}

impl RateLimit {
    /// Creates a new rate limit for the given `Quota`.
    pub fn from_quota(quota: &Quota, scoping: &Scoping, retry_after: RetryAfter) -> Self {
        Self {
            categories: quota.categories.clone(),
            scope: RateLimitScope::for_quota(scoping, quota.scope),
            reason_code: quota.reason_code.clone(),
            retry_after,
        }
    }

    /// Checks whether the rate limit applies to the given item.
    pub fn matches(&self, scoping: ItemScoping<'_>) -> bool {
        self.matches_scope(scoping) && scoping.matches_categories(&self.categories)
    }

    /// Returns `true` if the rate limiting scope matches the given item.
    fn matches_scope(&self, scoping: ItemScoping<'_>) -> bool {
        match self.scope {
            RateLimitScope::Organization(org_id) => scoping.organization_id == org_id,
            RateLimitScope::Project(project_id) => scoping.project_id == project_id,
            RateLimitScope::Key(ref key) => scoping.public_key == *key,
        }
    }
}

/// A collection of scoped rate limits.
///
/// This collection may be empty, indicated by `is_ok`. If this instance carries rate limits, they
/// can be iterated over using `iter`. Additionally, rate limits can be checked for items by
/// invoking `check` with the respective `ItemScoping`.
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Serialize))]
pub struct RateLimits {
    limits: Vec<RateLimit>,
}

impl RateLimits {
    /// Creates an empty RateLimits instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a limit to this collection.
    ///
    /// If a rate limit with an overlapping scope already exists, the `retry_after` count is merged
    /// with the existing limit. Otherwise, the new rate limit is added.
    pub fn add(&mut self, mut limit: RateLimit) {
        // Categories are logically a set, but not implemented as such.
        limit.categories.sort();

        let limit_opt = self
            .limits
            .iter_mut()
            .find(|l| l.categories == limit.categories && l.scope == limit.scope);

        match limit_opt {
            None => self.limits.push(limit),
            Some(existing) if existing.retry_after < limit.retry_after => *existing = limit,
            Some(_) => (), // keep existing, longer limit
        }
    }

    /// Merges all limits into this instance.
    ///
    /// This keeps all existing rate limits, adding new ones, and updating ones with a longer
    /// `retry_after` count. The resulting `RateLimits` contains the merged maximum.
    pub fn merge(&mut self, limits: Self) {
        for limit in limits {
            self.add(limit);
        }
    }

    /// Returns `true` if this instance contains no active limits.
    pub fn is_ok(&self) -> bool {
        !self.is_limited()
    }

    /// Returns `true` if this instance contains active rate limits.
    pub fn is_limited(&self) -> bool {
        self.iter().any(|limit| !limit.retry_after.expired())
    }

    /// Checks whether any rate limits apply to the given scoping returning the matched limits.
    ///
    /// If no limits match, then the returned `RateLimits` instance evalutes `is_ok`. Otherwise, it
    /// contains rate limits that match the given scoping.
    pub fn check(&mut self, scoping: ItemScoping<'_>) -> Self {
        let mut applied_limits = Self::new();

        self.limits.retain(|limit| {
            if limit.retry_after.expired() {
                return false;
            }

            if limit.matches(scoping) {
                applied_limits.add(limit.clone());
            }

            true
        });

        applied_limits
    }

    /// Returns an iterator over the rate limits.
    pub fn iter(&self) -> RateLimitsIter<'_> {
        RateLimitsIter {
            iter: self.limits.iter(),
        }
    }

    /// Returns the longest rate limit.
    ///
    /// If multiple rate limits have the same retry after count, any of the limits is returned.
    pub fn longest(&self) -> Option<&RateLimit> {
        self.iter().max_by_key(|limit| limit.retry_after)
    }
}

/// Immutable rate limits iterator.
///
/// This struct is created by the `iter` method on `RateLimits`.
pub struct RateLimitsIter<'a> {
    iter: std::slice::Iter<'a, RateLimit>,
}

impl<'a> Iterator for RateLimitsIter<'a> {
    type Item = &'a RateLimit;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl IntoIterator for RateLimits {
    type IntoIter = RateLimitsIntoIter;
    type Item = RateLimit;

    fn into_iter(self) -> Self::IntoIter {
        RateLimitsIntoIter {
            iter: self.limits.into_iter(),
        }
    }
}

/// An iterator that moves out of `RateLimtis`.
///
/// This struct is created by the `into_iter` method on `RateLimits`, provided by the `IntoIterator`
/// trait.
pub struct RateLimitsIntoIter {
    iter: std::vec::IntoIter<RateLimit>,
}

impl Iterator for RateLimitsIntoIter {
    type Item = RateLimit;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a> IntoIterator for &'a RateLimits {
    type IntoIter = RateLimitsIter<'a>;
    type Item = &'a RateLimit;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    #[test]
    fn test_parse_quota_reject_all() {
        let json = r#"{
            "limit": 0,
            "reasonCode": "not_yet"
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: None,
          categories: [],
          scope: organization,
          limit: Some(0),
          reasonCode: Some(ReasonCode("not_yet")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_reject_transactions() {
        let json = r#"{
            "limit": 0,
            "categories": ["transaction"],
            "reasonCode": "not_yet"
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: None,
          categories: [
            transaction,
          ],
          scope: organization,
          limit: Some(0),
          reasonCode: Some(ReasonCode("not_yet")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_limited() {
        let json = r#"{
            "id": "o",
            "limit": 4711,
            "window": 42,
            "reasonCode": "not_so_fast"
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("o"),
          categories: [],
          scope: organization,
          limit: Some(4711),
          window: Some(42),
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_project() {
        let json = r#"{
            "id": "p",
            "scope": "project",
            "scopeId": "1",
            "limit": 4711,
            "window": 42,
            "reasonCode": "not_so_fast"
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("p"),
          categories: [],
          scope: project,
          scopeId: Some("1"),
          limit: Some(4711),
          window: Some(42),
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_key() {
        let json = r#"{
            "id": "k",
            "scope": "key",
            "scopeId": "1",
            "limit": 4711,
            "window": 42,
            "reasonCode": "not_so_fast"
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("k"),
          categories: [],
          scope: key,
          scopeId: Some("1"),
          limit: Some(4711),
          window: Some(42),
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_unknown_variants() {
        let json = r#"{
            "id": "f",
            "categories": ["future"],
            "scope": "future",
            "scopeId": "1",
            "limit": 4711,
            "window": 42,
            "reasonCode": "not_so_fast"
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("f"),
          categories: [
            unknown,
          ],
          scope: unknown,
          scopeId: Some("1"),
          limit: Some(4711),
          window: Some(42),
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_unlimited() {
        let json = r#"{
            "id": "o",
            "window": 42
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("o"),
          categories: [],
          scope: organization,
          limit: None,
          window: Some(42),
        )
        "###);
    }

    #[test]
    fn test_quota_matches_no_categories() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));
    }

    #[test]
    fn test_quota_matches_unknown_category() {
        let quota = Quota {
            id: None,
            categories: smallvec![DataCategory::Unknown],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
        };

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));
    }

    #[test]
    fn test_quota_matches_multiple_categores() {
        let quota = Quota {
            id: None,
            categories: smallvec![DataCategory::Unknown, DataCategory::Error],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Transaction,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));
    }

    #[test]
    fn test_quota_matches_no_invalid_scope() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: Some("not_a_number".to_owned()),
            limit: None,
            window: None,
            reason_code: None,
        };

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));
    }

    #[test]
    fn test_quota_matches_organization_scope() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: Some("42".to_owned()),
            limit: None,
            window: None,
            reason_code: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 0,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));
    }

    #[test]
    fn test_quota_matches_project_scope() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Project,
            scope_id: Some("21".to_owned()),
            limit: None,
            window: None,
            reason_code: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(0),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));
    }

    #[test]
    fn test_quota_matches_key_scope() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Key,
            scope_id: Some("17".to_owned()),
            limit: None,
            window: None,
            reason_code: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(17),
            }
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: Some(0),
            }
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            }
        }));
    }

    #[test]
    fn test_parse_retry_after() {
        // positive float
        let retry_after = "17.7".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 18);
        assert!(!retry_after.expired());

        // positive int
        let retry_after = "17".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 17);
        assert!(!retry_after.expired());

        // negative number
        let retry_after = "-2".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 0);
        assert!(retry_after.expired());

        // invalid string
        "nope".parse::<RetryAfter>().expect_err("error RetryAfter");
    }

    #[test]
    fn test_rate_limit_matches_categories() {
        let rate_limit = RateLimit {
            categories: smallvec![DataCategory::Unknown, DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        };

        assert!(rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            }
        }));

        assert!(!rate_limit.matches(ItemScoping {
            category: DataCategory::Transaction,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            }
        }));
    }

    #[test]
    fn test_rate_limit_matches_organization() {
        let rate_limit = RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        };

        assert!(rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            }
        }));

        assert!(!rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 0,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            }
        }));
    }

    #[test]
    fn test_rate_limit_matches_project() {
        let rate_limit = RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        };

        assert!(rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            }
        }));

        assert!(!rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(0),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            }
        }));
    }

    #[test]
    fn test_rate_limit_matches_key() {
        let rate_limit = RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Key("a94ae32be2584e0bbd7a4cbb95971fee".to_owned()),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        };

        assert!(rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            }
        }));

        assert!(!rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 0,
                project_id: ProjectId::new(21),
                public_key: "deadbeefdeadbeefdeadbeefdeadbeef".to_owned(),
                key_id: None,
            }
        }));
    }

    #[test]
    fn test_rate_limits_add_replacement() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Default, DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("first")),
            retry_after: RetryAfter::from_secs(1),
        });

        // longer rate limit shadows shorter one
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error, DataCategory::Default],
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("second")),
            retry_after: RetryAfter::from_secs(10),
        });

        insta::assert_ron_snapshot!(rate_limits, @r###"
        RateLimits(
          limits: [
            RateLimit(
              categories: [
                default,
                error,
              ],
              scope: Organization(42),
              reason_code: Some(ReasonCode("second")),
              retry_after: RetryAfter(10),
            ),
          ],
        )
        "###);
    }

    #[test]
    fn test_rate_limits_add_shadowing() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Default, DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("first")),
            retry_after: RetryAfter::from_secs(10),
        });

        // shorter rate limit is shadowed by existing one
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error, DataCategory::Default],
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("second")),
            retry_after: RetryAfter::from_secs(1),
        });

        insta::assert_ron_snapshot!(rate_limits, @r###"
        RateLimits(
          limits: [
            RateLimit(
              categories: [
                default,
                error,
              ],
              scope: Organization(42),
              reason_code: Some(ReasonCode("first")),
              retry_after: RetryAfter(10),
            ),
          ],
        )
        "###);
    }

    #[test]
    fn test_rate_limits_add_buckets() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        // Same scope but different categories
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        // Same categories but different scope
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        insta::assert_ron_snapshot!(rate_limits, @r###"
        RateLimits(
          limits: [
            RateLimit(
              categories: [
                error,
              ],
              scope: Organization(42),
              reason_code: None,
              retry_after: RetryAfter(1),
            ),
            RateLimit(
              categories: [
                transaction,
              ],
              scope: Organization(42),
              reason_code: None,
              retry_after: RetryAfter(1),
            ),
            RateLimit(
              categories: [
                error,
              ],
              scope: Project(ProjectId(21)),
              reason_code: None,
              retry_after: RetryAfter(1),
            ),
          ],
        )
        "###);
    }

    #[test]
    fn test_rate_limits_longest() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("first")),
            retry_after: RetryAfter::from_secs(1),
        });

        // Distinct scope to prevent deduplication
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction],
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("second")),
            retry_after: RetryAfter::from_secs(10),
        });

        let rate_limit = rate_limits.longest().unwrap();
        insta::assert_ron_snapshot!(rate_limit, @r###"
        RateLimit(
          categories: [
            transaction,
          ],
          scope: Organization(42),
          reason_code: Some(ReasonCode("second")),
          retry_after: RetryAfter(10),
        )
        "###);
    }

    #[test]
    fn test_rate_limits_check() {
        let mut rate_limits = RateLimits::new();

        // Active error limit
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        // Inactive error limit with distinct scope
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(0),
        });

        // Active transaction limit
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        // Sanity check before running `check`
        assert_eq!(rate_limits.iter().count(), 3);

        let applied_limits = rate_limits.check(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
                key_id: None,
            },
        });

        // Check that the error limit is applied
        insta::assert_ron_snapshot!(applied_limits, @r###"
        RateLimits(
          limits: [
            RateLimit(
              categories: [
                error,
              ],
              scope: Organization(42),
              reason_code: None,
              retry_after: RetryAfter(1),
            ),
          ],
        )
        "###);

        // Check that the expired limit has been removed
        insta::assert_ron_snapshot!(rate_limits, @r###"
        RateLimits(
          limits: [
            RateLimit(
              categories: [
                error,
              ],
              scope: Organization(42),
              reason_code: None,
              retry_after: RetryAfter(1),
            ),
            RateLimit(
              categories: [
                transaction,
              ],
              scope: Organization(42),
              reason_code: None,
              retry_after: RetryAfter(1),
            ),
          ],
        )
        "###);
        assert_eq!(rate_limits.iter().count(), 2);
    }

    #[test]
    fn test_rate_limits_merge() {
        let mut rate_limits1 = RateLimits::new();
        let mut rate_limits2 = RateLimits::new();

        rate_limits1.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("first")),
            retry_after: RetryAfter::from_secs(1),
        });

        rate_limits1.add(RateLimit {
            categories: smallvec![DataCategory::Transaction],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        rate_limits2.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: Some(ReasonCode::new("second")),
            retry_after: RetryAfter::from_secs(10),
        });

        rate_limits1.merge(rate_limits2);

        insta::assert_ron_snapshot!(rate_limits1, @r###"
        RateLimits(
          limits: [
            RateLimit(
              categories: [
                error,
              ],
              scope: Organization(42),
              reason_code: Some(ReasonCode("second")),
              retry_after: RetryAfter(10),
            ),
            RateLimit(
              categories: [
                transaction,
              ],
              scope: Organization(42),
              reason_code: None,
              retry_after: RetryAfter(1),
            ),
          ],
        )
        "###);
    }
}
