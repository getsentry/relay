use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use relay_common::ProjectId;

/// Data categorization and scoping information.
///
/// This structure holds information of all scopes required for attributing an item to quotas.
#[derive(Clone, Debug)]
pub struct ItemScoping {
    /// The data category of the item.
    pub category: DataCategory,
    /// The organization id.
    pub organization_id: u64,
    /// The project id.
    pub project_id: ProjectId,
    /// The public key's id (not the public key).
    pub key_id: Option<u64>,
}

impl ItemScoping {
    /// Returns the identifier of the given scope.
    pub fn scope_id(&self, scope: QuotaScope) -> Option<u64> {
        match scope {
            QuotaScope::Organization => Some(self.organization_id),
            QuotaScope::Project => Some(self.project_id.value()),
            QuotaScope::Key => self.key_id,
            QuotaScope::Unknown => None,
        }
    }
}

/// Classifies the type of data that is being ingested.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename = "lowercase")]
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

/// The scope that a quota applies to.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename = "lowercase")]
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

fn default_scope() -> QuotaScope {
    QuotaScope::Organization
}

/// Configuration for a data ingestion quota (rate limiting).
///
/// Sentry applies multiple quotas to incoming data before accepting it, some of which can be
/// configured by the customer. Each piece of data (such as event, attachment) will be counted
/// against all quotas that it matches with based on the `category`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename = "camelCase")]
pub struct Quota {
    /// The unique identifier for counting this quota. Required, except for quotas with a `limit` of
    /// `0`, since they are statically enforced.
    #[serde(default)]
    pub id: Option<String>,

    /// A set of data categories that this quota applies to. If missing or empty, this quota
    /// applies to all data.
    #[serde(default = "Vec::new")]
    pub categories: Vec<DataCategory>,

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
    pub reason_code: Option<String>,
}

impl Quota {
    /// Checks whether this quota's scope matches the given item scoping.
    ///
    /// This quota matches, if:
    ///  - there is no `scope_id` constraint
    ///  - the `scope_id` constraint is not numeric
    ///  - the scope identifier matches the one from ascoping and the scope is known
    fn matches_scope(&self, scoping: &ItemScoping) -> bool {
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

    /// Checks whether the item matches any of the quota's categories.
    fn matches_category(&self, scoping: &ItemScoping) -> bool {
        // An empty list of categories means that this quota matches all categories. Note that we
        // skip `Unknown` categories silently. If the list of categories only contains `Unknown`s,
        // we do **not** match, since apparently the quota is meant for some data this Relay does
        // not support yet.
        self.categories.is_empty() || self.categories.iter().any(|cat| *cat == scoping.category)
    }

    /// Checks whether the quota's constraints match the current item.
    pub fn matches(&self, scoping: &ItemScoping) -> bool {
        self.matches_scope(scoping) && self.matches_category(scoping)
    }
}

/// A rate limit with optional reason code.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq)]
pub struct RetryAfter {
    when: Instant,
    reason_code: Option<String>,
}

impl RetryAfter {
    /// Creates a retry after instance.
    #[inline]
    pub fn new(seconds: u64) -> Self {
        Self::with_reason(seconds, None)
    }

    /// Creates a retry after instance with the given reason code.
    #[inline]
    pub fn with_reason(seconds: u64, reason_code: Option<String>) -> Self {
        let when = Instant::now() + Duration::from_secs(seconds);
        Self { when, reason_code }
    }

    /// Returns the remaining seconds until the rate limit expires.
    ///
    /// If the rate limit has expired, this function returns `0`.
    pub fn remaining_seconds(&self) -> u64 {
        let now = Instant::now();
        if now > self.when {
            return 0;
        }

        // Compensate for the missing subsec part by adding 1s
        (self.when - now).as_secs() + 1
    }

    /// Returns the optional reason for this rate limit.
    #[cfg_attr(not(feature = "processing"), allow(dead_code))]
    pub fn reason_code(&self) -> Option<&str> {
        self.reason_code.as_deref()
    }

    /// Returns whether this rate limit has expired.
    pub fn expired(&self) -> bool {
        self.remaining_seconds() == 0
    }
}

impl PartialOrd for RetryAfter {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.when.partial_cmp(&other.when)
    }
}
