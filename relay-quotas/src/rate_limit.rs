use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use relay_common::{ProjectId, ProjectKey};

use crate::quota::{DataCategories, ItemScoping, Quota, QuotaScope, ReasonCode, Scoping};
use crate::REJECT_ALL_SECS;

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
            Some(duration) if duration.subsec_nanos() == 0 => duration.as_secs(),
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
#[cfg_attr(test, derive(serde::Serialize))]
pub enum RateLimitScope {
    /// An organization with identifier.
    Organization(u64),
    /// A project with identifier.
    Project(ProjectId),
    /// A DSN public key.
    Key(ProjectKey),
}

impl RateLimitScope {
    /// Extracts a rate limiting scope from the given item scoping for a specific quota.
    pub fn for_quota(scoping: &Scoping, scope: QuotaScope) -> Self {
        match scope {
            QuotaScope::Organization => RateLimitScope::Organization(scoping.organization_id),
            QuotaScope::Project => RateLimitScope::Project(scoping.project_id),
            QuotaScope::Key => RateLimitScope::Key(scoping.public_key),
            // For unknown scopes, assume the most specific scope:
            QuotaScope::Unknown => RateLimitScope::Key(scoping.public_key),
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
#[cfg_attr(test, derive(serde::Serialize))]
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
#[cfg_attr(test, derive(serde::Serialize))]
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
        self.get_active_limit().is_some()
    }

    /// Returns an active rate limit from this instance, if there is one.
    pub fn get_active_limit(&self) -> Option<&RateLimit> {
        self.iter().find(|limit| !limit.retry_after.expired())
    }

    /// Removes expired rate limits from this instance.
    pub fn clean_expired(&mut self) {
        self.limits.retain(|limit| !limit.retry_after.expired());
    }

    /// Checks whether any rate limits apply to the given scoping.
    ///
    /// If no limits match, then the returned `RateLimits` instance evalutes `is_ok`. Otherwise, it
    /// contains rate limits that match the given scoping.
    pub fn check(&self, scoping: ItemScoping<'_>) -> Self {
        self.check_with_quotas(&[], scoping)
    }

    /// Checks whether any rate limits apply to the given scoping.
    ///
    /// This is similar to `check`. Additionally, it checks for quotas with a static limit `0`, and
    /// rejects items even if there is no active rate limit in this instance.
    ///
    /// If no limits or quotas match, then the returned `RateLimits` instance evalutes `is_ok`.
    /// Otherwise, it contains rate limits that match the given scoping.
    pub fn check_with_quotas(&self, quotas: &[Quota], scoping: ItemScoping<'_>) -> Self {
        let mut applied_limits = Self::new();

        for quota in quotas {
            if quota.limit == Some(0) && quota.matches(scoping) {
                let retry_after = RetryAfter::from_secs(REJECT_ALL_SECS);
                applied_limits.add(RateLimit::from_quota(quota, &*scoping, retry_after));
            }
        }

        for limit in &self.limits {
            if limit.matches(scoping) {
                applied_limits.add(limit.clone());
            }
        }

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

    /// Returns the longest rate limit that is error releated.
    ///
    /// The most relevant rate limit from the point of view of an error generating an outcome
    /// is the longest rate limit for error messages.
    pub fn longest_error(&self) -> Option<&RateLimit> {
        let is_event_related = |rate_limit: &&RateLimit| {
            rate_limit.categories.is_empty()
                || rate_limit.categories.iter().any(|cat| cat.is_error())
        };

        self.iter()
            .filter(is_event_related)
            .max_by_key(|limit| limit.retry_after)
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
    use crate::quota::DataCategory;
    use smallvec::smallvec;

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
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            }
        }));

        assert!(!rate_limit.matches(ItemScoping {
            category: DataCategory::Transaction,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
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
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            }
        }));

        assert!(!rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 0,
                project_id: ProjectId::new(21),
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
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
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            }
        }));

        assert!(!rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(0),
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            }
        }));
    }

    #[test]
    fn test_rate_limit_matches_key() {
        let rate_limit = RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Key(
                ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        };

        assert!(rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            }
        }));

        assert!(!rate_limit.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 0,
                project_id: ProjectId::new(21),
                public_key: ProjectKey::parse("deadbeefdeadbeefdeadbeefdeadbeef").unwrap(),
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
    fn test_rate_limits_longest_error_none() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Attachment],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Session],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        // only non event rate limits so nothing relevant
        assert_eq!(rate_limits.longest_error(), None)
    }

    #[test]
    fn test_rate_limits_longest_error() {
        let mut rate_limits = RateLimits::new();
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction],
            scope: RateLimitScope::Organization(40),
            reason_code: None,
            retry_after: RetryAfter::from_secs(100),
        });
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Organization(41),
            reason_code: None,
            retry_after: RetryAfter::from_secs(5),
        });
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(7),
        });

        let rate_limit = rate_limits.longest().unwrap();
        insta::assert_ron_snapshot!(rate_limit, @r###"
        RateLimit(
          categories: [
            transaction,
          ],
          scope: Organization(40),
          reason_code: None,
          retry_after: RetryAfter(100),
        )
        "###);
    }

    #[test]
    fn test_rate_limits_clean_expired() {
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

        // Sanity check before running `clean_expired`
        assert_eq!(rate_limits.iter().count(), 2);

        rate_limits.clean_expired();

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
          ],
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

        // Active transaction limit
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        let applied_limits = rate_limits.check(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
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
    }

    #[test]
    fn test_rate_limits_check_quotas() {
        let mut rate_limits = RateLimits::new();

        // Active error limit
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Error],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        // Active transaction limit
        rate_limits.add(RateLimit {
            categories: smallvec![DataCategory::Transaction],
            scope: RateLimitScope::Organization(42),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
        });

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                public_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
        };

        let quotas = &[Quota {
            id: None,
            categories: smallvec![DataCategory::Error],
            scope: QuotaScope::Organization,
            scope_id: Some("42".to_owned()),
            limit: Some(0),
            window: None,
            reason_code: Some(ReasonCode::new("zero")),
        }];

        let applied_limits = rate_limits.check_with_quotas(quotas, item_scoping);

        insta::assert_ron_snapshot!(applied_limits, @r###"
        RateLimits(
          limits: [
            RateLimit(
              categories: [
                error,
              ],
              scope: Organization(42),
              reason_code: Some(ReasonCode("zero")),
              retry_after: RetryAfter(60),
            ),
          ],
        )
        "###);
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
