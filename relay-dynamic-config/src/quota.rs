//! Types defining usage quotas and their scopes.
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[doc(inline)]
pub use relay_common::DataCategory;

/// The unit in which a data category is measured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CategoryUnit {
    Count,
    Bytes,
    Batched,
}

impl CategoryUnit {
    fn from(category: &DataCategory) -> Option<Self> {
        match category {
            DataCategory::Default
            | DataCategory::Error
            | DataCategory::Transaction
            | DataCategory::Replay
            | DataCategory::Security
            | DataCategory::Profile
            | DataCategory::TransactionProcessed
            | DataCategory::TransactionIndexed => Some(Self::Count),
            DataCategory::Attachment => Some(Self::Bytes),
            DataCategory::Session => Some(Self::Batched),
            DataCategory::Unknown => None,
        }
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct ReasonCode(String);

impl ReasonCode {
    /// Creates a new reason code from a string.
    ///
    /// This method is only to be used by tests. Reason codes should only be deserialized from
    /// quotas, but never constructed manually.
    pub fn new<S: Into<String>>(code: S) -> Self {
        Self(code.into())
    }

    /// Returns the string representation of this reason code.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
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

    /// Maximum number of matching events allowed. Can be `0` to reject all events, `None` for an
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
    /// Returns whether this quota is valid for tracking.
    ///
    /// There are a few conditions at which quotas are invalid:
    ///  - The quota only applies to `Unknown` data categories.
    ///  - The quota is counted (not limit `0`) but specifies categories with different units.
    pub fn is_valid(&self) -> bool {
        let mut units = self.categories.iter().filter_map(CategoryUnit::from);

        match units.next() {
            // There are only unknown categories, which is always invalid
            None if !self.categories.is_empty() => false,
            // This is a reject all quota, which is always valid
            _ if self.limit == Some(0) => true,
            // Applies to all categories, which implies multiple units
            None => false,
            // There are multiple categories, which must all have the same units
            Some(unit) => units.all(|u| u == unit),
        }
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
    fn test_quota_valid_reject_all() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
        };

        assert!(quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_only_unknown() {
        let quota = Quota {
            id: None,
            categories: smallvec![DataCategory::Unknown, DataCategory::Unknown],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
        };

        assert!(!quota.is_valid());
    }

    #[test]
    fn test_quota_valid_reject_all_mixed() {
        let quota = Quota {
            id: None,
            categories: smallvec![DataCategory::Error, DataCategory::Attachment],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
        };

        assert!(quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_limited_mixed() {
        let quota = Quota {
            id: None,
            categories: smallvec![DataCategory::Error, DataCategory::Attachment],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(1000),
            window: None,
            reason_code: None,
        };

        // This category is limited and counted, but has multiple units.
        assert!(!quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_unlimited_mixed() {
        let quota = Quota {
            id: None,
            categories: smallvec![DataCategory::Error, DataCategory::Attachment],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
        };

        // This category is unlimited and counted, but has multiple units.
        assert!(!quota.is_valid());
    }
}
