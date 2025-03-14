use std::fmt;
use std::str::FromStr;

use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::{ProjectId, ProjectKey};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[doc(inline)]
pub use relay_base_schema::data_category::DataCategory;

/// Data scoping information.
///
/// This structure holds information of all scopes required for attributing an item to quotas.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Scoping {
    /// The organization id.
    pub organization_id: OrganizationId,

    /// The project id.
    pub project_id: ProjectId,

    /// The DSN public key.
    pub project_key: ProjectKey,

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
            namespace: MetricNamespaceScoping::None,
        }
    }

    /// Returns an `ItemScoping` for metric buckets in this scope.
    ///
    /// The item scoping will contain a reference to this scope, the category
    /// [`MetricBucket](DataCategory::MetricBucket), and the information passed to this function.
    /// This is a cheap operation to allow rate limiting for an individual item.
    pub fn metric_bucket(&self, namespace: MetricNamespace) -> ItemScoping<'_> {
        ItemScoping {
            category: DataCategory::MetricBucket,
            scoping: self,
            namespace: MetricNamespaceScoping::Some(namespace),
        }
    }
}

/// Item scoping of metric namespaces.
///
/// This enum is used in [`ItemScoping`] to declare the contents of an item's metric namespace.
#[derive(Clone, Copy, Debug, Default, PartialEq, Hash, PartialOrd)]
pub enum MetricNamespaceScoping {
    /// This item does not contain metrics of any namespace. This should only be used for non-metric
    /// items.
    #[default]
    None,

    /// This item contains metrics of a specific namespace.
    Some(MetricNamespace),

    /// This item contains metrics of any namespace.
    ///
    /// The namespace of metrics contained in this item is not known. This can be used to check rate
    /// limits or quotas of any namespace.
    Any,
}

impl MetricNamespaceScoping {
    /// Returns `true` if the given namespace matches the namespace of the item.
    ///
    /// If the self is `Any`, this method returns `true` for any namespace.
    pub fn matches(&self, namespace: MetricNamespace) -> bool {
        match self {
            Self::None => false,
            Self::Some(ns) => *ns == namespace,
            Self::Any => true,
        }
    }
}

impl From<MetricNamespace> for MetricNamespaceScoping {
    fn from(namespace: MetricNamespace) -> Self {
        Self::Some(namespace)
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

    /// Namespace for metric items, requiring [`DataCategory::MetricBucket`].
    pub namespace: MetricNamespaceScoping,
}

impl AsRef<Scoping> for ItemScoping<'_> {
    fn as_ref(&self) -> &Scoping {
        self.scoping
    }
}

impl std::ops::Deref for ItemScoping<'_> {
    type Target = Scoping;

    fn deref(&self) -> &Self::Target {
        self.scoping
    }
}

impl ItemScoping<'_> {
    /// Returns the identifier of the given scope.
    pub fn scope_id(&self, scope: QuotaScope) -> Option<u64> {
        match scope {
            QuotaScope::Global => None,
            QuotaScope::Organization => Some(self.organization_id.value()),
            QuotaScope::Project => Some(self.project_id.value()),
            QuotaScope::Key => self.key_id,
            QuotaScope::Unknown => None,
        }
    }

    /// Checks whether the category matches any of the quota's categories.
    pub(crate) fn matches_categories(&self, categories: &DataCategories) -> bool {
        // An empty list of categories means that this quota matches all categories. Note that we
        // skip `Unknown` categories silently. If the list of categories only contains `Unknown`s,
        // we do **not** match, since apparently the quota is meant for some data this Relay does
        // not support yet.
        categories.is_empty() || categories.iter().any(|cat| *cat == self.category)
    }

    /// Returns `true` if the rate limit namespace matches the namespace of the item.
    ///
    /// Matching behavior depends on the passed namespaces and the namespace of the scoping:
    ///  - If the list of namespaces is empty, this check always returns `true`.
    ///  - If the list of namespaces contains at least one namespace, a namespace on the scoping is
    ///    required. [`MetricNamespaceScoping::None`] will not match.
    ///  - If the namespace of this scoping is [`MetricNamespaceScoping::Any`], this check will
    ///    always return true.
    ///  - Otherwise, an exact match of the scoping's namespace must be found in the list.
    ///
    /// `namespace` can be either a slice, an iterator, or a reference to an
    /// `Option<MetricNamespace>`. In case of `None`, this method behaves like an empty list and
    /// permits any namespace.
    pub(crate) fn matches_namespaces<'a, I>(&self, namespaces: I) -> bool
    where
        I: IntoIterator<Item = &'a MetricNamespace>,
    {
        let mut iter = namespaces.into_iter().peekable();
        iter.peek().is_none() || iter.any(|ns| self.namespace.matches(*ns))
    }
}

/// The unit in which a data category is measured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CategoryUnit {
    /// Counts the number of items.
    Count,
    /// Counts the number of bytes across items.
    Bytes,
    /// Counts the accumulated times across items.
    Milliseconds,
}

impl CategoryUnit {
    fn from(category: &DataCategory) -> Option<Self> {
        match category {
            DataCategory::Default
            | DataCategory::Error
            | DataCategory::Transaction
            | DataCategory::Replay
            | DataCategory::DoNotUseReplayVideo
            | DataCategory::Security
            | DataCategory::Profile
            | DataCategory::ProfileIndexed
            | DataCategory::TransactionProcessed
            | DataCategory::TransactionIndexed
            | DataCategory::LogItem
            | DataCategory::LogByte
            | DataCategory::Span
            | DataCategory::SpanIndexed
            | DataCategory::MonitorSeat
            | DataCategory::Monitor
            | DataCategory::MetricBucket
            | DataCategory::UserReportV2
            | DataCategory::ProfileChunk
            | DataCategory::Uptime
            | DataCategory::MetricSecond
            | DataCategory::AttachmentItem
            | DataCategory::Session => Some(Self::Count),
            DataCategory::Attachment => Some(Self::Bytes),
            DataCategory::ProfileDuration | DataCategory::ProfileDurationUi => {
                Some(Self::Milliseconds)
            }

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
    /// Global scope, matches everything.
    Global,
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
            "global" => Self::Global,
            "organization" => Self::Organization,
            "project" => Self::Project,
            "key" => Self::Key,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical name of this scope.
    pub fn name(self) -> &'static str {
        match self {
            Self::Global => "global",
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
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
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
    ///
    /// For attachments, this limit expresses the number of allowed bytes.
    #[serde(default)]
    pub limit: Option<u64>,

    /// The time window in seconds to enforce this quota in. Required in all cases except `limit=0`,
    /// since those quotas are not measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<u64>,

    /// The namespace the quota applies to.
    ///
    /// If `None`, it will match any namespace.
    pub namespace: Option<MetricNamespace>,

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
    ///  - The quota references an unsupported namespace.
    pub fn is_valid(&self) -> bool {
        if self.namespace == Some(MetricNamespace::Unsupported) {
            return false;
        }

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

    /// Checks whether this quota's scope matches the given item scoping.
    ///
    /// This quota matches, if:
    ///  - there is no `scope_id` constraint
    ///  - the `scope_id` constraint is not numeric
    ///  - the scope identifier matches the one from ascoping and the scope is known
    fn matches_scope(&self, scoping: ItemScoping<'_>) -> bool {
        if self.scope == QuotaScope::Global {
            return true;
        }

        // Check for a scope identifier constraint. If there is no constraint, this means that the
        // quota matches any scope. In case the scope is unknown, it will be coerced to the most
        // specific scope later.
        let Some(scope_id) = self.scope_id.as_ref() else {
            return true;
        };

        // Check if the scope identifier in the quota is parseable. If not, this means we cannot
        // fulfill the constraint, so the quota does not match.
        let Ok(parsed) = scope_id.parse::<u64>() else {
            return false;
        };

        // At this stage, require that the scope is known since we have to fulfill the constraint.
        scoping.scope_id(self.scope) == Some(parsed)
    }

    /// Checks whether the quota's constraints match the current item.
    pub fn matches(&self, scoping: ItemScoping<'_>) -> bool {
        self.matches_scope(scoping)
            && scoping.matches_categories(&self.categories)
            && scoping.matches_namespaces(&self.namespace)
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;

    use super::*;

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
          namespace: None,
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
          namespace: None,
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
          namespace: None,
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
          namespace: None,
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_project_large() {
        let json = r#"{
            "id": "p",
            "scope": "project",
            "scopeId": "1",
            "limit": 4294967296,
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
          limit: Some(4294967296),
          window: Some(42),
          namespace: None,
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
          namespace: None,
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
          namespace: None,
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
          namespace: None,
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
            namespace: None,
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
            namespace: None,
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
            namespace: None,
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
            namespace: None,
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
            namespace: None,
        };

        // This category is unlimited and counted, but has multiple units.
        assert!(!quota.is_valid());
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
            namespace: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
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
            namespace: None,
        };

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
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
            namespace: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Transaction,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
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
            namespace: None,
        };

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
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
            namespace: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(0),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
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
            namespace: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(0),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
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
            namespace: None,
        };

        assert!(quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(0),
            },
            namespace: MetricNamespaceScoping::None,
        }));

        assert!(!quota.matches(ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
        }));
    }
}
