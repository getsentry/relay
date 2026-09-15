use std::collections::HashSet;
use std::fmt::{self};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::{ProjectId, ProjectKey};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[doc(inline)]
pub use relay_base_schema::data_category::{CategoryUnit, DataCategory};

use crate::EMPTY_DIMENSIONS;

/// Data scoping information for rate limiting and quota enforcement.
///
/// [`Scoping`] holds all the identifiers needed to attribute data to specific
/// organizations, projects, and keys. This allows the rate limiting and quota
/// systems to enforce limits at the appropriate scope levels.
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
    /// Creates an [`ItemScoping`] for a specific data category in this scope.
    ///
    /// The returned item scoping contains a reference to this scope and the provided
    /// data category. This is a cheap operation that allows for efficient rate limiting
    /// of individual items.
    pub fn item(&self, category: DataCategory) -> ItemScoping {
        ItemScoping {
            category,
            scoping: *self,
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }
    }

    /// Creates an [`ItemScoping`] for a specific data category, along with specified dimensions
    pub fn item_with_dimensions(
        &self,
        category: DataCategory,
        dimensions: Arc<[(Dimension, String)]>,
    ) -> ItemScoping {
        ItemScoping {
            category,
            scoping: *self,
            namespace: MetricNamespaceScoping::None,
            dimensions: Some(dimensions),
        }
    }

    /// Creates an [`ItemScoping`] specifically for metric buckets in this scope.
    ///
    /// The returned item scoping contains a reference to this scope, the
    /// [`DataCategory::MetricBucket`] category, and the provided metric namespace.
    /// This is specialized for handling metrics with namespaces.
    pub fn metric_bucket(&self, namespace: MetricNamespace) -> ItemScoping {
        ItemScoping {
            category: DataCategory::MetricBucket,
            scoping: *self,
            namespace: MetricNamespaceScoping::Some(namespace),
            dimensions: None,
        }
    }
}

/// Describes the metric namespace scoping of an item.
///
/// This enum is used within [`ItemScoping`] to represent the metric namespace of an item.
/// It handles the different cases: no namespace, a specific namespace, or any namespace.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, PartialOrd)]
pub enum MetricNamespaceScoping {
    /// The item does not contain metrics of any namespace.
    ///
    /// This should only be used for non-metric items.
    #[default]
    None,

    /// The item contains metrics of a specific namespace.
    Some(MetricNamespace),

    /// The item contains metrics of any namespace.
    ///
    /// The specific namespace is not known or relevant. This can be used to check rate
    /// limits or quotas that should apply to any namespace.
    Any,
}

impl MetricNamespaceScoping {
    /// Checks if the given namespace matches this namespace scoping.
    ///
    /// Returns `true` in the following cases:
    /// - If `self` is [`MetricNamespaceScoping::Some`] with the same namespace
    /// - If `self` is [`MetricNamespaceScoping::Any`], matching any namespace
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

/// Data categorization and scoping information for a single item.
///
/// [`ItemScoping`] combines a data category, scoping information, and optional
/// metric namespace to fully define an item for rate limiting purposes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ItemScoping {
    /// The data category of the item.
    pub category: DataCategory,

    /// Scoping of the data.
    pub scoping: Scoping,

    /// Namespace for metric items, requiring [`DataCategory::MetricBucket`].
    pub namespace: MetricNamespaceScoping,

    /// Dimensions this quota will be matched on.
    pub dimensions: Option<Arc<[(Dimension, String)]>>,
}

impl std::ops::Deref for ItemScoping {
    type Target = Scoping;

    fn deref(&self) -> &Self::Target {
        &self.scoping
    }
}

impl ItemScoping {
    /// Returns the identifier for the given quota scope.
    ///
    /// Maps the quota scope type to the corresponding identifier from this scoping,
    /// or `None` if the scope type doesn't have an applicable identifier.
    pub fn scope_id(&self, scope: QuotaScope) -> Option<u64> {
        match scope {
            QuotaScope::Organization => Some(self.organization_id.value()),
            QuotaScope::Project => Some(self.project_id.value()),
            QuotaScope::Key => self.key_id,
            QuotaScope::Unknown => None,
        }
    }

    /// Converts the dimensions of this item scoping, for the supplied quota, into a string of the
    /// dimension name and hashed value.  This looks like ':key1:hash1:key2:hash2'.
    /// This function assumes that the quota passed to it already matches this ItemScoping.
    pub fn dimensions_as_string(&self, quota: &Quota) -> String {
        let mut result = String::new();

        let Some(item_dimensions) = &self.dimensions else {
            return EMPTY_DIMENSIONS.to_owned();
        };

        let Some(quota_dimensions) = &quota.dimensions else {
            return EMPTY_DIMENSIONS.to_owned();
        };

        for dim in quota_dimensions.dimensions.iter() {
            for item in item_dimensions.iter() {
                if item.0 == *dim {
                    let mut hasher = fnv::FnvHasher::with_key(1);
                    item.1.hash(&mut hasher);
                    let h = &hasher.finish();

                    result.push(':');
                    result += &item.0.to_string();
                    result.push(':');
                    result += &h.to_string();

                    break;
                }
            }
        }

        if result.is_empty() {
            result = EMPTY_DIMENSIONS.to_owned();
        }

        result
    }

    /// Checks whether the category matches any of the quota's categories.
    pub(crate) fn matches_categories(&self, categories: DataCategories) -> bool {
        // An empty list of categories means that this quota matches all categories. Note that we
        // skip `Unknown` categories silently. If the list of categories only contains `Unknown`s,
        // we do **not** match, since apparently the quota is meant for some data this Relay does
        // not support yet.
        categories.is_empty() || categories.contains(&self.category)
    }

    /// Checks wether this item matches all of the supplied quota's dimensions.
    pub(crate) fn matches_dimensions(&self, dimensions: &Option<Dimensions>) -> bool {
        let Some(quota_dims) = dimensions else {
            // If the quota has no dimensions, we trivially match it.
            return true;
        };

        let Some(item_dims) = &self.dimensions else {
            // If the quota has dimensions, but we do not, we trivially do not match it.
            return false;
        };

        // Ensure each of the quota's required dimensions are present in us.
        for dim in quota_dims.dimensions.iter() {
            if item_dims.iter().find(|i| i.0 == *dim).is_none() {
                return false;
            }
        }

        true
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

/// An efficient container for data categories that avoids allocations.
///
/// It is a read only and has set like properties, allowing for fast comparisons.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
pub struct DataCategories(u64);

impl Serialize for DataCategories {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut ser = serializer.serialize_seq(Some(self.len()))?;
        for category in self.iter() {
            ser.serialize_element(&category)?;
        }
        ser.end()
    }
}

impl DataCategories {
    fn category_to_mask(category: DataCategory) -> u64 {
        if matches!(category, DataCategory::Unknown) {
            // Handle Unknown by throwing it into the last bit.
            1u64 << 63
        } else {
            1u64 << (category as u8)
        }
    }

    fn bit_number_to_category(bit_number: u8) -> DataCategory {
        bit_number.try_into().unwrap_or(DataCategory::Unknown)
    }

    /// Creates new and empty [`DataCategories`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Creates a new [`DataCategories`] from the supplied slice of [`DataCategory`] values.
    pub fn from_slice(slice: &[DataCategory]) -> Self {
        let mut categories = 0;
        for category in slice {
            categories |= DataCategories::category_to_mask(*category);
        }

        Self(categories)
    }

    /// Adds a data category to [`Self`].
    ///
    /// Returns `None` if the category was already contained, otherwise creates a new [`Self`] with
    /// the `category` added.
    pub fn add(&self, category: DataCategory) -> Option<Self> {
        let category_mask = DataCategories::category_to_mask(category);

        if (self.0 & category_mask) != 0 {
            return None;
        }

        Some(Self(self.0 | category_mask))
    }

    /// Returns true iff the category is contained.
    pub fn contains(&self, category: &DataCategory) -> bool {
        let category_mask = DataCategories::category_to_mask(*category);
        (self.0 & category_mask) != 0
    }

    /// Returns an iterator over this [`DataCategories`] container.
    pub fn iter(&self) -> DataCategoryIterator {
        // We start our iteration from the lsb, so the number of trailings zeroes is our 0-based
        // starting index.
        let bit_start = self.0.trailing_zeros();

        // Shift our bitfield so that the first bit is in the 0th place.
        let (categories, _) = self.0.overflowing_shr(bit_start);
        DataCategoryIterator {
            categories,
            current_bit: bit_start as u8,
        }
    }

    /// Returns the number of categories in this container.
    pub fn len(&self) -> usize {
        self.0.count_ones() as usize
    }

    /// Returns true iff this container contains no categories.
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

/// An iterator over a [`DataCategories`] container.
pub struct DataCategoryIterator {
    categories: u64,
    current_bit: u8,
}

impl Iterator for DataCategoryIterator {
    type Item = DataCategory;

    fn next(&mut self) -> Option<Self::Item> {
        if self.categories == 0 {
            return None;
        }
        let category = DataCategories::bit_number_to_category(self.current_bit);

        // Consume the last bit
        self.categories &= u64::MAX - 1;

        // Find the next set bit
        let trailing_zeroes = self.categories.trailing_zeros() as u8;

        // Put that bit in the 0th (to-consume) position
        (self.categories, _) = self.categories.overflowing_shr(trailing_zeroes as u32);

        // Update the current bit state
        self.current_bit += trailing_zeroes;

        Some(category)
    }
}

impl<'de> Deserialize<'de> for DataCategories {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        SmallVec::<[DataCategory; 12]>::deserialize(deserializer).map(Self::from)
    }
}

impl<const N: usize> From<SmallVec<[DataCategory; N]>> for DataCategories {
    fn from(categories: SmallVec<[DataCategory; N]>) -> Self {
        Self::from_slice(&categories)
    }
}

impl<const N: usize> From<[DataCategory; N]> for DataCategories {
    fn from(categories: [DataCategory; N]) -> Self {
        Self::from_slice(&categories)
    }
}

impl FromIterator<DataCategory> for DataCategories {
    fn from_iter<T: IntoIterator<Item = DataCategory>>(iter: T) -> Self {
        let v: SmallVec<[DataCategory; 12]> = iter.into_iter().collect();
        Self::from_slice(&v)
    }
}

/// The scope at which a quota is applied.
///
/// Defines the granularity at which quotas are enforced, from organizations
/// down to individual project keys. This enum only defines the type of scope,
/// not the specific instance.
///
/// This type is directly related to [`crate::rate_limit::RateLimitScope`], which
/// includes the specific scope identifiers.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QuotaScope {
    /// The organization level.
    ///
    /// This is the top-level scope.
    Organization,

    /// The project level.
    ///
    /// Projects are contained within organizations.
    Project,

    /// The project key level (corresponds to a DSN).
    ///
    /// This is the most specific scope level and is contained within projects.
    Key,

    /// Any scope type not recognized by this Relay.
    #[serde(other)]
    Unknown,
}

impl QuotaScope {
    /// Returns the quota scope corresponding to the given name string.
    ///
    /// If the string doesn't match any known scope, returns [`QuotaScope::Unknown`].
    pub fn from_name(string: &str) -> Self {
        match string {
            "organization" => Self::Organization,
            "project" => Self::Project,
            "key" => Self::Key,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical string name of this scope.
    ///
    /// This is the lowercase string representation used in serialization.
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

/// A machine-readable reason code for rate limits.
///
/// Reason codes provide a standardized way to communicate why a particular
/// item was rate limited.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct ReasonCode(Arc<str>);

impl ReasonCode {
    /// Creates a new reason code from a string.
    ///
    /// This method is primarily intended for testing. In production, reason codes
    /// should typically be deserialized from quota configurations rather than
    /// constructed manually.
    pub fn new<S: Into<Arc<str>>>(code: S) -> Self {
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

/// Configuration for a data ingestion quota.
///
/// A quota defines restrictions on data ingestion based on data categories, scopes,
/// and time windows. The system applies multiple quotas to incoming data, and items
/// are counted against all matching quotas based on their categories.
///
/// Quotas can either:
/// - Reject all data (`limit` = 0)
/// - Limit data to a specific quantity per time window (`limit` > 0)
/// - Count data without limiting it (`limit` = None)
///
/// Different quotas may apply at different scope levels (organization, project, key).
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Quota {
    /// The unique identifier for counting this quota.
    ///
    /// Required for all quotas except those with `limit` = 0, which are statically enforced.
    #[serde(default)]
    pub id: Option<Arc<str>>,

    /// Data categories this quota applies to.
    ///
    /// If missing or empty, this quota applies to all data categories.
    #[serde(default)]
    pub categories: DataCategories,

    /// The scope level at which this quota is enforced.
    ///
    /// The quota is enforced separately within each instance of this scope
    /// (e.g., for each project key separately). Defaults to [`QuotaScope::Organization`].
    #[serde(default = "default_scope")]
    pub scope: QuotaScope,

    /// Specific scope instance identifier this quota applies to.
    ///
    /// If set, this quota only applies to the specified scope instance
    /// (e.g., a specific project key). Requires `scope` to be set explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_id: Option<Arc<str>>,

    /// Maximum number of events allowed within the time window.
    ///
    /// Possible values:
    /// - `Some(0)`: Reject all matching events
    /// - `Some(n)`: Allow up to n events per time window
    /// - `None`: Unlimited quota (counts but doesn't limit)
    ///
    /// Requires `window` to be set if the limit is not 0.
    #[serde(default)]
    pub limit: Option<u64>,

    /// The time window in seconds for quota enforcement.
    ///
    /// Required in all cases except `limit` = 0, since those quotas
    /// are not measured over time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<u64>,

    /// The metric namespace this quota applies to.
    ///
    /// If `None`, it matches any namespace.
    pub namespace: Option<MetricNamespace>,

    /// A machine-readable reason code returned when this quota is exceeded.
    ///
    /// Required for all quotas except those with `limit` = None, since
    /// unlimited quotas can never be exceeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<ReasonCode>,

    /// The optional list of dimensions that this quota will use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<Dimensions>,
}

/// The dimensions that a quota can key on--this is for things like monitors, that require even
/// more fine-grained rate-limiting.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Dimensions {
    /// The maximum number combinations of dimensions on this quota to allow.  Set to None
    /// for "infinite".
    pub max_cardinality: Option<u32>,

    /// The list of dimensions this quota will use.
    pub dimensions: Arc<[Dimension]>,
}

/// The kinds of dimensions that can be applied to a given quota.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase")]
pub enum Dimension {
    /// The environment used in a monitor check-in.
    CheckInEnvironment = 1,

    /// The slug used in a monitor check-in.
    CheckInSlug = 2,

    /// An unknown dimension.
    #[serde(other)]
    Unknown = 0,
}

impl fmt::Display for Dimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}", *self as u32))
    }
}

impl Quota {
    /// Returns whether this quota is valid for tracking.
    ///
    /// A quota is considered invalid if any of the following conditions are true:
    ///  - The quota only applies to [`DataCategory::Unknown`] data categories.
    ///  - The quota is counted (not limit `0`) but specifies categories with different units.
    ///  - The quota references an unsupported namespace.
    ///  - The dimensions contain Unknown, or are not distinct, or is empty.
    pub fn is_valid(&self) -> bool {
        if self.namespace == Some(MetricNamespace::Unsupported) {
            return false;
        }

        if let Some(dims) = &self.dimensions {
            // A non-none dimension set should also be non-empty.
            if dims.dimensions.is_empty() {
                return false;
            }

            let mut distinct = HashSet::new();
            for dim in dims.dimensions.iter() {
                if *dim == Dimension::Unknown {
                    return false;
                }

                distinct.insert(dim);
            }

            if distinct.len() != dims.dimensions.len() {
                return false;
            }
        }

        let mut units = self
            .categories
            .iter()
            .filter_map(CategoryUnit::from_category);

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
    fn matches_scope(&self, scoping: &ItemScoping) -> bool {
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
    ///
    /// This method determines if this quota should be applied to a given item
    /// based on its scope, categories, and namespace.
    pub fn matches(&self, scoping: &ItemScoping) -> bool {
        self.matches_scope(scoping)
            && scoping.matches_categories(self.categories)
            && scoping.matches_namespaces(&self.namespace)
            && scoping.matches_dimensions(&self.dimensions)
    }

    /// Builds the dimensions key for this quota, which is a colon-separated list of the tags of
    /// the dimensions on this quota.
    pub fn dimensions_key(&self) -> String {
        let mut result = String::new();
        if let Some(dims) = &self.dimensions {
            for dim in dims.dimensions.iter() {
                result += &dim.to_string();
                result.push(':');
            }
        } else {
            result = EMPTY_DIMENSIONS.to_owned();
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use relay_base_schema::data_category::UnknownDataCategory;

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

        insta::assert_ron_snapshot!(quota, @r#"
        Quota(
          id: None,
          categories: [
            "transaction",
          ],
          scope: organization,
          limit: Some(0),
          namespace: None,
          reasonCode: Some(ReasonCode("not_yet")),
        )
        "#);
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

        insta::assert_ron_snapshot!(quota, @r#"
        Quota(
          id: Some("f"),
          categories: [
            "unknown",
          ],
          scope: unknown,
          scopeId: Some("1"),
          limit: Some(4711),
          window: Some(42),
          namespace: None,
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "#);
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
            categories: Default::default(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_only_unknown() {
        let quota = Quota {
            id: None,
            categories: [DataCategory::Unknown, DataCategory::Unknown].into(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(!quota.is_valid());
    }

    #[test]
    fn test_quota_valid_reject_all_mixed() {
        let quota = Quota {
            id: None,
            categories: [DataCategory::Error, DataCategory::Attachment].into(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(0),
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_limited_mixed() {
        let quota = Quota {
            id: None,
            categories: [DataCategory::Error, DataCategory::Attachment].into(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(1000),
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        // This category is limited and counted, but has multiple units.
        assert!(!quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_unlimited_mixed() {
        let quota = Quota {
            id: None,
            categories: [DataCategory::Error, DataCategory::Attachment].into(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        // This category is unlimited and counted, but has multiple units.
        assert!(!quota.is_valid());
    }

    #[test]
    fn test_quota_matches_no_categories() {
        let quota = Quota {
            id: None,
            categories: Default::default(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));
    }

    #[test]
    fn test_quota_matches_unknown_category() {
        let quota = Quota {
            id: None,
            categories: [DataCategory::Unknown].into(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(!quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));
    }

    #[test]
    fn test_quota_matches_multiple_categores() {
        let quota = Quota {
            id: None,
            categories: [DataCategory::Unknown, DataCategory::Error].into(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));

        assert!(!quota.matches(&ItemScoping {
            category: DataCategory::Transaction,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));
    }

    #[test]
    fn test_quota_matches_no_invalid_scope() {
        let quota = Quota {
            id: None,
            categories: Default::default(),
            scope: QuotaScope::Organization,
            scope_id: Some("not_a_number".into()),
            limit: None,
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(!quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));
    }

    #[test]
    fn test_quota_matches_organization_scope() {
        let quota = Quota {
            id: None,
            categories: Default::default(),
            scope: QuotaScope::Organization,
            scope_id: Some("42".into()),
            limit: None,
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));

        assert!(!quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(0),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));
    }

    #[test]
    fn test_quota_matches_project_scope() {
        let quota = Quota {
            id: None,
            categories: Default::default(),
            scope: QuotaScope::Project,
            scope_id: Some("21".into()),
            limit: None,
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));

        assert!(!quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(0),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));
    }

    #[test]
    fn test_quota_matches_key_scope() {
        let quota = Quota {
            id: None,
            categories: Default::default(),
            scope: QuotaScope::Key,
            scope_id: Some("17".into()),
            limit: None,
            window: None,
            reason_code: None,
            namespace: None,
            dimensions: None,
        };

        assert!(quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));

        assert!(!quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(0),
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));

        assert!(!quota.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: None,
        }));
    }

    /// Builds a monitor quota with the passed dimensions.
    fn dimensioned_quota(dimensions: Option<Dimensions>) -> Quota {
        Quota {
            id: Some("q".into()),
            categories: [DataCategory::Monitor].into(),
            scope: QuotaScope::Project,
            scope_id: None,
            limit: Some(10),
            window: Some(60),
            reason_code: None,
            namespace: None,
            dimensions,
        }
    }

    /// Builds a monitor item scoping with the passed dimensions.
    fn dimensioned_scoping(dimensions: Option<&[(Dimension, &str)]>) -> ItemScoping {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        match dimensions {
            None => scoping.item(DataCategory::Monitor),
            Some(dims) => scoping.item_with_dimensions(
                DataCategory::Monitor,
                dims.iter().map(|(d, v)| (*d, (*v).to_owned())).collect(),
            ),
        }
    }

    #[test]
    fn test_quota_valid_dimensions() {
        let quota = dimensioned_quota(Some(Dimensions {
            max_cardinality: Some(100),
            dimensions: [Dimension::CheckInSlug, Dimension::CheckInEnvironment].into(),
        }));

        assert!(quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_empty_dimensions() {
        let quota = dimensioned_quota(Some(Dimensions {
            max_cardinality: None,
            dimensions: [].into(),
        }));

        // A dimension set which is present must not be empty.
        assert!(!quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_unknown_dimension() {
        let quota = dimensioned_quota(Some(Dimensions {
            max_cardinality: None,
            dimensions: [Dimension::CheckInSlug, Dimension::Unknown].into(),
        }));

        assert!(!quota.is_valid());
    }

    #[test]
    fn test_quota_invalid_duplicate_dimensions() {
        let quota = dimensioned_quota(Some(Dimensions {
            max_cardinality: None,
            dimensions: [Dimension::CheckInSlug, Dimension::CheckInSlug].into(),
        }));

        assert!(!quota.is_valid());
    }

    #[test]
    fn test_quota_matches_dimensions() {
        let quota = dimensioned_quota(Some(Dimensions {
            max_cardinality: None,
            dimensions: [Dimension::CheckInEnvironment, Dimension::CheckInSlug].into(),
        }));

        // Exactly the required dimensions, in either order.
        assert!(quota.matches(&dimensioned_scoping(Some(&[
            (Dimension::CheckInEnvironment, "prod"),
            (Dimension::CheckInSlug, "cron1"),
        ]))));
        assert!(quota.matches(&dimensioned_scoping(Some(&[
            (Dimension::CheckInSlug, "cron1"),
            (Dimension::CheckInEnvironment, "prod"),
        ]))));

        // A subset of the required dimensions does not match.
        assert!(!quota.matches(&dimensioned_scoping(Some(&[(
            Dimension::CheckInSlug,
            "cron1"
        )]))));

        // Neither does an empty or absent dimension set.
        assert!(!quota.matches(&dimensioned_scoping(Some(&[]))));
        assert!(!quota.matches(&dimensioned_scoping(None)));
    }

    #[test]
    fn test_quota_without_dimensions_matches_any_item() {
        let quota = dimensioned_quota(None);

        // A quota without dimensions applies to every item, dimensioned or not.
        assert!(quota.matches(&dimensioned_scoping(None)));
        assert!(quota.matches(&dimensioned_scoping(Some(&[
            (Dimension::CheckInEnvironment, "prod"),
            (Dimension::CheckInSlug, "cron1"),
        ]))));
    }

    #[test]
    fn test_dimensions_as_string() {
        let quota = dimensioned_quota(Some(Dimensions {
            max_cardinality: None,
            dimensions: [Dimension::CheckInEnvironment, Dimension::CheckInSlug].into(),
        }));

        let key = dimensioned_scoping(Some(&[
            (Dimension::CheckInEnvironment, "prod"),
            (Dimension::CheckInSlug, "cron1"),
        ]))
        .dimensions_as_string(&quota);

        // `:<dimension>:<hash>` per dimension, ordered by the quota's dimensions.
        let parts = key.split(':').collect::<Vec<_>>();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[0], "");
        assert_eq!(parts[1], Dimension::CheckInEnvironment.to_string());
        assert_eq!(parts[3], Dimension::CheckInSlug.to_string());
        assert!(parts[2].parse::<u64>().is_ok());
        assert!(parts[4].parse::<u64>().is_ok());
    }

    #[test]
    fn test_dimensions_as_string_ignores_extra_dimensions() {
        let quota = dimensioned_quota(Some(Dimensions {
            max_cardinality: None,
            dimensions: [Dimension::CheckInSlug].into(),
        }));

        let expected = dimensioned_scoping(Some(&[(Dimension::CheckInSlug, "cron1")]))
            .dimensions_as_string(&quota);

        assert_eq!(
            dimensioned_scoping(Some(&[
                (Dimension::CheckInSlug, "cron1"),
                (Dimension::CheckInEnvironment, "prod"),
            ]))
            .dimensions_as_string(&quota),
            expected
        );
    }

    #[test]
    fn test_dimensions_as_string_without_dimensions() {
        let dimensions = Some(Dimensions {
            max_cardinality: None,
            dimensions: [Dimension::CheckInSlug].into(),
        });

        let dimensioned_item =
            dimensioned_scoping(Some(&[(Dimension::CheckInEnvironment, "prod")]));

        // Neither side having dimensions, or only one side having them, collapses to the
        // single "no dimensions" bucket.
        assert_eq!(
            dimensioned_scoping(None).dimensions_as_string(&dimensioned_quota(None)),
            "_"
        );
        assert_eq!(
            dimensioned_scoping(None).dimensions_as_string(&dimensioned_quota(dimensions.clone())),
            "_"
        );
        assert_eq!(
            dimensioned_item.dimensions_as_string(&dimensioned_quota(None)),
            "_"
        );

        // So does an item which shares no dimension with the quota.
        assert_eq!(
            dimensioned_item.dimensions_as_string(&dimensioned_quota(dimensions)),
            "_"
        );
    }

    #[test]
    fn test_dimensions_key() {
        assert_eq!(dimensioned_quota(None).dimensions_key(), "_");

        assert_eq!(
            dimensioned_quota(Some(Dimensions {
                max_cardinality: None,
                dimensions: [Dimension::CheckInEnvironment, Dimension::CheckInSlug].into(),
            }))
            .dimensions_key(),
            "1:2:"
        );

        // The key follows the quota's dimension order.
        assert_eq!(
            dimensioned_quota(Some(Dimensions {
                max_cardinality: None,
                dimensions: [Dimension::CheckInSlug, Dimension::CheckInEnvironment].into(),
            }))
            .dimensions_key(),
            "2:1:"
        );
    }

    #[test]
    fn test_parse_quota_dimensions() {
        let json = r#"{
            "id": "o",
            "categories": ["monitor"],
            "limit": 4711,
            "window": 42,
            "dimensions": {
                "maxCardinality": 100,
                "dimensions": ["checkInSlug", "checkInEnvironment"]
            }
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r#"
        Quota(
          id: Some("o"),
          categories: [
            "monitor",
          ],
          scope: organization,
          limit: Some(4711),
          window: Some(42),
          namespace: None,
          dimensions: Some(Dimensions(
            maxCardinality: Some(100),
            dimensions: [
              checkInSlug,
              checkInEnvironment,
            ],
          )),
        )
        "#);

        assert!(quota.is_valid());
    }

    #[test]
    fn test_parse_quota_dimensions_unknown() {
        let json = r#"{
            "id": "o",
            "limit": 4711,
            "window": 42,
            "dimensions": {
                "maxCardinality": null,
                "dimensions": ["checkInSlug", "somethingNew"]
            }
        }"#;

        let quota = serde_json::from_str::<Quota>(json).expect("parse quota");

        // Unknown dimensions parse, but invalidate the quota, since we cannot bucket by a
        // dimension we do not understand.
        insta::assert_ron_snapshot!(quota, @r#"
        Quota(
          id: Some("o"),
          categories: [],
          scope: organization,
          limit: Some(4711),
          window: Some(42),
          namespace: None,
          dimensions: Some(Dimensions(
            maxCardinality: None,
            dimensions: [
              checkInSlug,
              unknown,
            ],
          )),
        )
        "#);

        assert!(!quota.is_valid());
    }

    #[test]
    fn test_data_categories_sorted_deduplicated() {
        let a = DataCategories::from([
            DataCategory::Transaction,
            DataCategory::Span,
            DataCategory::Transaction,
        ]);
        let b = DataCategories::from([
            DataCategory::Span,
            DataCategory::Transaction,
            DataCategory::Span,
        ]);
        let c = DataCategories::from([DataCategory::Span, DataCategory::Transaction]);

        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(a, c);
    }

    #[test]
    fn test_data_categories_serde() {
        let s: DataCategories = serde_json::from_str(r#"["span", "transaction", "span"]"#).unwrap();
        insta::assert_json_snapshot!(s, @r#"
        [
          "transaction",
          "span"
        ]
        "#);
    }

    #[test]
    fn test_data_categories_add() {
        let c = DataCategories::new();
        let c = c.add(DataCategory::Span).unwrap();
        assert!(c.add(DataCategory::Span).is_none());
        let c = c.add(DataCategory::Transaction).unwrap();
        assert_eq!(c, [DataCategory::Span, DataCategory::Transaction].into());
    }

    #[test]
    fn test_reserve_last_bit_data_category() {
        // Ensure we don't accidentally use 63 as a valid DataCategory.
        let r: Result<DataCategory, UnknownDataCategory> = 63u32.try_into();
        assert!(r.is_err());
    }

    #[test]
    fn test_bitmapping_identity() {
        assert_eq!(
            DataCategories::category_to_mask(DataCategory::Unknown),
            1u64 << 63
        );
        for i in 0..63u32 {
            let r: Result<DataCategory, UnknownDataCategory> = i.try_into();
            if let Ok(dc) = r {
                assert_eq!(DataCategories::bit_number_to_category(i as u8), dc);
                assert_eq!(DataCategories::category_to_mask(dc), 1 << i);
            } else {
                break;
            }
        }
    }
}
