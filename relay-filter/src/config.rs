//! Config structs for all filters.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

use indexmap::IndexMap;
use relay_common::glob3::GlobPatterns;
use relay_protocol::RuleCondition;
use serde::ser::SerializeSeq;
use serde::{de, Deserialize, Serialize, Serializer};

/// Common configuration for event filters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterConfig {
    /// Specifies whether this filter is enabled.
    pub is_enabled: bool,
}

impl FilterConfig {
    /// Returns true if no configuration for this filter is given.
    pub fn is_empty(&self) -> bool {
        !self.is_enabled
    }
}

/// A browser class to be filtered by the legacy browser filter.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LegacyBrowser {
    /// Applies the default set of min-version filters for all known browsers.
    Default,
    /// Apply to Internet Explorer 8 and older.
    IePre9,
    /// Apply to Internet Explorer 9.
    Ie9,
    /// Apply to Internet Explorer 10.
    Ie10,
    /// Apply to Internet Explorer 11.
    Ie11,
    /// Apply to Opera 14 and older.
    OperaPre15,
    /// Apply to OperaMini 7 and older.
    OperaMiniPre8,
    /// Apply to Android (Chrome) 3 and older.
    AndroidPre4,
    /// Apply to Safari 5 and older.
    SafariPre6,
    /// Edge legacy i.e. 12-18.
    EdgePre79,
    /// Apply to Internet Explorer
    Ie,
    /// Apply to Safari
    Safari,
    /// Apply to Opera
    Opera,
    /// Apply to OperaMini
    OperaMini,
    /// Apply to Android Browser
    Android,
    /// Apply to Firefox
    Firefox,
    /// Apply to Chrome
    Chrome,
    /// Apply to Edge
    Edge,
    /// An unknown browser configuration for forward compatibility.
    Unknown(String),
}

impl FromStr for LegacyBrowser {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = match s {
            "default" => LegacyBrowser::Default,
            "ie_pre_9" => LegacyBrowser::IePre9,
            "ie9" => LegacyBrowser::Ie9,
            "ie10" => LegacyBrowser::Ie10,
            "ie11" => LegacyBrowser::Ie11,
            "opera_pre_15" => LegacyBrowser::OperaPre15,
            "opera_mini_pre_8" => LegacyBrowser::OperaMiniPre8,
            "android_pre_4" => LegacyBrowser::AndroidPre4,
            "safari_pre_6" => LegacyBrowser::SafariPre6,
            "edge_pre_79" => LegacyBrowser::EdgePre79,
            "ie" => LegacyBrowser::Ie,
            "safari" => LegacyBrowser::Safari,
            "opera" => LegacyBrowser::Opera,
            "opera_mini" => LegacyBrowser::OperaMini,
            "android" => LegacyBrowser::Android,
            "firefox" => LegacyBrowser::Firefox,
            "chrome" => LegacyBrowser::Chrome,
            "edge" => LegacyBrowser::Edge,
            _ => LegacyBrowser::Unknown(s.to_owned()),
        };
        Ok(v)
    }
}

impl<'de> Deserialize<'de> for LegacyBrowser {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s = Cow::<str>::deserialize(deserializer)?;
        Ok(LegacyBrowser::from_str(s.as_ref()).unwrap())
    }
}

impl Serialize for LegacyBrowser {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(match self {
            LegacyBrowser::Default => "default",
            LegacyBrowser::IePre9 => "ie_pre_9",
            LegacyBrowser::Ie9 => "ie9",
            LegacyBrowser::Ie10 => "ie10",
            LegacyBrowser::Ie11 => "ie11",
            LegacyBrowser::OperaPre15 => "opera_pre_15",
            LegacyBrowser::OperaMiniPre8 => "opera_mini_pre_8",
            LegacyBrowser::AndroidPre4 => "android_pre_4",
            LegacyBrowser::SafariPre6 => "safari_pre_6",
            LegacyBrowser::EdgePre79 => "edge_pre_79",
            LegacyBrowser::Ie => "ie",
            LegacyBrowser::Safari => "safari",
            LegacyBrowser::Opera => "opera",
            LegacyBrowser::OperaMini => "opera_mini",
            LegacyBrowser::Android => "android",
            LegacyBrowser::Firefox => "firefox",
            LegacyBrowser::Chrome => "chrome",
            LegacyBrowser::Edge => "edge",
            LegacyBrowser::Unknown(string) => string,
        })
    }
}

/// Configuration for the client ips filter.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientIpsFilterConfig {
    /// Blacklisted client ip addresses.
    pub blacklisted_ips: Vec<String>,
}

impl ClientIpsFilterConfig {
    /// Returns true if no configuration for this filter is given.
    pub fn is_empty(&self) -> bool {
        self.blacklisted_ips.is_empty()
    }
}

/// Configuration for the CSP filter.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CspFilterConfig {
    /// Disallowed sources for CSP reports.
    pub disallowed_sources: Vec<String>,
}

impl CspFilterConfig {
    /// Returns true if no configuration for this filter is given.
    pub fn is_empty(&self) -> bool {
        self.disallowed_sources.is_empty()
    }
}

/// Configuration for the error messages filter.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ErrorMessagesFilterConfig {
    /// List of error message patterns that will be filtered.
    pub patterns: GlobPatterns,
}

/// Configuration for transaction name filter.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IgnoreTransactionsFilterConfig {
    /// List of patterns for ignored transactions that should be filtered.
    pub patterns: GlobPatterns,
    /// True if the filter is enabled
    #[serde(default)]
    pub is_enabled: bool,
}

impl IgnoreTransactionsFilterConfig {
    /// Returns true if no configuration for this filter is given.
    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty() || !self.is_enabled
    }
}

impl ErrorMessagesFilterConfig {
    /// Returns true if no configuration for this filter is given.
    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }
}

/// Configuration for the releases filter.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReleasesFilterConfig {
    /// List of release names that will be filtered.
    pub releases: GlobPatterns,
}

impl ReleasesFilterConfig {
    /// Returns true if no configuration for this filter is given.
    pub fn is_empty(&self) -> bool {
        self.releases.is_empty()
    }
}

/// Configuration for the legacy browsers filter.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyBrowsersFilterConfig {
    /// Specifies whether this filter is enabled.
    pub is_enabled: bool,
    /// The browsers to filter.
    #[serde(default, rename = "options")]
    pub browsers: BTreeSet<LegacyBrowser>,
}

impl LegacyBrowsersFilterConfig {
    /// Returns true if no configuration for this filter is given.
    pub fn is_empty(&self) -> bool {
        !self.is_enabled && self.browsers.is_empty()
    }
}

/// Configuration for a generic filter.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenericFilterConfig {
    /// Unique identifier of the generic filter.
    pub id: String,
    /// Specifies whether this filter is enabled.
    pub is_enabled: bool,
    /// The condition for the filter.
    pub condition: Option<RuleCondition>,
}

impl GenericFilterConfig {
    /// Returns true if the filter is not enabled or no condition was supplied.
    pub fn is_empty(&self) -> bool {
        !self.is_enabled || self.condition.is_none()
    }
}

impl PartialEq for GenericFilterConfig {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

/// Configuration for generic filters.
///
/// # Deserialization
///
/// `filters` is expected to be a [`Vec<GenericfilterConfig>`].
/// Only the first occurrence of a filter is kept, and duplicates are removed.
/// Two filters are considered duplicates if they have the same ID,
/// independently of the condition.
///
/// The list of filters is deserialized into an [`IndexMap`], where the key is
/// the filter's id and the value is the filter itself. The map is converted
/// back to a list when serializing it, without the filters that were discarded
/// as duplicates. See examples below.
///
/// # Examples
///
/// Deserialization:
///
/// ```
/// # use relay_filter::GenericFiltersConfig;
/// # use insta::assert_debug_snapshot;
///
/// let json = r#"{
///     "version": 1,
///     "filters": [
///         {
///             "id": "filter1",
///             "isEnabled": false,
///             "condition": null
///         },
///         {
///             "id": "filter1",
///             "isEnabled": true,
///             "condition": {
///                 "op": "eq",
///                 "name": "event.exceptions",
///                 "value": "drop-error"
///             }
///         }
///     ]
/// }"#;
/// let deserialized = serde_json::from_str::<GenericFiltersConfig>(json).unwrap();
/// assert_debug_snapshot!(deserialized, @r#"
///     GenericFiltersConfig {
///         version: 1,
///         filters: {
///             "filter1": GenericFilterConfig {
///                 id: "filter1",
///                 is_enabled: false,
///                 condition: None,
///             },
///         },
///     }
/// "#);
/// ```
///
/// Serialization:
///
/// ```
/// # use relay_filter::{GenericFiltersConfig, GenericFilterConfig};
/// # use relay_protocol::condition::RuleCondition;
/// # use insta::assert_display_snapshot;
/// # use indexmap::IndexMap;
///
/// let filter = GenericFiltersConfig {
///     version: 1,
///     filters: IndexMap::from([(
///         "filter1".to_owned(),
///         GenericFilterConfig {
///             id: "filter1".to_owned(),
///             is_enabled: true,
///             condition: Some(RuleCondition::eq("event.exceptions", "drop-error")),
///         },
///     )]),
/// };
/// let serialized = serde_json::to_string_pretty(&filter).unwrap();
/// assert_display_snapshot!(serialized, @r#"{
///   "version": 1,
///   "filters": [
///     {
///       "id": "filter1",
///       "isEnabled": true,
///       "condition": {
///         "op": "eq",
///         "name": "event.exceptions",
///         "value": "drop-error"
///       }
///     }
///   ]
/// }"#);
/// ```
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GenericFiltersConfig {
    /// Version of the filters configuration.
    pub version: u16,
    /// Map of generic filters, sorted by the order in the payload from upstream.
    ///
    /// The map contains unique filters, meaning there are no two filters with
    /// the same id. See struct docs for more details.
    #[serde(
        deserialize_with = "deserialize_generic_filters",
        serialize_with = "serialize_generic_filters"
    )]
    pub filters: IndexMap<String, GenericFilterConfig>,
}

impl GenericFiltersConfig {
    /// Returns true if the filters are not declared.
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }
}

fn deserialize_generic_filters<'de, D>(
    deserializer: D,
) -> Result<IndexMap<String, GenericFilterConfig>, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct GenericFiltersVisitor;

    impl<'de> serde::de::Visitor<'de> for GenericFiltersVisitor {
        type Value = IndexMap<String, GenericFilterConfig>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a vector of filters: Vec<GenericFilterConfig>")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut filters = IndexMap::new();
            while let Some(filter) = seq.next_element::<GenericFilterConfig>()? {
                if filters.contains_key(&filter.id) {
                    continue;
                }
                filters.insert(filter.id.clone(), filter);
            }
            Ok(filters)
        }
    }

    deserializer.deserialize_seq(GenericFiltersVisitor)
}

fn serialize_generic_filters<S>(
    index_map: &IndexMap<String, GenericFilterConfig>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(index_map.len()))?;
    for (_, filter) in index_map.iter() {
        seq.serialize_element(filter)?;
    }
    seq.end()
}

/// Configuration for all event filters from project configs.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectFiltersConfig {
    /// Configuration for the Browser Extensions filter.
    #[serde(default, skip_serializing_if = "FilterConfig::is_empty")]
    pub browser_extensions: FilterConfig,

    /// Configuration for the Client IPs filter.
    #[serde(default, skip_serializing_if = "ClientIpsFilterConfig::is_empty")]
    pub client_ips: ClientIpsFilterConfig,

    /// Configuration for the Web Crawlers filter
    #[serde(default, skip_serializing_if = "FilterConfig::is_empty")]
    pub web_crawlers: FilterConfig,

    /// Configuration for the CSP filter.
    #[serde(default, skip_serializing_if = "CspFilterConfig::is_empty")]
    pub csp: CspFilterConfig,

    /// Configuration for the Error Messages filter.
    #[serde(default, skip_serializing_if = "ErrorMessagesFilterConfig::is_empty")]
    pub error_messages: ErrorMessagesFilterConfig,

    /// Configuration for the Legacy Browsers filter.
    #[serde(default, skip_serializing_if = "LegacyBrowsersFilterConfig::is_empty")]
    pub legacy_browsers: LegacyBrowsersFilterConfig,

    /// Configuration for the Localhost filter.
    #[serde(default, skip_serializing_if = "FilterConfig::is_empty")]
    pub localhost: FilterConfig,

    /// Configuration for the releases filter.
    #[serde(default, skip_serializing_if = "ReleasesFilterConfig::is_empty")]
    pub releases: ReleasesFilterConfig,

    /// Configuration for ignore transactions filter.
    #[serde(
        default,
        skip_serializing_if = "IgnoreTransactionsFilterConfig::is_empty"
    )]
    pub ignore_transactions: IgnoreTransactionsFilterConfig,

    /// Configuration for generic filters from the project configs.
    #[serde(default, skip_serializing_if = "GenericFiltersConfig::is_empty")]
    pub generic: GenericFiltersConfig,
}

impl ProjectFiltersConfig {
    /// Returns true if there are no filter configurations declared.
    pub fn is_empty(&self) -> bool {
        self.browser_extensions.is_empty()
            && self.client_ips.is_empty()
            && self.web_crawlers.is_empty()
            && self.csp.is_empty()
            && self.error_messages.is_empty()
            && self.legacy_browsers.is_empty()
            && self.localhost.is_empty()
            && self.releases.is_empty()
            && self.ignore_transactions.is_empty()
            && self.generic.is_empty()
    }
}

/// Configuration of generic filters combined from project and global config.
///
/// See [`CombinedFiltersConfigIter`] for details on how to iterate easily
/// through the applicable filters.
#[derive(Copy, Clone, Debug)]
pub(crate) struct CombinedFiltersConfig<'a> {
    /// Configuration of generic filters from project configs.
    project_filters: &'a GenericFiltersConfig,
    /// Configuration of generic filters from global config.
    global_filters: Option<&'a GenericFiltersConfig>,
    /// Maximum supported version for generic filters.
    ///
    /// It applies to all filters, from both project and global configs.
    max_version: u16,
}

impl<'a> CombinedFiltersConfig<'a> {
    /// Creates a [`CombinedFiltersConfig`] from the project and global configs.
    pub fn new(
        project_filters: &'a GenericFiltersConfig,
        global_filters: Option<&'a GenericFiltersConfig>,
        max_version: u16,
    ) -> Self {
        CombinedFiltersConfig {
            project_filters,
            global_filters,
            max_version,
        }
    }
}

impl<'a> IntoIterator for CombinedFiltersConfig<'a> {
    type Item = &'a GenericFilterConfig;
    type IntoIter = CombinedFiltersConfigIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        CombinedFiltersConfigIter::new(self)
    }
}

/// Iterator over [`GenericFilterConfig`] of a project and global config.
///
/// Iterates in order through the generic filters in project configs and global
/// configs yielding the filters according to the principles below:
///
/// - Filters from project configs are evaluated before filters from global
/// configs.
/// - No duplicates: once a filter is evaluated (yielded or skipped), no filter
/// with the same id is evaluated again.
/// - Filters in project configs override filters from global configs, but the
/// opposite is never the case.
/// - A filter in the project config can be a flag, where only `is_enabled` is
/// defined and `condition` is not. In that case:
///   - If `is_enabled` is true, the filter with a matching ID from global
///   configs is yielded without evaluating its `is_enabled`. Unless the filter
///   in the global config also has an empty condition, in which case the filter
///   is not yielded.
///   - If `is_enabled` is false, no filters with the same IDs are returned,
///   including matching filters from global configs.
#[derive(Debug)]
pub(crate) struct CombinedFiltersConfigIter<'a> {
    /// Configuration of project and global filters.
    config: CombinedFiltersConfig<'a>,
    /// Index of the next filter in project config to evaluate.
    project_index: usize,
    /// Index of the next filter in global config to evaluate.
    global_index: usize,
    /// Filters that have been evaluated, either yielded or ignored.
    evaluated: HashSet<&'a String>,
}

impl<'a> CombinedFiltersConfigIter<'a> {
    /// Creates an iterator over the filters in [`CombinedFiltersConfig`].
    pub fn new(config: CombinedFiltersConfig<'a>) -> Self {
        CombinedFiltersConfigIter {
            config,
            project_index: 0,
            global_index: 0,
            evaluated: HashSet::new(),
        }
    }
}

impl<'a> CombinedFiltersConfigIter<'a> {
    /// Returns whether the inbound filters support the maximum version.
    ///
    /// Filters are supported if the versions of filters of both project and
    /// global configs are not greater than the given maximum version.
    ///
    /// Filters from the project and global configs are complementary, and in
    /// isolation, they don't provide enough valuable information to perform the
    /// filtering. Additionally, new versions may include features not supported
    /// in the current Relay.
    fn is_version_supported(&self) -> bool {
        if self.config.max_version < self.config.project_filters.version {
            return false;
        }
        if let Some(gc) = self.config.global_filters {
            if self.config.max_version < gc.version {
                return false;
            }
        }
        true
    }

    fn next_project_filter(&mut self) -> Option<&'a GenericFilterConfig> {
        let Some((id, filter)) = self
            .config
            .project_filters
            .filters
            .get_index(self.project_index)
        else {
            return None;
        };
        self.project_index += 1;

        // Mark the filter as evaluated first. If the filter is disabled, it
        // should not be applied from global configs.
        let newly_evaluated = self.evaluated.insert(id);

        if !filter.is_enabled || !newly_evaluated {
            return None;
        }

        if filter.condition.is_none() {
            let Some(global_filters) = self.config.global_filters else {
                return None;
            };
            let Some(filter) = global_filters.filters.get(id) else {
                return None;
            };
            return Some(filter);
        }

        Some(filter)
    }

    fn next_global_filter(&mut self) -> Option<&'a GenericFilterConfig> {
        let Some(global_filters) = self.config.global_filters else {
            return None;
        };
        let Some((id, filter)) = global_filters.filters.get_index(self.global_index) else {
            return None;
        };
        self.global_index += 1;

        if filter.is_empty() {
            return None;
        }

        let newly_added = self.evaluated.insert(id);
        if !newly_added {
            return None;
        }

        Some(filter)
    }
}

impl<'a> Iterator for CombinedFiltersConfigIter<'a> {
    type Item = &'a GenericFilterConfig;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_version_supported() {
            return None;
        }

        loop {
            if self.project_index < self.config.project_filters.filters.len() {
                let filter = self.next_project_filter();
                if filter.is_some() {
                    return filter;
                }
                continue;
            }

            if let Some(global_filters) = self.config.global_filters {
                if self.global_index < global_filters.filters.len() {
                    let filter = self.next_global_filter();
                    if filter.is_some() {
                        return filter;
                    }
                    continue;
                }
            }

            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::iter;

    #[test]
    fn test_empty_config() -> Result<(), serde_json::Error> {
        let filters_config = serde_json::from_str::<ProjectFiltersConfig>("{}")?;
        insta::assert_debug_snapshot!(filters_config, @r###"
        ProjectFiltersConfig {
            browser_extensions: FilterConfig {
                is_enabled: false,
            },
            client_ips: ClientIpsFilterConfig {
                blacklisted_ips: [],
            },
            web_crawlers: FilterConfig {
                is_enabled: false,
            },
            csp: CspFilterConfig {
                disallowed_sources: [],
            },
            error_messages: ErrorMessagesFilterConfig {
                patterns: [],
            },
            legacy_browsers: LegacyBrowsersFilterConfig {
                is_enabled: false,
                browsers: {},
            },
            localhost: FilterConfig {
                is_enabled: false,
            },
            releases: ReleasesFilterConfig {
                releases: [],
            },
            ignore_transactions: IgnoreTransactionsFilterConfig {
                patterns: [],
                is_enabled: false,
            },
            generic: GenericFiltersConfig {
                version: 0,
                filters: {},
            },
        }
        "###);
        Ok(())
    }

    #[test]
    fn test_serialize_empty() {
        let filters_config = ProjectFiltersConfig::default();
        insta::assert_json_snapshot!(filters_config, @"{}");
    }

    #[test]
    fn test_serialize_full() {
        let filters_config = ProjectFiltersConfig {
            browser_extensions: FilterConfig { is_enabled: true },
            client_ips: ClientIpsFilterConfig {
                blacklisted_ips: vec!["127.0.0.1".to_string()],
            },
            web_crawlers: FilterConfig { is_enabled: true },
            csp: CspFilterConfig {
                disallowed_sources: vec!["https://*".to_string()],
            },
            error_messages: ErrorMessagesFilterConfig {
                patterns: GlobPatterns::new(vec!["Panic".to_string()]),
            },
            legacy_browsers: LegacyBrowsersFilterConfig {
                is_enabled: false,
                browsers: [LegacyBrowser::Ie9, LegacyBrowser::EdgePre79]
                    .iter()
                    .cloned()
                    .collect(),
            },
            localhost: FilterConfig { is_enabled: true },
            releases: ReleasesFilterConfig {
                releases: GlobPatterns::new(vec!["1.2.3".to_string()]),
            },
            ignore_transactions: IgnoreTransactionsFilterConfig {
                patterns: GlobPatterns::new(vec!["*health*".to_string()]),
                is_enabled: true,
            },
            generic: GenericFiltersConfig {
                version: 1,
                filters: IndexMap::from([(
                    "hydrationError".to_owned(),
                    GenericFilterConfig {
                        id: "hydrationError".to_string(),
                        is_enabled: true,
                        condition: Some(RuleCondition::eq("event.exceptions", "HydrationError")),
                    },
                )]),
            },
        };

        insta::assert_json_snapshot!(filters_config, @r#"
        {
          "browserExtensions": {
            "isEnabled": true
          },
          "clientIps": {
            "blacklistedIps": [
              "127.0.0.1"
            ]
          },
          "webCrawlers": {
            "isEnabled": true
          },
          "csp": {
            "disallowedSources": [
              "https://*"
            ]
          },
          "errorMessages": {
            "patterns": [
              "Panic"
            ]
          },
          "legacyBrowsers": {
            "isEnabled": false,
            "options": [
              "ie9",
              "edge_pre_79"
            ]
          },
          "localhost": {
            "isEnabled": true
          },
          "releases": {
            "releases": [
              "1.2.3"
            ]
          },
          "ignoreTransactions": {
            "patterns": [
              "*health*"
            ],
            "isEnabled": true
          },
          "generic": {
            "version": 1,
            "filters": [
              {
                "id": "hydrationError",
                "isEnabled": true,
                "condition": {
                  "op": "eq",
                  "name": "event.exceptions",
                  "value": "HydrationError"
                }
              }
            ]
          }
        }
        "#);
    }

    #[test]
    fn test_regression_legacy_browser_missing_options() {
        let json = r#"{"isEnabled":false}"#;
        let config = serde_json::from_str::<LegacyBrowsersFilterConfig>(json).unwrap();
        insta::assert_debug_snapshot!(config, @r###"
        LegacyBrowsersFilterConfig {
            is_enabled: false,
            browsers: {},
        }
        "###);
    }

    #[test]
    fn test_deserialize_generic_filters() {
        let json = r#"{
            "version": 1,
            "filters": [
                {
                  "id": "hydrationError",
                  "isEnabled": true,
                  "condition": {
                    "op": "eq",
                    "name": "event.exceptions",
                    "value": "HydrationError"
                  }
                },
                {
                  "id": "chunkLoadError",
                  "isEnabled": false
                }
           ]
        }"#;
        let config = serde_json::from_str::<GenericFiltersConfig>(json).unwrap();
        insta::assert_debug_snapshot!(config, @r###"
        GenericFiltersConfig {
            version: 1,
            filters: {
                "hydrationError": GenericFilterConfig {
                    id: "hydrationError",
                    is_enabled: true,
                    condition: Some(
                        Eq(
                            EqCondition {
                                name: "event.exceptions",
                                value: String("HydrationError"),
                                options: EqCondOptions {
                                    ignore_case: false,
                                },
                            },
                        ),
                    ),
                },
                "chunkLoadError": GenericFilterConfig {
                    id: "chunkLoadError",
                    is_enabled: false,
                    condition: None,
                },
            },
        }
        "###);
    }

    fn empty_filter() -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::new(),
        }
    }

    /// Returns a complete and enabled [`GenericFiltersConfig`].
    fn enabled_filter(id: &str) -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                id.to_owned(),
                GenericFilterConfig {
                    id: id.to_owned(),
                    is_enabled: true,
                    condition: Some(RuleCondition::eq("event.exceptions", "myError")),
                },
            )]),
        }
    }

    /// Returns an enabled flag of a [`GenericFiltersConfig`].
    fn enabled_flag_filter(id: &str) -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                id.to_owned(),
                GenericFilterConfig {
                    id: id.to_owned(),
                    is_enabled: true,
                    condition: None,
                },
            )]),
        }
    }

    /// Returns a complete but disabled [`GenericFiltersConfig`].
    fn disabled_filter(id: &str) -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                id.to_owned(),
                GenericFilterConfig {
                    id: id.to_owned(),
                    is_enabled: false,
                    condition: Some(RuleCondition::eq("event.exceptions", "myError")),
                },
            )]),
        }
    }

    /// Returns a disabled flag of a [`GenericFiltersConfig`].
    fn disabled_flag_filter(id: &str) -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                id.to_owned(),
                GenericFilterConfig {
                    id: id.to_owned(),
                    is_enabled: false,
                    condition: None,
                },
            )]),
        }
    }

    macro_rules! assert_filters {
        ($combined_filters:expr, $expected:expr) => {
            let actual_ids = $combined_filters.into_iter();
            assert!(actual_ids.eq($expected));
        };
    }

    #[test]
    fn test_no_combined_filters_on_unsupported_project_version() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        let global = enabled_filter("supported-global");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_filters_on_unsupported_project_version_no_global() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        assert_filters!(
            CombinedFiltersConfig::new(&project, None, 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_filters_on_unsupported_global_version() {
        let project = enabled_filter("supported-project");
        let mut global = enabled_filter("unsupported-global");
        global.version = 2;
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_filters_on_unsupported_project_and_global_version() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        let mut global = enabled_filter("unsupported-global");
        global.version = 2;
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_combined_filters_on_supported_project_and_global_version() {
        let project = enabled_filter("supported-project");
        let global = enabled_filter("supported-global");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            [
                project.filters.first().unwrap().1,
                global.filters.first().unwrap().1
            ]
            .into_iter()
        );
    }

    #[test]
    fn test_no_combined_filters_when_no_filters() {
        let project = empty_filter();
        let global = empty_filter();
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_global_filters() {
        let project = empty_filter();
        let global = disabled_filter("disabled-global");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_global_combined_when_enabled_global_filters() {
        let project = empty_filter();
        let global = enabled_filter("enabled-global");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            [global.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_no_combined_when_global_is_flag() {
        let project = empty_filter();
        let global = enabled_flag_filter("skip");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_project_filters_empty_global() {
        let project = disabled_filter("disabled-project");
        let global = empty_filter();
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }
    #[test]
    fn test_no_combined_when_disabled_project_filters_missing_global() {
        let project = disabled_filter("disabled-project");
        assert_filters!(
            CombinedFiltersConfig::new(&project, None, 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_in_project_and_global() {
        let project = disabled_filter("disabled-project");
        let global = disabled_filter("disabled-global");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_project_enabled_global() {
        let project = disabled_filter("filter");
        let global = enabled_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_enabled_flag_project_empty_global() {
        let project = enabled_flag_filter("filter");
        let global = empty_filter();
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_enabled_flag_project_missing_global() {
        let project = enabled_flag_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, None, 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_flag_project_empty_global() {
        let project = disabled_flag_filter("filter");
        let global = empty_filter();
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_flag_project_missing_global() {
        let project = disabled_flag_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, None, 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_project_combined_when_only_project_enabled_empty_global() {
        let project = enabled_filter("enabled-project");
        let global = empty_filter();
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_project_combined_when_only_project_enabled_missing_global() {
        let project = enabled_filter("enabled-project");
        assert_filters!(
            CombinedFiltersConfig::new(&project, None, 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_project_combined_when_enabled_flag_project_disabled_global() {
        let project = enabled_filter("filter");
        let global = disabled_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_flag_project_disabled_global() {
        let project = disabled_flag_filter("filter");
        let global = disabled_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_project_combined_when_enabled_project_disabled_global() {
        let project = enabled_filter("filter");
        let global = disabled_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_common_combined_when_enabled_flag_project_enabled_global() {
        let project = enabled_flag_filter("filter");
        let global = enabled_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            [global.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_flag_project_enabled_global() {
        let project = disabled_flag_filter("filter");
        let global = enabled_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_common_combined_when_enabled_project_enabled_global() {
        let project = enabled_filter("filter");
        let global = enabled_flag_filter("filter");
        assert_filters!(
            CombinedFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }
}
