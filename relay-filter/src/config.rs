//! Config structs for all filters.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

use relay_common::glob3::GlobPatterns;
use relay_protocol::RuleCondition;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

/// Configuration for a set of ordered filters.
#[derive(Clone, Debug, Default)]
pub(crate) struct OrderedFilters(pub Vec<(String, GenericFilterConfig)>);

impl OrderedFilters {
    /// Returns true if there are no generic filters configured.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty() || self.0.iter().all(|(_, value)| value.is_empty())
    }
}

impl Serialize for OrderedFilters {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (filter_name, filter_config) in self.0.iter() {
            map.serialize_entry(filter_name, filter_config)?
        }
        map.end()
    }
}

struct OrderedFiltersVisitor;

impl<'de> Visitor<'de> for OrderedFiltersVisitor {
    type Value = OrderedFilters;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .write_str("a map where keys are filter names and values are filter configurations")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut filters = OrderedFilters(vec![]);

        // We don't perform any kind of de-duplication in case of duplicate keys, since this might
        // lead to an opaque behavior from the outside. The resolution of multiple filters with
        // the same name, will be a responsibility of the matching algorithm.
        while let Some((key, value)) = access.next_entry()? {
            filters.0.push((key, value));
        }

        Ok(filters)
    }
}

impl<'de> Deserialize<'de> for OrderedFilters {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(OrderedFiltersVisitor())
    }
}

/// Configuration for generic filters.
///
/// We use a vector in order to guarantee consistent total ordering of filters that is required
/// by the matching algorithm.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GenericFiltersConfig {
    /// Version of the filters configuration.
    pub version: u16,
    /// Filters configuration as an ordered map.
    pub filters: OrderedFilters,
}

impl GenericFiltersConfig {
    /// Returns true if the filters are not declared.
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }
}

/// Configuration for all event filters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FiltersConfig {
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

    /// Configuration for generic filters.
    #[serde(default, skip_serializing_if = "GenericFiltersConfig::is_empty")]
    pub generic: GenericFiltersConfig,
}

impl FiltersConfig {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_config() -> Result<(), serde_json::Error> {
        let filters_config = serde_json::from_str::<FiltersConfig>("{}")?;
        insta::assert_debug_snapshot!(filters_config, @r###"
        FiltersConfig {
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
                filters: OrderedFilters(
                    [],
                ),
            },
        }
        "###);
        Ok(())
    }

    #[test]
    fn test_serialize_empty() {
        let filters_config = FiltersConfig::default();
        insta::assert_json_snapshot!(filters_config, @"{}");
    }

    #[test]
    fn test_serialize_full() {
        let filters_config = FiltersConfig {
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
                browsers: [LegacyBrowser::Ie9].iter().cloned().collect(),
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
                filters: OrderedFilters(vec![(
                    "hydrationError".to_string(),
                    GenericFilterConfig {
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
              "ie9"
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
            "filters": {
              "hydrationError": {
                "isEnabled": true,
                "condition": {
                  "op": "eq",
                  "name": "event.exceptions",
                  "value": "HydrationError"
                }
              }
            }
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
            "filters": {
                "hydrationError": {
                  "isEnabled": true,
                  "condition": {
                    "op": "eq",
                    "name": "event.exceptions",
                    "value": "HydrationError"
                  }
                },
                "chunkLoadError": {
                    "isEnabled": false
                }
           }
        }"#;
        let config = serde_json::from_str::<GenericFiltersConfig>(json).unwrap();
        insta::assert_debug_snapshot!(config, @r###"
        GenericFiltersConfig {
            version: 1,
            filters: OrderedFilters(
                [
                    (
                        "hydrationError",
                        GenericFilterConfig {
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
                    ),
                    (
                        "chunkLoadError",
                        GenericFilterConfig {
                            is_enabled: false,
                            condition: None,
                        },
                    ),
                ],
            ),
        }
        "###);
    }
}
