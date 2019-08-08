//! Config structs for all filters.

use std::borrow::Cow;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

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
    Default,
    IePre9,
    Ie9,
    Ie10,
    OperaPre15,
    OperaMiniPre8,
    AndroidPre4,
    SafariPre6,
    Unknown(String),
}

impl<'de> Deserialize<'de> for LegacyBrowser {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let string = Cow::<str>::deserialize(deserializer)?;

        Ok(match string.as_ref() {
            "default" => LegacyBrowser::Default,
            "ie_pre_9" => LegacyBrowser::IePre9,
            "ie9" => LegacyBrowser::Ie9,
            "ie10" => LegacyBrowser::Ie10,
            "opera_pre_15" => LegacyBrowser::OperaPre15,
            "opera_mini_pre_8" => LegacyBrowser::OperaMiniPre8,
            "android_pre_4" => LegacyBrowser::AndroidPre4,
            "safari_pre_6" => LegacyBrowser::SafariPre6,
            _ => LegacyBrowser::Unknown(string.into_owned()),
        })
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
            LegacyBrowser::OperaPre15 => "opera_pre_15",
            LegacyBrowser::OperaMiniPre8 => "opera_mini_pre_8",
            LegacyBrowser::AndroidPre4 => "android_pre_4",
            LegacyBrowser::SafariPre6 => "safari_pre_6",
            LegacyBrowser::Unknown(string) => &string,
        })
    }
}

/// Configuration for the legacy browsers filter.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LegacyBrowsersFilterConfig {
    /// Specifies whether this filter is enabled.
    pub is_enabled: bool,
    /// The browsers to filter.
    #[serde(rename = "options")]
    pub browsers: BTreeSet<LegacyBrowser>,
}

impl LegacyBrowsersFilterConfig {
    /// Returns true if no configuration for this filter is given.
    pub fn is_empty(&self) -> bool {
        !self.is_enabled && self.browsers.is_empty()
    }
}

/// Configuration for all event filters.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FiltersConfig {
    /// Configuration for the Browser Extensions filter.
    #[serde(default)]
    pub browser_extensions: FilterConfig,

    /// Configuration for the Web Crawlers filter
    #[serde(default)]
    pub web_crawlers: FilterConfig,

    /// Configuration for the Legacy Browsers filter.
    #[serde(default)]
    pub legacy_browsers: LegacyBrowsersFilterConfig,

    /// Configuration for the Localhost filter.
    #[serde(default)]
    pub localhost: FilterConfig,
}

impl FiltersConfig {
    /// Returns true if there are no filter configurations delcared.
    pub fn is_empty(&self) -> bool {
        self.browser_extensions.is_empty()
            && self.web_crawlers.is_empty()
            && self.legacy_browsers.is_empty()
            && self.localhost.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_config() -> Result<(), serde_json::Error> {
        let filters_config = serde_json::from_str::<FiltersConfig>("{}")?;
        insta::assert_debug_snapshot_matches!(filters_config, @r###"
       ⋮FiltersConfig {
       ⋮    browser_extensions: FilterConfig {
       ⋮        is_enabled: false,
       ⋮    },
       ⋮    web_crawlers: FilterConfig {
       ⋮        is_enabled: false,
       ⋮    },
       ⋮    legacy_browsers: LegacyBrowsersFilterConfig {
       ⋮        is_enabled: false,
       ⋮        browsers: {},
       ⋮    },
       ⋮    localhost: FilterConfig {
       ⋮        is_enabled: false,
       ⋮    },
       ⋮}
        "###);
        Ok(())
    }

    #[test]
    fn test_should_serialize() {
        let filters_config = FiltersConfig {
            browser_extensions: FilterConfig { is_enabled: true },
            web_crawlers: FilterConfig { is_enabled: false },
            legacy_browsers: LegacyBrowsersFilterConfig {
                is_enabled: false,
                browsers: [LegacyBrowser::Ie9].iter().cloned().collect(),
            },
            localhost: FilterConfig { is_enabled: true },
        };

        insta::assert_json_snapshot_matches!(filters_config, @r###"
       ⋮{
       ⋮  "browserExtensions": {
       ⋮    "isEnabled": true
       ⋮  },
       ⋮  "webCrawlers": {
       ⋮    "isEnabled": false
       ⋮  },
       ⋮  "legacyBrowsers": {
       ⋮    "is_enabled": false,
       ⋮    "options": [
       ⋮      "ie9"
       ⋮    ]
       ⋮  },
       ⋮  "localhost": {
       ⋮    "isEnabled": true
       ⋮  }
       ⋮}
        "###);
    }
}
