use std::convert::TryFrom;
use std::fmt;

use serde::Serialize;

/// Identifies which filter dropped an event for which reason.
///
/// Ported from Sentry's same-named "enum". The enum variants are fed into outcomes in kebap-case
/// (e.g.  "browser-extensions")
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Hash)]
pub enum FilterStatKey {
    /// Filtered by ip address.
    IpAddress,

    /// Filtered by release name (version).
    ReleaseVersion,

    /// Filtered by error message.
    ErrorMessage(String),

    /// Filtered by browser extension.
    BrowserExtensions,

    /// Filtered by legacy browser version.
    LegacyBrowsers,

    /// Filtered due to localhost restriction.
    Localhost,

    /// Filtered as known web crawler.
    WebCrawlers,

    /// Filtered due to invalid CSP policy.
    InvalidCsp,

    /// Filtered due to the fact that it was a call to a filtered transaction
    FilteredTransactions,
}

// An event grouped to a removed group.
//
// Not returned by any filters implemented in Rust.
// DiscardedHash,

// Invalid CORS header.
//
// NOTE: Although cors is in the Sentry's FilterStatKey enum it is used for
// Invalid outcomes and therefore should logically belong to OutcomeInvalidReason
// that is why it was commented here and moved to OutcomeInvalidReason enum
// Cors,

impl FilterStatKey {
    /// Returns the string identifier of the filter stat key.
    pub fn name(self) -> &'static str {
        match self {
            FilterStatKey::IpAddress => "ip-address",
            FilterStatKey::ReleaseVersion => "release-version",
            FilterStatKey::ErrorMessage(name) => format!("error-message@{name}"),
            FilterStatKey::BrowserExtensions => "browser-extensions",
            FilterStatKey::LegacyBrowsers => "legacy-browsers",
            FilterStatKey::Localhost => "localhost",
            FilterStatKey::WebCrawlers => "web-crawlers",
            FilterStatKey::InvalidCsp => "invalid-csp",
            FilterStatKey::FilteredTransactions => "filtered-transaction",
        }
    }
}

impl fmt::Display for FilterStatKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl<'a> TryFrom<&'a str> for FilterStatKey {
    type Error = &'a str;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Ok(match value {
            "ip-address" => FilterStatKey::IpAddress,
            "release-version" => FilterStatKey::ReleaseVersion,
            "browser-extensions" => FilterStatKey::BrowserExtensions,
            "legacy-browsers" => FilterStatKey::LegacyBrowsers,
            "localhost" => FilterStatKey::Localhost,
            "web-crawlers" => FilterStatKey::WebCrawlers,
            "invalid-csp" => FilterStatKey::InvalidCsp,
            "filtered-transaction" => FilterStatKey::FilteredTransactions,
            other => {
                match value.strip_prefix("error-message@") {
                    Some(name) => FilterStatKey::ErrorMessage(name.to_string()),
                    None => Err(other)
                }
            }
        })
    }
}
