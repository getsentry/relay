use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;

use serde::Serialize;

/// Identifies which filter dropped an event for which reason.
///
/// Ported from Sentry's same-named "enum". The enum variants are fed into outcomes in kebap-case
/// (e.g.  "browser-extensions")
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Hash)]
pub enum FilterStatKey {
    /// Filtered by ip address.
    IpAddress,

    /// Filtered by release name (version).
    ReleaseVersion,

    /// Filtered by error message.
    ErrorMessage,

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

    /// Filtered due to a generic filter.
    GenericFilter(String),
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
    pub fn name(self) -> Cow<'static, str> {
        Cow::Borrowed(match self {
            FilterStatKey::IpAddress => "ip-address",
            FilterStatKey::ReleaseVersion => "release-version",
            FilterStatKey::ErrorMessage => "error-message",
            FilterStatKey::BrowserExtensions => "browser-extensions",
            FilterStatKey::LegacyBrowsers => "legacy-browsers",
            FilterStatKey::Localhost => "localhost",
            FilterStatKey::WebCrawlers => "web-crawlers",
            FilterStatKey::InvalidCsp => "invalid-csp",
            FilterStatKey::FilteredTransactions => "filtered-transaction",
            FilterStatKey::GenericFilter(filter_name) => {
                return Cow::Owned(filter_name);
            }
        })
    }
}

impl fmt::Display for FilterStatKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.clone().name())
    }
}

impl<'a> TryFrom<&'a str> for FilterStatKey {
    type Error = &'a str;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Ok(match value {
            "ip-address" => FilterStatKey::IpAddress,
            "release-version" => FilterStatKey::ReleaseVersion,
            "error-message" => FilterStatKey::ErrorMessage,
            "browser-extensions" => FilterStatKey::BrowserExtensions,
            "legacy-browsers" => FilterStatKey::LegacyBrowsers,
            "localhost" => FilterStatKey::Localhost,
            "web-crawlers" => FilterStatKey::WebCrawlers,
            "invalid-csp" => FilterStatKey::InvalidCsp,
            "filtered-transaction" => FilterStatKey::FilteredTransactions,
            other => FilterStatKey::GenericFilter(other.to_string()),
        })
    }
}
