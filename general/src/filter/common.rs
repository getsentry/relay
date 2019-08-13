use std::fmt;

use globset::GlobBuilder;

/// Pattern matching for event filters.
///
/// This function intends to work roughly the same as Python's `fnmatch`.
pub fn is_glob_match(pattern: &str, data: &str) -> bool {
    if pattern.contains('*') {
        // we have a pattern, try a glob match
        if let Ok(pattern) = GlobBuilder::new(pattern).case_insensitive(true).build() {
            let pattern = pattern.compile_matcher();
            return pattern.is_match(data);
        }
    }

    //if we don't use glob patterns just do a simple comparison
    pattern.to_lowercase() == data.to_lowercase()
}

/// Identifies which filter dropped an event for which reason.
///
/// Ported from Sentry's same-named "enum". The enum variants are fed into outcomes in kebap-case
/// (e.g.  "browser-extensions")
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FilterStatKey {
    IpAddress,
    ReleaseVersion,
    ErrorMessage,
    BrowserExtensions,
    LegacyBrowsers,
    Localhost,
    WebCrawlers,
    InvalidCsp,
    Cors,

    /// note: Not returned by any filters implemented in Rust.
    DiscardedHash,
}

impl FilterStatKey {
    pub fn as_str(&self) -> &'static str {
        match *self {
            FilterStatKey::IpAddress => "ip-address",
            FilterStatKey::ReleaseVersion => "release-version",
            FilterStatKey::ErrorMessage => "error-message",
            FilterStatKey::BrowserExtensions => "browser-extensions",
            FilterStatKey::LegacyBrowsers => "legacy-browsers",
            FilterStatKey::Localhost => "localhost",
            FilterStatKey::WebCrawlers => "web-crawlers",
            FilterStatKey::InvalidCsp => "invalid-csp",
            FilterStatKey::Cors => "cors",
            FilterStatKey::DiscardedHash => "discarded-hash",
        }
    }
}

impl fmt::Display for FilterStatKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
