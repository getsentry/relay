use std::fmt;

use globset::GlobBuilder;
use regex::bytes::{Regex, RegexBuilder};
use relay_common::UpsertingLazyCell;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Returns `true` if any of the patterns match the given message.
fn is_match(globs: &[Regex], message: &[u8]) -> bool {
    globs.iter().any(|regex| regex.is_match(message.as_ref()))
}

/// A list of patterns for glob matching.
#[derive(Clone, Default)]
pub struct GlobPatterns {
    patterns: Vec<String>,
    globs: UpsertingLazyCell<Vec<Regex>>,
}

impl GlobPatterns {
    /// Creates a new
    pub fn new(patterns: Vec<String>) -> Self {
        Self {
            patterns,
            globs: UpsertingLazyCell::new(),
        }
    }

    /// Returns `true` if the list of patterns is empty.
    pub fn is_empty(&self) -> bool {
        // Check the list of patterns and not globs. Even if there are no globs to parse, we still
        // want to serialize the "invalid" patterns to a downstream Relay.
        self.patterns.is_empty()
    }

    /// Returns `true` if any of the patterns match the given message.
    pub fn is_match<S>(&self, message: S) -> bool
    where
        S: AsRef<[u8]>,
    {
        let message = message.as_ref();
        if message.is_empty() {
            return false;
        }

        let globs = self.globs.get_or_insert_with(|| self.parse_globs());
        is_match(&*globs, message)
    }

    /// Parses valid patterns from the list.
    fn parse_globs(&self) -> Vec<Regex> {
        let mut globs = Vec::with_capacity(self.patterns.len());

        for pattern in &self.patterns {
            let glob_result = GlobBuilder::new(pattern)
                .case_insensitive(true)
                .backslash_escape(true)
                .build();

            if let Ok(glob) = glob_result {
                let regex_result = RegexBuilder::new(glob.regex())
                    .dot_matches_new_line(true)
                    .build();

                if let Ok(regex) = regex_result {
                    globs.push(regex);
                }
            }
        }

        globs
    }
}

impl fmt::Debug for GlobPatterns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.patterns.fmt(f)
    }
}

impl Serialize for GlobPatterns {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.patterns.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GlobPatterns {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let patterns = Deserialize::deserialize(deserializer)?;
        Ok(GlobPatterns::new(patterns))
    }
}

/// Identifies which filter dropped an event for which reason.
///
/// Ported from Sentry's same-named "enum". The enum variants are fed into outcomes in kebap-case
/// (e.g.  "browser-extensions")
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Hash, PartialOrd, Ord)]
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
            FilterStatKey::ErrorMessage => "error-message",
            FilterStatKey::BrowserExtensions => "browser-extensions",
            FilterStatKey::LegacyBrowsers => "legacy-browsers",
            FilterStatKey::Localhost => "localhost",
            FilterStatKey::WebCrawlers => "web-crawlers",
            FilterStatKey::InvalidCsp => "invalid-csp",
        }
    }
}

impl fmt::Display for FilterStatKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! globs {
        ($($pattern:literal),*) => {
            GlobPatterns::new(vec![
                $($pattern.to_string()),*
            ])
        };
    }

    #[test]
    fn test_match_literal() {
        let globs = globs!("foo");
        assert!(globs.is_match("foo"));
    }

    #[test]
    fn test_match_negative() {
        let globs = globs!("foo");
        assert!(!globs.is_match("nope"));
    }

    #[test]
    fn test_match_prefix() {
        let globs = globs!("foo*");
        assert!(globs.is_match("foobarblub"));
    }

    #[test]
    fn test_match_suffix() {
        let globs = globs!("*blub");
        assert!(globs.is_match("foobarblub"));
    }

    #[test]
    fn test_match_inner() {
        let globs = globs!("*bar*");
        assert!(globs.is_match("foobarblub"));
    }

    #[test]
    fn test_match_utf8() {}

    #[test]
    fn test_match_newline() {
        let globs = globs!("*foo*");
        assert!(globs.is_match("foo\n"));
    }

    #[test]
    fn test_match_newline_inner() {
        let globs = globs!("foo*bar");
        assert!(globs.is_match("foo\nbar"));
    }

    #[test]
    fn test_match_newline_pattern() {
        let globs = globs!("foo*\n*bar");
        assert!(globs.is_match("foo \n bar"));
    }

    #[test]
    fn test_match_range() {
        let globs = globs!("1.18.[0-4].*");
        assert!(globs.is_match("1.18.4.2153-2aa83397b"));
        assert!(!globs.is_match("1.18.5.2153-2aa83397b"));
    }
}
