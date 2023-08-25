//! Alternative implementation of serializable glob patterns.

use std::fmt;

use globset::GlobBuilder;
use once_cell::sync::OnceCell;
use regex::bytes::{Regex, RegexBuilder};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Returns `true` if any of the patterns match the given message.
fn is_match(globs: &[Regex], message: &[u8]) -> bool {
    globs.iter().any(|regex| regex.is_match(message.as_ref()))
}

/// A list of patterns for glob matching.
#[derive(Clone, Default)]
pub struct GlobPatterns {
    patterns: Vec<String>,
    globs: OnceCell<Vec<Regex>>,
}

impl GlobPatterns {
    /// Creates a new
    pub fn new(patterns: Vec<String>) -> Self {
        Self {
            patterns,
            globs: OnceCell::new(),
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

        let globs = self.globs.get_or_init(|| self.parse_globs());
        is_match(globs, message)
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

impl PartialEq for GlobPatterns {
    fn eq(&self, other: &Self) -> bool {
        self.patterns == other.patterns
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

    #[test]
    fn test_match_range_neg() {
        let globs = globs!("1.18.[!0-4].*");
        assert!(!globs.is_match("1.18.4.2153-2aa83397b"));
        assert!(globs.is_match("1.18.5.2153-2aa83397b"));
    }

    #[test]
    fn test_match_neg_unsupported() {
        // this is not necessarily desirable behavior, but it is our current one: negation (!)
        // outside of [] doesn't work
        let globs = globs!("!1.18.4.*");
        assert!(!globs.is_match("1.18.4.2153-2aa83397b"));
        assert!(!globs.is_match("1.18.5.2153-2aa83397b"));
    }
}
