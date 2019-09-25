use std::fmt;

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use lazycell::AtomicLazyCell;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A list of patterns for glob matching.
#[derive(Clone)]
pub struct GlobPatterns {
    patterns: Vec<String>,
    globs: AtomicLazyCell<GlobSet>,
}

impl GlobPatterns {
    /// Creates a new
    pub fn new(patterns: Vec<String>) -> Self {
        Self {
            patterns,
            globs: AtomicLazyCell::new(),
        }
    }

    /// Returns `true` if the list of patterns is empty.
    pub fn is_empty(&self) -> bool {
        // Check the list of patterns and not globs. Even if there are no globs to parse, we still
        // want to serialize the "invalid" patterns to a downstream Relay.
        self.patterns.is_empty()
    }

    /// Returns `true` if any of the patterns match the given message.
    pub fn is_match<S: AsRef<str>>(&self, message: S) -> bool {
        let message = message.as_ref();
        if message.is_empty() {
            return false;
        }

        // Parse globs lazily to ensure that this work is not done upon deserialization.
        // Deserialization usually happens in web workers but parsing / matching in CPU pools.
        if let Some(globs) = self.globs.borrow() {
            return globs.is_match(message);
        }

        // If filling the lazy cell fails, another thread has filled it in the meanwhile. Use the
        // globs to respond right away, instead of borrowing again.
        if let Err(globs) = self.globs.fill(self.parse_globs()) {
            return globs.is_match(message);
        }

        // The lazy cell was filled successfully, so it is safe to assume that this cannot panic.
        match self.globs.borrow() {
            Some(globs) => globs.is_match(message),
            None => unreachable!(),
        }
    }

    /// Parses valid patterns from the list.
    fn parse_globs(&self) -> GlobSet {
        let mut builder = GlobSetBuilder::new();

        for pattern in &self.patterns {
            if let Ok(glob) = GlobBuilder::new(&pattern).case_insensitive(true).build() {
                builder.add(glob);
            }
        }

        builder.build().unwrap_or_else(|_| GlobSet::empty())
    }
}

impl Default for GlobPatterns {
    fn default() -> Self {
        GlobPatterns {
            patterns: Vec::new(),
            globs: AtomicLazyCell::new(),
        }
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
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Hash)]
pub enum FilterStatKey {
    IpAddress,
    ReleaseVersion,
    ErrorMessage,
    BrowserExtensions,
    LegacyBrowsers,
    Localhost,
    WebCrawlers,
    InvalidCsp,
    // Cors, // NOTE: Although cors is in the Sentry's FilterStatKey enum it is used for
    // Invalid outcomes and therefore should logically belong to OutcomeInvalidReason
    // that is why it was commented here and moved to OutcomeInvalidReason enum
    /// note: Not returned by any filters implemented in Rust.
    DiscardedHash,
}

impl FilterStatKey {
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
            FilterStatKey::DiscardedHash => "discarded-hash",
        }
    }
}

impl fmt::Display for FilterStatKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
