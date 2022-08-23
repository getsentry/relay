use std::fmt;
use std::str;

use lazy_static::lazy_static;
use regex::Regex;

use crate::macros::impl_str_serde;

lazy_static! {
    static ref GLOB_RE: Regex = Regex::new(r#"\?|\*\*|\*"#).unwrap();
}

/// A simple glob matcher.
///
/// Supported are `?` for a single char, `*` for all but a slash and
/// `**` to match with slashes.
#[derive(Clone)]
pub struct Glob {
    value: String,
    pattern: Regex,
}

impl Glob {
    /// Creates a new glob from a string.
    pub fn new(glob: &str) -> Glob {
        let mut pattern = String::with_capacity(glob.len() + 100);
        let mut last = 0;

        pattern.push('^');
        for m in GLOB_RE.find_iter(glob) {
            pattern.push_str(&regex::escape(&glob[last..m.start()]));
            match m.as_str() {
                "?" => pattern.push_str("(.)"),
                "**" => pattern.push_str("(.*?)"),
                "*" => pattern.push_str("([^/]*?)"),
                _ => {}
            }
            last = m.end();
        }
        pattern.push_str(&regex::escape(&glob[last..]));
        pattern.push('$');

        Glob {
            value: glob.to_string(),
            pattern: Regex::new(&pattern).unwrap(),
        }
    }

    /// Returns the pattern as str.
    pub fn pattern(&self) -> &str {
        &self.value
    }

    /// Checks if some value matches the glob.
    pub fn is_match(&self, value: &str) -> bool {
        self.pattern.is_match(value)
    }

    /// Checks if the value matches and returns the wildcard matches.
    pub fn matches<'t>(&self, value: &'t str) -> Option<Vec<&'t str>> {
        self.pattern.captures(value).map(|caps| {
            caps.iter()
                .skip(1)
                .map(|x| x.map_or("", |x| x.as_str()))
                .collect()
        })
    }
}

impl str::FromStr for Glob {
    type Err = ();

    fn from_str(value: &str) -> Result<Glob, ()> {
        Ok(Glob::new(value))
    }
}

impl fmt::Display for Glob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.pattern())
    }
}

impl fmt::Debug for Glob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Glob").field(&self.pattern()).finish()
    }
}

impl<'a> From<&'a str> for Glob {
    fn from(value: &'a str) -> Glob {
        Glob::new(value)
    }
}

impl From<String> for Glob {
    fn from(value: String) -> Glob {
        Glob::new(&value)
    }
}

impl_str_serde!(Glob, "a glob pattern");

/// Helper for glob matching
#[derive(Debug)]
pub struct GlobMatcher<T> {
    globs: Vec<(Glob, T)>,
}

impl<T: Clone> Default for GlobMatcher<T> {
    fn default() -> GlobMatcher<T> {
        GlobMatcher::new()
    }
}

impl<T: Clone> GlobMatcher<T> {
    /// Initializes an empty matcher
    pub fn new() -> GlobMatcher<T> {
        GlobMatcher { globs: vec![] }
    }

    /// Adds a new glob to the matcher
    pub fn add<G: Into<Glob>>(&mut self, glob: G, ident: T) {
        self.globs.push((glob.into(), ident));
    }

    /// Matches a string against the stored globs.
    pub fn test(&self, s: &str) -> Option<T> {
        for (glob, ident) in &self.globs {
            if glob.is_match(s) {
                return Some(ident.clone());
            }
        }
        None
    }

    /// Matches a string against the stored glob and get the matches.
    pub fn matches<'a>(&self, s: &'a str) -> Option<(Vec<&'a str>, T)> {
        for (pat, ident) in &self.globs {
            if let Some(matches) = pat.matches(s) {
                return Some((matches, ident.clone()));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob() {
        let g = Glob::new("foo/*/bar");
        assert!(g.is_match("foo/blah/bar"));
        assert!(!g.is_match("foo/blah/bar/aha"));

        let g = Glob::new("foo/???/bar");
        assert!(g.is_match("foo/aha/bar"));
        assert!(!g.is_match("foo/ah/bar"));
        assert!(!g.is_match("foo/ahah/bar"));

        let g = Glob::new("*/foo.txt");
        assert!(g.is_match("prefix/foo.txt"));
        assert!(!g.is_match("double/prefix/foo.txt"));

        let g = Glob::new("api/**/store/");
        assert!(g.is_match("api/some/stuff/here/store/"));
        assert!(g.is_match("api/some/store/"));
    }

    #[test]
    fn test_glob_matcher() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        enum Paths {
            Root,
            Store,
        }
        let mut matcher = GlobMatcher::new();
        matcher.add("/api/*/store/", Paths::Store);
        matcher.add("/api/0/", Paths::Root);

        assert_eq!(matcher.test("/api/42/store/"), Some(Paths::Store));
        assert_eq!(matcher.test("/api/0/"), Some(Paths::Root));
        assert_eq!(matcher.test("/api/0/"), Some(Paths::Root));

        assert_eq!(matcher.matches("/api/0/"), Some((vec![], Paths::Root)));
        assert_eq!(
            matcher.matches("/api/42/store/"),
            Some((vec!["42"], Paths::Store))
        );
    }
}
