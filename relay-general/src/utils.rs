use std::{fmt, str};

use once_cell::sync::OnceCell;
use regex::Regex;
use relay_common::impl_str_serde;

/// Glob options represent the underlying regex emulating the globs.
#[derive(Debug)]
struct GlobPatternGroups<'g> {
    star: &'g str,
    double_star: &'g str,
    question_mark: &'g str,
}

/// `GlobBuilder` provides the posibility to fine tune the final [`Glob`], mainly what capture
/// groups will be enabled in the underlying regex.
#[derive(Debug)]
pub struct GlobBuilder<'g> {
    value: &'g str,
    groups: GlobPatternGroups<'g>,
}

impl<'g> GlobBuilder<'g> {
    /// Create a new builder with all the captures enabled by default.
    pub fn new(value: &'g str) -> Self {
        let opts = GlobPatternGroups {
            star: "([^/]*?)",
            double_star: "(.*?)",
            question_mark: "(.)",
        };
        Self {
            value,
            groups: opts,
        }
    }

    /// Enable capture groups for `*` in the pattern.
    pub fn capture_star(mut self, enable: bool) -> Self {
        if !enable {
            self.groups.star = "(?:[^/]*?)";
        }
        self
    }

    /// Enable capture groups for `**` in the pattern.
    pub fn capture_double_star(mut self, enable: bool) -> Self {
        if !enable {
            self.groups.double_star = "(?:.*?)";
        }
        self
    }

    /// Enable capture groups for `?` in the pattern.
    pub fn capture_question_mark(mut self, enable: bool) -> Self {
        if !enable {
            self.groups.question_mark = "(?:.)";
        }
        self
    }

    /// Create a new [`Glob`] from this builder.
    pub fn build(self) -> Glob {
        let mut pattern = String::with_capacity(&self.value.len() + 100);
        let mut last = 0;

        pattern.push('^');

        static GLOB_RE: OnceCell<Regex> = OnceCell::new();
        let regex = GLOB_RE.get_or_init(|| Regex::new(r"\\\?|\\\*\\\*|\\\*|\?|\*\*|\*").unwrap());

        for m in regex.find_iter(self.value) {
            pattern.push_str(&regex::escape(&self.value[last..m.start()]));
            match m.as_str() {
                "?" => pattern.push_str(self.groups.question_mark),
                "**" => pattern.push_str(self.groups.double_star),
                "*" => pattern.push_str(self.groups.star),
                _ => pattern.push_str(m.as_str()),
            }
            last = m.end();
        }
        pattern.push_str(&regex::escape(&self.value[last..]));
        pattern.push('$');

        Glob {
            value: self.value.to_owned(),
            pattern: Regex::new(&pattern).unwrap(),
        }
    }
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
    /// Creates the [`GlobBuilder`], which can be fine-tunned using helper methods.
    pub fn builder(glob: &'_ str) -> GlobBuilder {
        GlobBuilder::new(glob)
    }

    /// Creates a new glob from a string.
    ///
    /// All the glob patterns (wildcards) are enabled in the captures, and can be returned by
    /// `matches` function.
    pub fn new(glob: &str) -> Glob {
        GlobBuilder::new(glob).build()
    }

    /// Returns the pattern as str.
    pub fn pattern(&self) -> &str {
        &self.value
    }

    /// Checks if some value matches the glob.
    pub fn is_match(&self, value: &str) -> bool {
        self.pattern.is_match(value)
    }

    /// Currently support replacing only all `*` in the input string with provided replacement.
    /// If no match is found, then a copy of the string is returned unchanged.
    pub fn replace_captures(&self, input: &str, replacement: &str) -> String {
        let mut output = String::new();
        let mut current = 0;

        for caps in self.pattern.captures_iter(input) {
            // Create the iter on subcaptures and ignore the first capture, since this is always
            // the entire string.
            for cap in caps.iter().flatten().skip(1) {
                output.push_str(&input[current..cap.start()]);
                output.push_str(replacement);
                current = cap.end();
            }
        }

        output.push_str(&input[current..]);
        output
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

impl PartialEq for Glob {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for Glob {}

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

        let g = Glob::new("/api/*/stuff/**");
        assert!(g.is_match("/api/some/stuff/here/store/"));
        assert!(!g.is_match("/api/some/store/"));

        let g = Glob::new(r"/api/\*/stuff");
        assert!(g.is_match("/api/*/stuff"));
        assert!(!g.is_match("/api/some/stuff"));

        let g = Glob::new(r"*stuff");
        assert!(g.is_match("some-stuff"));
        assert!(!g.is_match("not-stuff-but-things"));

        let g = Glob::new(r"\*stuff");
        assert!(g.is_match("*stuff"));
        assert!(!g.is_match("some-stuff"));
    }

    #[test]
    fn test_glob_replace() {
        for (transaction, pattern, result, star, double_star, question_mark) in [
            (
                "/foo/some/bar/here/store",
                "/foo/*/bar/**",
                "/foo/*/bar/here/store",
                true,
                false,
                false,
            ),
            (
                "/foo/some/bar/here/store",
                "/foo/*/bar/*/**",
                "/foo/*/bar/*/store",
                true,
                false,
                false,
            ),
            (
                "/foo/some/bar/here/store/1234",
                "/foo/*/bar/*/**",
                "/foo/*/bar/*/*",
                true,
                true,
                false,
            ),
            ("/foo/1/", "/foo/?/**", "/foo/*/", false, false, true),
            ("/foo/1/end", "/foo/*/**", "/foo/*/end", true, false, true),
            (
                "/foo/1/this/and/that/end",
                "/foo/**/end",
                "/foo/*/end",
                false,
                true,
                false,
            ),
        ] {
            let g = Glob::builder(pattern)
                .capture_star(star)
                .capture_double_star(double_star)
                .capture_question_mark(question_mark)
                .build();

            assert_eq!(g.replace_captures(transaction, "*"), result);
        }
    }

    #[test]
    fn test_do_not_replace() {
        let g = Glob::builder(r"/foo/\*/*")
            .capture_star(true)
            .capture_double_star(false)
            .capture_question_mark(false)
            .build();

        // A literal asterisk matches
        assert_eq!(g.replace_captures("/foo/*/bar", "_"), "/foo/*/_");

        // But only a literal asterisk
        assert_eq!(g.replace_captures("/foo/nope/bar", "_"), "/foo/nope/bar");
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
