//! A glob like pattern used throught Relay and its APIs.
//!
//! # Behaviour
//!
//! A pattern is similar to a glob but without support for brace expansions and without any support
//! for filesystem specific behaviour like platform dependent separators and file extension
//! matching.
//!
//! By default a [`Pattern`] does not support any separator characters.
//! For example `*` usually only matches up to the next path separator (often platform dependent),
//! in a [`Pattern`] `*` matches all characters. Optional support for a single separator character
//! is available.
//!
//! The empty glob `""` never matches.
//!
//! # Syntax
//!
//! Basic glob like syntax is supported with Unix style negations.
//!
//! * `?` matches any single character.
//! * `*` matches any number of any characters, including none.
//! * `[abc]` matches one character in the given bracket.
//! * `[!abc]` matches one character that is not in the given bracket.
//! * `[a-z]` matches one character in the given range.
//! * `[!a-z]` matches one character that is not in the given range.
//! * `\` escapes any of the above special characters and treats it as a literal.

use std::fmt;

/// Pattern parsing error.
#[derive(Debug)]
pub struct Error {
    pattern: String,
    kind: ErrorKind,
}

#[derive(Debug)]
enum ErrorKind {
    /// The specified range is invalid. The `end` character is lexicographically
    /// after the `start` character.
    InvalidRange(char, char),
    /// Unbalanced character class. The pattern contains unbalanced `[`, `]` characters .
    UnbalancedCharacterClass,
    /// Character class is invalid and cannot be parsed.
    InvalidCharacterClass,
    /// Dangling escape character.
    DanglingEscape,
    /// The pattern produced an invalid regex pattern which couldn't be compiled.
    Regex(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error parsing pattern '{}': ", self.pattern)?;
        match &self.kind {
            ErrorKind::InvalidRange(start, end) => {
                write!(f, "Invalid character range `{start}-{end}`")
            }
            ErrorKind::UnbalancedCharacterClass => write!(f, "Unbalanced character class"),
            ErrorKind::InvalidCharacterClass => write!(f, "Invalid character class"),
            ErrorKind::DanglingEscape => write!(f, "Dangling escape"),
            ErrorKind::Regex(s) => f.write_str(s),
        }
    }
}

/// `Pattern` represents a successfully parsed Relay pattern.
#[derive(Debug)]
pub struct Pattern {
    pattern: String,
    options: Options,
    strategy: MatchStrategy,
}

impl Pattern {
    /// Create a new [`Pattern`] from a string with the default settings.
    pub fn new(pattern: &str) -> Result<Self, Error> {
        Self::builder(pattern).build()
    }

    /// Create a new [`PatternBuilder`]. The builder can be used to adjust the
    /// pattern settings.
    pub fn builder(pattern: &str) -> PatternBuilder {
        PatternBuilder {
            pattern,
            options: Options::default(),
        }
    }

    /// Returns `true` if the pattern matches the passed string.
    pub fn is_match(&self, haystack: &str) -> bool {
        match &self.strategy {
            MatchStrategy::Literal(literal) => match_literal(literal, haystack, self.options),
            MatchStrategy::Prefix(prefix) => match_prefix(prefix, haystack, self.options),
            MatchStrategy::Suffix(suffix) => match_suffix(suffix, haystack, self.options),
            MatchStrategy::Contains(contains) => match_contains(contains, haystack, self.options),
            MatchStrategy::Static(matches) => *matches,
            MatchStrategy::Regex(re) => re.is_match(haystack),
        }
    }
}

impl fmt::Display for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.pattern)
    }
}

/// A builder for a [`Pattern`].
#[derive(Debug)]
pub struct PatternBuilder<'a> {
    pattern: &'a str,
    options: Options,
}

impl<'a> PatternBuilder<'a> {
    /// If enabled matches the pattern case insensitive.
    ///
    /// This is disabled by default.
    pub fn case_insensitive(&mut self, enabled: bool) -> &mut Self {
        self.options.case_insensitive = enabled;
        self
    }

    /// build a new [`Pattern`] from the passed pattern and configured options.
    pub fn build(&self) -> Result<Pattern, Error> {
        let mut parser = Parser::new(self.pattern);
        parser.parse().map_err(|kind| Error {
            pattern: self.pattern.to_owned(),
            kind,
        })?;

        let strategy =
            MatchStrategy::from_tokens(parser.tokens, self.options).map_err(|kind| Error {
                pattern: self.pattern.to_owned(),
                kind,
            })?;

        Ok(Pattern {
            pattern: self.pattern.to_owned(),
            options: self.options,
            strategy,
        })
    }
}

/// Options to influence [`Pattern`] matching behaviour.
#[derive(Debug, Clone, Copy, Default)]
struct Options {
    case_insensitive: bool,
}

/// Matching strategy for a [`Pattern`].
///
/// Certain patterns can be matched more efficiently while the complex
/// patterns fallback to a regex.
#[derive(Debug)]
enum MatchStrategy {
    /// The pattern is a single literal string.
    ///
    /// The stored string is converted to lowercase for case insensitive patterns.
    ///
    /// Example pattern: `foobar`.
    Literal(String),
    /// The pattern only has a single wildcard in the end and can be
    /// matched with a simple prefix check.
    ///
    /// The stored string is converted to lowercase for case insensitive patterns.
    ///
    /// Example pattern: `foobar*`.
    Prefix(String),
    /// The pattern only has a single wildcard at the start and can be
    /// matched with a simple suffix check.
    ///
    /// The stored string is converted to lowercase for case insensitive patterns.
    ///
    /// Example pattern: `*foobar`.
    Suffix(String),
    /// The pattern is surrounded with wildcards and contains a literal in the middle
    /// and can be matched with a simple contains check.
    ///
    /// The stored string is converted to lowercase for case insensitive patterns.
    ///
    /// Example pattern: `*foobar*`.
    Contains(String),
    /// The pattern always evaluates to a static boolean.
    ///
    /// Example: `*`
    Static(bool),
    /// The pattern is complex and compiled to a [`regex_lite::Regex`].
    Regex(regex_lite::Regex),
    // Possible future optimizations for `Any` variations:
    // Examples: `??`. `??suffix`, `prefix??` and `?contains?`.
}

impl MatchStrategy {
    /// Create a [`MatchStrategy`] from [`Tokens`].
    fn from_tokens(mut tokens: Tokens, options: Options) -> Result<Self, ErrorKind> {
        let take_case = |s: &mut String| match options.case_insensitive {
            true => std::mem::take(s).to_lowercase(),
            false => std::mem::take(s),
        };

        let s = match tokens.as_mut_slice() {
            [] => Self::Static(false),
            [Token::Wildcard] => Self::Static(true),
            [Token::Literal(literal)] => Self::Literal(take_case(literal)),
            [Token::Literal(literal), Token::Wildcard] => Self::Prefix(take_case(literal)),
            [Token::Wildcard, Token::Literal(literal)] => Self::Suffix(take_case(literal)),
            [Token::Wildcard, Token::Literal(literal), Token::Wildcard] => {
                Self::Contains(take_case(literal))
            }
            tokens => Self::Regex(to_regex(tokens, options)?),
        };

        Ok(s)
    }
}

/// Convert a list of tokens to a [`regex_lite::Regex`].
fn to_regex(tokens: &[Token], options: Options) -> Result<regex_lite::Regex, ErrorKind> {
    fn push_class_escaped(sink: &mut String, c: char) {
        match c {
            '\\' => sink.push_str(r"\\"),
            '[' => sink.push_str(r"\["),
            ']' => sink.push_str(r"\]"),
            c => sink.push(c),
        }
    }

    let mut re = String::new();
    re.push_str("(?-u)");
    if options.case_insensitive {
        re.push_str("(?i)");
    }
    re.push('^');
    for token in tokens {
        match token {
            Token::Literal(literal) => re.push_str(&regex_lite::escape(literal)),
            Token::Any(n) => match n {
                0 => debug_assert!(false, "empty any token"),
                1 => re.push('.'),
                // The simple case with a 'few' any matchers.
                &i @ 2..=20 => {
                    re.reserve(i);
                    for _ in 0..i {
                        re.push('.');
                    }
                }
                // The generic case with a 'a lot of' any matchers.
                i => {
                    re.push('.');
                    re.push('{');
                    re.push_str(&i.to_string());
                    re.push('}')
                }
            },
            Token::Wildcard => re.push_str(".*"),
            Token::Class { negated, ranges } => {
                if ranges.is_empty() {
                    continue;
                }

                re.push('[');
                if *negated {
                    re.push('^');
                }
                for range in ranges.iter() {
                    push_class_escaped(&mut re, range.start);
                    if range.start != range.end {
                        re.push('-');
                        push_class_escaped(&mut re, range.end);
                    }
                }
                re.push(']');
            }
        }
    }
    re.push('$');

    regex_lite::RegexBuilder::new(&re)
        .dot_matches_new_line(true)
        .build()
        .map_err(|err| ErrorKind::Regex(err.to_string()))
}

#[inline(always)]
fn match_literal(literal: &str, haystack: &str, options: Options) -> bool {
    if options.case_insensitive {
        // Can't do an explicit len compare first here `literal.len() == haystack.len()`,
        // the amount of characters can change when converting case.
        debug_assert!(literal.to_lowercase() == literal);
        let mut literal = literal.chars();
        let mut haystack = haystack.chars().flat_map(|c| c.to_lowercase());

        loop {
            match (literal.next(), haystack.next()) {
                // Both iterators exhausted -> literal matches.
                (None, None) => break true,
                // Either iterator exhausted while the other one is not -> no match.
                (None, _) | (_, None) => break false,
                (Some(p), Some(h)) if p != h => break false,
                _ => {}
            }
        }
    } else {
        literal == haystack
    }
}

#[inline(always)]
fn match_prefix(prefix: &str, haystack: &str, options: Options) -> bool {
    if options.case_insensitive {
        debug_assert!(prefix.to_lowercase() == prefix);
        let mut prefix = prefix.chars();
        let mut haystack = haystack.chars().flat_map(|c| c.to_lowercase());

        loop {
            match (prefix.next(), haystack.next()) {
                // If the prefix is exhausted it matched.
                (None, _) => break true,
                // If the haystack is exhausted, but the pattern is not -> no match.
                (Some(_), None) => break false,
                (Some(p), Some(h)) if p != h => break false,
                _ => {}
            }
        }
    } else {
        haystack.starts_with(prefix)
    }
}

#[inline(always)]
fn match_suffix(suffix: &str, haystack: &str, options: Options) -> bool {
    if options.case_insensitive {
        debug_assert!(suffix.to_lowercase() == suffix);
        let mut suffix = suffix.chars().rev();
        let mut haystack = haystack.chars().flat_map(|c| c.to_lowercase()).rev();

        loop {
            match (suffix.next(), haystack.next()) {
                // If the prefix is exhausted it matched.
                (None, _) => break true,
                // If the haystack is exhausted, but the pattern is not -> no match.
                (Some(_), None) => break false,
                (Some(s), Some(h)) if s != h => break false,
                _ => {}
            }
        }
    } else {
        haystack.ends_with(suffix)
    }
}

#[inline(always)]
fn match_contains(contains: &str, haystack: &str, options: Options) -> bool {
    if options.case_insensitive {
        debug_assert!(contains.to_lowercase() == contains);
        let haystack = haystack.to_lowercase();
        memchr::memmem::find(haystack.as_bytes(), contains.as_bytes()).is_some()
    } else {
        memchr::memmem::find(haystack.as_bytes(), contains.as_bytes()).is_some()
    }
}

struct Parser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    tokens: Tokens,
    current_literal: Option<String>,
}

impl<'a> Parser<'a> {
    fn new(pattern: &'a str) -> Self {
        Self {
            chars: pattern.chars().peekable(),
            tokens: Default::default(),
            current_literal: None,
        }
    }

    fn parse(&mut self) -> Result<(), ErrorKind> {
        while let Some(c) = self.advance() {
            match c {
                '?' => self.push_token(Token::Any(1)),
                '*' => self.push_token(Token::Wildcard),
                '[' => self.parse_class()?, // TODO escapes in classes
                ']' => return Err(ErrorKind::UnbalancedCharacterClass),
                '\\' => match self.advance() {
                    Some(c) => self.push_literal(c),
                    None => return Err(ErrorKind::DanglingEscape),
                },
                c => self.push_literal(c),
            }
        }

        // Finish off the parsing with creating a token for any remaining literal buffered.
        if let Some(literal) = self.current_literal.take() {
            self.push_token(Token::Literal(literal));
        }

        Ok(())
    }

    fn parse_class(&mut self) -> Result<(), ErrorKind> {
        let negated = self.advance_if(|c| c == '!');

        let mut ranges = Ranges::default();

        let mut first = true;
        let mut in_range = false;
        loop {
            let Some(c) = self.advance() else {
                return Err(ErrorKind::UnbalancedCharacterClass);
            };

            match c {
                // Another opening bracket is invalid, literal `[` need to be escaped.
                '[' => return Err(ErrorKind::InvalidCharacterClass),
                ']' => break,
                '-' => {
                    if first {
                        ranges.push(Range::single('-'));
                    } else if in_range {
                        // safe to unwrap, `in_range` is only true if there is already
                        // a range pushed.
                        ranges.last_mut().unwrap().set_end('-')?;
                        in_range = false;
                    } else {
                        assert!(!ranges.is_empty());
                        in_range = true;
                    }
                }
                c => {
                    let c = match c {
                        '\\' => self.advance().ok_or(ErrorKind::DanglingEscape)?,
                        c => c,
                    };

                    if in_range {
                        // safe to unwrap, `in_range` is only true if there is already
                        // a range pushed.
                        ranges.last_mut().unwrap().set_end(c)?;
                        in_range = false;
                    } else {
                        ranges.push(Range::single(c))
                    }
                }
            }

            first = false;
        }

        if in_range {
            // A pattern which ends with a `-`.
            ranges.push(Range::single('-'));
        }

        self.push_token(Token::Class { negated, ranges });

        Ok(())
    }

    fn push_literal(&mut self, c: char) {
        self.current_literal.get_or_insert_with(String::new).push(c);
    }

    /// Pushes the passed `token` and finishes the currently in progress token.
    fn push_token(&mut self, token: Token) {
        // Finish the currently active literal token if there is one.
        if let Some(literal) = self.current_literal.take() {
            self.tokens.push(Token::Literal(literal));
        }
        self.tokens.push(token);
    }

    fn advance(&mut self) -> Option<char> {
        self.chars.next()
    }

    fn advance_if(&mut self, matcher: impl FnOnce(char) -> bool) -> bool {
        if self.peek().is_some_and(matcher) {
            let _ = self.advance();
            true
        } else {
            false
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.chars.peek().copied()
    }
}

/// A container of tokens.
///
/// Automatically folds redundant tokens.
#[derive(Debug, Default)]
struct Tokens(Vec<Token>);

impl Tokens {
    fn push(&mut self, token: Token) {
        let Some(last) = self.0.last_mut() else {
            self.0.push(token);
            return;
        };

        match (last, token) {
            // Collapse Any's.
            (Token::Any(n), Token::Any(n2)) => *n += n2,
            // We can collapse multiple wildcards into a single one.
            // TODO: separator special handling (?)
            (Token::Wildcard, Token::Wildcard) => {}
            // `*` followed by `?` is just `*`.
            (Token::Wildcard, Token::Any(_)) => {}
            // `?` followed by `*` is just `*`.
            (last @ Token::Any(_), Token::Wildcard) => *last = Token::Wildcard,
            // Collapse multiple literals into one.
            (Token::Literal(ref mut last), Token::Literal(s)) => last.push_str(&s),
            // Everything else is just another token.
            (_, token) => self.0.push(token),
        }
    }

    fn as_mut_slice(&mut self) -> &mut [Token] {
        self.0.as_mut_slice()
    }
}

/// Represents a token in a Relay pattern.
#[derive(Debug)]
enum Token {
    /// A literal token.
    Literal(String),
    /// The any token `?` and how many `?` are seen in a row.
    Any(usize),
    /// The wildcard token `*`.
    Wildcard,
    /// A class token `[abc]` or its negated variant `[!abc]`.
    Class { negated: bool, ranges: Ranges },
}

/// A [`Range`] contains whatever is contained between `[` and `]` of
/// a glob pattern, except the negation.
///
/// For example the pattern `[a-z]` contains the range from `a` to `z`,
/// the pattern `[ax-zbf-h]` contains the ranges `x-z`, `f-h`, `a-a` and `b-b`.
#[derive(Debug, Default)]
enum Ranges {
    /// An empty, default range not containing any characters.
    ///
    /// The empty range matches nothing.
    #[default]
    Empty,
    /// The pattern only contains a single range.
    ///
    /// For example: `[a]` or `[a-z]`.
    Single(Range),
    /// The pattern contains more than one range.
    ///
    /// The `Empty` and `Single` states are just explicit and optimized
    /// cases for `Multiple` with a vector containing zero or just one range.
    Multiple(Vec<Range>),
}

impl Ranges {
    /// Pushes another range into the current [`Range`].
    ///
    /// Returns [`Error`] if the range starts with a lexicographically larger character than it
    /// ends with.
    fn push(&mut self, range: Range) {
        match self {
            Self::Empty => *self = Self::Single(range),
            Self::Single(single) => *self = Self::Multiple(vec![*single, range]),
            Self::Multiple(ref mut v) => v.push(range),
        }
    }

    /// Returns a mutable reference to the last range contained.
    fn last_mut(&mut self) -> Option<&mut Range> {
        match self {
            Ranges::Empty => None,
            Ranges::Single(range) => Some(range),
            Ranges::Multiple(ranges) => ranges.last_mut(),
        }
    }

    /// Returns `true` if there is no contained range.
    fn is_empty(&self) -> bool {
        match self {
            Ranges::Empty => true,
            Ranges::Single(_) => false,
            Ranges::Multiple(ranges) => {
                // While not technically wrong, the invariant should uphold.
                debug_assert!(!ranges.is_empty());
                ranges.is_empty()
            }
        }
    }

    /// Returns an iterator over all contained ranges.
    fn iter(&self) -> impl Iterator<Item = Range> + '_ {
        let single = match self {
            Ranges::Single(range) => Some(*range),
            _ => None,
        }
        .into_iter();

        let multiple = match self {
            Ranges::Multiple(ranges) => Some(ranges.iter().copied()),
            _ => None,
        }
        .into_iter()
        .flatten();

        single.chain(multiple)
    }
}

/// Represents a character range in a [`Token::Class`].
#[derive(Clone, Copy, Debug)]
struct Range {
    start: char,
    end: char,
}

impl Range {
    /// Create a new range which matches a single character.
    fn single(c: char) -> Self {
        Self { start: c, end: c }
    }

    /// Changes the end of the range to a new character.
    ///
    /// Returns an error if the new end character is lexicographically before
    /// the start character.
    fn set_end(&mut self, end: char) -> Result<(), ErrorKind> {
        if self.start > end {
            return Err(ErrorKind::InvalidRange(self.start, end));
        }
        self.end = end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_pattern {
        ($pattern:expr, $s:expr $(,$options:tt)?) => {{
            let mut pattern = Pattern::builder($pattern);
            for opt in stringify!($($options)?).chars() {
                match opt {
                    'i' => { pattern.case_insensitive(true); }
                    _ => {}
                }
            }
            let pattern = pattern.build().unwrap();
            assert!(
                pattern.is_match($s),
                "expected pattern '{}' to match '{}' - {pattern:?}",
                $pattern,
                $s
            );
        }};
        ($pattern:expr, NOT $s:expr $(,$options:tt)?) => {{
            let mut pattern = Pattern::builder($pattern);
            for opt in stringify!($($options)?).chars() {
                match opt {
                    'i' => { pattern.case_insensitive(true); }
                    _ => {}
                }
            }
            let pattern = pattern.build().unwrap();
            assert!(
                !pattern.is_match($s),
                "expected pattern '{}' to not match '{}' - {pattern:?}",
                $pattern,
                $s
            );
        }};
    }

    macro_rules! assert_strategy {
        ($pattern:expr, $expected:pat) => {{
            let pattern = Pattern::new($pattern).unwrap();
            let kind = match &pattern.strategy {
                MatchStrategy::Literal(_) => "Literal",
                MatchStrategy::Prefix(_) => "Prefix",
                MatchStrategy::Suffix(_) => "Suffix",
                MatchStrategy::Contains(_) => "Contains",
                MatchStrategy::Static(_) => "Static",
                MatchStrategy::Regex(_) => "Regex",
            };
            assert_eq!(
                kind,
                stringify!($expected),
                "expected pattern '{}' to have strategy '{}' - {pattern:?}",
                $pattern,
                stringify!($expected)
            );
        }};
    }

    macro_rules! assert_invalid {
        ($pattern:expr) => {{
            if let Ok(pattern) = Pattern::new($pattern) {
                assert!(
                    false,
                    "expected pattern '{}' to not compile - {pattern:?}",
                    $pattern
                );
            }
        }};
    }

    #[test]
    fn test_empty() {
        assert_pattern!("", NOT "");
        assert_pattern!("", NOT "foo");
    }

    #[test]
    fn test_literal() {
        assert_pattern!("foo", "foo");
        assert_pattern!("foo", NOT "fOo");
        assert_pattern!("foo", NOT "FOO");
        assert_pattern!("f{}o", "f{}o");
        assert_pattern!("f}o", "f}o");
        assert_pattern!("f{o", "f{o");
        assert_pattern!("f{b,a,r}o", "f{b,a,r}o");
        assert_pattern!(r"f\\o", r"f\o");
        assert_pattern!("fඞo", "fඞo");
    }

    #[test]
    fn test_literal_case_insensitive() {
        assert_pattern!("foo", "foo", i);
        assert_pattern!("foo", "fOo", i);
        assert_pattern!("fOo", "Foo", i);
        assert_pattern!("İ", "i\u{307}", i);
        assert_pattern!("İ", "i̇", i);
    }

    #[test]
    fn test_literal_strategy() {
        assert_strategy!("foo", Literal);
        assert_strategy!("f{b,a,r}o", Literal);
        assert_strategy!(r"f\\o", Literal);
        assert_strategy!("fඞo", Literal);
    }

    #[test]
    fn test_prefix() {
        assert_pattern!("foo*", "foo___");
        assert_pattern!("foo*", "foo");
        assert_pattern!("foo**", "foo");
        assert_pattern!("foo*?*", "foo");
        assert_pattern!("foo*?*?", "foo");
        assert_pattern!("foo**", "foo___");
        assert_pattern!("foo*?*", "foo___");
        assert_pattern!("foo*?*?", "foo___");
        assert_pattern!("foo*", "fooඞ");
        assert_pattern!("ඞ*", "ඞfoo");
        assert_pattern!("foo*", NOT "___");
        assert_pattern!("foo*", NOT "fo");
        assert_pattern!("foo*", NOT "fob");
        assert_pattern!("foo*", NOT "boo");
        assert_pattern!("foo*", NOT "Foo");
        assert_pattern!("foo*", NOT "FOO___");

        // No special slash handling
        assert_pattern!("foo/bar*", "foo/bar___");
        assert_pattern!("foo*", "foo/bar___");
        assert_pattern!("foo*", NOT "/foo");
    }

    #[test]
    fn test_prefix_case_insensitive() {
        assert_pattern!("foo*", "foo___", i);
        assert_pattern!("foo*", "fOo___", i);
        assert_pattern!("foo*", "FOO___", i);
        assert_pattern!("fOo*", "FOO___", i);
        assert_pattern!("fOo*", "Foo___", i);

        assert_pattern!("İ*", "İ___", i);
        assert_pattern!("İ*", "İ", i);
        assert_pattern!("İ*", "i̇", i);
        assert_pattern!("İ*", "i\u{307}___", i);
        assert_pattern!("İ*", NOT "i____", i);
    }

    #[test]
    fn test_prefix_strategy() {
        assert_strategy!("foo*", Prefix);
        assert_strategy!("foo*?", Prefix);
        assert_strategy!("foo?*", Prefix);
        assert_strategy!("foo*?*", Prefix);
        assert_strategy!("foo??***???****", Prefix);
        assert_strategy!("foo***???****??", Prefix);
    }

    #[test]
    fn test_suffix() {
        assert_pattern!("*foo", "___foo");
        assert_pattern!("*foo", "foo");
        assert_pattern!("**foo", "foo");
        assert_pattern!("*?*foo", "foo");
        assert_pattern!("?*?*foo", "foo");
        assert_pattern!("**foo", "___foo");
        assert_pattern!("*?*foo", "___foo");
        assert_pattern!("*?*?foo", "__foo");
        assert_pattern!("*foo", "ඞfoo");
        assert_pattern!("*ඞ", "fooඞ");
        assert_pattern!("*foo", NOT "bar");
        assert_pattern!("*foo", NOT "fo");
        assert_pattern!("*foo", NOT "fob");
        assert_pattern!("*foo", NOT "boo");
        assert_pattern!("*foo", NOT "Foo");
        assert_pattern!("*foo", NOT "___FOO");

        // No special slash handling
        assert_pattern!("*foo/bar", "___foo/bar");
        assert_pattern!("*bar", "___foo/bar");
        assert_pattern!("*foo", NOT "foo/");
    }

    #[test]
    fn test_suffix_case_insensitive() {
        assert_pattern!("foo*", "foo___", i);
        assert_pattern!("foo*", "fOo___", i);
        assert_pattern!("foo*", "FOO___", i);
        assert_pattern!("fOo*", "FOO___", i);
        assert_pattern!("fOo*", "Foo___", i);

        assert_pattern!("*İ", "___İ", i);
        assert_pattern!("*İ", "İ", i);
        assert_pattern!("*İ", "i̇", i);
        assert_pattern!("*İ", "___i\u{307}", i);
        assert_pattern!("*İ", NOT "___i", i);
    }

    #[test]
    fn test_suffix_strategy() {
        assert_strategy!("*foo", Suffix);
        assert_strategy!("*?foo", Suffix);
        assert_strategy!("?*foo", Suffix);
        assert_strategy!("*?*foo", Suffix);
        assert_strategy!("??***???****foo", Suffix);
        assert_strategy!("***???****??foo", Suffix);
    }

    #[test]
    fn test_contains() {
        assert_pattern!("*foo*", "foo");
        assert_pattern!("*foo*", "foo___");
        assert_pattern!("*foo*", "___foo");
        assert_pattern!("*foo*", "___foo___");
        assert_pattern!("*foo*", NOT "___fo");
        assert_pattern!("*foo*", NOT "oo___");
        assert_pattern!("*foo*", NOT "___fo___");
        assert_pattern!("*foo*", NOT "fඞo");
        assert_pattern!("*foo*", NOT "___fOo___");
        assert_pattern!("*foo*", NOT "___FOO___");

        // No special slash handling
        assert_pattern!("*foo*", "foo");
        assert_pattern!("*foo*", "foo_/_");
        assert_pattern!("*foo*", "_/_foo");
        assert_pattern!("*foo*", "_/_foo_/_");
        assert_pattern!("*f/o*", "_/_f/o_/_");
    }

    #[test]
    fn test_contains_case_insensitive() {
        assert_pattern!("*foo*", "foo", i);
        assert_pattern!("*foo*", "fOo", i);
        assert_pattern!("*fOo*", "Foo", i);
        assert_pattern!("*foo*", "___foo___", i);
        assert_pattern!("*foo*", "___fOo___", i);
        assert_pattern!("*foo*", "___FOO___", i);
        assert_pattern!("*fOo*", "___FOO___", i);
        assert_pattern!("*fOo*", "___Foo___", i);

        assert_pattern!("*İ*", "___İ___", i);
        assert_pattern!("*İ*", "İ", i);
        assert_pattern!("*İ*", "___İ", i);
        assert_pattern!("*İ*", "İ___", i);
        assert_pattern!("*İ*", "___İ___", i);
        assert_pattern!("*İ*", "i̇", i);
        assert_pattern!("*İ*", "___i̇", i);
        assert_pattern!("*İ*", "i̇___", i);
        assert_pattern!("*İ*", "___i̇___", i);
        assert_pattern!("*İ*", "___i\u{307}", i);
        assert_pattern!("*İ*", "i\u{307}___", i);
        assert_pattern!("*İ*", "___i\u{307}___", i);
        assert_pattern!("*İ*", NOT "i", i);
        assert_pattern!("*İ*", NOT "i___", i);
        assert_pattern!("*İ*", NOT "___i", i);
        assert_pattern!("*İ*", NOT "___i___", i);
    }

    #[test]
    fn test_contains_strategy() {
        assert_strategy!("*foo*", Contains);
        assert_strategy!("*?foo?*", Contains);
        assert_strategy!("?*foo*?", Contains);
        assert_strategy!("*?*foo*?*", Contains);
        assert_strategy!("??***???****foo??***???****", Contains);
        assert_strategy!("***???****??foo***???****??", Contains);
    }

    #[test]
    fn test_wildcard() {
        assert_pattern!("*", "");
        assert_pattern!("*", "a");
        assert_pattern!("*", "\n");
        assert_pattern!("*", "\n\n");
        assert_pattern!("*", "\na\n");
        assert_pattern!("*", "\na\nb\nc");
        assert_pattern!("*", "ඞfooඞfooඞ");

        // No special slash handling
        assert_pattern!("*", "/");
        assert_pattern!("*", "_/");
        assert_pattern!("*", "/_");
        assert_pattern!("*", "_/_");
        assert_pattern!("*", "/?/?/");
    }

    #[test]
    fn test_wildcard_strategy() {
        assert_strategy!("*", Static);
    }

    #[test]
    fn test_any() {
        assert_pattern!("?", NOT "");
        assert_pattern!("?", "ඞ");
        assert_pattern!("?", "?");
        assert_pattern!("?", "\n");
        assert_pattern!("?", "_");
        assert_pattern!("?", NOT "aa");
        assert_pattern!("?", NOT "aaaaaaaaaaaaaaaaaa");
        assert_pattern!("??", "aa");
        assert_pattern!("??", NOT "aaa");
        assert_pattern!("a?a?a", "aaaaa");
        assert_pattern!("a?a?a", "abaca");
        assert_pattern!("a?a?a", NOT "ab_ca");
        assert_pattern!("a?a?a", NOT "aaAaa");
        assert_pattern!("???????????x???????????", "???????????x???????????");
        assert_pattern!("???????????x???????????", "??______???x?????_?????");
        assert_pattern!("???????????x???????????", NOT "?______???x?????_?????");
        assert_pattern!("???????????x???????????", NOT "??______???_?????_?????");

        // No special slash handling
        assert_pattern!("?", "/");
    }

    #[test]
    fn test_escapes() {
        assert_pattern!(r"f\\o", r"f\o");
        assert_pattern!(r"f\*o", r"f*o");
        assert_pattern!(r"f\*o", NOT r"f\*o");
        assert_pattern!(r"f\\*o", r"f\*o");
        assert_pattern!(r"f\\*o", r"f\o");
        assert_pattern!(r"f\\*o", r"f\___o");
        assert_pattern!(r"f\\\*o", r"f\*o");
        assert_pattern!(r"f\[\]o", r"f[]o");
        assert_pattern!(r"f\[?\]o", r"f[?]o");
        assert_pattern!(r"f\[a-z\]o", r"f[a-z]o");
        assert_pattern!(r"f\[o", r"f[o");
        assert_pattern!(r"f\]o", r"f]o");
        assert_pattern!(r"\[", r"[");
    }

    #[test]
    fn test_invalid() {
        assert_invalid!(r"\");
        assert_invalid!(r"f\");
        assert_invalid!(r"*\");
        assert_invalid!("[");
        assert_invalid!("[a-z");
        assert_invalid!(r"[a-z\");
        assert_invalid!("[[]");
        assert_invalid!("[]]");
        assert_invalid!("]");
        assert_invalid!("[a-z");
        assert_invalid!("[b-a]");
        assert_invalid!("[a-A]");
    }

    #[test]
    fn test_classes() {
        assert_pattern!("[]", "");
        assert_pattern!("[]", NOT "_");
        assert_pattern!("[a]", "a");
        assert_pattern!("[a]", NOT "[a]");
        assert_pattern!("[a]", NOT "b");
        assert_pattern!("[a]", NOT "A");
        assert_pattern!("[ඞ]", "ඞ");
        assert_pattern!("[ඞ]", NOT "a");
        assert_pattern!("[ඞa]", "a");
        assert_pattern!("[aඞ]", "ඞ");
        assert_pattern!("[ඞa]", NOT "b");
        assert_pattern!("[ab]", "a");
        assert_pattern!("[ab]", "b");
        assert_pattern!("[ab]", NOT "c");
        assert_pattern!("x[ab]x", "xax");
        assert_pattern!("x[ab]x", "xbx");
        assert_pattern!("x[ab]x", NOT "xBx");
        assert_pattern!("x[ab]x", NOT "xcx");
        assert_pattern!("x[ab]x", NOT "aax");
        assert_pattern!("x[ab]x", NOT "xaa");
        assert_pattern!("x[ab]x", NOT "xaax");
        assert_pattern!("x[ab]x", NOT "xxax");
        assert_pattern!("x[ab]x", NOT "xaxx");
        assert_pattern!("[a-b]", "a");
        assert_pattern!("[a-b]", "b");
        assert_pattern!("[a-b]", NOT "c");
        assert_pattern!("[a-c]", "a");
        assert_pattern!("[a-c]", "b");
        assert_pattern!("[a-c]", "c");
        assert_pattern!("[a-c]", NOT "d");
        assert_pattern!("[a-c]", NOT "1");
        assert_pattern!("[a-c]", NOT "ඞ");
        assert_pattern!("[A-z]", "a");
        assert_pattern!("[A-z]", "z");
        assert_pattern!("[A-z]", "["); // `[` is actually inbetween here in the ascii table
        assert_pattern!("[A-z]", "A");
        assert_pattern!("[A-z]", "Z");
        assert_pattern!("[A-z]", NOT "0");
        assert_pattern!("[0-9]", "0");
        assert_pattern!("[0-9]", "1");
        assert_pattern!("[0-9]", "2");
        assert_pattern!("[0-9]", "3");
        assert_pattern!("[0-9]", "4");
        assert_pattern!("[0-9]", "5");
        assert_pattern!("[0-9]", "6");
        assert_pattern!("[0-9]", "7");
        assert_pattern!("[0-9]", "8");
        assert_pattern!("[0-9]", "9");
        assert_pattern!(
            "[0-9a-bX-ZfF][0-9a-bX-ZfF][0-9a-bX-ZfF][0-9a-bX-ZfF]",
            "3bYf"
        );
        assert_pattern!(
            "[0-9a-bX-ZfF][0-9a-bX-ZfF][0-9a-bX-ZfF][0-9a-bX-ZfF]",
            NOT "3cYf"
        );
        assert_pattern!(
            "[0-9a-bX-ZfF][0-9a-bX-ZfF][0-9a-bX-ZfF][0-9a-bX-ZfF]",
            NOT "3ඞYf"
        );
        assert_pattern!("[0-9]", NOT "a9");
        assert_pattern!("[0-9]", NOT "9a");
        assert_pattern!("[0-9]", NOT "a9a");
        assert_pattern!("[0-9]", NOT "");
        assert_pattern!("[0-9!]", "!");
        assert_pattern!("[0-9][a-b]", NOT "");
        assert_pattern!("[0-9][a-b]", NOT "a");
        assert_pattern!("[0-9][a-b]", NOT "a0");
        assert_pattern!("[0-9][a-b]", "0a");
        assert_pattern!(r"a[\]a\-]b", "aab");

        // TODO: lenient: assert_pattern!("a[]-]b", NOT "aab");
        // TODO: lenient: assert_pattern!("]", "]");
        // TODO: lenient: assert_pattern!("a[]-]b", "a-]b");
        // TODO: lenient: assert_pattern!("a[]-]b", "a]b");
        // TODO: lenient: assert_pattern!("a[]]b", "a]b");

        // Escapes in character classes
        assert_pattern!(r"[\\]", r"\");
        assert_pattern!(r"[\\]", NOT "a");
        assert_pattern!(r"[\]]", "]");
        assert_pattern!(r"[\]]", NOT "a");
        assert_pattern!(r"[\[]", "[");
        assert_pattern!(r"[\[]", NOT "a");
        assert_pattern!(r"[\]]", NOT r"\");

        assert_pattern!("a[X-]b", "a-b");
        assert_pattern!("a[X-]b", "aXb");
    }

    #[test]
    fn test_classes_case_insensitive() {
        assert_pattern!("[a]", "a", i);
        assert_pattern!("[a]", "A", i);
        assert_pattern!("x[ab]x", "xAX", i);
        assert_pattern!("x[ab]x", "XBx", i);
        assert_pattern!("x[ab]x", NOT "Xcx", i);
        assert_pattern!("x[ab]x", NOT "aAx", i);
        assert_pattern!("x[ab]x", NOT "xAa", i);
    }

    #[test]
    fn test_classes_negated() {
        assert_pattern!("[!]", "");
        assert_pattern!("[!a]", "b");
        assert_pattern!("[!a]", "A");
        assert_pattern!("[!a]", "B");
        assert_pattern!("[!b]", NOT "b");
        assert_pattern!("[!ab]", NOT "a");
        assert_pattern!("[!ab]", NOT "b");
        assert_pattern!("[!ab]", "c");
        assert_pattern!("x[!ab]x", "xcx");
        assert_pattern!("x[!ab]x", NOT "xax");
        assert_pattern!("x[!ab]x", NOT "xbx");
        assert_pattern!("x[!ab]x", NOT "xxcx");
        assert_pattern!("x[!ab]x", NOT "xcxx");
        assert_pattern!("x[!ab]x", NOT "xc");
        assert_pattern!("x[!ab]x", NOT "cx");
        assert_pattern!("x[!ab]x", NOT "x");
        assert_pattern!("[!a-c]", NOT "a");
        assert_pattern!("[!a-c]", NOT "b");
        assert_pattern!("[!a-c]", NOT "c");
        assert_pattern!("[!a-c]", "d");
        assert_pattern!("[!a-c]", "A");
        assert_pattern!("[!a-c]", "ඞ");
        assert_pattern!(r"[!a-c\\]", "d");
        assert_pattern!(r"[!a-c\\]", NOT r"\");
        assert_pattern!(r"[!\]]", "a");
        assert_pattern!(r"[!\]]", NOT "]");
        assert_pattern!(r"[!!]", "a");
        assert_pattern!(r"[!!]", NOT "!");
    }

    #[test]
    fn test_classes_negated_case_insensitive() {
        assert_pattern!("[!a]", "b", i);
        assert_pattern!("[!a]", "B", i);
        assert_pattern!("[!b]", NOT "b", i);
        assert_pattern!("[!b]", NOT "B", i);
        assert_pattern!("[!ab]", NOT "a", i);
        assert_pattern!("[!ab]", NOT "A", i);
        assert_pattern!("[!ab]", NOT "b", i);
        assert_pattern!("[!ab]", NOT "B", i);
        assert_pattern!("[!ab]", "c", i);
        assert_pattern!("[!ab]", "C", i);
    }

    #[test]
    fn test_complex() {
        assert_pattern!("*?", "\n");
        assert_pattern!("?*", "\n");
        assert_pattern!("*?*", "\n");
        assert_pattern!("1.18.[!0-4].*", "1.18.5.");
        assert_pattern!("1.18.[!0-4].*", "1.18.5.aBc");
        assert_pattern!("1.18.[!0-4].*", NOT "1.18.3.abc");
        assert_pattern!("!*!*.md", "!foo!.md"); // no `!` outside of character classes
        assert_pattern!("foo*foofoo*foobar", "foofoofooxfoofoobar");

        assert_pattern!(
            r"/api/0/organizations/\{organization_slug\}/event*",
            "/api/0/organizations/{organization_slug}/event/foobar"
        );
        assert_pattern!(
            r"/api/0/organizations/\{organization_slug\}/event*",
            NOT r"/api/0/organizations/\{organization_slug\}/event/foobar"
        );
    }
}
