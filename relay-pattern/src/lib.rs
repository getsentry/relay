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
//! * `{a,b}` matches any pattern within the alternation group.
//! * `\` escapes any of the above special characters and treats it as a literal.
//!
//! # Complexity
//!
//! Patterns can be limited to a maximum complexity using [`PatternBuilder::max_complexity`].
//! Complexity of a pattern is calculated by the amount of possible combinations created with
//! alternations.
//!
//! For example, the pattern `{foo,bar}` has a complexity of 2, the pattern `{foo,bar}/{*.html,*.js,*.css}`
//! has a complexity of `2 * 3`.
//!
//! For untrusted user input it is highly recommended to limit the maximum complexity.

use std::fmt;
use std::num::NonZeroUsize;

mod typed;
mod wildmatch;

pub use typed::*;

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
    /// Unbalanced character class. The pattern contains unbalanced `[`, `]` characters.
    UnbalancedCharacterClass,
    /// Character class is invalid and cannot be parsed.
    InvalidCharacterClass,
    /// Nested alternates are not valid.
    NestedAlternates,
    /// Unbalanced alternates. The pattern contains unbalanced `{`, `}` characters.
    UnbalancedAlternates,
    /// Dangling escape character.
    DanglingEscape,
    /// The pattern's complexity exceeds the maximum allowed complexity.
    Complexity {
        complexity: u64,
        max_complexity: u64,
    },
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
            ErrorKind::NestedAlternates => write!(f, "Nested alternates"),
            ErrorKind::UnbalancedAlternates => write!(f, "Unbalanced alternates"),
            ErrorKind::DanglingEscape => write!(f, "Dangling escape"),
            ErrorKind::Complexity { complexity, max_complexity } =>
                write!(f, "Pattern complexity ({complexity}) exceeds maximum allowed complexity ({max_complexity})"),
        }
    }
}

/// `Pattern` represents a successfully parsed Relay pattern.
#[derive(Debug, Clone)]
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
            max_complexity: u64::MAX,
        }
    }

    /// Returns `true` if the pattern matches the passed string.
    pub fn is_match(&self, haystack: &str) -> bool {
        self.strategy.is_match(haystack, self.options)
    }
}

impl fmt::Display for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.pattern)
    }
}

/// A collection of [`Pattern`]s sharing the same configuration.
#[derive(Debug, Clone)]
pub struct Patterns {
    strategies: Vec<MatchStrategy>,
    options: Options,
}

impl Patterns {
    /// Creates an empty [`Patterns`] instance which never matches anything.
    ///
    /// ```
    /// # use relay_pattern::Patterns;
    /// let patterns = Patterns::empty();
    ///
    /// assert!(!patterns.is_match(""));
    /// assert!(!patterns.is_match("foobar"));
    /// ```
    pub fn empty() -> Self {
        Self {
            strategies: Vec::new(),
            options: Options::default(),
        }
    }

    /// Returns a [`PatternsBuilder`].
    pub fn builder() -> PatternsBuilder {
        PatternsBuilder {
            options: Options::default(),
        }
    }

    /// Returns `true` if any of the contained patterns matches the passed string.
    pub fn is_match(&self, haystack: &str) -> bool {
        self.strategies
            .iter()
            .any(|s| s.is_match(haystack, self.options))
    }

    /// Returns `true` if this instance contains no patterns.
    ///
    /// An empty [`Patterns`] never matches any input.
    pub fn is_empty(&self) -> bool {
        self.strategies.is_empty()
    }
}

/// A builder for a [`Pattern`].
#[derive(Debug)]
pub struct PatternBuilder<'a> {
    pattern: &'a str,
    max_complexity: u64,
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

    /// Sets the max complexity for this pattern.
    ///
    /// Attempting to build a pattern with a complexity higher
    /// than the maximum specified here will fail.
    ///
    /// Defaults to `u64::MAX`.
    pub fn max_complexity(&mut self, max_complexity: u64) -> &mut Self {
        self.max_complexity = max_complexity;
        self
    }

    /// Build a new [`Pattern`] from the passed pattern and configured options.
    pub fn build(&self) -> Result<Pattern, Error> {
        let mut parser = Parser::new(self.pattern, self.options);
        parser.parse().map_err(|kind| Error {
            pattern: self.pattern.to_owned(),
            kind,
        })?;

        if parser.complexity > self.max_complexity {
            return Err(Error {
                pattern: self.pattern.to_owned(),
                kind: ErrorKind::Complexity {
                    complexity: parser.complexity,
                    max_complexity: self.max_complexity,
                },
            });
        }

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

/// A builder for a collection of [`Patterns`].
#[derive(Debug)]
pub struct PatternsBuilder {
    options: Options,
}

impl PatternsBuilder {
    /// If enabled matches the pattern case insensitive.
    ///
    /// This is disabled by default.
    pub fn case_insensitive(&mut self, enabled: bool) -> &mut Self {
        self.options.case_insensitive = enabled;
        self
    }

    /// Returns a [`PatternsBuilderConfigured`] builder which allows adding patterns.
    pub fn patterns(&mut self) -> PatternsBuilderConfigured {
        PatternsBuilderConfigured {
            strategies: Vec::new(),
            options: self.options,
        }
    }

    /// Adds a pattern to the builder and returns the resulting [`PatternsBuilderConfigured`].
    pub fn add(&mut self, pattern: &str) -> Result<PatternsBuilderConfigured, Error> {
        let mut builder = PatternsBuilderConfigured {
            strategies: Vec::with_capacity(1),
            options: self.options,
        };
        builder.add(pattern)?;
        Ok(builder)
    }
}

/// A [`PatternsBuilder`] with all options configured.
///
/// The second step after [`PatternsBuilder`].
#[derive(Debug)]
pub struct PatternsBuilderConfigured {
    strategies: Vec<MatchStrategy>,
    options: Options,
}

impl PatternsBuilderConfigured {
    /// Adds a pattern to the builder.
    pub fn add(&mut self, pattern: &str) -> Result<&mut Self, Error> {
        let mut parser = Parser::new(pattern, self.options);
        parser.parse().map_err(|kind| Error {
            pattern: pattern.to_owned(),
            kind,
        })?;

        let strategy =
            MatchStrategy::from_tokens(parser.tokens, self.options).map_err(|kind| Error {
                pattern: pattern.to_owned(),
                kind,
            })?;

        self.strategies.push(strategy);

        Ok(self)
    }

    /// Builds a [`Patterns`] from the contained patterns.
    pub fn build(self) -> Patterns {
        Patterns {
            strategies: self.strategies,
            options: self.options,
        }
    }

    /// Returns [`Patterns`] containing all added patterns and removes them from the builder.
    ///
    /// The builder can still be used afterwards, it keeps the configuration.
    pub fn take(&mut self) -> Patterns {
        Patterns {
            strategies: std::mem::take(&mut self.strategies),
            options: self.options,
        }
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
/// patterns fallback to [`wildmatch::is_match`].
#[derive(Debug, Clone)]
enum MatchStrategy {
    /// The pattern is a single literal string.
    ///
    /// The stored string is converted to lowercase for case insensitive patterns.
    ///
    /// Example pattern: `foobar`.
    Literal(Literal),
    /// The pattern only has a single wildcard in the end and can be
    /// matched with a simple prefix check.
    ///
    /// The stored string is converted to lowercase for case insensitive patterns.
    ///
    /// Example pattern: `foobar*`.
    Prefix(Literal),
    /// The pattern only has a single wildcard at the start and can be
    /// matched with a simple suffix check.
    ///
    /// The stored string is converted to lowercase for case insensitive patterns.
    ///
    /// Example pattern: `*foobar`.
    Suffix(Literal),
    /// The pattern is surrounded with wildcards and contains a literal in the middle
    /// and can be matched with a simple contains check.
    ///
    /// The stored string is converted to lowercase for case insensitive patterns.
    ///
    /// Example pattern: `*foobar*`.
    Contains(Literal),
    /// The pattern always evaluates to a static boolean.
    ///
    /// Example: `*`
    Static(bool),
    /// The pattern is complex and needs to be evaluated using [`wildmatch`].
    Wildmatch(Tokens),
    // Possible future optimizations for `Any` variations:
    // Examples: `??`. `??suffix`, `prefix??` and `?contains?`.
}

impl MatchStrategy {
    /// Create a [`MatchStrategy`] from [`Tokens`].
    fn from_tokens(mut tokens: Tokens, _options: Options) -> Result<Self, ErrorKind> {
        let s = match tokens.as_mut_slice() {
            [] => Self::Static(false),
            [Token::Wildcard] => Self::Static(true),
            [Token::Literal(literal)] => Self::Literal(std::mem::take(literal)),
            [Token::Literal(literal), Token::Wildcard] => Self::Prefix(std::mem::take(literal)),
            [Token::Wildcard, Token::Literal(literal)] => Self::Suffix(std::mem::take(literal)),
            [Token::Wildcard, Token::Literal(literal), Token::Wildcard] => {
                Self::Contains(std::mem::take(literal))
            }
            _ => Self::Wildmatch(tokens),
        };

        Ok(s)
    }

    /// Returns `true` if the pattern matches the passed string.
    pub fn is_match(&self, haystack: &str, options: Options) -> bool {
        match &self {
            MatchStrategy::Literal(literal) => match_literal(literal, haystack, options),
            MatchStrategy::Prefix(prefix) => match_prefix(prefix, haystack, options),
            MatchStrategy::Suffix(suffix) => match_suffix(suffix, haystack, options),
            MatchStrategy::Contains(contains) => match_contains(contains, haystack, options),
            MatchStrategy::Static(matches) => *matches,
            MatchStrategy::Wildmatch(tokens) => wildmatch::is_match(haystack, tokens, options),
        }
    }
}

#[inline(always)]
fn match_literal(literal: &Literal, haystack: &str, options: Options) -> bool {
    if options.case_insensitive {
        // Can't do an explicit len compare first here `literal.len() == haystack.len()`,
        // the amount of characters can change when converting case.
        let mut literal = literal.as_case_converted_str().chars();
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
        literal.as_case_converted_str() == haystack
    }
}

#[inline(always)]
fn match_prefix(prefix: &Literal, haystack: &str, options: Options) -> bool {
    if options.case_insensitive {
        let mut prefix = prefix.as_case_converted_str().chars();
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
        haystack.starts_with(prefix.as_case_converted_str())
    }
}

#[inline(always)]
fn match_suffix(suffix: &Literal, haystack: &str, options: Options) -> bool {
    if options.case_insensitive {
        let mut suffix = suffix.as_case_converted_str().chars().rev();
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
        haystack.ends_with(suffix.as_case_converted_str())
    }
}

#[inline(always)]
fn match_contains(contains: &Literal, haystack: &str, options: Options) -> bool {
    if options.case_insensitive {
        let haystack = haystack.to_lowercase();
        memchr::memmem::find(haystack.as_bytes(), contains.as_case_converted_bytes()).is_some()
    } else {
        memchr::memmem::find(haystack.as_bytes(), contains.as_case_converted_bytes()).is_some()
    }
}

struct Parser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    tokens: Tokens,
    alternates: Option<Vec<Tokens>>,
    current_literal: Option<String>,
    options: Options,
    complexity: u64,
}

impl<'a> Parser<'a> {
    fn new(pattern: &'a str, options: Options) -> Self {
        Self {
            chars: pattern.chars().peekable(),
            tokens: Default::default(),
            alternates: None,
            current_literal: None,
            options,
            complexity: 0,
        }
    }

    fn parse(&mut self) -> Result<(), ErrorKind> {
        while let Some(c) = self.advance() {
            match c {
                '?' => self.push_token(Token::Any(NonZeroUsize::MIN)),
                '*' => self.push_token(Token::Wildcard),
                '[' => self.parse_class()?,
                ']' => return Err(ErrorKind::UnbalancedCharacterClass),
                '{' => self.start_alternates()?,
                '}' => self.end_alternates()?,
                '\\' => match self.advance() {
                    Some(c) => self.push_literal(c),
                    None => return Err(ErrorKind::DanglingEscape),
                },
                ',' if self.alternates.is_some() => {
                    self.finish_literal();
                    // safe to unwrap, we just checked for `some`.
                    let alternates = self.alternates.as_mut().unwrap();
                    alternates.push(Tokens::default());
                }
                c => self.push_literal(c),
            }
        }

        // Finish off the parsing with creating a token for any remaining literal buffered.
        self.finish_literal();

        Ok(())
    }

    fn start_alternates(&mut self) -> Result<(), ErrorKind> {
        if self.alternates.is_some() {
            return Err(ErrorKind::NestedAlternates);
        }
        self.finish_literal();
        self.alternates = Some(Vec::new());
        Ok(())
    }

    fn end_alternates(&mut self) -> Result<(), ErrorKind> {
        self.finish_literal();
        match self.alternates.take() {
            None => return Err(ErrorKind::UnbalancedAlternates),
            Some(alternates) => {
                if !alternates.is_empty() {
                    self.complexity = self
                        .complexity
                        .max(1)
                        .saturating_mul(alternates.len() as u64);
                    self.push_token(Token::Alternates(alternates));
                }
            }
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

    /// Pushes a new character into the currently active literal token.
    ///
    /// Starts a new literal token if there is none already.
    fn push_literal(&mut self, c: char) {
        self.current_literal.get_or_insert_with(String::new).push(c);
    }

    /// Finishes and pushes the currently in progress literal token.
    fn finish_literal(&mut self) {
        if let Some(literal) = self.current_literal.take() {
            self.push_token(Token::Literal(Literal::new(literal, self.options)));
        }
    }

    /// Pushes the passed `token` and finishes the currently in progress literal token.
    fn push_token(&mut self, token: Token) {
        self.finish_literal();
        match self.alternates.as_mut() {
            Some(alternates) => match alternates.last_mut() {
                Some(tokens) => tokens.push(token),
                None => {
                    let mut tokens = Tokens::default();
                    tokens.push(token);
                    alternates.push(tokens);
                }
            },
            None => self.tokens.push(token),
        }
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
///
/// The contained tokens are guaranteed to uphold the following invariants:
/// - A [`Token::Wildcard`] is never followed by [`Token::Wildcard`].
/// - A [`Token::Any`] is never followed by [`Token::Any`].
/// - A [`Token::Literal`] is never followed by [`Token::Literal`].
/// - A [`Token::Class`] is never empty.
#[derive(Clone, Debug, Default)]
struct Tokens(Vec<Token>);

impl Tokens {
    fn push(&mut self, mut token: Token) {
        // Normalize / clean the token.
        if let Token::Alternates(mut alternates) = token {
            let mut contains_empty = false;
            let mut contains_wildcard = false;
            alternates.retain_mut(|alternate| {
                if alternate.0.is_empty() {
                    contains_empty = true;
                    return false;
                }

                if matches!(alternate.0.as_slice(), [Token::Wildcard]) {
                    contains_wildcard = true;
                }

                true
            });

            // At this point `alternates` contains only the nonempty branches
            // and we additionally know
            // * if one of the branches was just a wildcard
            // * if there were any empty branches.
            //
            // We can push different tokens based on this.
            if contains_wildcard {
                // Case: {foo,*,} -> reduces to *
                token = Token::Wildcard;
            } else if alternates.len() == 1 {
                if contains_empty {
                    // Case: {foo*bar,} -> Optional(foo*bar)
                    token = Token::Optional(alternates.remove(0).0);
                } else {
                    // Case: {foo*bar} -> remove the alternation and
                    // push foo*bar directly
                    for t in alternates.remove(0).0 {
                        self.push(t);
                    }
                    return;
                }
            } else if alternates.len() > 1 {
                if contains_empty {
                    // Case: {foo,bar,} -> Optional({foo,bar})
                    token = Token::Optional(vec![Token::Alternates(alternates)]);
                } else {
                    // Case: {foo, bar} -> can stay as it is
                    token = Token::Alternates(alternates);
                }
            } else {
                // Case: {,,,} -> reduces to {}
                token = Token::Alternates(vec![]);
            }
        }

        match (self.0.last_mut(), token) {
            // Collapse Any's.
            (Some(Token::Any(n)), Token::Any(n2)) => *n = n.saturating_add(n2.get()),
            // We can collapse multiple wildcards into a single one.
            // TODO: separator special handling (?)
            (Some(Token::Wildcard), Token::Wildcard) => {}
            // Collapse multiple literals into one.
            (Some(Token::Literal(ref mut last)), Token::Literal(s)) => last.push(&s),
            // Ignore empty class tokens.
            (_, Token::Class { negated: _, ranges }) if ranges.is_empty() => {}
            // Everything else is just another token.
            (_, token) => self.0.push(token),
        }
    }

    fn as_mut_slice(&mut self) -> &mut [Token] {
        self.0.as_mut_slice()
    }

    fn as_slice(&self) -> &[Token] {
        self.0.as_slice()
    }
}

/// Represents a token in a Relay pattern.
#[derive(Clone, Debug)]
enum Token {
    /// A literal token.
    Literal(Literal),
    /// The any token `?` and how many `?` are seen in a row.
    Any(NonZeroUsize),
    /// The wildcard token `*`.
    Wildcard,
    /// A class token `[abc]` or its negated variant `[!abc]`.
    Class { negated: bool, ranges: Ranges },
    /// A list of nested alternate tokens `{a,b}`.
    Alternates(Vec<Tokens>),
    /// A list of optional tokens.
    ///
    /// This has no syntax of its own, it's parsed
    /// from alternatives containing empty branches
    /// like `{a,b,}`.
    Optional(Vec<Token>),
}

/// A string literal.
///
/// The contained literal is only available as a case converted string.
/// Depending on whether the pattern is case sensitive or case insensitive the literal is either
/// the original string or converted to lowercase.
#[derive(Clone, Debug, Default)]
struct Literal(String);

impl Literal {
    /// Creates a new literal from `s` and `options`.
    fn new(s: String, options: Options) -> Self {
        match options.case_insensitive {
            false => Self(s),
            true => Self(s.to_lowercase()),
        }
    }

    /// Adds a literal to this literal.
    ///
    /// This function does not validate case conversion, both literals must be for the same caseing.
    fn push(&mut self, Literal(other): &Literal) {
        self.0.push_str(other);
    }

    /// Returns a reference to the case converted string.
    fn as_case_converted_str(&self) -> &str {
        &self.0
    }

    /// Returns a reference to the case converted string as bytes.
    fn as_case_converted_bytes(&self) -> &[u8] {
        self.as_case_converted_str().as_bytes()
    }
}

/// A [`Range`] contains whatever is contained between `[` and `]` of
/// a glob pattern, except the negation.
///
/// For example the pattern `[a-z]` contains the range from `a` to `z`,
/// the pattern `[ax-zbf-h]` contains the ranges `x-z`, `f-h`, `a-a` and `b-b`.
#[derive(Clone, Debug, Default)]
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

    /// Returns `true` if the character `c` is contained matches any contained range.
    #[inline(always)]
    fn contains(&self, c: char) -> bool {
        // TODO: optimize this into a `starts_with` which gets a `&str`, this can be optimized to
        // byte matches
        match self {
            Self::Empty => false,
            Self::Single(range) => range.contains(c),
            Self::Multiple(ranges) => ranges.iter().any(|range| range.contains(c)),
        }
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

    /// Returns `true` if the character `c` is contained in the range.
    #[inline(always)]
    fn contains(&self, c: char) -> bool {
        self.start <= c && c <= self.end
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
                MatchStrategy::Wildmatch(_) => "Wildmatch",
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
        assert_pattern!(r"f\{\}o", "f{}o");
        assert_pattern!(r"f\}o", "f}o");
        assert_pattern!(r"f\{o", "f{o");
        assert_pattern!(r"f\{b,a,r\}o", "f{b,a,r}o");
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
        assert_strategy!(r"f\{b,a,r\}o", Literal);
        assert_strategy!(r"f\\o", Literal);
        assert_strategy!("fඞo", Literal);
    }

    #[test]
    fn test_prefix() {
        assert_pattern!("foo*", "foo___");
        assert_pattern!("foo*", "foo");
        assert_pattern!("foo**", "foo");
        assert_pattern!("foo*?*", NOT "foo");
        assert_pattern!("foo*?*", "foo_");
        assert_pattern!("foo*?*?", "foo_____");
        assert_pattern!("foo*?*?", NOT "foo");
        assert_pattern!("foo*?*?", "foo__");
        assert_pattern!("foo*?*?", "foo_____");
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
        assert_strategy!("foo**", Prefix);
        assert_strategy!("foo?*", Wildmatch);
    }

    #[test]
    fn test_suffix() {
        assert_pattern!("*foo", "___foo");
        assert_pattern!("*foo", "foo");
        assert_pattern!("**foo", "foo");
        assert_pattern!("*?*foo", NOT "foo");
        assert_pattern!("*?*foo", "_foo");
        assert_pattern!("*?*foo", "_____foo");
        assert_pattern!("?*?*foo", NOT "foo");
        assert_pattern!("?*?*foo", NOT "_foo");
        assert_pattern!("?*?*foo", "__foo");
        assert_pattern!("?*?*foo", "_____foo");
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
        assert_strategy!("**foo", Suffix);
        assert_strategy!("*?foo", Wildmatch);
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
        assert_strategy!("**foo**", Contains);
        assert_strategy!("*?foo*", Wildmatch);
        assert_strategy!("*foo?*", Wildmatch);
        assert_strategy!("*foo*?", Wildmatch);
        assert_strategy!("?*foo*", Wildmatch);
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
        assert_strategy!("{*}", Static);
        assert_strategy!("{*,}", Static);
        assert_strategy!("{foo,*}", Static);
        assert_strategy!("{foo,*}?{*,bar}", Wildmatch);
        assert_strategy!("{*,}?{*,}", Wildmatch);
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
        assert_pattern!(
            "??????????????????????????????????????????????????",
            "?????????????????????????????????????????????????!"
        );
        assert_pattern!("foo?bar", "foo?bar");
        assert_pattern!("foo?bar", "foo!bar");
        assert_pattern!("a??a", "aඞඞa");

        // No special slash handling
        assert_pattern!("?", "/");
    }

    #[test]
    fn test_any_wildcard() {
        assert_pattern!("??*", NOT "");
        assert_pattern!("??*", NOT "a");
        assert_pattern!("??*", "ab");
        assert_pattern!("??*", "abc");
        assert_pattern!("??*", "abcde");

        assert_pattern!("*??", NOT "");
        assert_pattern!("*??", NOT "a");
        assert_pattern!("*??", "ab");
        assert_pattern!("*??", "abc");
        assert_pattern!("*??", "abcde");

        assert_pattern!("*??*", NOT "");
        assert_pattern!("*??*", NOT "a");
        assert_pattern!("*??*", "ab");
        assert_pattern!("*??*", "abc");
        assert_pattern!("*??*", "abcde");

        assert_pattern!("*?*?*", NOT "");
        assert_pattern!("*?*?*", NOT "a");
        assert_pattern!("*?*?*", "ab");
        assert_pattern!("*?*?*", "abc");
        assert_pattern!("*?*?*", "abcde");
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
        assert_pattern!("[]", NOT "");
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
        assert_pattern!("[ǧ]", "Ǧ", i);
        assert_pattern!("[Ǧ]", "ǧ", i);
    }

    #[test]
    fn test_classes_negated() {
        assert_pattern!("[!]", NOT "");
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
    fn test_alternates() {
        assert_pattern!("{}foo{}", "foo");
        assert_pattern!("foo{}bar", "foobar");
        assert_pattern!("foo{}{}bar", "foobar");
        assert_pattern!("{foo}", "foo");
        assert_pattern!("{foo}", NOT "fOo");
        assert_pattern!("{foo}", NOT "bar");
        assert_pattern!("{foo,bar}", "foo");
        assert_pattern!("{foo,bar}", "bar");
        assert_pattern!("{foo,bar}", NOT "Foo");
        assert_pattern!("{foo,bar}", NOT "fOo");
        assert_pattern!("{foo,bar}", NOT "BAR");
        assert_pattern!("{foo,bar}", NOT "fooo");
        assert_pattern!("{foo,bar}", NOT "baar");
        assert_pattern!("{foo,bar,}baz", "foobaz");
        assert_pattern!("{foo,bar,}baz", "barbaz");
        assert_pattern!("{foo,bar,}baz", "baz");
        assert_pattern!("{[fb][oa][or]}", "foo");
        assert_pattern!("{[fb][oa][or]}", "bar");
        assert_pattern!("{[fb][oa][or],baz}", "foo");
        assert_pattern!("{[fb][oa][or],baz}", "bar");
        assert_pattern!("{[fb][oa][or],baz}", "baz");
        assert_pattern!("{baz,[fb][oa][or]}", "baz");
        assert_pattern!("{baz,[fb][oa][or]}", "foo");
        assert_pattern!("{baz,[fb][oa][or]}", "bar");
        assert_pattern!("{baz,[fb][oa][or]}", "bor");
        assert_pattern!("{baz,[fb][oa][or]}", NOT "barr");
        assert_pattern!("{baz,[fb][oa][or]}", NOT "boz");
        assert_pattern!("{baz,[fb][oa][or]}", NOT "fbar");
        assert_pattern!("{baz,[fb][oa][or]}", NOT "bAr");
        assert_pattern!("{baz,[fb][oa][or]}", NOT "Foo");
        assert_pattern!("{baz,[fb][oa][or]}", NOT "Bar");
        assert_pattern!("{baz,b[aA]r}", NOT "bAz");
        assert_pattern!("{baz,b[!aA]r}", NOT "bar");
        assert_pattern!("{baz,b[!aA]r}", "bXr");
        assert_pattern!("{[a-z],[0-9]}", "a");
        assert_pattern!("{[a-z],[0-9]}", "3");
        assert_pattern!("a{[a-z],[0-9]}a", "a3a");
        assert_pattern!("a{[a-z],[0-9]}a", "aba");
        assert_pattern!("a{[a-z],[0-9]}a", NOT "aAa");
        assert_pattern!("a{[a-z],?}a", "aXa");
        assert_pattern!(r"a{[a-z],\?}a", "a?a");
        assert_pattern!(r"a{[a-z],\?}a", NOT "aXa");
        assert_pattern!(r"a{[a-z],\?}a", NOT r"a\a");
        assert_pattern!(r"a{\[\],\{\}}a", "a[]a");
        assert_pattern!(r"a{\[\],\{\}}a", "a{}a");
        assert_pattern!(r"a{\[\],\{\}}a", NOT "a[}a");
        assert_pattern!(r"a{\[\],\{\}}a", NOT "a{]a");
        assert_pattern!(r"a{\[\],\{\}}a", NOT "a[a");
        assert_pattern!(r"a{\[\],\{\}}a", NOT "a]a");
        assert_pattern!(r"a{\[\],\{\}}a", NOT "a{a");
        assert_pattern!(r"a{\[\],\{\}}a", NOT "a}a");
        assert_pattern!("foo/{*.js,*.html}", "foo/.js");
        assert_pattern!("foo/{*.js,*.html}", "foo/.html");
        assert_pattern!("foo/{*.js,*.html}", "foo/bar.js");
        assert_pattern!("foo/{*.js,*.html}", "foo/bar.html");
        assert_pattern!("foo/{*.js,*.html}", NOT "foo/bar.png");
        assert_pattern!("foo/{*.js,*.html}", NOT "bar/bar.js");
        assert_pattern!("{foo,abc}{def,bar}", "foodef");
        assert_pattern!("{foo,abc}{def,bar}", "foobar");
        assert_pattern!("{foo,abc}{def,bar}", "abcdef");
        assert_pattern!("{foo,abc}{def,bar}", "abcbar");
        assert_pattern!("{foo,abc}{def,bar}", NOT "foofoo");
        assert_pattern!("{foo,abc}{def,bar}", NOT "fooabc");
        assert_pattern!("{foo,abc}{def,bar}", NOT "defdef");
        assert_pattern!("{foo,abc}{def,bar}", NOT "defabc");
    }

    #[test]
    fn test_alternate_strategy() {
        // Empty alternates can be simplified.
        assert_strategy!("{}foo{}", Literal);
        assert_strategy!("foo{}bar", Literal);
        assert_strategy!("foo{}{}{}bar", Literal);
    }

    #[test]
    fn test_alternates_case_insensitive() {
        assert_pattern!("{foo}", "foo", i);
        assert_pattern!("{foo}", "fOo", i);
        assert_pattern!("{foo}", NOT "bar", i);
        assert_pattern!("{foo,bar}", "foo", i);
        assert_pattern!("{foo,bar}", "bar", i);
        assert_pattern!("{foo,bar}", "Foo", i);
        assert_pattern!("{foo,bar}", "fOo", i);
        assert_pattern!("{foo,bar}", "BAR", i);
        assert_pattern!("{foo,bar}", NOT "bao", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "foo", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "fOo", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "f1o", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "foO", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "FOO", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "bar", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "bAr", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "baR", i);
        assert_pattern!("{f[o0-9]o,b[a]r}", "BAR", i);
        assert_pattern!("{f[o0-9]o,b[!a]r}", "foo", i);
        assert_pattern!("{f[o0-9]o,b[!a]r}", "bXr", i);
        assert_pattern!("{f[o0-9]o,b[!a]r}", NOT "bar", i);
        assert_pattern!("{f[o0-9]o,b[!a]r}", NOT "bAr", i);
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
        assert_pattern!("foo*fooFOO*fOobar", "fooFoofooXfoofooBAR", i);
        assert_pattern!("[0-9]*a", "0aaaaaaaaa", i);
        assert_pattern!("[0-9]*Bar[x]", "0foobarx", i);

        assert_pattern!(
            r"/api/0/organizations/\{organization_slug\}/event*",
            "/api/0/organizations/{organization_slug}/event/foobar"
        );
        assert_pattern!(
            r"/api/0/organizations/\{organization_slug\}/event*",
            NOT r"/api/0/organizations/\{organization_slug\}/event/foobar"
        );

        assert_pattern!(
            "*b??{foo,bar,baz,with}*cr{x,y,z,?}zyl[a-z]ng{suffix,pr?*ix}*it?**?uallymatches",
            "foobarwithacrazylongprefixandanditactuallymatches"
        );
        assert_pattern!(
            "*b??{foo,bar,baz,with}*cr{x,y,z,?}zyl[a-z]ng{suffix,pr?*ix}*it?**?uallymatches",
            "FOOBARWITHACRAZYLONGPREFIXANDANDITACTUALLYMATCHES",
            i
        );
    }

    /// Tests collected by [Kirk J Krauss].
    ///
    /// Kirk J Krauss: http://developforperformance.com/MatchingWildcards_AnImprovedAlgorithmForBigData.html.
    #[test]
    fn test_kirk_j_krauss() {
        // Case with first wildcard after total match.
        assert_pattern!("Hi*", "Hi");

        // Case with mismatch after '*'
        assert_pattern!("ab*d", NOT "abc");

        // Cases with repeating character sequences.
        assert_pattern!("*ccd", "abcccd");
        assert_pattern!("*issip*ss*", "mississipissippi");
        assert_pattern!("xxxx*zzy*fffff", NOT "xxxx*zzzzzzzzy*f");
        assert_pattern!("xxx*zzy*f", "xxxx*zzzzzzzzy*f");
        assert_pattern!("xxxx*zzy*fffff", NOT "xxxxzzzzzzzzyf");
        assert_pattern!("xxxx*zzy*f", "xxxxzzzzzzzzyf");
        assert_pattern!("xy*z*xyz", "xyxyxyzyxyz");
        assert_pattern!("*sip*", "mississippi");
        assert_pattern!("xy*xyz", "xyxyxyxyz");
        assert_pattern!("mi*sip*", "mississippi");
        assert_pattern!("*abac*", "ababac");
        assert_pattern!("*abac*", "ababac");
        assert_pattern!("a*zz*", "aaazz");
        assert_pattern!("*12*23", NOT "a12b12");
        assert_pattern!("a12b", NOT "a12b12");
        assert_pattern!("*12*12*", "a12b12");

        // From DDJ reader Andy Belf
        assert_pattern!("*a?b", "caaab");

        // Additional cases where the '*' char appears in the tame string.
        assert_pattern!("*", "*");
        assert_pattern!("a*b", "a*abab");
        assert_pattern!("a*", "a*r");
        assert_pattern!("a*aar", NOT "a*ar");

        // More double wildcard scenarios.
        assert_pattern!("XY*Z*XYz", "XYXYXYZYXYz");
        assert_pattern!("*SIP*", "missisSIPpi");
        assert_pattern!("*issip*PI", "mississipPI");
        assert_pattern!("xy*xyz", "xyxyxyxyz");
        assert_pattern!("mi*sip*", "miSsissippi");
        assert_pattern!("mi*Sip*", NOT "miSsissippi");
        assert_pattern!("*Abac*", "abAbac");
        assert_pattern!("*Abac*", "abAbac");
        assert_pattern!("a*zz*", "aAazz");
        assert_pattern!("*12*23", NOT "A12b12");
        assert_pattern!("*12*12*", "a12B12");
        assert_pattern!("*oWn*", "oWn");

        // Completely tame (no wildcards) cases.
        assert_pattern!("bLah", "bLah");
        assert_pattern!("bLaH", NOT "bLah");

        // Simple mixed wildcard tests suggested by Marlin Deckert.
        assert_pattern!("*?", "a");
        assert_pattern!("*?", "ab");
        assert_pattern!("*?", "abc");

        // More mixed wildcard tests including coverage for false positives.
        assert_pattern!("??", NOT "a");
        assert_pattern!("?*?", "ab");
        assert_pattern!("*?*?*", "ab");
        assert_pattern!("?**?*?", "abc");
        assert_pattern!("?**?*&?", NOT "abc");
        assert_pattern!("?b*??", "abcd");
        assert_pattern!("?a*??", NOT "abcd");
        assert_pattern!("?**?c?", "abcd");
        assert_pattern!("?**?d?", NOT "abcd");
        assert_pattern!("?*b*?*d*?", "abcde");

        // Single-character-match cases.
        assert_pattern!("bL?h", "bLah");
        assert_pattern!("bLa?", NOT "bLaaa");
        assert_pattern!("bLa?", "bLah");
        assert_pattern!("?Lah", NOT "bLaH");
        assert_pattern!("?LaH", "bLaH");

        // Many-wildcard scenarios.
        assert_pattern!(
            "a*a*a*a*a*a*aa*aaa*a*a*b",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"
        );
        assert_pattern!(
            "*a*b*ba*ca*a*aa*aaa*fa*ga*b*",
            "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab"
        );
        assert_pattern!(
            "*a*b*ba*ca*a*x*aaa*fa*ga*b*",
            NOT "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab"
        );
        assert_pattern!(
            "*a*b*ba*ca*aaaa*fa*ga*gggg*b*",
            NOT "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab"
        );
        assert_pattern!(
            "*a*b*ba*ca*aaaa*fa*ga*ggg*b*",
            "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab"
        );
        assert_pattern!("*aabbaa*a*", "aaabbaabbaab");
        assert_pattern!(
            "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*",
            "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*"
        );
        assert_pattern!("*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", "aaaaaaaaaaaaaaaaa");
        assert_pattern!(
            "*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*",
            NOT "aaaaaaaaaaaaaaaa"
        );
        assert_pattern!(
            "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*",
            NOT "abc*abcd*abcde*abcdef*abcdefg*abcdefgh*abcdefghi*abcdefghij*abcdefghijk*abcdefghijkl*abcdefghijklm*abcdefghijklmn"
        );
        assert_pattern!(
            "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*",
            "abc*abcd*abcde*abcdef*abcdefg*abcdefgh*abcdefghi*abcdefghij*abcdefghijk*abcdefghijkl*abcdefghijklm*abcdefghijklmn"
        );
        assert_pattern!("abc*abc*abc*abc*abc", NOT "abc*abcd*abcd*abc*abcd");
        assert_pattern!(
            "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abcd",
            "abc*abcd*abcd*abc*abcd*abcd*abc*abcd*abc*abc*abcd"
        );
        assert_pattern!("********a********b********c********", "abc");
        assert_pattern!("abc", NOT "********a********b********c********");
        assert_pattern!("********a********b********b********", NOT "abc");
        assert_pattern!("***a*b*c***", "*abc*");

        // A case-insensitive algorithm test.
        assert_pattern!("*issip*PI", "mississippi", i);

        // Tests suggested by other DDJ readers
        assert_pattern!("?", NOT "");
        assert_pattern!("*?", NOT "");
        // assert_pattern!("", ""); - Removed, relay-pattern behaves differently for empty strings.
        assert_pattern!("", NOT "a");

        // Tame tests:
        assert_pattern!("abd", NOT "abc");

        // Cases with repeating character sequences.
        assert_pattern!("abcccd", "abcccd");
        assert_pattern!("mississipissippi", "mississipissippi");
        assert_pattern!("xxxxzzzzzzzzyfffff", NOT "xxxxzzzzzzzzyf");
        assert_pattern!("xxxxzzzzzzzzyf", "xxxxzzzzzzzzyf");
        assert_pattern!("xxxxzzy.fffff", NOT "xxxxzzzzzzzzyf");
        assert_pattern!("xxxxzzzzzzzzyf", "xxxxzzzzzzzzyf");
        assert_pattern!("xyxyxyzyxyz", "xyxyxyzyxyz");
        assert_pattern!("mississippi", "mississippi");
        assert_pattern!("xyxyxyxyz", "xyxyxyxyz");
        assert_pattern!("m ississippi", "m ississippi");
        assert_pattern!("ababac?", NOT "ababac");
        assert_pattern!("ababac", NOT "dababac");
        assert_pattern!("aaazz", "aaazz");
        assert_pattern!("1212", NOT "a12b12");
        assert_pattern!("a12b", NOT "a12b12");
        assert_pattern!("a12b12", "a12b12");

        // A mix of cases
        assert_pattern!("n", "n");
        assert_pattern!("aabab", "aabab");
        assert_pattern!("ar", "ar");
        assert_pattern!("aaar", NOT "aar");
        assert_pattern!("XYXYXYZYXYz", "XYXYXYZYXYz");
        assert_pattern!("missisSIPpi", "missisSIPpi");
        assert_pattern!("mississipPI", "mississipPI");
        assert_pattern!("xyxyxyxyz", "xyxyxyxyz");
        assert_pattern!("miSsissippi", "miSsissippi");
        assert_pattern!("miSsisSippi", NOT "miSsissippi");
        assert_pattern!("abAbac", "abAbac");
        assert_pattern!("abAbac", "abAbac");
        assert_pattern!("aAazz", "aAazz");
        assert_pattern!("A12b123", NOT "A12b12");
        assert_pattern!("a12B12", "a12B12");
        assert_pattern!("oWn", "oWn");
        assert_pattern!("bLah", "bLah");
        assert_pattern!("bLaH", NOT "bLah");

        // Single '?' cases.
        assert_pattern!("a", "a");
        assert_pattern!("a?", "ab");
        assert_pattern!("ab?", "abc");

        // Mixed '?' cases.
        assert_pattern!("??", NOT "a");
        assert_pattern!("??", "ab");
        assert_pattern!("???", "abc");
        assert_pattern!("????", "abcd");
        assert_pattern!("????", NOT "abc");
        assert_pattern!("?b??", "abcd");
        assert_pattern!("?a??", NOT "abcd");
        assert_pattern!("??c?", "abcd");
        assert_pattern!("??d?", NOT "abcd");
        assert_pattern!("?b?d*?", "abcde");

        // Longer string scenarios.
        assert_pattern!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"
        );
        assert_pattern!(
            "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
            "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab"
        );
        assert_pattern!(
            "abababababababababababababababababababaacacacacacacacadaeafagahaiajaxalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
            NOT "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab"
        );
        assert_pattern!(
            "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaggggagaaaaaaaab",
            NOT "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab"
        );
        assert_pattern!(
            "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
            "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab"
        );
        assert_pattern!("aaabbaabbaab", "aaabbaabbaab");
        assert_pattern!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_pattern!("aaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaa");
        assert_pattern!("aaaaaaaaaaaaaaaaa", NOT "aaaaaaaaaaaaaaaa");
        assert_pattern!(
            "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc",
            NOT "abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn"
        );
        assert_pattern!(
            "abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn",
            "abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn"
        );
        assert_pattern!("abcabc?abcabcabc", NOT "abcabcdabcdabcabcd");
        assert_pattern!(
            "abcabc?abc?abcabc?abc?abc?bc?abc?bc?bcd",
            "abcabcdabcdabcabcdabcdabcabcdabcabcabcd"
        );
        assert_pattern!("?abc?", "?abc?");
    }

    #[test]
    fn test_builder_complexity() {
        assert!(Pattern::builder("{foo,bar}")
            .max_complexity(1)
            .build()
            .is_err());
        assert!(Pattern::builder("{foo,bar}")
            .max_complexity(2)
            .build()
            .is_ok());
        assert!(Pattern::builder("{foo,bar}/{*.html,*.js,*.css}")
            .max_complexity(5)
            .build()
            .is_err());
        assert!(Pattern::builder("{foo,bar}/{*.html,*.js,*.css}")
            .max_complexity(6)
            .build()
            .is_ok());
    }

    #[test]
    fn test_patterns() {
        let patterns = Patterns::builder()
            .add("foobaR")
            .unwrap()
            .add("a*")
            .unwrap()
            .add("*a")
            .unwrap()
            .add("[0-9]*baz")
            .unwrap()
            .take();

        assert!(patterns.is_match("foobaR"));
        assert!(patterns.is_match("abc"));
        assert!(patterns.is_match("cba"));
        assert!(patterns.is_match("3baz"));
        assert!(patterns.is_match("123456789baz"));
        assert!(!patterns.is_match("foobar"));
        assert!(!patterns.is_match("FOOBAR"));
    }

    #[test]
    fn test_patterns_case_insensitive() {
        let patterns = Patterns::builder()
            .case_insensitive(true)
            .add("fOObar")
            .unwrap()
            .add("a*")
            .unwrap()
            .add("*a")
            .unwrap()
            .add("[0-9]*baz")
            .unwrap()
            .take();

        assert!(patterns.is_match("FooBar"));
        assert!(patterns.is_match("abC"));
        assert!(patterns.is_match("cbA"));
        assert!(patterns.is_match("3BAZ"));
        assert!(patterns.is_match("123456789baz"));
        assert!(!patterns.is_match("b"));
    }

    #[test]
    fn test_patterns_take_clears_builder() {
        let mut builder = Patterns::builder().add("foo").unwrap();

        let patterns = builder.take();
        assert!(patterns.is_match("foo"));
        assert!(!patterns.is_match("bar"));

        builder.add("bar").unwrap();
        let patterns = builder.build();
        assert!(!patterns.is_match("foo"));
        assert!(patterns.is_match("bar"));
    }
}
