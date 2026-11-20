use std::ops::Range;
use std::sync::OnceLock;

use regex::bytes::{Captures, Regex, RegexBuilder};

type MatchValidator = fn(&[u8]) -> bool;

/// A compiled regular expression for PII scrubbing.
///
/// All matching runs on bytes, in one of two modes:
///
/// - Unicode mode for haystacks that are valid UTF-8, such as event strings and text decoded from
///   UTF-16 attachments. Patterns that could match invalid UTF-8 are rejected, so matches behave
///   like matches of [`regex::Regex`].
/// - ASCII-compatible mode for raw attachment bytes, which do not have to be valid UTF-8. This
///   variant is compiled on first use.
#[derive(Debug, Clone)]
pub struct PiiRegex {
    unicode: Regex,
    ascii: OnceLock<Option<Regex>>,
    case_insensitive: bool,
    size_limit: Option<usize>,
}

impl PiiRegex {
    /// Compiles a pattern with default options.
    pub(crate) fn new(pattern: &str) -> Result<Self, regex::Error> {
        Self::build(pattern, false, None)
    }

    /// Compiles a pattern with the given case sensitivity and compiled size limit.
    pub(crate) fn build(
        pattern: &str,
        case_insensitive: bool,
        size_limit: Option<usize>,
    ) -> Result<Self, regex::Error> {
        let mut builder = RegexBuilder::new(pattern);
        builder.case_insensitive(case_insensitive);
        if let Some(size_limit) = size_limit {
            builder.size_limit(size_limit);
        }
        let unicode = builder.build()?;
        ensure_utf8(pattern, case_insensitive)?;

        Ok(Self {
            unicode,
            ascii: OnceLock::new(),
            case_insensitive,
            size_limit,
        })
    }

    /// Returns the original pattern string.
    pub(crate) fn as_str(&self) -> &str {
        self.unicode.as_str()
    }

    /// Returns `true` if the regex matches anywhere in the haystack.
    fn is_match(&self, haystack: &str) -> bool {
        self.unicode.is_match(haystack.as_bytes())
    }

    /// Returns the capture groups of all non-overlapping matches in the haystack.
    ///
    /// The range of every group can be used to slice `haystack`.
    fn captures_iter<'h>(
        &self,
        haystack: &'h str,
    ) -> impl Iterator<Item = Captures<'h>> + use<'_, 'h> {
        self.unicode
            .captures_iter(haystack.as_bytes())
            .filter(|captures| {
                captures
                    .iter()
                    .flatten()
                    .all(|m| is_char_range(haystack, &m.range()))
            })
    }

    /// Returns the ASCII-compatible regex for haystacks that may not be valid UTF-8.
    ///
    /// In this mode, `.` matches any byte, and `\w`, `\d`, `\s`, `\b` and case-insensitive
    /// matching only consider ASCII. Returns `None` if the pattern cannot be compiled in this
    /// mode, for example if it uses a Unicode class such as `\p{L}`.
    fn ascii(&self) -> Option<&Regex> {
        self.ascii
            .get_or_init(|| {
                let mut builder = RegexBuilder::new(self.as_str());
                builder
                    // https://github.com/rust-lang/regex/issues/697
                    .unicode(false)
                    .multi_line(false)
                    .dot_matches_new_line(true)
                    .case_insensitive(self.case_insensitive);
                if let Some(size_limit) = self.size_limit {
                    builder.size_limit(size_limit);
                }

                match builder.build() {
                    Ok(regex) => Some(regex),
                    Err(e) => {
                        // If we see this error in production, it means we need to add more regex
                        // validation to `validate_pii_config` (which is called sentry-side).
                        relay_log::error!(
                            error = &e as &dyn std::error::Error,
                            pattern = self.as_str(),
                            "Regex failed to compile in non-unicode mode",
                        );
                        None
                    }
                }
            })
            .as_ref()
    }
}

/// Rejects patterns that could match invalid UTF-8, like [`regex::Regex`] does.
///
/// A bytes regex accepts patterns such as `(?-u:\xFF)`, whose matches can split a UTF-8 encoded
/// character.
fn ensure_utf8(pattern: &str, case_insensitive: bool) -> Result<(), regex::Error> {
    let parsed = regex_syntax::ParserBuilder::new()
        .case_insensitive(case_insensitive)
        .utf8(true)
        .build()
        .parse(pattern);

    match parsed {
        Ok(_) => Ok(()),
        // `regex::Error` cannot be constructed directly. Compile the pattern as `regex::Regex`
        // to get the error it returns for this pattern.
        Err(_) => regex::RegexBuilder::new(pattern)
            .case_insensitive(case_insensitive)
            .build()
            .map(|_| ()),
    }
}

/// Returns `true` if both ends of the range are on character boundaries of the haystack.
///
/// A bytes regex can report empty matches that split a UTF-8 encoded character, which
/// [`regex::Regex`] never reports.
fn is_char_range(haystack: &str, range: &Range<usize>) -> bool {
    haystack.is_char_boundary(range.start) && haystack.is_char_boundary(range.end)
}

/// A scrubbing regex with an optional validator for each match.
#[derive(Clone, Copy, Debug)]
pub struct ScrubbingRegex<T = PiiRegex> {
    regex: T,
    validator: Option<MatchValidator>,
}

pub type ScrubbingRegexRef<'pattern, T = PiiRegex> = ScrubbingRegex<&'pattern T>;

impl<T> ScrubbingRegex<T> {
    /// Creates a new [`ScrubbingRegex`] used for PII scrubbing.
    pub fn new(regex: T) -> Self {
        Self {
            regex,
            validator: None,
        }
    }

    /// Attaches a validator for regex captures.
    ///
    /// This allows a stronger validation for more lenient regex matches. Note however that the
    /// filter runs after the regex has found its matches. This means the regex will not consider an
    /// overlapping match which would pass the filter.
    ///
    /// For example the [`ScrubbingRegex`] `..` with validator `x != 'ab'` on the input `abc`
    /// does _not_ match on `bc`.
    pub fn with_validator(mut self, validator: MatchValidator) -> Self {
        self.validator = Some(validator);
        self
    }

    /// Returns a borrowed [`ScrubbingRegex`].
    pub fn as_ref(&self) -> ScrubbingRegex<&T> {
        ScrubbingRegex {
            regex: &self.regex,
            validator: self.validator,
        }
    }
}

impl<'pattern> ScrubbingRegexRef<'pattern> {
    /// Returns the ASCII-compatible variant for binary attachments, keeping its match validator.
    ///
    /// Returns `None` if the pattern cannot be compiled in this mode, see [`PiiRegex`].
    pub fn ascii(self) -> Option<ScrubbingRegexRef<'pattern, Regex>> {
        Some(ScrubbingRegex {
            regex: self.regex.ascii()?,
            validator: self.validator,
        })
    }

    /// Returns `true` if the [`ScrubbingRegex`] matches the `haystack`.
    pub fn is_match(&self, haystack: &str) -> bool {
        match self.validator {
            Some(_) => self.captures_iter(haystack).next().is_some(),
            // This could use `self.captures_iter` as well, though using `is_match` directly is a
            // bit more performant.
            None => self.regex.is_match(haystack),
        }
    }

    /// Returns the capture groups of all matches in the `haystack` that pass the validator.
    ///
    /// The range of every group can be used to slice `haystack`.
    pub fn captures_iter<'haystack>(
        &self,
        haystack: &'haystack str,
    ) -> impl Iterator<Item = Captures<'haystack>> + use<'pattern, 'haystack> {
        let validator = self.validator;
        self.regex
            .captures_iter(haystack)
            .filter(move |captures| validator.is_none_or(|validate| validate(&captures[0])))
    }
}

impl<'pattern> ScrubbingRegexRef<'pattern, Regex> {
    pub fn captures_iter<'haystack>(
        &self,
        haystack: &'haystack [u8],
    ) -> impl Iterator<Item = Captures<'haystack>> + use<'pattern, 'haystack> {
        let validator = self.validator;
        self.regex
            .captures_iter(haystack)
            .filter(move |captures| validator.is_none_or(|validate| validate(&captures[0])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the example in [`ScrubbingRegex::with_validator`].
    #[test]
    fn test_validator_skips_overlapping_match() {
        let regex = ScrubbingRegex::new(PiiRegex::new("..").unwrap())
            .with_validator(|matched| matched != b"ab");
        let regex = regex.as_ref();

        // regex matches `ab` and validator rejects it -> no match
        assert!(!regex.is_match("abc"));
        assert!(regex.captures_iter("abc").next().is_none());

        // `bc` matches just fine.
        assert!(regex.is_match("bc"));
    }

    #[test]
    fn test_custom_validator() {
        let regex = ScrubbingRegex::new(PiiRegex::new(r"(.).").unwrap())
            .with_validator(|matched| matched == b"bc");
        let regex = regex.as_ref();

        assert!(!regex.is_match("ab"));
        assert!(regex.is_match("éabc"));
        assert_eq!(&regex.captures_iter("éabc").next().unwrap()[1], b"b");

        let bytes_regex = regex.ascii().unwrap();
        assert_eq!(
            &bytes_regex.captures_iter(b"\xffabc").next().unwrap()[1],
            b"b"
        );
    }

    #[test]
    fn test_rejects_patterns_matching_invalid_utf8() {
        for pattern in [r"(?-u:\xFF)", r"(?-u:.)", r"(?-u:\W)"] {
            assert!(regex::Regex::new(pattern).is_err());
            assert!(PiiRegex::new(pattern).is_err(), "{pattern}");
        }
    }

    #[test]
    fn test_unicode_semantics() {
        let regex = PiiRegex::new(r"\w+").unwrap();
        let matches: Vec<_> = regex
            .captures_iter("Müller ſecret")
            .map(|c| c[0].to_vec())
            .collect();
        assert_eq!(matches, vec!["Müller".as_bytes(), "ſecret".as_bytes()]);

        let regex = PiiRegex::build("secret", true, None).unwrap();
        assert!(regex.is_match("ſecret"));
    }

    #[test]
    fn test_skips_empty_matches_inside_chars() {
        let regex = PiiRegex::new("x*").unwrap();
        let matches: Vec<_> = regex
            .captures_iter("💩")
            .map(|c| c.get(0).unwrap().range())
            .collect();
        assert_eq!(matches, vec![0..0, 4..4]);
    }

    #[test]
    fn test_ascii_keeps_case_insensitive() {
        let regex = PiiRegex::build("password", true, None).unwrap();
        assert!(regex.ascii().unwrap().is_match(b"PASSWORD"));
    }

    #[test]
    fn test_ascii_unicode_class() {
        let regex = PiiRegex::new(r"\p{L}+").unwrap();
        assert!(regex.is_match("Müller"));
        assert!(regex.ascii().is_none());
    }
}
