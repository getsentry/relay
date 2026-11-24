use regex::bytes::{
    Captures as BytesCaptures, Regex as BytesRegex, RegexBuilder as BytesRegexBuilder,
};
use regex::{Captures, Regex};

type MatchValidator = fn(&[u8]) -> bool;

/// A scrubbing regex with an optional validator for each match.
#[derive(Clone, Copy, Debug)]
pub struct ScrubbingRegex<T = Regex> {
    regex: T,
    validator: Option<MatchValidator>,
}

pub type ScrubbingRegexRef<'pattern, T = Regex> = ScrubbingRegex<&'pattern T>;

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
    pub fn as_str(&self) -> &str {
        self.regex.as_str()
    }

    /// Compiles this pattern for binary attachments, keeping its match validator.
    pub fn to_bytes(self) -> Result<ScrubbingRegex<BytesRegex>, regex::Error> {
        let regex = BytesRegexBuilder::new(self.as_str())
            .unicode(false)
            .multi_line(false)
            .dot_matches_new_line(true)
            .build()?;

        Ok(ScrubbingRegex {
            regex,
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

    pub fn captures_iter<'haystack>(
        &self,
        haystack: &'haystack str,
    ) -> impl Iterator<Item = Captures<'haystack>> + use<'pattern, 'haystack> {
        let validator = self.validator;
        self.regex.captures_iter(haystack).filter(move |captures| {
            validator.is_none_or(|validate| validate(captures[0].as_bytes()))
        })
    }
}

impl ScrubbingRegex<BytesRegex> {
    pub fn captures_iter<'regex, 'haystack>(
        &'regex self,
        haystack: &'haystack [u8],
    ) -> impl Iterator<Item = BytesCaptures<'haystack>> + use<'regex, 'haystack> {
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
        let regex = ScrubbingRegex::new(Regex::new("..").unwrap())
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
        let regex = ScrubbingRegex::new(Regex::new(r"(.).").unwrap())
            .with_validator(|matched| matched == b"bc");
        let regex = regex.as_ref();

        assert!(!regex.is_match("ab"));
        assert!(regex.is_match("éabc"));
        assert_eq!(&regex.captures_iter("éabc").next().unwrap()[1], "b");

        let bytes_regex = regex.to_bytes().unwrap();
        assert_eq!(
            &bytes_regex.captures_iter(b"\xffabc").next().unwrap()[1],
            b"b"
        );
    }
}
