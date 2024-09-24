use std::marker::PhantomData;
use std::ops::Deref;

use crate::{Error, Pattern};

/// Compile time configuration for a [`TypedPattern`].
pub trait PatternConfig {
    /// Configures the pattern to match case insensitive.
    const CASE_INSENSITIVE: bool = false;
}

/// The default pattern.
///
/// Equivalent to [`Pattern::new`].
pub struct DefaultPatternConfig;

impl PatternConfig for DefaultPatternConfig {}

/// The default pattern but with case insensitive matching.
///
/// See: [`crate::PatternBuilder::case_insensitive`].
pub struct CaseInsensitive;

impl PatternConfig for CaseInsensitive {
    const CASE_INSENSITIVE: bool = true;
}

/// A [`Pattern`] with compile time encoded [`PatternConfig`].
///
/// Encoding the pattern configuration allows context dependent serialization
/// and usage of patterns and ensures a consistent usage of configuration options
/// throught the code.
///
/// Often repeated configuration can be grouped into custom and importable configurations.
///
/// ```
/// struct MetricConfig;
///
/// impl relay_pattern::PatternConfig for MetricConfig {
///     const CASE_INSENSITIVE: bool = false;
///     // More configuration ...
/// }
///
/// type MetricPattern = relay_pattern::TypedPattern<MetricConfig>;
///
/// let pattern = MetricPattern::new("[cd]:foo/bar").unwrap();
/// assert!(pattern.is_match("c:foo/bar"));
/// ```
pub struct TypedPattern<C = DefaultPatternConfig> {
    pattern: Pattern,
    _phantom: PhantomData<C>,
}

impl<C: PatternConfig> TypedPattern<C> {
    /// Creates a new [`TypedPattern`] using the provided pattern and config `C`.
    ///
    /// ```
    /// use relay_pattern::{Pattern, TypedPattern, CaseInsensitive};
    ///
    /// let pattern = TypedPattern::<CaseInsensitive>::new("foo*").unwrap();
    /// assert!(pattern.is_match("FOOBAR"));
    ///
    /// // Equivalent to:
    /// let pattern = Pattern::builder("foo*").case_insensitive(true).build().unwrap();
    /// assert!(pattern.is_match("FOOBAR"));
    /// ```
    pub fn new(pattern: &str) -> Result<Self, Error> {
        Pattern::builder(pattern)
            .case_insensitive(C::CASE_INSENSITIVE)
            .build()
            .map(|pattern| Self {
                pattern,
                _phantom: PhantomData,
            })
    }
}

#[cfg(feature = "serde")]
impl<'de, C: PatternConfig> serde::Deserialize<'de> for TypedPattern<C> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let pattern = <std::borrow::Cow<'_, str>>::deserialize(deserializer)?;
        Self::new(&pattern).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "serde")]
impl<C> serde::Serialize for TypedPattern<C> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.pattern)
    }
}

impl<C> AsRef<Pattern> for TypedPattern<C> {
    fn as_ref(&self) -> &Pattern {
        &self.pattern
    }
}

impl<C> Deref for TypedPattern<C> {
    type Target = Pattern;

    fn deref(&self) -> &Self::Target {
        &self.pattern
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let pattern: TypedPattern = TypedPattern::new("*[rt]x").unwrap();
        assert!(pattern.is_match("f/o_rx"));
        assert!(pattern.is_match("f/o_tx"));
        assert!(pattern.is_match("F/o_tx"));
        // case sensitive
        assert!(!pattern.is_match("f/o_Tx"));
        assert!(!pattern.is_match("f/o_rX"));
    }

    #[test]
    fn test_case_insensitive() {
        let pattern: TypedPattern<CaseInsensitive> = TypedPattern::new("*[rt]x").unwrap();
        // case insensitive
        assert!(pattern.is_match("f/o_Tx"));
        assert!(pattern.is_match("f/o_rX"));
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_deserialize() {
        let pattern: TypedPattern<CaseInsensitive> = serde_json::from_str(r#""*[rt]x""#).unwrap();
        assert!(pattern.is_match("foobar_rx"));
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_deserialize_err() {
        let r: Result<TypedPattern<CaseInsensitive>, _> = serde_json::from_str(r#""[invalid""#);
        assert!(r.is_err());
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_serialize() {
        let pattern: TypedPattern = TypedPattern::new("*[rt]x").unwrap();
        assert_eq!(serde_json::to_string(&pattern).unwrap(), r#""*[rt]x""#);
        let pattern: TypedPattern<CaseInsensitive> = TypedPattern::new("*[rt]x").unwrap();
        assert_eq!(serde_json::to_string(&pattern).unwrap(), r#""*[rt]x""#);
    }
}
