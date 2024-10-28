use core::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;

use crate::{Error, Pattern, Patterns, PatternsBuilderConfigured};

/// Compile time configuration for a [`TypedPattern`].
pub trait PatternConfig {
    /// Configures the pattern to match case insensitive.
    const CASE_INSENSITIVE: bool = false;
    /// Configures the maximum allowed complexity of the pattern.
    const MAX_COMPLEXITY: u64 = u64::MAX;
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
#[derive(Debug)]
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
            .max_complexity(C::MAX_COMPLEXITY)
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

impl<C> From<TypedPattern<C>> for Pattern {
    fn from(value: TypedPattern<C>) -> Self {
        value.pattern
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

/// [`Patterns`] with a compile time configured [`PatternConfig`].
pub struct TypedPatterns<C = DefaultPatternConfig> {
    patterns: Patterns,
    raw: Vec<String>,
    _phantom: PhantomData<C>,
}

impl<C: PatternConfig> TypedPatterns<C> {
    pub fn builder() -> TypedPatternsBuilder<C> {
        let builder = Patterns::builder()
            .case_insensitive(C::CASE_INSENSITIVE)
            .patterns();

        TypedPatternsBuilder {
            builder,
            raw: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

impl<C: PatternConfig> Default for TypedPatterns<C> {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl<C> PartialEq for TypedPatterns<C> {
    fn eq(&self, other: &Self) -> bool {
        self.raw.eq(&other.raw)
    }
}

impl<C: PatternConfig> From<String> for TypedPatterns<C> {
    fn from(value: String) -> Self {
        [value].into_iter().collect()
    }
}

impl<C: PatternConfig> From<Vec<String>> for TypedPatterns<C> {
    fn from(value: Vec<String>) -> Self {
        value.into_iter().collect()
    }
}

impl<C: PatternConfig, const N: usize> From<[String; N]> for TypedPatterns<C> {
    fn from(value: [String; N]) -> Self {
        value.into_iter().collect()
    }
}

/// Creates [`Patterns`] from an iterator of strings.
///
/// Invalid patterns are ignored.
impl<C: PatternConfig> FromIterator<String> for TypedPatterns<C> {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        let mut builder = Self::builder();
        for pattern in iter.into_iter() {
            let _err = builder.add(pattern);
            #[cfg(debug_assertions)]
            _err.expect("all patterns should be valid patterns");
        }
        builder.build()
    }
}

impl<C> From<TypedPatterns<C>> for Patterns {
    fn from(value: TypedPatterns<C>) -> Self {
        value.patterns
    }
}

impl<C> AsRef<Patterns> for TypedPatterns<C> {
    fn as_ref(&self) -> &Patterns {
        &self.patterns
    }
}

impl<C> Deref for TypedPatterns<C> {
    type Target = Patterns;

    fn deref(&self) -> &Self::Target {
        &self.patterns
    }
}

impl<C> Clone for TypedPatterns<C> {
    fn clone(&self) -> Self {
        Self {
            patterns: self.patterns.clone(),
            raw: self.raw.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<C> fmt::Debug for TypedPatterns<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.fmt(f)
    }
}

/// Deserializes patterns from a sequence of strings.
///
/// Invalid patterns are ignored while deserializing.
#[cfg(feature = "serde")]
impl<'de, C: PatternConfig> serde::Deserialize<'de> for TypedPatterns<C> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor<C>(PhantomData<C>);

        impl<'a, C: PatternConfig> serde::de::Visitor<'a> for Visitor<C> {
            type Value = TypedPatterns<C>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence of patterns")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'a>,
            {
                let mut builder = TypedPatterns::<C>::builder();

                while let Some(item) = seq.next_element()? {
                    // Ignore invalid patterns as documented.
                    let _err = builder.add(item);
                    #[cfg(debug_assertions)]
                    _err.expect("all patterns should be valid patterns");
                }

                Ok(builder.build())
            }
        }

        deserializer.deserialize_seq(Visitor(PhantomData))
    }
}

#[cfg(feature = "serde")]
impl<C> serde::Serialize for TypedPatterns<C> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.raw.serialize(serializer)
    }
}

pub struct TypedPatternsBuilder<C> {
    builder: PatternsBuilderConfigured,
    raw: Vec<String>,
    _phantom: PhantomData<C>,
}

impl<C: PatternConfig> TypedPatternsBuilder<C> {
    /// Adds a pattern to the builder.
    pub fn add(&mut self, pattern: String) -> Result<&mut Self, Error> {
        self.builder.add(&pattern)?;
        self.raw.push(pattern);
        Ok(self)
    }

    /// Builds a [`TypedPatterns`] from the contained patterns.
    pub fn build(self) -> TypedPatterns<C> {
        TypedPatterns {
            patterns: self.builder.build(),
            raw: self.raw,
            _phantom: PhantomData,
        }
    }

    /// Builds a [`TypedPatterns`] from the contained patterns and clears the builder.
    pub fn take(&mut self) -> TypedPatterns<C> {
        TypedPatterns {
            patterns: self.builder.take(),
            raw: std::mem::take(&mut self.raw),
            _phantom: PhantomData,
        }
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
    fn test_deserialize_complexity() {
        struct Test;
        impl PatternConfig for Test {
            const MAX_COMPLEXITY: u64 = 2;
        }
        let r: Result<TypedPattern<Test>, _> = serde_json::from_str(r#""{foo,bar}""#);
        assert!(r.is_ok());
        let r: Result<TypedPattern<Test>, _> = serde_json::from_str(r#""{foo,bar,baz}""#);
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

    #[test]
    fn test_patterns_default() {
        let patterns: TypedPatterns = TypedPatterns::builder()
            .add("*[rt]x".to_owned())
            .unwrap()
            .add("foobar".to_owned())
            .unwrap()
            .take();
        assert!(patterns.is_match("f/o_rx"));
        assert!(patterns.is_match("foobar"));
        assert!(!patterns.is_match("Foobar"));
    }

    #[test]
    fn test_patterns_case_insensitive() {
        let patterns: TypedPatterns<CaseInsensitive> = TypedPatterns::builder()
            .add("*[rt]x".to_owned())
            .unwrap()
            .add("foobar".to_owned())
            .unwrap()
            .take();
        assert!(patterns.is_match("f/o_rx"));
        assert!(patterns.is_match("f/o_Rx"));
        assert!(patterns.is_match("foobar"));
        assert!(patterns.is_match("Foobar"));
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_patterns_deserialize() {
        let pattern: TypedPatterns<CaseInsensitive> =
            serde_json::from_str(r#"["*[rt]x","foobar"]"#).unwrap();
        assert!(pattern.is_match("foobar_rx"));
        assert!(pattern.is_match("FOOBAR"));
    }

    #[test]
    #[cfg(all(feature = "serde", not(debug_assertions)))]
    fn test_patterns_deserialize_err() {
        let r: TypedPatterns<CaseInsensitive> =
            serde_json::from_str(r#"["[invalid","foobar"]"#).unwrap();
        assert!(r.is_match("foobar"));
        assert!(r.is_match("FOOBAR"));

        // The invalid element is dropped.
        assert_eq!(serde_json::to_string(&r).unwrap(), r#"["foobar"]"#);
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_patterns_serialize() {
        let pattern: TypedPatterns = TypedPatterns::builder()
            .add("*[rt]x".to_owned())
            .unwrap()
            .add("foobar".to_owned())
            .unwrap()
            .take();
        assert_eq!(
            serde_json::to_string(&pattern).unwrap(),
            r#"["*[rt]x","foobar"]"#
        );

        let pattern: TypedPatterns<CaseInsensitive> = TypedPatterns::builder()
            .add("*[rt]x".to_owned())
            .unwrap()
            .add("foobar".to_owned())
            .unwrap()
            .take();
        assert_eq!(
            serde_json::to_string(&pattern).unwrap(),
            r#"["*[rt]x","foobar"]"#
        );
    }
}
