use std::hash::Hasher as _;

use hash32::{FnvHasher, Hasher as _};

#[doc(inline)]
pub use relay_base_schema::metrics::{
    CustomUnit, DurationUnit, FractionUnit, InformationUnit, MetricName, MetricNamespace,
    MetricResourceIdentifier, MetricType, MetricUnit, ParseMetricError, ParseMetricUnitError,
};
#[doc(inline)]
pub use relay_common::time::UnixTimestamp;
#[doc(inline)]
pub use unescaper::Error as UnescapeError;

use crate::FiniteF64;

/// Type used for Counter metric
pub type CounterType = FiniteF64;

/// Type of distribution entries
pub type DistributionType = FiniteF64;

/// Type used for set elements in Set metric
pub type SetType = u32;

/// Type used for Gauge entries
pub type GaugeType = FiniteF64;

/// Validates a tag key.
///
/// Tag keys currently only need to not contain ASCII control characters. This might change.
pub(crate) fn is_valid_tag_key(tag_key: &str) -> bool {
    // iterating over bytes produces better asm, and we're only checking for ascii chars
    for &byte in tag_key.as_bytes() {
        if (byte as char).is_ascii_control() {
            return false;
        }
    }
    true
}

/// Replaces restricted characters with escape sequences.
///
/// All other characters are replaced with the following rules:
///  - Tab is escaped as `\t`.
///  - Carriage return is escaped as `\r`.
///  - Line feed is escaped as `\n`.
///  - Backslash is escaped as `\\`.
///  - Commas and pipes are given unicode escapes in the form `\u{2c}` and `\u{7c}`, respectively.
#[allow(unused)]
pub(crate) fn escape_tag_value(raw: &str) -> String {
    let mut escaped = String::with_capacity(raw.len());

    for c in raw.chars() {
        match c {
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\\' => escaped.push_str("\\\\"),
            '|' => escaped.push_str("\\u{7c}"),
            ',' => escaped.push_str("\\u{2c}"),
            _ => escaped.push(c),
        }
    }

    escaped
}

/// Decodes and normalizes a potentially escaped tag value into a raw string.
///
/// This replaces escape sequences following the rules of [`escape_tag_value`] with their original
/// unicode characters. In addition to that, unicode escape sequences for all characters will be
/// resolved.
///
/// ASCII control characters are stripped from the resulting string. This is equivalent to
/// [`validate_tag_value`].
pub(crate) fn unescape_tag_value(escaped: &str) -> Result<String, UnescapeError> {
    let mut unescaped = unescaper::unescape(escaped)?;
    validate_tag_value(&mut unescaped);
    Ok(unescaped)
}

/// Validates a tag value.
///
/// Tag values are never entirely rejected, but invalid characters (ASCII control characters) are
/// stripped out.
pub(crate) fn validate_tag_value(tag_value: &mut String) {
    tag_value.retain(|c| !c.is_ascii_control());
}

/// Hashes the given set value.
///
/// Sets only guarantee 32-bit accuracy, but arbitrary strings are allowed on the protocol. Upon
/// parsing, they are hashed and only used as hashes subsequently.
pub(crate) fn hash_set_value(string: &str) -> u32 {
    let mut hasher = FnvHasher::default();
    hasher.write(string.as_bytes());
    hasher.finish32()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unescape_tag_value() {
        // No escaping
        assert_eq!(unescape_tag_value("plain").unwrap(), "plain");
        assert_eq!(unescape_tag_value("plain text").unwrap(), "plain text");
        assert_eq!(unescape_tag_value("plain%text").unwrap(), "plain%text");

        // Escape sequences
        assert_eq!(
            unescape_tag_value("plain \\\\ text").unwrap(),
            "plain \\ text"
        );
        assert_eq!(
            unescape_tag_value("plain\\u{2c}text").unwrap(),
            "plain,text"
        );
        assert_eq!(
            unescape_tag_value("plain\\u{7c}text").unwrap(),
            "plain|text"
        );

        // Alternate escape sequences
        assert_eq!(
            unescape_tag_value("plain \\u{5c} text").unwrap(),
            "plain \\ text"
        );

        // These are control characters and therefore stripped
        assert_eq!(unescape_tag_value("plain\\ntext").unwrap(), "plaintext");
        assert_eq!(unescape_tag_value("plain\\rtext").unwrap(), "plaintext");
        assert_eq!(unescape_tag_value("plain\\ttext").unwrap(), "plaintext");
    }

    #[test]
    fn test_escape_tag_value() {
        // No escaping
        assert_eq!(escape_tag_value("plain"), "plain");
        assert_eq!(escape_tag_value("plain text"), "plain text");
        assert_eq!(escape_tag_value("plain%text"), "plain%text");

        // Escape sequences
        assert_eq!(escape_tag_value("plain \\ text"), "plain \\\\ text");
        assert_eq!(escape_tag_value("plain,text"), "plain\\u{2c}text");
        assert_eq!(escape_tag_value("plain|text"), "plain\\u{7c}text");

        // These are control characters and therefore stripped
        assert_eq!(escape_tag_value("plain\ntext"), "plain\\ntext");
        assert_eq!(escape_tag_value("plain\rtext"), "plain\\rtext");
        assert_eq!(escape_tag_value("plain\ttext"), "plain\\ttext");
    }
}
