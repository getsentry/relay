//! Attribute definitions extracted from [`sentry-conventions`](https://github.com/getsentry/sentry-conventions).
//!
//! This crate contains the `sentry-conventions` repository as a git submodule. Attribute definitions in the submodule
//! are parsed at compile time and can be accessed via the `attribute_info` function.

include!(concat!(env!("OUT_DIR"), "/attribute_map.rs"));

/// Whether an attribute should be PII-strippable/should be subject to datascrubbers
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Pii {
    /// The field will be stripped by default
    True,
    /// The field cannot be stripped at all
    False,
    /// The field will only be stripped when addressed with a specific path selector, but generic
    /// selectors such as `$string` do not apply.
    Maybe,
}

/// Under which names an attribute should be saved.
#[derive(Debug, Clone, Copy)]
pub enum WriteBehavior {
    /// Save the attribute under its current name.
    ///
    /// This is the only choice for attributes that aren't deprecated.
    CurrentName,
    /// Save the attribute under its replacement name instead.
    NewName(&'static str),
    /// Save the attribute under both its current name and
    /// its replacement name.
    BothNames(&'static str),
}

/// Information about an attribute, as defined in `sentry-conventions`.
#[derive(Debug, Clone)]
pub struct AttributeInfo {
    /// How this attribute should be saved.
    pub write_behavior: WriteBehavior,
    /// Whether this attribute can contain PII.
    pub pii: Pii,
    /// Other attribute names that alias to this attribute.
    pub aliases: &'static [&'static str],
}

/// Returns information about an attribute, as defined in `sentry-conventions`.
pub fn attribute_info(key: &str) -> Option<&'static AttributeInfo> {
    ATTRIBUTES.get(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_response_content_length() {
        let info = ATTRIBUTES.get("http.response_content_length").unwrap();

        insta::assert_debug_snapshot!(info, @r###"
        AttributeInfo {
            write_behavior: BothNames(
                "http.response.body.size",
            ),
            pii: False,
            aliases: [
                "http.response.body.size",
                "http.response.header.content-length",
            ],
        }
        "###);
    }
}
