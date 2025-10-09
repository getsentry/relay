//! Attribute definitions extracted from [`sentry-conventions`](https://github.com/getsentry/sentry-conventions).
//!
//! This crate contains the `sentry-conventions` repository as a git submodule. Attribute definitions in the submodule
//! are parsed at compile time and can be accessed via the `attribute_info` function.
//!
//! It also exposes a number of constants for attribute names that Relay has specific logic for. It is recommended
//! to use these constants instead of the bare attribute names to ensure consistency.

mod consts;

pub use consts::*;

include!(concat!(env!("OUT_DIR"), "/attribute_map.rs"));
include!(concat!(env!("OUT_DIR"), "/name_fn.rs"));

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

struct AttributeNode {
    info: Option<AttributeInfo>,
    children: phf::Map<&'static str, AttributeNode>,
}

/// Returns information about an attribute, as defined in `sentry-conventions`.
pub fn attribute_info(key: &str) -> Option<&'static AttributeInfo> {
    let mut node = &ATTRIBUTES;
    for part in key.split('.') {
        let (_, child) = node
            .children
            .entries()
            .find(|(segment, _)| is_match(part, segment))?;
        node = child;
    }
    node.info.as_ref()
}

fn is_match(needle: &str, haystack: &str) -> bool {
    let is_wildcard = haystack.starts_with('<') && haystack.ends_with('>');
    is_wildcard || needle == haystack
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use relay_protocol::{Getter, Val};

    use super::*;

    #[test]
    fn test_http_response_content_length() {
        let info = attribute_info("http.response_content_length").unwrap();

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

    #[test]
    fn test_url_path_parameter() {
        // See https://github.com/getsentry/sentry-conventions/blob/d80504a40ba3a0a23eb746e2608425cf8d8e68bf/model/attributes/url/url__path__parameter__%5Bkey%5D.json.
        let info = attribute_info("url.path.parameter.'id=123'").unwrap();

        insta::assert_debug_snapshot!(info, @r###"
        AttributeInfo {
            write_behavior: CurrentName,
            pii: Maybe,
            aliases: [
                "params.<key>",
            ],
        }
        "###);
    }

    struct GetterMap<'a>(HashMap<&'a str, Val<'a>>);

    impl Getter for GetterMap<'_> {
        fn get_value(&self, path: &str) -> Option<Val<'_>> {
            self.0.get(path).copied()
        }
    }

    mod test_name_fn {
        include!(concat!(env!("OUT_DIR"), "/test_name_fn.rs"));
    }
    use test_name_fn::name_for_op_and_attributes;

    #[test]
    fn only_literal_template() {
        let attributes = GetterMap(HashMap::new());
        assert_eq!(
            name_for_op_and_attributes("op_with_literal_name", &attributes,),
            "literal name"
        );
    }

    #[test]
    fn multiple_ops_same_template() {
        let attributes = GetterMap(HashMap::from([("attr1", Val::String("foo"))]));
        assert_eq!(
            name_for_op_and_attributes("op_with_attributes_1", &attributes),
            "foo"
        );
        assert_eq!(
            name_for_op_and_attributes("op_with_attributes_2", &attributes),
            "foo"
        );
    }

    #[test]
    fn skips_templates_when_attrs_are_missing() {
        let attributes = GetterMap(HashMap::from([
            ("attr2", Val::String("bar")),
            ("attr3", Val::String("baz")),
        ]));
        assert_eq!(
            name_for_op_and_attributes("op_with_attributes_1", &attributes),
            "bar baz"
        );
    }

    #[test]
    fn handles_literal_prefixes_and_suffixes() {
        let attributes = GetterMap(HashMap::from([("attr3", Val::String("baz"))]));
        assert_eq!(
            name_for_op_and_attributes("op_with_attributes_1", &attributes),
            "prefix baz suffix",
        );
    }

    #[test]
    fn considers_multiple_files() {
        let attributes = GetterMap(HashMap::new());
        assert_eq!(
            name_for_op_and_attributes("op_in_second_name_file", &attributes),
            "second file literal name",
        );
    }

    #[test]
    fn falls_back_to_op_for_unknown_ops() {
        let attributes = GetterMap(HashMap::new());
        assert_eq!(
            name_for_op_and_attributes("unknown_op", &attributes),
            "unknown_op",
        );
    }

    #[test]
    fn handles_multiple_value_types() {
        let attributes = GetterMap(HashMap::from([("attr1", Val::Bool(true))]));
        assert_eq!(
            name_for_op_and_attributes("op_with_attributes_1", &attributes),
            "true",
        );

        let attributes = GetterMap(HashMap::from([("attr1", Val::I64(123))]));
        assert_eq!(
            name_for_op_and_attributes("op_with_attributes_1", &attributes),
            "123",
        );

        let attributes = GetterMap(HashMap::from([("attr1", Val::U64(123))]));
        assert_eq!(
            name_for_op_and_attributes("op_with_attributes_1", &attributes),
            "123",
        );

        let attributes = GetterMap(HashMap::from([("attr1", Val::F64(1.23))]));
        assert_eq!(
            name_for_op_and_attributes("op_with_attributes_1", &attributes),
            "1.23",
        );
    }
}
