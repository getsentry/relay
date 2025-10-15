//! Contains type definitions for deserializing attribute definitions from JSON files in `sentry-conventions/model`.

use std::collections::BTreeMap;

use serde::Deserialize;

/// Whether an attribute can contain PII.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case", tag = "key")]
enum Pii {
    /// The field will be stripped by default
    True,
    /// The field will only be stripped when addressed with a specific path selector, but generic
    /// selectors such as `$string` do not apply.
    Maybe,
    /// The field cannot be stripped at all
    False,
}

/// How to handle an attribute's deprecation.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DeprecationStatus {
    /// Write both the original and replacement name.
    Backfill,
    /// Only write the replacement name.
    Normalize,
}

/// Information about an attribute's deprecation.
#[derive(Debug, Clone, Deserialize)]
struct Deprecation {
    /// The attribute's new name.
    replacement: Option<String>,
    /// How to handle the attribute's deprecation.
    #[serde(rename = "_status")]
    status: Option<DeprecationStatus>,
}

/// An attribute, according to the `sentry-conventions` schema.
///
/// Omitted fields: `brief`, `has_dynamic_suffix`, `is_in_otel`, `example`, `sdks`, `ty`.
#[derive(Debug, Clone, Deserialize)]
pub struct Attribute {
    /// The attribute's name.
    key: String,
    /// Whether the attribute can contain PII.
    pii: Pii,
    /// If the attribute is deprecated, this contains
    /// information on its replacement name and what should
    /// be done when the original name is encountered.
    deprecation: Option<Deprecation>,
    /// Other attributes that alias to this attribute.
    #[serde(default)]
    alias: Vec<String>,
}

/// Formats an attribute's deprecation information as a `WriteBehavior`.
fn format_write_behavior(deprecation: Option<&Deprecation>) -> String {
    let Some((status, replacement)) =
        deprecation.and_then(|d| d.status.zip(d.replacement.as_ref()))
    else {
        return "WriteBehavior::CurrentName".to_owned();
    };

    match status {
        DeprecationStatus::Backfill => {
            format!("WriteBehavior::BothNames({:?})", replacement)
        }
        DeprecationStatus::Normalize => {
            format!("WriteBehavior::NewName({:?})", replacement)
        }
    }
}

/// Format an attribute as an `AttributeInfo`.
pub fn format_attribute_info(attr: Attribute) -> (String, String) {
    let Attribute {
        key,
        pii,
        deprecation,
        alias,
    } = attr;
    let write_behavior = format_write_behavior(deprecation.as_ref());
    let value = format!(
        "AttributeInfo {{
            write_behavior: {write_behavior},
            pii: Pii::{pii:?},
            aliases: &{alias:?},
        }}"
    );
    (key, value)
}

/// Parse a path-like attribute key into individual segments.
///
/// NOTE: This does not yet support escaped segments, e.g. `"foo.'my.thing'.bar"` will split into
/// `["foo.'my", "thing'.bar"]`.
pub fn parse_segments(key: &str) -> impl Iterator<Item = &str> {
    key.split('.')
}

#[derive(Default)]
pub struct RawNode {
    pub children: BTreeMap<String, RawNode>,
    pub info: Option<String>,
}

impl RawNode {
    pub fn build(&self, w: &mut impl std::io::Write) -> Result<(), std::io::Error> {
        let Self { children, info } = self;
        write!(w, "Node {{ info: ")?;
        match info {
            Some(info) => write!(w, "Some({})", info)?,
            None => write!(w, "None")?,
        };
        write!(w, ", children: ::phf::phf_map!{{",)?;
        for (segment, child) in children {
            write!(w, "\"{segment}\" => ")?;
            child.build(w)?;
            write!(w, ",")?;
        }
        write!(w, "}} }}")
    }
}
