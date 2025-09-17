//! Contains type definitions for deserializing attribute definitions from JSON files in `sentry-conventions/model`.

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
