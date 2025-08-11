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

/// The type of an attribute value.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AttributeType {
    String,
    Boolean,
    Integer,
    Double,
    #[serde(rename = "string[]")]
    StringArray,
    #[serde(rename = "boolean[]")]
    BooleanArray,
    #[serde(rename = "integer[]")]
    IntegerArray,
    #[serde(rename = "double[]")]
    DoubleArray,
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
    replacement: String,
    /// How to handle the attribute's deprecation.
    #[serde(rename = "_status")]
    status: Option<DeprecationStatus>,
}

/// An attribute, according to the `sentry-conventions` schema.
///
/// Omitted fields: `has_dynamic_suffix`, `is_in_otel`, `example`, `alias`, `sdks`.
#[derive(Debug, Clone, Deserialize)]
pub struct Attribute {
    /// The attribute's name.
    key: String,
    /// A description of the attribute.
    brief: String,
    /// The type of the attribute's value.
    #[serde(rename = "type")]
    ty: AttributeType,
    /// Whether the attribute can contain PII.
    pii: Pii,
    /// If the attribute is deprecated, this contains
    /// information on its replacement name and what should
    /// be done when the original name is encountered.
    deprecation: Option<Deprecation>,
}

/// Formats an attribute's deprecation information as a `WriteBehavior`.
fn format_write_behavior(deprecation: Option<&Deprecation>) -> String {
    let Some(deprecation) = deprecation else {
        return "WriteBehavior::CurrentName".to_owned();
    };

    match deprecation.status {
        Some(DeprecationStatus::Backfill) => {
            format!("WriteBehavior::BothNames({:?})", deprecation.replacement)
        }
        Some(DeprecationStatus::Normalize) => {
            format!("WriteBehavior::NewName({:?})", deprecation.replacement)
        }
        None => "WriteBehavior::CurrentName".to_owned(),
    }
}

/// Format an attribute as an `AttributeInfo`.
pub fn format_attribute_info(attr: Attribute) -> (String, String) {
    let Attribute {
        brief,
        ty,
        pii,
        deprecation,
        key,
    } = attr;
    let write_behavior = format_write_behavior(deprecation.as_ref());
    let value = format!(
        "AttributeInfo {{
            description: {brief:?},
            write_behavior: {write_behavior},
            pii: Pii::{pii:?},
            ty: AttributeType::{ty:?},
        }}"
    );
    (key, value)
}
