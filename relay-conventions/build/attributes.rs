//! Contains type definitions for deserializing attribute definitions from JSON files in `sentry-conventions/model`.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;

/// Regex to find "unfenced" attribute names containing `<key>`.
///
/// Capture groups:
/// 1. Character before
/// 2. Attribute name
/// 3. Character after
static KEY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    r"(^|[^`])((?:[a-zA-Z_]+\.)*<key>(?:\.[a-zA-Z_]+)*)($|[^`])"
        .parse()
        .unwrap()
});

/// Whether an attribute can contain PII.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case", tag = "key")]
pub enum Pii {
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
pub enum DeprecationStatus {
    /// Write both the original and replacement name.
    Backfill,
    /// Only write the replacement name.
    Normalize,
}

/// Information about an attribute's deprecation.
#[derive(Debug, Clone, Deserialize)]
pub struct Deprecation {
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
    pub key: String,
    /// Short description of the attribute.
    pub brief: String,
    /// Whether the attribute can contain PII.
    pub pii: Pii,
    /// If the attribute is deprecated, this contains
    /// information on its replacement name and what should
    /// be done when the original name is encountered.
    pub deprecation: Option<Deprecation>,
    /// Other attributes that alias to this attribute.
    #[serde(default)]
    pub alias: Vec<String>,
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
pub fn format_attribute_info(attr: &Attribute) -> String {
    let Attribute {
        key: _,
        brief: _,
        pii,
        deprecation,
        alias,
    } = attr;

    let write_behavior = format_write_behavior(deprecation.as_ref());

    format!(
        "AttributeInfo {{
            write_behavior: {write_behavior},
            pii: Pii::{pii:?},
            aliases: &{alias:?},
        }}"
    )
}

/// Formats an attribute as a constant definition.
pub fn format_constant(attr: &Attribute) -> String {
    let Attribute {
        key,
        brief,
        pii,
        deprecation,
        alias,
    } = attr;

    let name = name_constant(key);

    let mut out = String::new();

    if !brief.is_empty() {
        // Surround attribute names containing `<key>` with backticks, otherwise
        // rustdoc complains about unclosed html tags
        let brief = KEY_REGEX.replace_all(brief, "$1`$2`$3");
        write!(&mut out, "/// {brief}").unwrap();

        if !brief.ends_with('.') {
            write!(&mut out, ".").unwrap();
        }

        writeln!(&mut out).unwrap();
    }

    writeln!(&mut out, "/// * PII: {pii:?}").unwrap();
    writeln!(
        &mut out,
        "/// * Rewriting behavior: {}",
        deprecation
            .as_ref()
            .and_then(|d| d.status)
            .map(|s| format!("{s:?}"))
            .unwrap_or("None".to_owned())
    )
    .unwrap();

    if !alias.is_empty() {
        writeln!(&mut out, "/// # Aliases").unwrap();

        for a in alias {
            writeln!(&mut out, r#"/// * [`{}`] (`{a}`)"#, name_constant(a)).unwrap();
        }

        writeln!(&mut out, "///").unwrap();
    }

    if let Some(deprecation) = deprecation {
        write!(&mut out, "#[deprecated").unwrap();

        if let Some(ref replacement) = deprecation.replacement {
            let replacement_name = name_constant(replacement);
            write!(
                &mut out,
                r#"(note="Use [`{replacement_name}`] (`{replacement}`) instead.")"#
            )
            .unwrap();
        }
        writeln!(&mut out, "]").unwrap();
    }

    writeln!(&mut out, r#"pub const {name}: &str = "{key}";"#).unwrap();

    out
}

/// Formats an attributes name as a constant identifier.
fn name_constant(name: &str) -> String {
    name.replace(['<', '>'], "")
        .replace('.', "__")
        .replace('-', "_")
        .to_ascii_uppercase()
}

/// Parse a path-like attribute key into individual segments.
///
/// NOTE: This does not yet support escaped segments, e.g. `"foo.'my.thing'.bar"` will split into
/// `["foo.'my", "thing'.bar"]`.
pub fn parse_segments(key: &str) -> impl Iterator<Item = &str> {
    key.split('.')
}

/// Returns:
/// * `Some(attribute, replacement)` if `attribute` is deprecated with `replacement`
/// * `Some(attribute, attribute)` if `attribute` is not deprecated
/// * `None` if `attribute` is deprecated without replacement
pub fn constant_pair(attribute: &Attribute) -> Option<(String, String)> {
    let Attribute {
        key,
        brief: _,
        pii: _,
        deprecation,
        alias: _,
    } = attribute;

    let replacement = match deprecation {
        Some(d) => d.replacement.as_deref()?,
        None => key,
    };

    Some((name_constant(key), name_constant(replacement)))
}

/// Writes a function that returns the replacement name for an attribute.
pub fn write_replacement_fn(
    out: &mut impl std::io::Write,
    constants: impl Iterator<Item = (String, String)>,
) {
    writeln!(
        out,
        r#"/// Returns the "undeprecated" version of an attribute."#
    )
    .unwrap();
    writeln!(out, r#"/// This means:"#).unwrap();
    writeln!(
        out,
        r#"/// * If the attribute is not deprecated, it is returned itself."#
    )
    .unwrap();
    writeln!(out, r#"/// * If the attribute is deprecated and has a replacement, the replacement is returned."#).unwrap();
    writeln!(
        out,
        r#"/// * If the attribute is deprecated without a replacement, `None` is returned."#
    )
    .unwrap();
    writeln!(
        out,
        r#"/// * If the attribute is not defined in `sentry-conventions`, `None` is returned."#
    )
    .unwrap();
    writeln!(out, "#[allow(deprecated)]").unwrap();

    writeln!(out, r#"pub fn replacement(key: &str) -> Option<&str> {{"#).unwrap();
    writeln!(out, r#"    match key {{"#).unwrap();

    for (old, new) in constants {
        writeln!(out, r#"        {old} => Some({new}),"#).unwrap();
    }

    writeln!(out, r#"        _ => None,"#).unwrap();
    writeln!(out, r#"    }}"#).unwrap();
    writeln!(out, r#"}}"#).unwrap();
}

#[derive(Default)]
pub struct RawNode {
    pub children: BTreeMap<String, RawNode>,
    pub info: Option<String>,
}

impl RawNode {
    pub fn write(&self, w: &mut impl std::io::Write) -> Result<(), std::io::Error> {
        let Self { children, info } = self;
        write!(w, "Node {{ info: ")?;
        match info {
            Some(info) => write!(w, "Some({})", info)?,
            None => write!(w, "None")?,
        };
        write!(w, ", children: ::phf::phf_map!{{",)?;
        for (segment, child) in children {
            write!(w, "\"{segment}\" => ")?;
            child.write(w)?;
            write!(w, ",")?;
        }
        write!(w, "}} }}")
    }
}
