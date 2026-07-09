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

/// Whether an attribute should be scrubbed.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case", tag = "key")]
pub enum ApplyScrubbing {
    /// The attribute will be stripped by default.
    Auto,
    /// The attribute will only be stripped when addressed with a specific path selector, but generic
    /// selectors such as `$string` do not apply.
    Manual,
    /// The attribute cannot be stripped at all.
    Never,
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
/// Omitted fields: `has_dynamic_suffix`, `is_in_otel`, `example`, `sdks`, `ty`.
#[derive(Debug, Clone, Deserialize)]
pub struct Attribute {
    /// The attribute's name.
    pub key: String,
    /// Short description of the attribute.
    pub brief: String,
    /// Whether the attribute should be scrubbed.
    pub apply_scrubbing: ApplyScrubbing,
    /// If the attribute is deprecated, this contains
    /// information on its replacement name and what should
    /// be done when the original name is encountered.
    pub deprecation: Option<Deprecation>,
    /// Other attributes that alias to this attribute.
    #[serde(default)]
    pub alias: Vec<String>,
}

/// Sanity check: if an attribute
/// * is deprecated,
/// * has status "backfill" or "normalize",
/// * and it contains a placeholder while its replacement doesn't, or vice versa,
///
/// then we panic. There is no general way to normalize such attributes, so it's
/// better to reject them at compile time.
pub fn check_attribute(attribute: &Attribute) {
    let Attribute {
        key,
        brief: _,
        apply_scrubbing: _,
        deprecation,
        alias: _,
    } = attribute;

    if let Some(deprecation) = deprecation
        && let Some(replacement) = deprecation.replacement.as_ref()
        && deprecation.status.is_some()
    {
        let attribute_contains_placeholder = key.contains("<key>");
        let replacement_contains_placeholder = replacement.contains("<key>");

        assert_eq!(
            attribute_contains_placeholder, replacement_contains_placeholder,
            r"One of attribute `{key}` and its replacement `{replacement}` contains a placeholder and the other doesn't. Such attributes can't be automatically backfilled or normalized by Relay."
        )
    }
}

/// Formats an attribute's deprecation information as a `WriteBehavior`.
fn format_write_behavior(deprecation: Option<&Deprecation>) -> String {
    let Some((status, replacement)) =
        deprecation.and_then(|d| d.status.zip(d.replacement.as_ref()))
    else {
        return "WriteBehavior::CurrentName".to_owned();
    };

    let name = if replacement.contains("<key>") {
        format!(
            "ReplacementName::Dynamic(|s| crate::interpolate::{}(s))",
            name_fn(replacement)
        )
    } else {
        format!("ReplacementName::Static({replacement:?})")
    };

    match status {
        DeprecationStatus::Backfill => {
            format!("WriteBehavior::BothNames({name})")
        }
        DeprecationStatus::Normalize => {
            format!("WriteBehavior::NewName({name})")
        }
    }
}

/// Format an attribute as an `AttributeInfo`.
pub fn format_attribute_info(attr: &Attribute) -> String {
    let Attribute {
        key: _,
        brief: _,
        apply_scrubbing,
        deprecation,
        alias,
    } = attr;

    let write_behavior = format_write_behavior(deprecation.as_ref());

    format!(
        "AttributeInfo {{
            write_behavior: {write_behavior},
            apply_scrubbing: ApplyScrubbing::{apply_scrubbing:?},
            aliases: &{alias:?},
        }}"
    )
}

/// Formats an attribute as a constant definition.
pub fn format_constant(attr: &Attribute) -> String {
    let Attribute {
        key,
        brief,
        apply_scrubbing,
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

    writeln!(&mut out, "/// * Scrubbing: {apply_scrubbing:?}").unwrap();
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
        write_deprecation_annotation(&mut out, deprecation);
    }

    writeln!(&mut out, r#"pub const {name}: &str = "{key}";"#).unwrap();

    out
}

/// Formats an attribute as a function that interpolates a value for
/// the `<key>` placeholder.
pub fn format_interpolating_fn(attribute: &Attribute) -> Option<String> {
    let Attribute {
        key,
        brief: _,
        apply_scrubbing: _,
        deprecation,
        alias: _,
    } = attribute;

    let needle = "<key>";
    let placeholder_start = key.find(needle)?;
    let placeholder_end = placeholder_start + needle.len();
    let before_placeholder = key.get(..placeholder_start).unwrap_or_default();
    let after_placeholder = key.get(placeholder_end..).unwrap_or_default();

    let constant_name = name_constant(key);
    let fn_name = name_fn(key);

    let mut out = String::new();

    let example_value = key.replace("<key>", "foobar");

    writeln!(
        &mut out,
        r#"/// Instantiates the `<key>` placeholder in the attribute
/// [`{constant_name}`](crate::attributes::{constant_name}) (`{key}`) with a concrete value.
/// # Example
/// ```
/// use relay_conventions::interpolate::{fn_name};
/// assert_eq!({fn_name}("foobar"), "{example_value}");
/// ```"#
    )
    .unwrap();

    if let Some(deprecation) = deprecation {
        write_deprecation_annotation(&mut out, deprecation);
    }

    writeln!(
        &mut out,
        r#"pub fn {fn_name}<T: std::fmt::Display>(value: T) -> String {{
    format!("{before_placeholder}{{value}}{after_placeholder}")
}}"#
    )
    .unwrap();

    Some(out)
}

fn write_deprecation_annotation(out: &mut impl Write, deprecation: &Deprecation) {
    write!(out, "#[deprecated").unwrap();

    if let Some(ref replacement) = deprecation.replacement {
        let replacement_name = name_constant(replacement);
        write!(
            out,
            r#"(note="Use [`{replacement_name}`](crate::attributes::{replacement_name}) (`{replacement}`) instead.")"#
        )
        .unwrap();
    }
    writeln!(out, "]").unwrap();
}

/// Formats an attributes name as a constant identifier.
pub fn name_constant(name: &str) -> String {
    name_fn(name).to_ascii_uppercase()
}

fn name_fn(name: &str) -> String {
    name.replace(['<', '>'], "")
        .replace('.', "__")
        .replace('-', "_")
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
        apply_scrubbing: _,
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
pub fn write_canonical_fn(
    out: &mut impl std::io::Write,
    constants: impl Iterator<Item = (String, String)>,
) {
    writeln!(
        out,
        r#"/// Returns the "canonical" version of an attribute.
/// This means:
/// * If the attribute is not deprecated, it is returned itself.
/// * If the attribute is deprecated and has a replacement, the replacement is returned.
/// * If the attribute is deprecated without a replacement, `None` is returned.
/// * If the attribute is not defined in `sentry-conventions`, `None` is returned.
#[allow(deprecated)]
pub fn canonical(key: &str) -> Option<&'static str> {{
    use crate::attributes::*;
    match key {{"#
    )
    .unwrap();

    for (old, new) in constants {
        writeln!(out, r#"        {old} => Some({new}),"#).unwrap();
    }

    writeln!(
        out,
        r#"        _ => None,
    }}
}}"#
    )
    .unwrap();
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
