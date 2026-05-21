//! Contains type definitions for deserializing measurement definitions from JSON files in `sentry-conventions/model`.

use std::fmt::Write;

use serde::Deserialize;

use crate::attributes;

#[derive(Debug, Clone, Deserialize)]
pub struct Measurement {
    pub key: String,
    #[serde(default)]
    pub brief: String,
    pub attribute: Option<String>,
}

/// Formats a measurement as a constant definition.
pub fn format_constant(attr: &Measurement) -> String {
    let Measurement {
        key,
        brief,
        attribute,
    } = attr;

    let name = name_constant(key);

    let mut out = String::new();

    if !brief.is_empty() {
        write!(&mut out, "/// {brief}").unwrap();

        if !brief.ends_with('.') {
            write!(&mut out, ".").unwrap();
        }

        writeln!(&mut out).unwrap();
    }

    if let Some(attribute) = attribute {
        let attribute_name = attributes::name_constant(attribute);
        writeln!(
            &mut out,
            "///\n/// Replacement attribute: [`{attribute_name}`](crate::attributes::{attribute_name})",
        )
        .unwrap();
    }

    writeln!(&mut out, r#"pub const {name}: &str = "{key}";"#).unwrap();

    out
}

/// Formats a measurement's name as a constant identifier.
fn name_constant(name: &str) -> String {
    name.replace(['<', '>'], "")
        .replace('.', "__")
        .replace('-', "_")
        .to_ascii_uppercase()
}

/// Returns `Some((measurement, replacement))` if `measurement`'s `attribute` field is
/// set to `replacement`, otherwise returns `None`.
pub fn measurement_attribute_pair(measurement: &Measurement) -> Option<(String, String)> {
    let Measurement {
        key,
        brief: _,
        attribute,
    } = measurement;

    let attribute = attribute.as_ref()?;

    Some((name_constant(key), attributes::name_constant(attribute)))
}

pub fn write_replacement_fn(
    out: &mut impl std::io::Write,
    constants: impl Iterator<Item = (String, String)>,
) {
    writeln!(
        out,
        r#"/// Returns the attribute replacing a measurement.
pub fn measurement_to_attribute(key: &str) -> Option<&'static str> {{
    match key {{"#
    )
    .unwrap();

    for (old, new) in constants {
        writeln!(
            out,
            r#"        crate::measurements::{old} => Some(crate::attributes::{new}),"#
        )
        .unwrap();
    }

    writeln!(
        out,
        r#"        _ => None,
    }}
}}"#
    )
    .unwrap();
}
