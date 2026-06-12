use relay_conventions::attributes::*;
use relay_event_schema::protocol::Attributes;
use relay_protocol::{Annotated, Value};

/// Derives a description for a V2 span, based on its name
/// and attributes.
///
/// For now, this tries the following steps, in order:
/// - returns the span's name if its [`SENTRY__ORIGIN`] attribute is `"manual"`
/// - returns the span's [`DB__QUERY__TEXT`] attribute if it exists
/// - returns a combination of the span's [`HTTP__REQUEST__METHOD`] and
///   [`URL__FULL`] attributes, if they both exists.
///
/// In the future, this logic will be partly moved to and extended in `sentry-conventions`.
pub fn derive_description_for_v2_span(
    attributes: &Attributes,
    name: &Annotated<String>,
) -> Option<String> {
    if attributes
        .get_value(SENTRY__ORIGIN)
        .and_then(|o| o.as_str())
        == Some("manual")
    {
        return name.value().cloned();
    }

    if let Some(&Value::String(db_query)) = attributes.get_value(DB__QUERY__TEXT).as_ref() {
        return Some(db_query.clone());
    }

    if let Some(&Value::String(method)) = attributes.get_value(HTTP__REQUEST__METHOD).as_ref()
        && let Some(&Value::String(url)) = attributes.get_value(URL__FULL).as_ref()
    {
        return Some(format!("{method} {url}"));
    }

    None
}
