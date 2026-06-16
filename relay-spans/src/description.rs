use relay_conventions::attributes::*;
use relay_conventions::description::description_for_op_and_attributes;
use relay_event_schema::protocol::Attributes;
use relay_protocol::{Annotated, Getter, Val};

/// Derives a description for a V2 span, based on its name
/// and attributes.
///
/// For now, this attempts to return the following values, in order:
/// - the span's name if its [`SENTRY__ORIGIN`] attribute is `"manual"`
/// - a name constructed following the rules defined in sentry-conventions
/// - the `[SENTRY__OP]` attribute if it exists
/// - `None`
pub fn derive_description_for_v2_span(
    attributes: &Attributes,
    name: &Annotated<String>,
) -> Option<String> {
    let origin = attributes
        .get_value(SENTRY__ORIGIN)
        .and_then(|o| o.as_str());

    let name = name.as_str();

    if let Some(name) = name
        && origin == Some("manual")
    {
        return Some(name.to_owned());
    }

    let op = attributes.get_value(SENTRY__OP)?.as_str()?;

    description_for_op_and_attributes(op, &AttributeGetter(attributes))
}

/// A custom getter for [`Attributes`] which only resolves values based on the attribute name.
///
/// This [`Getter`] does not implement nested traversals, which is the behaviour required for
/// [`description_for_op_and_attributes`].
struct AttributeGetter<'a>(&'a Attributes);

impl<'a> Getter for AttributeGetter<'a> {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        self.0.get_value(path).map(|value| value.into())
    }
}
