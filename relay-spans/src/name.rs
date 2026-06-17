use relay_conventions::attributes::{SENTRY__DESCRIPTION, SENTRY__OP, SENTRY__ORIGIN};
use relay_conventions::name_for_op_and_attributes;
use relay_event_schema::protocol::Attributes;
use relay_protocol::{Getter, Val};

/// Constructs a name attribute for a V2 span, based on its attributes.
///
/// If the attributes contain [`SENTRY__ORIGIN`] with the value `"manual"`,
/// the description (contained in [`SENTRY__DESCRIPTION`]) is used as the name.
/// Otherwise, the name is constructed following the rules defined in sentry-conventions.
///
/// If no rule in `sentry-conventions` matches the span's [`SENTRY__OP`], the op is
/// returned as the name.
///
/// Finally, if the span doesn't have an op, `None` is returned.
pub fn name_for_attributes(attributes: &Attributes) -> Option<String> {
    let origin = attributes
        .get_value(SENTRY__ORIGIN)
        .and_then(|o| o.as_str());
    let description = attributes
        .get_value(SENTRY__DESCRIPTION)
        .and_then(|d| d.as_str());

    if let Some(description) = description
        && origin == Some("manual")
    {
        return Some(description.to_owned());
    }

    let op = attributes.get_value(SENTRY__OP)?.as_str()?;
    Some(name_for_op_and_attributes(op, &AttributeGetter(attributes)).unwrap_or(op.to_owned()))
}

/// A custom getter for [`Attributes`] which only resolves values based on the attribute name.
///
/// This [`Getter`] does not implement nested traversals, which is the behaviour required for
/// [`name_for_op_and_attributes`].
struct AttributeGetter<'a>(&'a Attributes);

impl<'a> Getter for AttributeGetter<'a> {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        self.0.get_value(path).map(|value| value.into())
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use super::*;

    #[test]
    fn test_attributes_falls_back_to_op_when_no_templates_defined() {
        let attributes = Attributes::from([(
            "sentry.op".to_owned(),
            Annotated::new("foo".to_owned().into()),
        )]);

        assert_eq!(name_for_attributes(&attributes), Some("foo".to_owned()));
    }

    #[test]
    fn test_attributes_uses_the_first_matching_template() {
        let attributes = Attributes::from([
            (
                "sentry.op".to_owned(),
                Annotated::new("db".to_owned().into()),
            ),
            (
                "db.query.summary".to_owned(),
                Annotated::new("SELECT users".to_owned().into()),
            ),
            (
                "db.operation.name".to_owned(),
                Annotated::new("INSERT".to_owned().into()),
            ),
            (
                "db.collection.name".to_owned(),
                Annotated::new("widgets".to_owned().into()),
            ),
        ]);

        assert_eq!(
            name_for_attributes(&attributes),
            Some("SELECT users".to_owned())
        );
    }

    #[test]
    fn test_attributes_uses_fallback_templates_when_data_is_missing() {
        let attributes = Attributes::from([
            (
                "sentry.op".to_owned(),
                Annotated::new("db".to_owned().into()),
            ),
            (
                "db.operation.name".to_owned(),
                Annotated::new("INSERT".to_owned().into()),
            ),
            (
                "db.collection.name".to_owned(),
                Annotated::new("widgets".to_owned().into()),
            ),
        ]);

        assert_eq!(
            name_for_attributes(&attributes),
            Some("INSERT widgets".to_owned())
        );
    }

    #[test]
    fn test_attributes_falls_back_to_hardcoded_name_when_nothing_matches() {
        let attributes = Attributes::from([(
            "sentry.op".to_owned(),
            Annotated::new("db".to_owned().into()),
        )]);

        assert_eq!(
            name_for_attributes(&attributes),
            Some("Database operation".to_owned())
        );
    }

    #[test]
    fn test_manual_spans_use_description_v2() {
        let attributes = Attributes::from([
            (
                "sentry.origin".to_owned(),
                Annotated::new("manual".to_owned().into()),
            ),
            (
                "sentry.description".to_owned(),
                Annotated::new("Custom name".to_owned().into()),
            ),
            (
                "sentry.op".to_owned(),
                Annotated::new("db".to_owned().into()),
            ),
            (
                "db.query.summary".to_owned(),
                Annotated::new("SELECT users".to_owned().into()),
            ),
            (
                "db.operation.name".to_owned(),
                Annotated::new("INSERT".to_owned().into()),
            ),
            (
                "db.collection.name".to_owned(),
                Annotated::new("widgets".to_owned().into()),
            ),
        ]);

        assert_eq!(
            name_for_attributes(&attributes),
            Some("Custom name".to_owned())
        );
    }
}
