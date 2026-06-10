use relay_conventions::attributes::SENTRY__OP;
use relay_conventions::name_for_op_and_attributes;
use relay_event_schema::protocol::Attributes;
use relay_protocol::{Getter, Val};

/// Constructs a name attribute for a span, following the rules defined in sentry-conventions.
pub fn name_for_attributes(attributes: &Attributes) -> Option<String> {
    let op = attributes.get_value(SENTRY__OP)?.as_str()?;
    Some(name_for_op_and_attributes(op, &AttributeGetter(attributes)))
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
}
