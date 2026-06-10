use relay_conventions::attributes::{SENTRY__DESCRIPTION, SENTRY__OP, SENTRY__ORIGIN};
use relay_conventions::name_for_op_and_attributes;
use relay_event_schema::protocol::{Attributes, Span};
use relay_protocol::{Getter, GetterIter, Val};

/// Constructs a name attribute for a V1 span.
///
/// If the span's origin is `"manual"`, its description is used as the name.
/// Otherwise, the name is constructed following the rules defined in sentry-conventions.
pub fn name_for_span(span: &Span) -> Option<String> {
    let origin = span.origin.value().map(|o| o.as_str());
    let description = span.description.value().map(|d| d.as_str());

    if let Some(name) = name_for_origin_and_description(origin, description) {
        return Some(name);
    }

    let op = span.op.value()?;

    let Some(data) = span.data.value() else {
        return Some(name_for_op_and_attributes(op, &EmptyGetter {}));
    };

    Some(name_for_op_and_attributes(
        op,
        // SpanData's Getter impl treats dots in attribute names as object traversals.
        // They have to be escaped in order for an attribute name with dots to be treated as a root
        // attribute.
        &EscapedGetter(data),
    ))
}

/// Constructs a name attribute for a V2 span, based on its attributes.
///
/// If the attributes contain [`SENTRY__ORIGIN`] with the value `"manual"`,
/// the description (contained in [`SENTRY__DESCRIPTION`]) is used as the name.
/// Otherwise, the name is constructed following the rules defined in sentry-conventions.
pub fn name_for_attributes(attributes: &Attributes) -> Option<String> {
    let origin = attributes
        .get_value(SENTRY__ORIGIN)
        .and_then(|o| o.as_str());
    let description = attributes
        .get_value(SENTRY__DESCRIPTION)
        .and_then(|d| d.as_str());

    if let Some(name) = name_for_origin_and_description(origin, description) {
        return Some(name);
    }

    let op = attributes.get_value(SENTRY__OP)?.as_str()?;
    Some(name_for_op_and_attributes(op, &AttributeGetter(attributes)))
}

fn name_for_origin_and_description(
    origin: Option<&str>,
    description: Option<&str>,
) -> Option<String> {
    if origin == Some("manual") {
        description.map(String::from)
    } else {
        None
    }
}

struct EmptyGetter {}

impl Getter for EmptyGetter {
    fn get_value(&self, _path: &str) -> Option<Val<'_>> {
        None
    }
}

struct EscapedGetter<'a, T: Getter>(&'a T);

impl<'a, T: Getter> Getter for EscapedGetter<'a, T> {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        self.0.get_value(&path.replace(".", "\\."))
    }

    fn get_iter(&self, path: &str) -> Option<GetterIter<'_>> {
        self.0.get_iter(&path.replace(".", "\\."))
    }
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
    use relay_event_schema::protocol::SpanData;
    use relay_protocol::{Annotated, Object, Value};

    use super::*;

    #[test]
    fn test_span_falls_back_to_op_when_no_templates_defined() {
        let span = Span {
            op: Annotated::new("foo".to_owned()),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("foo".to_owned()));
    }

    #[test]
    fn test_attributes_falls_back_to_op_when_no_templates_defined() {
        let attributes = Attributes::from([(
            "sentry.op".to_owned(),
            Annotated::new("foo".to_owned().into()),
        )]);

        assert_eq!(name_for_attributes(&attributes), Some("foo".to_owned()));
    }

    #[test]
    fn test_span_uses_the_first_matching_template() {
        let span = Span {
            op: Annotated::new("db".to_owned()),
            data: Annotated::new(SpanData {
                other: Object::from([
                    (
                        "db.query.summary".to_owned(),
                        Value::String("SELECT users".to_owned()).into(),
                    ),
                    (
                        "db.operation.name".to_owned(),
                        Value::String("INSERT".to_owned()).into(),
                    ),
                    (
                        "db.collection.name".to_owned(),
                        Value::String("widgets".to_owned()).into(),
                    ),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("SELECT users".to_owned()));
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
    fn test_span_uses_fallback_templates_when_data_is_missing() {
        let span = Span {
            op: Annotated::new("db".to_owned()),
            data: Annotated::new(SpanData {
                other: Object::from([
                    (
                        "db.operation.name".to_owned(),
                        Value::String("INSERT".to_owned()).into(),
                    ),
                    (
                        "db.collection.name".to_owned(),
                        Value::String("widgets".to_owned()).into(),
                    ),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("INSERT widgets".to_owned()));
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
    fn test_span_falls_back_to_hardcoded_name_when_nothing_matches() {
        let span = Span {
            op: Annotated::new("db".to_owned()),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("Database operation".to_owned()));
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
    fn test_manual_spans_use_description_v1() {
        let span = Span {
            origin: Annotated::new("manual".to_owned()),
            description: Annotated::new("Custom name".to_owned()),
            op: Annotated::new("db".to_owned()),
            data: Annotated::new(SpanData {
                other: Object::from([
                    (
                        "db.query.summary".to_owned(),
                        Value::String("SELECT users".to_owned()).into(),
                    ),
                    (
                        "db.operation.name".to_owned(),
                        Value::String("INSERT".to_owned()).into(),
                    ),
                    (
                        "db.collection.name".to_owned(),
                        Value::String("widgets".to_owned()).into(),
                    ),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("Custom name".to_owned()));
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
