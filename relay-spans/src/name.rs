use relay_conventions::name_for_op_and_attributes;
use relay_event_schema::protocol::Span;
use relay_protocol::{Getter, GetterIter, Val};

/// Constructs a name attribute for a span, following the rules defined in sentry-conventions.
pub fn name_for_span(span: &Span) -> Option<String> {
    let op = span.op.value()?;

    let Some(data) = span.data.value() else {
        return Some(name_for_op_and_attributes(op, &EmptyGetter {}));
    };

    Some(name_for_op_and_attributes(
        op,
        // SpanData's Getter impl treats dots in attribute names as object traversals.
        // They have to be escaped in order for an attribute name with dots to be treated as a root
        // attribute.
        &EscapedGetter { getter: data },
    ))
}

struct EmptyGetter {}

impl Getter for EmptyGetter {
    fn get_value(&self, _path: &str) -> Option<Val<'_>> {
        None
    }
}

struct EscapedGetter<'a> {
    getter: &'a dyn Getter,
}

impl Getter for EscapedGetter<'_> {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        self.getter.get_value(&path.replace(".", "\\."))
    }

    fn get_iter(&self, path: &str) -> Option<GetterIter<'_>> {
        self.getter.get_iter(&path.replace(".", "\\."))
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::SpanData;
    use relay_protocol::{Annotated, Object, Value};

    use super::*;

    #[test]
    fn falls_back_to_op_when_no_templates_defined() {
        let span = Span {
            op: Annotated::new("foo".to_owned()),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("foo".to_owned()));
    }

    #[test]
    fn uses_the_first_matching_template() {
        let span = Span {
            op: Annotated::new("db".to_owned()),
            data: Annotated::new(SpanData {
                other: Object::from([
                    (
                        "db.query.summary".into(),
                        Value::String("SELECT users".into()).into(),
                    ),
                    (
                        "db.operation.name".into(),
                        Value::String("INSERT".into()).into(),
                    ),
                    (
                        "db.collection.name".into(),
                        Value::String("widgets".into()).into(),
                    ),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("SELECT users".to_owned()));
    }

    #[test]
    fn uses_fallback_templates_when_data_is_missing() {
        let span = Span {
            op: Annotated::new("db".to_owned()),
            data: Annotated::new(SpanData {
                other: Object::from([
                    (
                        "db.operation.name".into(),
                        Value::String("INSERT".into()).into(),
                    ),
                    (
                        "db.collection.name".into(),
                        Value::String("widgets".into()).into(),
                    ),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("INSERT widgets".to_owned()));
    }

    #[test]
    fn falls_back_to_hardcoded_name_when_nothing_matches() {
        let span = Span {
            op: Annotated::new("db".to_owned()),
            ..Default::default()
        };
        assert_eq!(name_for_span(&span), Some("Database operation".to_owned()));
    }
}
