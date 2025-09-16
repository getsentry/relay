use pest::Parser;
use relay_conventions::name_info;
use relay_event_schema::protocol::Span;
use relay_protocol::Getter;

enum TemplatePart<'a> {
    Literal(&'a str),
    Attribute(&'a str),
}

/// Constructs a name attribute for a span, following the rules defined in sentry-conventions.
pub fn name_for_span(span: &Span) -> Option<String> {
    let op = span.op.value()?;
    let Some(info) = name_info(op) else {
        return Some(op.to_owned());
    };

    let data = span.data.value();

    for template in info.templates {
        let Ok(mut parsed) = TemplateParser::parse(Rule::root, template) else {
            continue;
        };
        let root = parsed.next().unwrap();

        let mut parts: Vec<TemplatePart> = Vec::new();
        for part in root.into_inner() {
            match part.as_rule() {
                Rule::text => parts.push(TemplatePart::Literal(part.as_str())),
                Rule::attribute_name => parts.push(TemplatePart::Attribute(part.as_str())),
                Rule::EOI => {}
                Rule::root | Rule::attribute => unreachable!(),
            }
        }

        // Ensure that all referenced attributes are present and strings.
        // Otherwise, continue onwards through the template list.
        if parts.iter().any(|part| match part {
            TemplatePart::Attribute(name) => data
                .and_then(|data| data.get_value(name.replace(".", "\\.").as_str()))
                .is_none_or(|v| v.as_str().is_none()),
            _ => false,
        }) {
            continue;
        }

        // Build + return a name using this template.
        let mut name = String::new();
        parts.iter().for_each(|part| match part {
            TemplatePart::Literal(str) => name.push_str(str),
            TemplatePart::Attribute(attr) => {
                if let Some(str_val) = data
                    .and_then(|data| data.get_value(attr.replace(".", "\\.").as_str()))
                    .and_then(|v| v.as_str())
                {
                    name.push_str(str_val);
                }
            }
        });
        return Some(name);
    }

    // Name template lists typically have a final entry without attributes, but
    // in case we don't, fall back to op again here.
    Some(op.to_owned())
}

mod parser {
    use pest_derive::Parser;

    #[derive(Parser)]
    #[grammar = "name_template.pest"]
    pub struct TemplateParser;
}

use self::parser::{Rule, TemplateParser};

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
