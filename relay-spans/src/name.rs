use relay_conventions::attributes::{SENTRY__DESCRIPTION, SENTRY__OP, SENTRY__ORIGIN};
use relay_conventions::name::name_for_op_and_attributes;
use relay_event_schema::protocol::Attributes;

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
    Some(
        name_for_op_and_attributes(op, |name| attributes.get_value_with_deprecations(name))
            .unwrap_or(op.to_owned()),
    )
}

#[cfg(test)]
mod tests {
    use relay_protocol::{Annotated, Value};

    use crate::derive_description_for_v2_span;

    use super::*;

    fn assert_template_output(attributes: &Attributes, name: &str, description: Option<&str>) {
        let original = attributes.clone();
        assert_eq!(name_for_attributes(attributes).as_deref(), Some(name));
        assert_eq!(
            derive_description_for_v2_span(attributes, &Annotated::empty()).as_deref(),
            description
        );
        assert_eq!(attributes, &original);
    }

    #[test]
    fn generated_templates_resolve_deprecated_inputs_without_renaming() {
        type TemplateCase<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a str, Option<&'a str>);
        let cases: &[TemplateCase<'_>] = &[
            (
                "db",
                &[("db.query.summary", "SELECT users")],
                "SELECT users",
                None,
            ),
            (
                "db",
                &[
                    ("db.operation.name", "SELECT"),
                    ("db.collection.name", "users"),
                ],
                "SELECT users",
                None,
            ),
            (
                "db",
                &[
                    ("db.operation.name", "SELECT"),
                    ("db.namespace", "analytics"),
                ],
                "SELECT analytics",
                None,
            ),
            (
                "db",
                &[("db.system.name", "postgresql")],
                "postgresql",
                None,
            ),
            (
                "db",
                &[("server.address", "localhost"), ("server.port", "5432")],
                "localhost:5432",
                None,
            ),
            (
                "http.client",
                &[
                    ("http.request.method", "GET"),
                    ("url.full", "https://example.com/users"),
                ],
                "GET",
                Some("GET https://example.com/users"),
            ),
            (
                "queue.publish",
                &[
                    ("messaging.operation.type", "send"),
                    ("messaging.destination.name", "jobs"),
                ],
                "send jobs",
                Some("jobs"),
            ),
            (
                "queue.publish",
                &[
                    ("messaging.operation.type", "send"),
                    ("server.address", "localhost"),
                    ("server.port", "5672"),
                ],
                "send localhost:5672",
                None,
            ),
            (
                "gen_ai.chat",
                &[
                    ("gen_ai.operation.name", "chat"),
                    ("gen_ai.request.model", "gpt-4"),
                ],
                "chat gpt-4",
                Some("chat gpt-4"),
            ),
            (
                "gen_ai.invoke_agent",
                &[
                    ("gen_ai.operation.name", "invoke_agent"),
                    ("gen_ai.pipeline.name", "pipeline"),
                ],
                "invoke_agent pipeline",
                Some("invoke_agent"),
            ),
            (
                "gen_ai.execute_tool",
                &[
                    ("gen_ai.operation.name", "execute_tool"),
                    ("gen_ai.tool.name", "weather"),
                ],
                "execute_tool weather",
                Some("execute_tool weather"),
            ),
            (
                "mcp.server",
                &[
                    ("mcp.method.name", "prompts/get"),
                    ("gen_ai.prompt.name", "analyze-code"),
                ],
                "prompts/get analyze-code",
                Some("prompts/get analyze-code"),
            ),
            (
                "mcp.server",
                &[
                    ("mcp.method.name", "tools/call"),
                    ("gen_ai.tool.name", "weather"),
                ],
                "tools/call weather",
                Some("tools/call weather"),
            ),
            (
                "function",
                &[("code.function.name", "handler")],
                "handler",
                Some("handler"),
            ),
            (
                "function.aws",
                &[("faas.name", "handler")],
                "handler",
                Some("handler"),
            ),
            (
                "router",
                &[("router.navigation.route.id", "UserProfile")],
                "UserProfile",
                Some("UserProfile"),
            ),
            (
                "ui.action.click",
                &[("router.navigation.route.id", "UserProfile")],
                "UserProfile",
                Some("UserProfile"),
            ),
            (
                "graphql.query",
                &[("graphql.document", "query { users }")],
                "GraphQL Operation",
                Some("query { users }"),
            ),
            (
                "ui.webvital.lcp",
                &[("browser.web_vital.lcp.element", "body > img")],
                "Largest contentful paint",
                Some("body > img"),
            ),
            (
                "ui.webvital.cls",
                &[("browser.web_vital.cls.source.1", "body > div")],
                "Layout shift",
                Some("body > div"),
            ),
            (
                "resource.script",
                &[("url.full", "https://example.com/app.js")],
                "Resource",
                Some("https://example.com/app.js"),
            ),
            (
                "navigation",
                &[("url.full", "https://example.com/users")],
                "Navigation",
                Some("https://example.com/users"),
            ),
            (
                "browser.request",
                &[("url.full", "https://example.com/users")],
                "Request",
                Some("https://example.com/users"),
            ),
        ];
        for (op, fields, name, description) in cases {
            let mut current = Attributes::new();
            current.insert(SENTRY__OP, *op);
            for (key, value) in *fields {
                current.insert(*key, *value);
            }
            assert_template_output(&current, name, *description);
            let mut all_aliases = current.clone();
            for (key, _) in *fields {
                for alias in relay_conventions::attribute_names(key).skip(1) {
                    let mut attributes = current.clone();
                    let value = attributes.0.remove(*key).unwrap();
                    attributes.0.insert(alias.to_string(), value.clone());
                    assert_template_output(&attributes, name, *description);
                    attributes.0.insert((*key).to_owned(), value);
                    attributes.insert(alias.to_string(), "conflicting alias value");
                    assert_template_output(&attributes, name, *description);
                }
                if let Some(alias) = relay_conventions::attribute_names(key).nth(1) {
                    let value = all_aliases.0.remove(*key).unwrap();
                    all_aliases.0.insert(alias.into_owned(), value);
                }
            }
            assert_template_output(&all_aliases, name, *description);
        }
    }

    #[test]
    fn generated_templates_format_numeric_aliases_and_keep_template_priority() {
        for alias in ["port", "net.host.port"] {
            let mut attributes = Attributes::new();
            attributes.insert(SENTRY__OP, "db");
            attributes.insert("server_name", "localhost");
            attributes.insert(alias, 5432_i64);
            assert_template_output(&attributes, "localhost:5432", None);
            attributes.insert("db.operation", "SELECT");
            attributes.insert("db.mongodb.collection", "users");
            assert_template_output(&attributes, "SELECT users", None);
            attributes.insert("db.query.summary", "SELECT widgets");
            assert_template_output(&attributes, "SELECT widgets", None);
        }
    }

    #[test]
    fn generated_templates_do_not_fall_back_from_unsupported_current_values_to_aliases() {
        let mut attributes = Attributes::new();
        attributes.insert(SENTRY__OP, "function");
        attributes.insert("django.function_name", "handler");
        let mut current: relay_event_schema::protocol::Attribute = "current".into();
        current.value.value = Annotated::new(Value::Object(Default::default()));
        attributes
            .0
            .insert("code.function.name".to_owned(), Annotated::new(current));
        assert_template_output(&attributes, "Function execution", None);
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
