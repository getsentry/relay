use relay_conventions::attributes::*;
use relay_event_schema::protocol::{Attributes, SpanKind};
use relay_protocol::Annotated;

/// Generates a `sentry.op` attribute for V2 span, if possible.
///
/// This uses attributes of the span to figure out an appropriate operation name, inferring what the
/// SDK might have sent. Reliably infers an op for well-known OTel span kinds like database
/// operations. Does not infer an op for frontend and mobile spans sent by Sentry SDKs that don't
/// have an OTel equivalent (e.g., resource loads).
pub fn derive_op_for_v2_span(attributes: &Annotated<Attributes>) -> String {
    // NOTE: `op` is not a required field in the SDK, so the fallback is an empty string.
    let op = String::from("default");

    let Some(attributes) = attributes.value() else {
        return op;
    };

    if attributes
        .get_value_with_deprecations(HTTP__REQUEST__METHOD)
        .is_some()
    {
        let kind = attributes
            .get_value_with_deprecations(SENTRY__KIND)
            .and_then(|v| v.as_str());
        return match kind {
            Some(kind) if kind == SpanKind::Client.as_str() => String::from("http.client"),
            Some(kind) if kind == SpanKind::Server.as_str() => String::from("http.server"),
            _ => {
                if attributes
                    .get_value_with_deprecations(SENTRY__HTTP__PREFETCH)
                    .is_some()
                {
                    String::from("http.prefetch")
                } else {
                    String::from("http")
                }
            }
        };
    }

    if attributes
        .get_value_with_deprecations(DB__SYSTEM__NAME)
        .is_some()
    {
        return String::from("db");
    }

    if let Some(operation) = attributes
        .get_value_with_deprecations(GEN_AI__OPERATION__NAME)
        .and_then(|v| v.as_str())
    {
        return format!("gen_ai.{operation}");
    }

    if attributes
        .get_value_with_deprecations(RPC__SERVICE)
        .is_some()
    {
        return String::from("rpc");
    }

    if attributes
        .get_value_with_deprecations(MESSAGING__SYSTEM)
        .is_some()
    {
        return String::from("message");
    }

    if let Some(faas_trigger) = attributes
        .get_value_with_deprecations(FAAS__TRIGGER)
        .and_then(|v| v.as_str())
    {
        return faas_trigger.to_owned();
    }

    op
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_event_schema::protocol::Attribute;

    fn op_for(pairs: &[(&str, &str)]) -> String {
        let attributes = Attributes::from_iter(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), Annotated::new((*v).to_owned().into()))),
        );
        derive_op_for_v2_span(&Annotated::new(attributes))
    }

    #[test]
    fn infers_http_op_from_deprecated_method_and_kind_without_renaming() {
        for method in ["method", "http.method", "http.request_method"] {
            assert_eq!(op_for(&[(method, "GET")]), "http");
            assert_eq!(
                op_for(&[(method, "GET"), ("otel.kind", "client")]),
                "http.client"
            );
            assert_eq!(
                op_for(&[(method, "GET"), ("otel.kind", "server")]),
                "http.server"
            );
        }
    }

    #[test]
    fn infers_db_op_from_deprecated_system_without_renaming() {
        assert_eq!(op_for(&[("db.system", "postgresql")]), "db");
    }

    #[test]
    fn current_kind_takes_precedence_over_deprecated_kind() {
        assert_eq!(
            op_for(&[
                ("method", "GET"),
                ("otel.kind", "server"),
                ("sentry.kind", "client")
            ]),
            "http.client"
        );
    }

    #[test]
    fn inference_does_not_rewrite_attributes() {
        let attributes = Annotated::new(Attributes::from([
            (
                "http.method".to_owned(),
                Annotated::new(Attribute::from("GET")),
            ),
            (
                "otel.kind".to_owned(),
                Annotated::new(Attribute::from("client")),
            ),
        ]));
        let original = attributes.clone();
        assert_eq!(derive_op_for_v2_span(&attributes), "http.client");
        assert_eq!(attributes, original);
    }

    #[test]
    fn test_gen_ai_op_operation_name_takes_precedence_over_provider() {
        assert_eq!(
            op_for(&[
                (GEN_AI__OPERATION__NAME, "chat"),
                (GEN_AI__PROVIDER__NAME, "openai"),
            ]),
            "gen_ai.chat"
        );
        assert_eq!(
            op_for(&[
                (GEN_AI__OPERATION__NAME, "invoke_agent"),
                (GEN_AI__PROVIDER__NAME, "openai"),
            ]),
            "gen_ai.invoke_agent"
        );
    }

    #[test]
    fn test_gen_ai_op_execute_tool_without_provider() {
        assert_eq!(
            op_for(&[(GEN_AI__OPERATION__NAME, "execute_tool")]),
            "gen_ai.execute_tool"
        );
    }

    #[test]
    fn test_gen_ai_provider_does_not_imply_operation() {
        assert_eq!(op_for(&[(GEN_AI__PROVIDER__NAME, "openai")]), "default");
        assert_eq!(
            op_for(&[
                (GEN_AI__PROVIDER__NAME, "openai"),
                (RPC__SERVICE, "example.Service"),
            ]),
            "rpc"
        );
    }
}
