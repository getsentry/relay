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

    if attributes.contains_key(HTTP__REQUEST__METHOD) {
        let kind = attributes.get_value(SENTRY__KIND).and_then(|v| v.as_str());
        return match kind {
            Some(kind) if kind == SpanKind::Client.as_str() => String::from("http.client"),
            Some(kind) if kind == SpanKind::Server.as_str() => String::from("http.server"),
            _ => {
                if attributes.contains_key(SENTRY__HTTP__PREFETCH) {
                    String::from("http.prefetch")
                } else {
                    String::from("http")
                }
            }
        };
    }

    if attributes.contains_key(DB__SYSTEM__NAME) {
        return String::from("db");
    }

    if let Some(operation) = attributes
        .get_value(GEN_AI__OPERATION__NAME)
        .and_then(|v| v.as_str())
    {
        return format!("gen_ai.{operation}");
    }

    if attributes.contains_key(GEN_AI__PROVIDER__NAME) {
        return String::from("gen_ai");
    }

    if attributes.contains_key(RPC__SERVICE) {
        return String::from("rpc");
    }

    if attributes.contains_key(MESSAGING__SYSTEM) {
        return String::from("message");
    }

    if let Some(faas_trigger) = attributes.get_value(FAAS__TRIGGER).and_then(|v| v.as_str()) {
        return faas_trigger.to_owned();
    }

    op
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op_for(pairs: &[(&str, &str)]) -> String {
        let attributes = Attributes::from_iter(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), Annotated::new((*v).to_owned().into()))),
        );
        derive_op_for_v2_span(&Annotated::new(attributes))
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
    fn test_gen_ai_op_falls_back_to_provider() {
        assert_eq!(op_for(&[(GEN_AI__PROVIDER__NAME, "openai")]), "gen_ai");
    }
}
