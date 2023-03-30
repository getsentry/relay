use crate::protocol::EventId;
use crate::types::{Annotated, Object, Value};

/// Replay context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ReplayContext {
    /// The replay ID.
    pub replay_id: Annotated<EventId>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl ReplayContext {
    /// The key under which a replay context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "replay"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    pub(crate) fn test_trace_context_roundtrip() {
        let json = r#"{
  "replay_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "type": "replay"
}"#;
        let context = Annotated::new(Context::Replay(Box::new(ReplayContext {
            replay_id: Annotated::new(EventId("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap())),
            other: Object::default(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }

    #[test]
    pub(crate) fn test_replay_context_normalization() {
        let json = r#"{
  "replay_id": "4C79F60C11214EB38604F4AE0781BFB2",
  "type": "replay"
}"#;
        let context = Annotated::new(Context::Replay(Box::new(ReplayContext {
            replay_id: Annotated::new(EventId("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap())),
            other: Object::default(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
    }
}
