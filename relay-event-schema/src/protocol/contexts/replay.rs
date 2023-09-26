#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::EventId;

/// Replay context.
///
/// The replay context contains the replay_id of the session replay if the event
/// occurred during a replay. The replay_id is added onto the dynamic sampling context
/// on the javascript SDK which propagates it through the trace. In relay, we take
/// this value from the DSC and create a context which contains only the replay_id
/// This context is never set on the client for events, only on relay.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ReplayContext {
    /// The replay ID.
    pub replay_id: Annotated<EventId>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl super::DefaultContext for ReplayContext {
    fn default_key() -> &'static str {
        "replay"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Replay(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Replay(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Replay(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Replay(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_trace_context_roundtrip() {
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
    fn test_replay_context_normalization() {
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
