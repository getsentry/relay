#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Auxilliary data that Sentry attaches for reprocessed events.
// This context is explicitly typed out such that we can disable datascrubbing for it, and for
// documentation. We need to disble datascrubbing because it can retract information from the
// context that is necessary for basic operation, or worse, mangle it such that the Snuba consumer
// crashes: https://github.com/getsentry/snuba/pull/1896/
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ReprocessingContext {
    /// The issue ID that this event originally belonged to.
    #[metastructure(pii = "false")]
    pub original_issue_id: Annotated<u64>,

    #[metastructure(pii = "false")]
    pub original_primary_hash: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "false")]
    pub other: Object<Value>,
}

impl super::DefaultContext for ReprocessingContext {
    fn default_key() -> &'static str {
        "reprocessing"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Reprocessing(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Reprocessing(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Reprocessing(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Reprocessing(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_reprocessing_context_roundtrip() {
        let json = r#"{
  "original_issue_id": 123,
  "original_primary_hash": "9f3ee8ff49e6ca0a2bee80d76fee8f0c",
  "random": "stuff",
  "type": "reprocessing"
}"#;
        let context = Annotated::new(Context::Reprocessing(Box::new(ReprocessingContext {
            original_issue_id: Annotated::new(123),
            original_primary_hash: Annotated::new("9f3ee8ff49e6ca0a2bee80d76fee8f0c".to_string()),
            other: {
                let mut map = Object::new();
                map.insert(
                    "random".to_string(),
                    Annotated::new(Value::String("stuff".to_string())),
                );
                map
            },
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
