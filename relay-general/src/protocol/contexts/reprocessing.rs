use crate::types::{Annotated, Object, Value};

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

impl ReprocessingContext {
    /// The key under which a reprocessing context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "reprocessing"
    }
}

#[test]
fn test_reprocessing_context_roundtrip() {
    let json = r#"{
  "original_issue_id": 123,
  "original_primary_hash": "9f3ee8ff49e6ca0a2bee80d76fee8f0c",
  "random": "stuff",
  "type": "reprocessing"
}"#;
    use crate::protocol::Context;
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
