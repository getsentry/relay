use crate::types::Annotated;

use crate::protocol::EventId;

/// Profile context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ProfileContext {
    /// The profile ID.
    #[metastructure(required = "true")]
    pub profile_id: Annotated<EventId>,
}

impl ProfileContext {
    /// The key under which a profile context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "profile"
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::Context;

    use super::*;

    #[test]
    pub(crate) fn test_trace_context_roundtrip() {
        let json = r#"{
  "profile_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "type": "profile"
}"#;
        let context = Annotated::new(Context::Profile(Box::new(ProfileContext {
            profile_id: Annotated::new(EventId(
                uuid::Uuid::parse_str("4c79f60c11214eb38604f4ae0781bfb2").unwrap(),
            )),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }

    #[test]
    pub(crate) fn test_trace_context_normalization() {
        let json = r#"{
  "profile_id": "4C79F60C11214EB38604F4AE0781BFB2",
  "type": "profile"
}"#;
        let context = Annotated::new(Context::Profile(Box::new(ProfileContext {
            profile_id: Annotated::new(EventId(
                uuid::Uuid::parse_str("4c79f60c11214eb38604f4ae0781bfb2").unwrap(),
            )),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
    }
}
