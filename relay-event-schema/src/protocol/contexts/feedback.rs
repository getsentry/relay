#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Feedback context.
///
/// The replay context contains the replay_id of the session replay if the event
/// occurred during a replay. The replay_id is added onto the dynamic sampling context
/// on the javascript SDK which propagates it through the trace. In relay, we take
/// this value from the DSC and create a context which contains only the replay_id
/// This context is never set on the client for events, only on relay.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct FeedbackContext {
    /// The replay ID.
    pub message: Annotated<String>,

    #[metastructure(pii = "false")]
    pub contact_email: Annotated<String>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl super::DefaultContext for FeedbackContext {
    fn default_key() -> &'static str {
        "feedback"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Feedback(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Feedback(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Feedback(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Feedback(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_feedback_context() {
        let json = r#"{
  "message": "test message",
  "contact_email": "test@test.com",
  "type": "feedback"
}"#;
        let context = Annotated::new(Context::Feedback(Box::new(FeedbackContext {
            message: Annotated::new("test message".to_string()),
            contact_email: Annotated::new("test@test.com".to_string()),
            other: Object::default(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
