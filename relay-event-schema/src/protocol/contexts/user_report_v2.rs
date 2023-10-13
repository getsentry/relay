#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Feedback context.
///
/// This contexts contains user feedback specific attributes.
/// We don't PII scrub contact_email as that is provided by the user.
/// TODO(jferg): rename to FeedbackContext once old UserReport logic is deprecated.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct UserReportV2Context {
    /// The feedback message which contains what the user has to say.
    pub message: Annotated<String>,

    /// an email optionally provided by the user, which can be different from user.email
    #[metastructure(pii = "false")]
    pub contact_email: Annotated<String>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl super::DefaultContext for UserReportV2Context {
    fn default_key() -> &'static str {
        "feedback"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::UserReportV2(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::UserReportV2(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::UserReportV2(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::UserReportV2(Box::new(self))
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
  "type": "userreportv2"
}"#;
        let context = Annotated::new(Context::UserReportV2(Box::new(UserReportV2Context {
            message: Annotated::new("test message".to_string()),
            contact_email: Annotated::new("test@test.com".to_string()),
            other: Object::default(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
