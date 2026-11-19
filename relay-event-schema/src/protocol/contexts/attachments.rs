use relay_protocol::{Annotated, Array, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Information about attachments of the event that were not stored.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct AttachmentsContext {
    /// Attachments that Relay rate limited.
    #[metastructure(pii = "false")]
    pub rate_limited: Annotated<Array<RateLimitedAttachment>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "false")]
    pub other: Object<Value>,
}

/// An attachment that Relay rate limited.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct RateLimitedAttachment {
    /// The attachment type, for example `event.minidump`.
    #[metastructure(field = "type", pii = "false")]
    pub ty: Annotated<String>,

    /// The reason code of the quota that rate limited the attachment.
    #[metastructure(pii = "false")]
    pub reason_code: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "false")]
    pub other: Object<Value>,
}

impl super::DefaultContext for AttachmentsContext {
    fn default_key() -> &'static str {
        "attachments"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Attachments(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Attachments(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Attachments(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Attachments(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_attachments_context_roundtrip() {
        let json = r#"{
  "rate_limited": [
    {
      "type": "event.minidump",
      "reason_code": "attachment_usage_exceeded"
    }
  ],
  "type": "attachments"
}"#;
        let context = Annotated::new(Context::Attachments(Box::new(AttachmentsContext {
            rate_limited: Annotated::new(vec![Annotated::new(RateLimitedAttachment {
                ty: Annotated::new("event.minidump".to_owned()),
                reason_code: Annotated::new("attachment_usage_exceeded".to_owned()),
                other: Object::new(),
            })]),
            other: Object::new(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
