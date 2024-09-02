use relay_protocol::{Annotated, Empty, FromValue, IntoValue};

use crate::processor::ProcessValue;
use crate::protocol::EventId;

/// Profile context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct ProfileContext {
    /// The profile ID.
    pub profile_id: Annotated<EventId>,

    /// The profiler ID.
    pub profiler_id: Annotated<EventId>,
}

impl super::DefaultContext for ProfileContext {
    fn default_key() -> &'static str {
        "profile"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Profile(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Profile(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Profile(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Profile(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn roundtrip() {
        let json = r#"{
  "profile_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "type": "profile"
}"#;
        let context = Annotated::new(Context::Profile(Box::new(ProfileContext {
            profile_id: Annotated::new(EventId(
                "4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap(),
            )),
            ..ProfileContext::default()
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }

    #[test]
    fn normalization() {
        let json = r#"{
  "profile_id": "4C79F60C11214EB38604F4AE0781BFB2",
  "type": "profile"
}"#;
        let context = Annotated::new(Context::Profile(Box::new(ProfileContext {
            profile_id: Annotated::new(EventId(
                "4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap(),
            )),
            ..ProfileContext::default()
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn context_with_profiler_id() {
        let json = r#"{"profiler_id": "4C79F60C11214EB38604F4AE0781BFB2", "type": "profile"}"#;
        let context = Annotated::new(Context::Profile(Box::new(ProfileContext {
            profiler_id: Annotated::new(EventId(
                "4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap(),
            )),
            ..ProfileContext::default()
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
    }
}
