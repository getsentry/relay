use crate::processor::ProcessValue;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

/// Spring context.
///
/// The Spring context contains attributes that are specific to Spring / Spring Boot applications.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct SpringContext {
    /// A list of the active Spring profiles.
    pub active_profiles: Annotated<Vec<Annotated<String>>>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for SpringContext {
    fn default_key() -> &'static str {
        "spring"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Spring(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Spring(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Spring(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Spring(Box::new(self))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_spring_context_roundtrip() {
        let json = r#"{
  "active_profiles": [
    "some",
    "profiles"
  ],
  "type": "spring"
}"#;

        let active_profiles = Annotated::new(vec![
            Annotated::new("some".to_owned()),
            Annotated::new("profiles".to_owned()),
        ]);
        let context = Annotated::new(Context::Spring(Box::new(SpringContext {
            active_profiles,
            other: Object::default(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
