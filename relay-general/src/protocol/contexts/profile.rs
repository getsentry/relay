use crate::types::{Annotated, Error, FromValue, Value};
use once_cell::sync::OnceCell;
use regex::Regex;

#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ProfileId(pub String);

impl FromValue for ProfileId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                static PROFILE_ID: OnceCell<Regex> = OnceCell::new();
                let regex = PROFILE_ID.get_or_init(|| Regex::new("^[a-fA-F0-9]{32}$").unwrap());

                if !regex.is_match(&value) || value.bytes().all(|x| x == b'0') {
                    meta.add_error(Error::invalid("not a valid profile id"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                } else {
                    Annotated(Some(ProfileId(value.to_ascii_lowercase())), meta)
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("profile id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// Profile context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
// #[metastructure(process_func = "process_trace_context")]
pub struct ProfileContext {
    /// The profile ID.
    #[metastructure(required = "true")]
    pub profile_id: Annotated<ProfileId>,
}

impl ProfileContext {
    /// The key under which a trace context is generally stored (in `Contexts`)
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
            profile_id: Annotated::new(ProfileId("4c79f60c11214eb38604f4ae0781bfb2".into())),
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
            profile_id: Annotated::new(ProfileId("4c79f60c11214eb38604f4ae0781bfb2".into())),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
    }
}
