#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;

/// Runtime information.
///
/// Runtime context describes a runtime in more detail. Typically, this context is present in
/// `contexts` multiple times if multiple runtimes are involved (for instance, if you have a
/// JavaScript application running on top of JVM).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct RuntimeContext {
    /// Runtime name.
    pub name: Annotated<String>,

    /// Runtime version string.
    pub version: Annotated<String>,

    /// Application build string, if it is separate from the version.
    #[metastructure(pii = "maybe")]
    pub build: Annotated<LenientString>,

    /// Unprocessed runtime info.
    ///
    /// An unprocessed description string obtained by the runtime. For some well-known runtimes,
    /// Sentry will attempt to parse `name` and `version` from this string, if they are not
    /// explicitly given.
    #[metastructure(pii = "maybe")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for RuntimeContext {
    fn default_key() -> &'static str {
        "runtime"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Runtime(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Runtime(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Runtime(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Runtime(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_runtime_context_roundtrip() {
        let json = r#"{
  "name": "rustc",
  "version": "1.27.0",
  "build": "stable",
  "raw_description": "rustc 1.27.0 stable",
  "other": "value",
  "type": "runtime"
}"#;
        let context = Annotated::new(Context::Runtime(Box::new(RuntimeContext {
            name: Annotated::new("rustc".to_string()),
            version: Annotated::new("1.27.0".to_string()),
            build: Annotated::new(LenientString("stable".to_string())),
            raw_description: Annotated::new("rustc 1.27.0 stable".to_string()),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
