#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;

/// Operating system information.
///
/// OS context describes the operating system on which the event was created. In web contexts, this
/// is the operating system of the browser (generally pulled from the User-Agent string).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct OsContext {
    /// Name of the operating system.
    pub name: Annotated<String>,

    /// Version of the operating system.
    pub version: Annotated<String>,

    /// Internal build number of the operating system.
    #[metastructure(pii = "maybe")]
    pub build: Annotated<LenientString>,

    /// Current kernel version.
    ///
    /// This is typically the entire output of the `uname` syscall.
    #[metastructure(pii = "maybe")]
    pub kernel_version: Annotated<String>,

    /// Indicator if the OS is rooted (mobile mostly).
    pub rooted: Annotated<bool>,

    /// Unprocessed operating system info.
    ///
    /// An unprocessed description string obtained by the operating system. For some well-known
    /// runtimes, Sentry will attempt to parse `name` and `version` from this string, if they are
    /// not explicitly given.
    #[metastructure(pii = "maybe")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for OsContext {
    fn default_key() -> &'static str {
        "os"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Os(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Os(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Os(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Os(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_os_context_roundtrip() {
        let json = r#"{
  "name": "iOS",
  "version": "11.4.2",
  "build": "FEEDFACE",
  "kernel_version": "17.4.0",
  "rooted": true,
  "raw_description": "iOS 11.4.2 FEEDFACE (17.4.0)",
  "other": "value",
  "type": "os"
}"#;
        let context = Annotated::new(Context::Os(Box::new(OsContext {
            name: Annotated::new("iOS".to_string()),
            version: Annotated::new("11.4.2".to_string()),
            build: Annotated::new(LenientString("FEEDFACE".to_string())),
            kernel_version: Annotated::new("17.4.0".to_string()),
            rooted: Annotated::new(true),
            raw_description: Annotated::new("iOS 11.4.2 FEEDFACE (17.4.0)".to_string()),
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
