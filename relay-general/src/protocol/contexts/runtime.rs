use crate::protocol::LenientString;
use crate::types::{Annotated, Object, Value};

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

impl RuntimeContext {
    /// The key under which a runtime context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "runtime"
    }
}

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
    use crate::protocol::Context;
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
