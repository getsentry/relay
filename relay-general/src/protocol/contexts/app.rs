use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;
use crate::types::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};

/// Application information.
///
/// App context describes the application. As opposed to the runtime, this is the actual
/// application that was running and carries metadata about the current session.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct AppContext {
    /// Start time of the app.
    ///
    /// Formatted UTC timestamp when the user started the application.
    #[metastructure(pii = "maybe")]
    pub app_start_time: Annotated<String>,

    /// Application-specific device identifier.
    #[metastructure(pii = "maybe")]
    pub device_app_hash: Annotated<String>,

    /// String identifying the kind of build. For example, `testflight`.
    pub build_type: Annotated<String>,

    /// Version-independent application identifier, often a dotted bundle ID.
    pub app_identifier: Annotated<String>,

    /// Application name as it appears on the platform.
    pub app_name: Annotated<String>,

    /// Application version as it appears on the platform.
    pub app_version: Annotated<String>,

    /// Internal build ID as it appears on the platform.
    pub app_build: Annotated<LenientString>,

    /// Amount of memory used by the application in bytes.
    pub app_memory: Annotated<u64>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl AppContext {
    /// The key under which an app context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "app"
    }
}

#[test]
fn test_app_context_roundtrip() {
    let json = r#"{
  "app_start_time": "2018-02-08T22:21:57Z",
  "device_app_hash": "4c793e3776474877ae30618378e9662a",
  "build_type": "testflight",
  "app_identifier": "foo.bar.baz",
  "app_name": "Baz App",
  "app_version": "1.0",
  "app_build": "100001",
  "app_memory": 22883948,
  "other": "value",
  "type": "app"
}"#;
    let context = Annotated::new(Context::App(Box::new(AppContext {
        app_start_time: Annotated::new("2018-02-08T22:21:57Z".to_string()),
        device_app_hash: Annotated::new("4c793e3776474877ae30618378e9662a".to_string()),
        build_type: Annotated::new("testflight".to_string()),
        app_identifier: Annotated::new("foo.bar.baz".to_string()),
        app_name: Annotated::new("Baz App".to_string()),
        app_version: Annotated::new("1.0".to_string()),
        app_build: Annotated::new("100001".to_string().into()),
        app_memory: Annotated::new(22883948),
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

#[test]
fn test_browser_context_roundtrip() {
    let json = r#"{
  "name": "Google Chrome",
  "version": "67.0.3396.99",
  "other": "value",
  "type": "browser"
}"#;
    let context = Annotated::new(Context::Browser(Box::new(BrowserContext {
        name: Annotated::new("Google Chrome".to_string()),
        version: Annotated::new("67.0.3396.99".to_string()),
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
