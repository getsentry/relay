use chrono::{DateTime, Utc};

use crate::types::{Annotated, FromValue, Object, Value};

/// Device information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct DeviceContext {
    /// Name of the device.
    pub name: Annotated<String>,

    /// Family of the device model.
    pub family: Annotated<String>,

    /// Device model (human readable).
    pub model: Annotated<String>,

    /// Device model (internal identifier).
    pub model_id: Annotated<String>,

    /// Native cpu architecture of the device.
    pub arch: Annotated<String>,

    /// Current battery level (0-100).
    pub battery_level: Annotated<f64>,

    /// Current screen orientation.
    pub orientation: Annotated<String>,

    /// Simulator/prod indicator.
    pub simulator: Annotated<bool>,

    /// Total memory available in bytes.
    pub memory_size: Annotated<u64>,

    /// How much memory is still available in bytes.
    pub free_memory: Annotated<u64>,

    /// How much memory is usable for the app in bytes.
    pub usable_memory: Annotated<u64>,

    /// Total storage size of the device in bytes.
    pub storage_size: Annotated<u64>,

    /// How much storage is free in bytes.
    pub free_storage: Annotated<u64>,

    /// Total size of the attached external storage in bytes (eg: android SDK card).
    pub external_storage_size: Annotated<u64>,

    /// Free size of the attached external storage in bytes (eg: android SDK card).
    pub external_free_storage: Annotated<u64>,

    /// Indicator when the device was booted.
    pub boot_time: Annotated<DateTime<Utc>>,

    /// Timezone of the device.
    pub timezone: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Operating system information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct OsContext {
    /// Name of the operating system.
    #[metastructure(max_chars = "summary")]
    pub name: Annotated<String>,

    /// Version of the operating system.
    #[metastructure(max_chars = "summary")]
    pub version: Annotated<String>,

    /// Internal build number of the operating system.
    #[metastructure(max_chars = "summary")]
    pub build: Annotated<String>,

    /// Current kernel version.
    #[metastructure(max_chars = "summary")]
    pub kernel_version: Annotated<String>,

    /// Indicator if the OS is rooted (mobile mostly).
    pub rooted: Annotated<bool>,

    /// Unprocessed operating system info.
    #[metastructure(max_chars = "summary")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Runtime information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct RuntimeContext {
    /// Runtime name.
    #[metastructure(max_chars = "summary")]
    pub name: Annotated<String>,

    /// Runtime version.
    #[metastructure(max_chars = "summary")]
    pub version: Annotated<String>,

    /// Unprocessed runtime info.
    #[metastructure(max_chars = "summary")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Application information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct AppContext {
    /// Start time of the app.
    pub app_start_time: Annotated<DateTime<Utc>>,

    /// Device app hash (app specific device ID)
    #[metastructure(pii_kind = "id")]
    #[metastructure(max_chars = "summary")]
    pub device_app_hash: Annotated<String>,

    /// Build identicator.
    #[metastructure(max_chars = "summary")]
    pub build_type: Annotated<String>,

    /// App identifier (dotted bundle id).
    pub app_identifier: Annotated<String>,

    /// Application name as it appears on the platform.
    pub app_name: Annotated<String>,

    /// Application version as it appears on the platform.
    pub app_version: Annotated<String>,

    /// Internal build ID as it appears on the platform.
    #[metastructure(max_chars = "summary")]
    pub app_build: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Web browser information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct BrowserContext {
    /// Runtime name.
    #[metastructure(max_chars = "summary")]
    pub name: Annotated<String>,

    /// Runtime version.
    #[metastructure(max_chars = "summary")]
    pub version: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// A context describes environment info (e.g. device, os or browser).
#[derive(Debug, Clone, PartialEq, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_context")]
pub enum Context {
    /// Device information.
    Device(Box<DeviceContext>),
    /// Operating system information.
    Os(Box<OsContext>),
    /// Runtime information.
    Runtime(Box<RuntimeContext>),
    /// Application information.
    App(Box<AppContext>),
    /// Web browser information.
    Browser(Box<BrowserContext>),
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(fallback_variant)]
    Other(Object<Value>),
}

/// An object holding multiple contexts.
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
#[metastructure(process_func = "context")]
pub struct Contexts(pub Object<Context>);

impl std::ops::Deref for Contexts {
    type Target = Object<Context>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Contexts {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromValue for Contexts {
    fn from_value(mut annotated: Annotated<Value>) -> Annotated<Self> {
        if let Annotated(Some(Value::Object(ref mut items)), _) = annotated {
            for (key, value) in items.iter_mut() {
                if let Annotated(Some(Value::Object(ref mut items)), _) = value {
                    if !items.contains_key("type") {
                        items.insert(
                            "type".to_string(),
                            Annotated::new(Value::String(key.to_string())),
                        );
                    }
                }
            }
        }
        FromValue::from_value(annotated).map_value(Contexts)
    }
}

#[test]
fn test_device_context_roundtrip() {
    let json = r#"{
  "name": "iphone",
  "family": "iphone",
  "model": "iphone7,3",
  "model_id": "AH223",
  "arch": "arm64",
  "battery_level": 58.5,
  "orientation": "landscape",
  "simulator": true,
  "memory_size": 3137978368,
  "free_memory": 322781184,
  "usable_memory": 2843525120,
  "storage_size": 63989469184,
  "free_storage": 31994734592,
  "external_storage_size": 2097152,
  "external_free_storage": 2097152,
  "boot_time": 1518094332.0,
  "timezone": "Europe/Vienna",
  "other": "value",
  "type": "device"
}"#;
    let context = Annotated::new(Context::Device(Box::new(DeviceContext {
        name: Annotated::new("iphone".to_string()),
        family: Annotated::new("iphone".to_string()),
        model: Annotated::new("iphone7,3".to_string()),
        model_id: Annotated::new("AH223".to_string()),
        arch: Annotated::new("arm64".to_string()),
        battery_level: Annotated::new(58.5),
        orientation: Annotated::new("landscape".to_string()),
        simulator: Annotated::new(true),
        memory_size: Annotated::new(3_137_978_368),
        free_memory: Annotated::new(322_781_184),
        usable_memory: Annotated::new(2_843_525_120),
        storage_size: Annotated::new(63_989_469_184),
        free_storage: Annotated::new(31_994_734_592),
        external_storage_size: Annotated::new(2_097_152),
        external_free_storage: Annotated::new(2_097_152),
        boot_time: Annotated::new("2018-02-08T12:52:12Z".parse().unwrap()),
        timezone: Annotated::new("Europe/Vienna".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

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
        build: Annotated::new("FEEDFACE".to_string()),
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

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

#[test]
fn test_runtime_context_roundtrip() {
    let json = r#"{
  "name": "rustc",
  "version": "1.27.0",
  "raw_description": "rustc 1.27.0 stable",
  "other": "value",
  "type": "runtime"
}"#;
    let context = Annotated::new(Context::Runtime(Box::new(RuntimeContext {
        name: Annotated::new("rustc".to_string()),
        version: Annotated::new("1.27.0".to_string()),
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

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

#[test]
fn test_app_context_roundtrip() {
    let json = r#"{
  "app_start_time": 1518128517.0,
  "device_app_hash": "4c793e3776474877ae30618378e9662a",
  "build_type": "testflight",
  "app_identifier": "foo.bar.baz",
  "app_name": "Baz App",
  "app_version": "1.0",
  "app_build": "100001",
  "other": "value",
  "type": "app"
}"#;
    let context = Annotated::new(Context::App(Box::new(AppContext {
        app_start_time: Annotated::new("2018-02-08T22:21:57Z".parse().unwrap()),
        device_app_hash: Annotated::new("4c793e3776474877ae30618378e9662a".to_string()),
        build_type: Annotated::new("testflight".to_string()),
        app_identifier: Annotated::new("foo.bar.baz".to_string()),
        app_name: Annotated::new("Baz App".to_string()),
        app_version: Annotated::new("1.0".to_string()),
        app_build: Annotated::new("100001".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
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

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json_pretty().unwrap());
}

#[test]
fn test_other_context_roundtrip() {
    use crate::types::Map;

    let json = r#"{"other":"value","type":"mytype"}"#;
    let context = Annotated::new(Context::Other({
        let mut map = Map::new();
        map.insert(
            "other".to_string(),
            Annotated::new(Value::String("value".to_string())),
        );
        map.insert(
            "type".to_string(),
            Annotated::new(Value::String("mytype".to_string())),
        );
        map
    }));

    assert_eq_dbg!(context, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, context.to_json().unwrap());
}

#[test]
fn test_untagged_context_deserialize() {
    let json = r#"{"os": {"name": "Linux"}}"#;

    let os_context = Annotated::new(Context::Os(Box::new(OsContext {
        name: Annotated::new("Linux".to_string()),
        ..Default::default()
    })));
    let mut map = Object::new();
    map.insert("os".to_string(), os_context);
    let contexts = Annotated::new(Contexts(map));

    assert_eq_dbg!(contexts, Annotated::from_json(json).unwrap());
}

#[test]
fn test_context_processing() {
    use crate::processor::{ProcessResult, ProcessingState, Processor};
    use crate::protocol::Event;
    use crate::types::Meta;

    let mut event = Annotated::new(Event {
        contexts: Annotated::new(Contexts({
            let mut contexts = Object::new();
            contexts.insert(
                "runtime".to_owned(),
                Annotated::new(Context::Runtime(Box::new(RuntimeContext {
                    name: Annotated::new("php".to_owned()),
                    version: Annotated::new("7.1.20-1+ubuntu16.04.1+deb.sury.org+1".to_owned()),
                    ..Default::default()
                }))),
            );
            contexts
        })),
        ..Default::default()
    });

    struct FooProcessor {
        called: bool,
    }

    impl Processor for FooProcessor {
        #[inline]
        fn process_context(
            &mut self,
            _value: &mut Context,
            _meta: &mut Meta,
            _state: ProcessingState,
        ) -> ProcessResult {
            self.called = true;
            ProcessResult::default()
        }
    }

    let mut processor = FooProcessor { called: false };
    crate::processor::process_value(&mut event, &mut processor, ProcessingState::default());
    assert!(processor.called);
}
