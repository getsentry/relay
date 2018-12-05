use debugid::DebugId;
use uuid::Uuid;

use crate::processor::ProcessValue;
use crate::protocol::Addr;
use crate::types::{Annotated, Array, FromValue, Object, ToValue, Value};

/// Holds information about the system SDK.
///
/// This is relevant for iOS and other platforms that have a system
/// SDK.  Not to be confused with the client SDK.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct SystemSdkInfo {
    /// The internal name of the SDK.
    pub sdk_name: Annotated<String>,

    /// The major version of the SDK as integer or 0.
    pub version_major: Annotated<u64>,

    /// The minor version of the SDK as integer or 0.
    pub version_minor: Annotated<u64>,

    /// The patch version of the SDK as integer or 0.
    pub version_patchlevel: Annotated<u64>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Apple debug image in
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct AppleDebugImage {
    /// Path and name of the debug image (required).
    #[metastructure(required = "true")]
    pub name: Annotated<String>,

    /// CPU architecture target.
    pub arch: Annotated<String>,

    /// MachO CPU type identifier.
    pub cpu_type: Annotated<u64>,

    /// MachO CPU subtype identifier.
    pub cpu_subtype: Annotated<u64>,

    /// Starting memory address of the image (required).
    #[metastructure(required = "true")]
    pub image_addr: Annotated<Addr>,

    /// Size of the image in bytes (required).
    #[metastructure(required = "true")]
    pub image_size: Annotated<u64>,

    /// Loading address in virtual memory.
    pub image_vmaddr: Annotated<Addr>,

    /// The unique UUID of the image.
    #[metastructure(required = "true")]
    pub uuid: Annotated<Uuid>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl FromValue for DebugId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(value) => Annotated(Some(value), meta),
                Err(err) => {
                    meta.add_error(err.to_string());
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("debug id", value);
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl ToValue for DebugId {
    fn to_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(self, s)
    }
}

impl ProcessValue for DebugId {}

/// Any debug information file supported by symbolic.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct SymbolicDebugImage {
    /// Path and name of the debug image (required).
    #[metastructure(required = "true")]
    pub name: Annotated<String>,

    /// CPU architecture target.
    pub arch: Annotated<String>,

    /// Starting memory address of the image (required).
    #[metastructure(required = "true")]
    pub image_addr: Annotated<Addr>,

    /// Size of the image in bytes (required).
    #[metastructure(required = "true")]
    pub image_size: Annotated<u64>,

    /// Loading address in virtual memory.
    pub image_vmaddr: Annotated<Addr>,

    /// Unique debug identifier of the image.
    #[metastructure(required = "true")]
    pub id: Annotated<DebugId>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Proguard mapping file.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct ProguardDebugImage {
    /// UUID computed from the file contents.
    #[metastructure(required = "true")]
    pub uuid: Annotated<Uuid>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// A debug information file (debug image).
#[derive(Debug, Clone, PartialEq, ToValue, FromValue, ProcessValue)]
pub enum DebugImage {
    /// Apple debug images (machos).  This is currently also used for non apple platforms with
    /// similar debug setups.
    Apple(Box<AppleDebugImage>),
    /// Symbolic (new style) debug infos.
    Symbolic(Box<SymbolicDebugImage>),
    /// A reference to a proguard debug file.
    Proguard(Box<ProguardDebugImage>),
    /// A debug image that is unknown to this protocol specification.
    #[metastructure(fallback_variant)]
    Other(Object<Value>),
}

/// Debugging and processing meta information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_debug_meta")]
pub struct DebugMeta {
    /// Information about the system SDK (e.g. iOS SDK).
    #[metastructure(field = "sdk_info")]
    pub system_sdk: Annotated<SystemSdkInfo>,

    /// List of debug information files (debug images).
    pub images: Annotated<Array<DebugImage>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[cfg(tests)]
mod tests {
    #[test]
    fn test_debug_image_proguard_roundtrip() {
        let json = r#"{
  "uuid": "395835f4-03e0-4436-80d3-136f0749a893",
  "other": "value",
  "type": "proguard"
}"#;
        let image = Annotated::new(DebugImage::Proguard(Box::new(ProguardDebugImage {
            uuid: Annotated::new(
                "395835f4-03e0-4436-80d3-136f0749a893"
                    .parse::<Uuid>()
                    .unwrap(),
            ),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        })));

        assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, image.to_json_pretty().unwrap());
    }

    #[test]
    fn test_debug_image_apple_roundtrip() {
        let json = r#"{
  "name": "CoreFoundation",
  "arch": "arm64",
  "cpu_type": 1233,
  "cpu_subtype": 3,
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "uuid": "494f3aea-88fa-4296-9644-fa8ef5d139b6",
  "other": "value",
  "type": "apple"
}"#;

        let image = Annotated::new(DebugImage::Apple(Box::new(AppleDebugImage {
            name: Annotated::new("CoreFoundation".to_string()),
            arch: Annotated::new("arm64".to_string()),
            cpu_type: Annotated::new(1233),
            cpu_subtype: Annotated::new(3),
            image_addr: Annotated::new(Addr(0)),
            image_size: Annotated::new(4096),
            image_vmaddr: Annotated::new(Addr(32768)),
            uuid: Annotated::new(
                "494f3aea-88fa-4296-9644-fa8ef5d139b6"
                    .parse::<Uuid>()
                    .unwrap(),
            ),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        })));

        assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, image.to_json_pretty().unwrap());
    }

    #[test]
    fn test_debug_image_apple_default_values() {
        let json = r#"{
  "name": "CoreFoundation",
  "image_addr": "0x0",
  "image_size": 4096,
  "uuid": "494f3aea-88fa-4296-9644-fa8ef5d139b6",
  "type": "apple"
}"#;

        let image = Annotated::new(DebugImage::Apple(Box::new(AppleDebugImage {
            name: Annotated::new("CoreFoundation".to_string()),
            image_addr: Annotated::new(Addr(0)),
            image_size: Annotated::new(4096),
            uuid: Annotated::new(
                "494f3aea-88fa-4296-9644-fa8ef5d139b6"
                    .parse::<Uuid>()
                    .unwrap(),
            ),
            ..Default::default()
        })));

        assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, image.to_json_pretty().unwrap());
    }

    #[test]
    fn test_debug_image_symbolic_roundtrip() {
        let json = r#"{
  "name": "CoreFoundation",
  "arch": "arm64",
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "id": "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234",
  "other": "value",
  "type": "symbolic"
}"#;

        let image = Annotated::new(DebugImage::Symbolic(Box::new(SymbolicDebugImage {
            name: Annotated::new("CoreFoundation".to_string()),
            arch: Annotated::new("arm64".to_string()),
            image_addr: Annotated::new(Addr(0)),
            image_size: Annotated::new(4096),
            image_vmaddr: Annotated::new(Addr(32768)),
            id: Annotated::new(
                "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234"
                    .parse::<DebugId>()
                    .unwrap(),
            ),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        })));

        assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, image.to_json_pretty().unwrap());
    }

    #[test]
    fn test_debug_image_symbolic_default_values() {
        let json = r#"{
  "name": "CoreFoundation",
  "image_addr": "0x0",
  "image_size": 4096,
  "id": "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234",
  "type": "symbolic"
}"#;

        let image = Annotated::new(DebugImage::Symbolic(Box::new(SymbolicDebugImage {
            name: Annotated::new("CoreFoundation".to_string()),
            image_addr: Annotated::new(Addr(0)),
            image_size: Annotated::new(4096),
            id: Annotated::new(
                "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234"
                    .parse::<DebugId>()
                    .unwrap(),
            ),
            ..Default::default()
        })));

        assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, image.to_json_pretty().unwrap());
    }

    #[test]
    fn test_debug_image_other_roundtrip() {
        use crate::types::Map;
        let json = r#"{"other":"value","type":"mytype"}"#;
        let image = Annotated::new(DebugImage::Other({
            let mut map = Map::new();
            map.insert(
                "type".to_string(),
                Annotated::new(Value::String("mytype".to_string())),
            );
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        }));

        assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, image.to_json().unwrap());
    }

    #[test]
    fn test_debug_image_untagged_roundtrip() {
        use crate::types::Map;
        let json = r#"{"other":"value"}"#;
        let image = Annotated::new(DebugImage::Other({
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        }));

        assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
        assert_eq_str!(json, image.to_json().unwrap());
    }
}

#[test]
fn test_debug_meta_roundtrip() {
    use crate::types::Map;
    // NOTE: images are tested separately
    let json = r#"{
  "sdk_info": {
    "sdk_name": "iOS",
    "version_major": 10,
    "version_minor": 3,
    "version_patchlevel": 0,
    "other": "value"
  },
  "other": "value"
}"#;
    let meta = Annotated::new(DebugMeta {
        system_sdk: Annotated::new(SystemSdkInfo {
            sdk_name: Annotated::new("iOS".to_string()),
            version_major: Annotated::new(10),
            version_minor: Annotated::new(3),
            version_patchlevel: Annotated::new(0),
            other: {
                let mut map = Map::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        }),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
        ..Default::default()
    });

    assert_eq_dbg!(meta, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, meta.to_json_pretty().unwrap());
}

#[test]
fn test_debug_meta_default_values() {
    let json = "{}";
    let meta = Annotated::new(DebugMeta::default());

    assert_eq_dbg!(meta, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, meta.to_json_pretty().unwrap());
}
