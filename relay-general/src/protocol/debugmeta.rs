use derive_more::{Deref, Display, FromStr};
use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::processor::{ProcessValue, ProcessingState, Processor, ValueType};
use crate::protocol::Addr;
use crate::types::{
    Annotated, Array, Empty, Error, FromValue, Meta, Object, ProcessingResult, SkipSerialization,
    ToValue, Value,
};

/// A type for strings that are generally paths, might contain system user names, but still cannot
/// be stripped liberally because it would break processing for certain platforms.
///
/// Those strings get special treatment in our PII processor to avoid stripping the basename.
#[derive(Debug, FromValue, ToValue, Empty, Clone, PartialEq, DocumentValue)]
pub struct NativeImagePath(pub String);

impl NativeImagePath {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<T: Into<String>> From<T> for NativeImagePath {
    fn from(value: T) -> NativeImagePath {
        NativeImagePath(value.into())
    }
}

impl ProcessValue for NativeImagePath {
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::String)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_native_image_path(self, meta, state)
    }

    fn process_child_values<P>(
        &mut self,
        _processor: &mut P,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        Ok(())
    }
}

/// Holds information about the system SDK.
///
/// This is relevant for iOS and other platforms that have a system
/// SDK.  Not to be confused with the client SDK.
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
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
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
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

macro_rules! impl_traits {
    ($type:ident, $expectation:literal) => {
        impl JsonSchema for $type {
            fn schema_name() -> String {
                stringify!($type).to_owned()
            }

            fn json_schema(gen: &mut SchemaGenerator) -> Schema {
                String::json_schema(gen)
            }
        }

        impl Empty for $type {
            #[inline]
            fn is_empty(&self) -> bool {
                self.is_nil()
            }
        }

        impl FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error(Error::invalid(err));
                            meta.set_original_value(Some(value));
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(value), mut meta) => {
                        meta.add_error(Error::expected($expectation));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
        }

        impl ToValue for $type {
            fn to_value(self) -> Value {
                Value::String(self.to_string())
            }

            fn serialize_payload<S>(
                &self,
                s: S,
                _behavior: SkipSerialization,
            ) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serde::Serialize::serialize(self, s)
            }
        }

        impl ProcessValue for $type {}
    };
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Display, FromStr, Deref)]
pub struct DebugId(pub debugid::DebugId);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Display, FromStr, Deref)]
pub struct CodeId(pub debugid::CodeId);

impl_traits!(CodeId, "a code identifier");
impl_traits!(DebugId, "a debug identifier");

impl<T> From<T> for DebugId
where
    T: Into<debugid::DebugId>,
{
    fn from(t: T) -> Self {
        DebugId(t.into())
    }
}

impl<T> From<T> for CodeId
where
    T: Into<debugid::CodeId>,
{
    fn from(t: T) -> Self {
        CodeId(t.into())
    }
}

/// A native platform debug information file.
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
pub struct NativeDebugImage {
    /// Optional identifier of the code file.
    ///
    /// If not specified, it is assumed to be identical to the debug identifier.
    pub code_id: Annotated<CodeId>,

    /// Path and name of the image file (required).
    #[metastructure(required = "true", legacy_alias = "name")]
    #[metastructure(pii = "maybe")]
    pub code_file: Annotated<NativeImagePath>,

    /// Unique debug identifier of the image.
    #[metastructure(required = "true", legacy_alias = "id")]
    pub debug_id: Annotated<DebugId>,

    /// Path and name of the debug companion file (required).
    #[metastructure(pii = "maybe")]
    pub debug_file: Annotated<NativeImagePath>,

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

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Proguard mapping file.
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
pub struct ProguardDebugImage {
    /// UUID computed from the file contents.
    #[metastructure(required = "true")]
    pub uuid: Annotated<Uuid>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// A debug information file (debug image).
#[derive(Clone, Debug, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue)]
#[metastructure(process_func = "process_debug_image")]
pub enum DebugImage {
    /// Legacy apple debug images (MachO).
    ///
    /// This was also used for non-apple platforms with similar debug setups.
    Apple(Box<AppleDebugImage>),
    /// Generic new style debug image.
    Symbolic(Box<NativeDebugImage>),
    /// MachO (macOS and iOS) debug image.
    MachO(Box<NativeDebugImage>),
    /// ELF (Linux) debug image.
    Elf(Box<NativeDebugImage>),
    /// PE (Windows) debug image.
    Pe(Box<NativeDebugImage>),
    /// A reference to a proguard debug file.
    Proguard(Box<ProguardDebugImage>),
    /// A debug image that is unknown to this protocol specification.
    #[metastructure(fallback_variant)]
    Other(Object<Value>),
}

/// Debugging and processing meta information.
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
#[metastructure(process_func = "process_debug_meta")]
pub struct DebugMeta {
    /// Information about the system SDK (e.g. iOS SDK).
    #[metastructure(field = "sdk_info")]
    #[metastructure(skip_serialization = "empty")]
    pub system_sdk: Annotated<SystemSdkInfo>,

    /// List of debug information files (debug images).
    #[metastructure(skip_serialization = "empty")]
    pub images: Annotated<Array<DebugImage>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[cfg(test)]
use crate::types::Map;

#[test]
fn test_debug_image_proguard_roundtrip() {
    let json = r#"{
  "uuid": "395835f4-03e0-4436-80d3-136f0749a893",
  "other": "value",
  "type": "proguard"
}"#;
    let image = Annotated::new(DebugImage::Proguard(Box::new(ProguardDebugImage {
        uuid: Annotated::new("395835f4-03e0-4436-80d3-136f0749a893".parse().unwrap()),
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
        uuid: Annotated::new("494f3aea-88fa-4296-9644-fa8ef5d139b6".parse().unwrap()),
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
        uuid: Annotated::new("494f3aea-88fa-4296-9644-fa8ef5d139b6".parse().unwrap()),
        ..Default::default()
    })));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json_pretty().unwrap());
}

#[test]
fn test_debug_image_symbolic_roundtrip() {
    let json = r#"{
  "code_id": "59b0d8f3183000",
  "code_file": "C:\\Windows\\System32\\ntdll.dll",
  "debug_id": "971f98e5-ce60-41ff-b2d7-235bbeb34578-1",
  "debug_file": "wntdll.pdb",
  "arch": "arm64",
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "other": "value",
  "type": "symbolic"
}"#;

    let image = Annotated::new(DebugImage::Symbolic(Box::new(NativeDebugImage {
        code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
        code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
        debug_id: Annotated::new("971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap()),
        debug_file: Annotated::new("wntdll.pdb".into()),
        arch: Annotated::new("arm64".to_string()),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::new(Addr(32768)),
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
fn test_debug_image_symbolic_legacy() {
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

    let image = Annotated::new(DebugImage::Symbolic(Box::new(NativeDebugImage {
        code_id: Annotated::empty(),
        code_file: Annotated::new("CoreFoundation".into()),
        debug_id: Annotated::new("494f3aea-88fa-4296-9644-fa8ef5d139b6-1234".parse().unwrap()),
        debug_file: Annotated::empty(),
        arch: Annotated::new("arm64".to_string()),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::new(Addr(32768)),
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
}

#[test]
fn test_debug_image_symbolic_default_values() {
    let json = r#"{
  "code_file": "CoreFoundation",
  "debug_id": "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234",
  "image_addr": "0x0",
  "image_size": 4096,
  "type": "symbolic"
}"#;

    let image = Annotated::new(DebugImage::Symbolic(Box::new(NativeDebugImage {
        code_file: Annotated::new("CoreFoundation".into()),
        debug_id: Annotated::new(
            "494f3aea-88fa-4296-9644-fa8ef5d139b6-1234"
                .parse::<DebugId>()
                .unwrap(),
        ),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        ..Default::default()
    })));

    assert_eq_dbg!(image, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, image.to_json_pretty().unwrap());
}

#[test]
fn test_debug_image_elf_roundtrip() {
    let json = r#"{
  "code_id": "f1c3bcc0279865fe3058404b2831d9e64135386c",
  "code_file": "crash",
  "debug_id": "c0bcc3f1-9827-fe65-3058-404b2831d9e6",
  "arch": "arm64",
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "other": "value",
  "type": "elf"
}"#;

    let image = Annotated::new(DebugImage::Elf(Box::new(NativeDebugImage {
        code_id: Annotated::new("f1c3bcc0279865fe3058404b2831d9e64135386c".parse().unwrap()),
        code_file: Annotated::new("crash".into()),
        debug_id: Annotated::new("c0bcc3f1-9827-fe65-3058-404b2831d9e6".parse().unwrap()),
        debug_file: Annotated::empty(),
        arch: Annotated::new("arm64".to_string()),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::new(Addr(32768)),
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
fn test_debug_image_macho_roundtrip() {
    let json = r#"{
  "code_id": "67E9247C-814E-392B-A027-DBDE6748FCBF",
  "code_file": "crash",
  "debug_id": "67e9247c-814e-392b-a027-dbde6748fcbf",
  "arch": "arm64",
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "other": "value",
  "type": "macho"
}"#;

    let image = Annotated::new(DebugImage::MachO(Box::new(NativeDebugImage {
        code_id: Annotated::new("67E9247C-814E-392B-A027-DBDE6748FCBF".parse().unwrap()),
        code_file: Annotated::new("crash".into()),
        debug_id: Annotated::new("67e9247c-814e-392b-a027-dbde6748fcbf".parse().unwrap()),
        debug_file: Annotated::empty(),
        arch: Annotated::new("arm64".to_string()),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::new(Addr(32768)),
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
}

#[test]
fn test_debug_image_pe_roundtrip() {
    let json = r#"{
  "code_id": "59b0d8f3183000",
  "code_file": "C:\\Windows\\System32\\ntdll.dll",
  "debug_id": "971f98e5-ce60-41ff-b2d7-235bbeb34578-1",
  "debug_file": "wntdll.pdb",
  "arch": "arm64",
  "image_addr": "0x0",
  "image_size": 4096,
  "image_vmaddr": "0x8000",
  "other": "value",
  "type": "pe"
}"#;

    let image = Annotated::new(DebugImage::Pe(Box::new(NativeDebugImage {
        code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
        code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
        debug_id: Annotated::new("971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap()),
        debug_file: Annotated::new("wntdll.pdb".into()),
        arch: Annotated::new("arm64".to_string()),
        image_addr: Annotated::new(Addr(0)),
        image_size: Annotated::new(4096),
        image_vmaddr: Annotated::new(Addr(32768)),
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
fn test_debug_image_other_roundtrip() {
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
