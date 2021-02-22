use std::fmt;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

#[cfg(feature = "jsonschema")]
use schemars::gen::SchemaGenerator;
#[cfg(feature = "jsonschema")]
use schemars::schema::Schema;

use enumset::EnumSet;
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
#[derive(Debug, FromValue, ToValue, Empty, Clone, PartialEq)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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
    fn value_type(&self) -> EnumSet<ValueType> {
        // Explicit decision not to expose NativeImagePath as valuetype, as people should not be
        // able to address processing internals.
        //
        // Also decided against exposing a $filepath ("things that may contain filenames") because
        // ruletypes/regexes are better suited for this, and in the case of $frame.package (where
        // it depends on platform) it's really not that useful.
        EnumSet::only(ValueType::String)
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
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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

/// Legacy apple debug images (MachO).
///
/// This was also used for non-apple platforms with similar debug setups.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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
    ($type:ident, $inner:path, $expectation:literal) => {
        #[cfg(feature = "jsonschema")]
        impl schemars::JsonSchema for $type {
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

        impl fmt::Display for $type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $type {
            type Err = <$inner as FromStr>::Err;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                FromStr::from_str(s).map($type)
            }
        }

        impl Deref for $type {
            type Target = $inner;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl DerefMut for $type {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DebugId(pub debugid::DebugId);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CodeId(pub debugid::CodeId);

impl_traits!(CodeId, debugid::CodeId, "a code identifier");
impl_traits!(DebugId, debugid::DebugId, "a debug identifier");

impl<T> From<T> for DebugId
where
    debugid::DebugId: From<T>,
{
    fn from(t: T) -> Self {
        DebugId(t.into())
    }
}

impl<T> From<T> for CodeId
where
    debugid::CodeId: From<T>,
{
    fn from(t: T) -> Self {
        CodeId(t.into())
    }
}

/// A generic (new-style) native platform debug information file.
///
/// The `type` key must be one of:
///
/// - `macho`
/// - `elf`: ELF images are used on Linux platforms. Their structure is identical to other native images.
/// - `pe`
///
/// Examples:
///
/// ```json
/// {
///   "type": "elf",
///   "code_id": "68220ae2c65d65c1b6aaa12fa6765a6ec2f5f434",
///   "code_file": "/lib/x86_64-linux-gnu/libgcc_s.so.1",
///   "debug_id": "e20a2268-5dc6-c165-b6aa-a12fa6765a6e",
///   "image_addr": "0x7f5140527000",
///   "image_size": 90112,
///   "image_vmaddr": "0x40000",
///   "arch": "x86_64"
/// }
/// ```
///
/// ```json
/// {
///   "type": "pe",
///   "code_id": "57898e12145000",
///   "code_file": "C:\\Windows\\System32\\dbghelp.dll",
///   "debug_id": "9c2a902b-6fdf-40ad-8308-588a41d572a0-1",
///   "debug_file": "dbghelp.pdb",
///   "image_addr": "0x70850000",
///   "image_size": "1331200",
///   "image_vmaddr": "0x40000",
///   "arch": "x86"
/// }
/// ```
///
/// ```json
/// {
///   "type": "macho",
///   "debug_id": "84a04d24-0e60-3810-a8c0-90a65e2df61a",
///   "debug_file": "libDiagnosticMessagesClient.dylib",
///   "code_file": "/usr/lib/libDiagnosticMessagesClient.dylib",
///   "image_addr": "0x7fffe668e000",
///   "image_size": 8192,
///   "image_vmaddr": "0x40000",
///   "arch": "x86_64",
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct NativeDebugImage {
    /// Optional identifier of the code file.
    ///
    /// - `elf`: If the program was compiled with a relatively recent compiler, this should be the hex representation of the `NT_GNU_BUILD_ID` program header (type `PT_NOTE`), or the value of the `.note.gnu.build-id` note section (type `SHT_NOTE`). Otherwise, leave this value empty.
    ///
    ///   Certain symbol servers use the code identifier to locate debug information for ELF images, in which case this field should be included if possible.
    ///
    /// - `pe`: Identifier of the executable or DLL. It contains the values of the `time_date_stamp` from the COFF header and `size_of_image` from the optional header formatted together into a hex string using `%08x%X` (note that the second value is not padded):
    ///
    ///   ```text
    ///   time_date_stamp: 0x5ab38077
    ///   size_of_image:           0x9000
    ///   code_id:           5ab380779000
    ///   ```
    ///
    ///   The code identifier should be provided to allow server-side stack walking of binary crash reports, such as Minidumps.
    ///
    ///
    /// - `macho`: Identifier of the dynamic library or executable. It is the value of the `LC_UUID` load command in the Mach header, formatted as UUID. Can be empty for Mach images, as it is equivalent to the debug identifier.
    pub code_id: Annotated<CodeId>,

    /// Path and name of the image file (required).
    ///
    /// The absolute path to the dynamic library or executable. This helps to locate the file if it is missing on Sentry.
    ///
    /// - `pe`: The code file should be provided to allow server-side stack walking of binary crash reports, such as Minidumps.
    #[metastructure(required = "true", legacy_alias = "name")]
    #[metastructure(pii = "maybe")]
    pub code_file: Annotated<NativeImagePath>,

    /// Unique debug identifier of the image.
    ///
    /// - `elf`: Debug identifier of the dynamic library or executable. If a code identifier is available, the debug identifier is the little-endian UUID representation of the first 16-bytes of that
    /// identifier. Spaces are inserted for readability, note the byte order of the first fields:
    ///
    ///   ```text
    ///   code id:  f1c3bcc0 2798 65fe 3058 404b2831d9e6 4135386c
    ///   debug id: c0bcc3f1-9827-fe65-3058-404b2831d9e6
    ///   ```
    ///
    ///   If no code id is available, the debug id should be computed by XORing the first 4096 bytes of the `.text` section in 16-byte chunks, and representing it as a little-endian UUID (again swapping the byte order).
    ///
    /// - `pe`: `signature` and `age` of the PDB file. Both values can be read from the CodeView PDB70 debug information header in the PE. The value should be represented as little-endian UUID, with the age appended at the end. Note that the byte order of the UUID fields must be swapped (spaces inserted for readability):
    ///
    ///   ```text
    ///   signature: f1c3bcc0 2798 65fe 3058 404b2831d9e6
    ///   age:                                            1
    ///   debug_id:  c0bcc3f1-9827-fe65-3058-404b2831d9e6-1
    ///   ```
    ///
    /// - `macho`: Identifier of the dynamic library or executable. It is the value of the `LC_UUID` load command in the Mach header, formatted as UUID.
    #[metastructure(required = "true", legacy_alias = "id")]
    pub debug_id: Annotated<DebugId>,

    /// Path and name of the debug companion file.
    ///
    /// - `elf`: Name or absolute path to the file containing stripped debug information for this image. This value might be _required_ to retrieve debug files from certain symbol servers.
    ///
    /// - `pe`: Name of the PDB file containing debug information for this image. This value is often required to retrieve debug files from specific symbol servers.
    ///
    /// - `macho`: Name or absolute path to the dSYM file containing debug information for this image. This value might be required to retrieve debug files from certain symbol servers.
    #[metastructure(pii = "maybe")]
    pub debug_file: Annotated<NativeImagePath>,

    /// CPU architecture target.
    ///
    /// Architecture of the module. If missing, this will be backfilled by Sentry.
    pub arch: Annotated<String>,

    /// Starting memory address of the image (required).
    ///
    /// Memory address, at which the image is mounted in the virtual address space of the process. Should be a string in hex representation prefixed with `"0x"`.
    pub image_addr: Annotated<Addr>,

    /// Size of the image in bytes (required).
    ///
    /// The size of the image in virtual memory. If missing, Sentry will assume that the image spans up to the next image, which might lead to invalid stack traces.
    pub image_size: Annotated<u64>,

    /// Loading address in virtual memory.
    ///
    /// Preferred load address of the image in virtual memory, as declared in the headers of the
    /// image. When loading an image, the operating system may still choose to place it at a
    /// different address.
    ///
    /// Symbols and addresses in the native image are always relative to the start of the image and do not consider the preferred load address. It is merely a hint to the loader.
    ///
    /// - `elf`/`macho`: If this value is non-zero, all symbols and addresses declared in the native image start at this address, rather than 0. By contrast, Sentry deals with addresses relative to the start of the image. For example, with `image_vmaddr: 0x40000`, a symbol located at `0x401000` has a relative address of `0x1000`.
    ///
    ///   Relative addresses used in Apple Crash Reports and `addr2line` are usually in the preferred address space, and not relative address space.
    pub image_vmaddr: Annotated<Addr>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Proguard mapping file.
///
/// Proguard images refer to `mapping.txt` files generated when Proguard obfuscates function names. The Java SDK integrations assign this file a unique identifier, which has to be included in the list of images.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ProguardDebugImage {
    /// UUID computed from the file contents, assigned by the Java SDK.
    #[metastructure(required = "true")]
    pub uuid: Annotated<Uuid>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// A debug information file (debug image).
#[derive(Clone, Debug, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_debug_image")]
pub enum DebugImage {
    /// Legacy apple debug images (MachO).
    Apple(Box<AppleDebugImage>),
    /// A generic (new-style) native platform debug information file.
    Symbolic(Box<NativeDebugImage>),
    /// MachO (macOS and iOS) debug image.
    MachO(Box<NativeDebugImage>),
    /// ELF (Linux) debug image.
    Elf(Box<NativeDebugImage>),
    /// PE (Windows) debug image.
    Pe(Box<NativeDebugImage>),
    /// A reference to a proguard debug file.
    Proguard(Box<ProguardDebugImage>),
    /// WASM debug image.
    Wasm(Box<NativeDebugImage>),
    /// A debug image that is unknown to this protocol specification.
    #[metastructure(fallback_variant)]
    Other(Object<Value>),
}

/// Debugging and processing meta information.
///
/// The debug meta interface carries debug information for processing errors and crash reports.
/// Sentry amends the information in this interface.
///
/// Example (look at field types to see more detail):
///
/// ```json
/// {
///   "debug_meta": {
///     "images": [],
///     "sdk_info": {
///       "sdk_name": "iOS",
///       "version_major": 10,
///       "version_minor": 3,
///       "version_patchlevel": 0
///     }
///   }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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
