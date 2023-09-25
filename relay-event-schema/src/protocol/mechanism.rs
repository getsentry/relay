#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, Error, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// POSIX signal with optional extended data.
///
/// Error codes set by Linux system calls and some library functions as specified in ISO C99,
/// POSIX.1-2001, and POSIX.1-2008. See
/// [`errno(3)`](https://man7.org/linux/man-pages/man3/errno.3.html) for more information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct CError {
    /// The error code as specified by ISO C99, POSIX.1-2001 or POSIX.1-2008.
    pub number: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,
}

/// Mach exception information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct MachException {
    /// The mach exception type.
    #[metastructure(field = "exception")]
    pub ty: Annotated<i64>,

    /// The mach exception code.
    pub code: Annotated<u64>,

    /// The mach exception subcode.
    pub subcode: Annotated<u64>,

    /// Optional name of the mach exception.
    pub name: Annotated<String>,
}

/// NSError informaiton.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct NsError {
    /// The error code.
    pub code: Annotated<i64>,

    /// A string containing the error domain.
    pub domain: Annotated<String>,
}

/// POSIX signal with optional extended data.
///
/// On Apple systems, signals also carry a code in addition to the signal number describing the
/// signal in more detail. On Linux, this code does not exist.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct PosixSignal {
    /// The POSIX signal number.
    pub number: Annotated<i64>,

    /// An optional signal code present on Apple systems.
    pub code: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,

    /// Optional name of the errno constant.
    pub code_name: Annotated<String>,
}

/// Operating system or runtime meta information to an exception mechanism.
///
/// The mechanism metadata usually carries error codes reported by the runtime or operating system,
/// along with a platform-dependent interpretation of these codes. SDKs can safely omit code names
/// and descriptions for well-known error codes, as it will be filled out by Sentry. For
/// proprietary or vendor-specific error codes, adding these values will give additional
/// information to the user.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct MechanismMeta {
    /// Optional ISO C standard error code.
    pub errno: Annotated<CError>,

    /// Information on the POSIX signal.
    pub signal: Annotated<PosixSignal>,

    /// A Mach Exception on Apple systems comprising a code triple and optional descriptions.
    pub mach_exception: Annotated<MachException>,

    /// An NSError on Apple systems comprising code and signal.
    pub ns_error: Annotated<NsError>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// The mechanism by which an exception was generated and handled.
///
/// The exception mechanism is an optional field residing in the [exception](#typedef-Exception).
/// It carries additional information about the way the exception was created on the target system.
/// This includes general exception values obtained from the operating system or runtime APIs, as
/// well as mechanism-specific values.
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Mechanism {
    /// Mechanism type (required).
    ///
    /// Required unique identifier of this mechanism determining rendering and processing of the
    /// mechanism data.
    ///
    /// In the Python SDK this is merely the name of the framework integration that produced the
    /// exception, while for native it is e.g. `"minidump"` or `"applecrashreport"`.
    #[metastructure(
        field = "type",
        required = "true",
        nonempty = "true",
        max_chars = "enumlike"
    )]
    pub ty: Annotated<String>,

    /// If this is set then the exception is not a real exception but some
    /// form of synthetic error for instance from a signal handler, a hard
    /// segfault or similar where type and value are not useful for grouping
    /// or display purposes.
    pub synthetic: Annotated<bool>,

    /// Optional human-readable description of the error mechanism.
    ///
    /// May include a possible hint on how to solve this error.
    #[metastructure(pii = "true", max_chars = "message")]
    pub description: Annotated<String>,

    /// Link to online resources describing this error.
    #[metastructure(required = "false", nonempty = "true", max_chars = "path")]
    pub help_link: Annotated<String>,

    /// Flag indicating whether this exception was handled.
    ///
    /// This is a best-effort guess at whether the exception was handled by user code or not. For
    /// example:
    ///
    /// - Exceptions leading to a 500 Internal Server Error or to a hard process crash are
    ///   `handled=false`, as the SDK typically has an integration that automatically captures the
    ///   error.
    ///
    /// - Exceptions captured using `capture_exception` (called from user code) are `handled=true`
    ///   as the user explicitly captured the exception (and therefore kind of handled it)
    pub handled: Annotated<bool>,

    /// An optional string value describing the source of the exception.
    ///
    /// For chained exceptions, this should contain the platform-specific name of the property or
    /// attribute (on the parent exception) that this exception was acquired from. In the case of
    /// an array, it should include the zero-based array index as well.
    ///
    /// - Python Examples: `"__context__"`, `"__cause__"`, `"exceptions[0]"`, `"exceptions[1]"`
    ///
    /// - .NET Examples:  `"InnerException"`, `"InnerExceptions[0]"`, `"InnerExceptions[1]"`
    ///
    /// - JavaScript Examples: `"cause"`, `"errors[0]"`, `"errors[1]"`
    #[metastructure(
        required = "false",
        nonempty = "true",
        max_chars = "enumlike",
        deny_chars = " \t\r\n"
    )]
    pub source: Annotated<String>,

    /// An optional boolean value, set `true` when the exception is the platform-specific exception
    /// group type.  Defaults to `false`.
    ///
    /// For example, exceptions of type `ExceptionGroup` (Python), `AggregateException` (.NET), and
    /// `AggregateError` (JavaScript) should have `"is_exception_group": true`.  Other exceptions
    /// can omit this field.
    pub is_exception_group: Annotated<bool>,

    /// An optional numeric value providing an ID for the exception relative to this specific event.
    /// It is referenced by the `parent_id` to reconstruct the logical tree of exceptions in an
    /// exception group.
    ///
    /// This should contain an unsigned integer value starting with `0` for the last exception in
    /// the exception values list, then `1` for the previous exception, etc.
    pub exception_id: Annotated<u64>,

    /// An optional numeric value pointing at the `exception_id` that is the direct parent of this
    /// exception, used to reconstruct the logical tree of exceptions in an exception group.
    ///
    /// The last exception in the exception values list should omit this field, because it is the
    /// root exception and thus has no parent.
    pub parent_id: Annotated<u64>,

    /// Arbitrary extra data that might help the user understand the error thrown by this mechanism.
    #[metastructure(pii = "true", bag_size = "medium")]
    #[metastructure(skip_serialization = "empty")]
    pub data: Annotated<Object<Value>>,

    /// Operating system or runtime meta information.
    #[metastructure(skip_serialization = "empty")]
    pub meta: Annotated<MechanismMeta>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl FromValue for Mechanism {
    fn from_value(annotated: Annotated<Value>) -> Annotated<Self> {
        #[derive(Debug, FromValue)]
        struct NewMechanism {
            #[metastructure(field = "type", required = "true")]
            pub ty: Annotated<String>,
            pub synthetic: Annotated<bool>,
            pub description: Annotated<String>,
            pub help_link: Annotated<String>,
            pub handled: Annotated<bool>,
            pub source: Annotated<String>,
            pub is_exception_group: Annotated<bool>,
            pub exception_id: Annotated<u64>,
            pub parent_id: Annotated<u64>,
            pub data: Annotated<Object<Value>>,
            pub meta: Annotated<MechanismMeta>,
            #[metastructure(additional_properties)]
            pub other: Object<Value>,
        }

        #[derive(Debug, FromValue)]
        struct LegacyPosixSignal {
            pub signal: Annotated<i64>,
            pub code: Annotated<i64>,
            pub name: Annotated<String>,
            pub code_name: Annotated<String>,
        }

        #[derive(Debug, FromValue)]
        struct LegacyMachException {
            pub exception: Annotated<i64>,
            pub code: Annotated<u64>,
            pub subcode: Annotated<u64>,
            pub exception_name: Annotated<String>,
        }

        #[derive(Debug, FromValue)]
        struct LegacyMechanism {
            posix_signal: Annotated<LegacyPosixSignal>,
            mach_exception: Annotated<LegacyMachException>,
            #[metastructure(additional_properties)]
            pub other: Object<Value>,
        }

        match annotated {
            Annotated(Some(Value::Object(object)), meta) => {
                if object.is_empty() {
                    Annotated(None, meta)
                } else if object.contains_key("type") {
                    let annotated = Annotated(Some(Value::Object(object)), meta);
                    NewMechanism::from_value(annotated).map_value(|mechanism| Mechanism {
                        ty: mechanism.ty,
                        synthetic: mechanism.synthetic,
                        description: mechanism.description,
                        help_link: mechanism.help_link,
                        handled: mechanism.handled,
                        source: mechanism.source,
                        is_exception_group: mechanism.is_exception_group,
                        exception_id: mechanism.exception_id,
                        parent_id: mechanism.parent_id,
                        data: mechanism.data,
                        meta: mechanism.meta,
                        other: mechanism.other,
                    })
                } else {
                    let annotated = Annotated(Some(Value::Object(object)), meta);
                    LegacyMechanism::from_value(annotated).map_value(|legacy| Mechanism {
                        ty: Annotated::new("generic".to_string()),
                        synthetic: Annotated::empty(),
                        description: Annotated::empty(),
                        help_link: Annotated::empty(),
                        handled: Annotated::empty(),
                        source: Annotated::empty(),
                        is_exception_group: Annotated::empty(),
                        exception_id: Annotated::empty(),
                        parent_id: Annotated::empty(),
                        data: Annotated::new(legacy.other),
                        meta: Annotated::new(MechanismMeta {
                            errno: Annotated::empty(),
                            signal: legacy.posix_signal.map_value(|legacy| PosixSignal {
                                number: legacy.signal,
                                code: legacy.code,
                                name: legacy.name,
                                code_name: legacy.code_name,
                            }),
                            mach_exception: legacy.mach_exception.map_value(|legacy| {
                                MachException {
                                    ty: legacy.exception,
                                    code: legacy.code,
                                    subcode: legacy.subcode,
                                    name: legacy.exception_name,
                                }
                            }),
                            ns_error: Annotated::empty(),
                            other: Object::default(),
                        }),
                        other: Object::default(),
                    })
                }
            }
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("exception mechanism"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::Map;
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_mechanism_roundtrip() {
        let json = r#"{
  "type": "mytype",
  "description": "mydescription",
  "help_link": "https://developer.apple.com/library/content/qa/qa1367/_index.html",
  "handled": false,
  "source": "errors[0]",
  "is_exception_group": false,
  "exception_id": 1,
  "parent_id": 0,
  "data": {
    "relevant_address": "0x1"
  },
  "meta": {
    "errno": {
      "number": 2,
      "name": "ENOENT"
    },
    "signal": {
      "number": 11,
      "code": 0,
      "name": "SIGSEGV",
      "code_name": "SEGV_NOOP"
    },
    "mach_exception": {
      "exception": 1,
      "code": 1,
      "subcode": 8,
      "name": "EXC_BAD_ACCESS"
    },
    "ns_error": {
      "code": -42,
      "domain": "SqlException"
    },
    "other": "value"
  },
  "other": "value"
}"#;
        let mechanism = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_string()),
            synthetic: Annotated::empty(),
            description: Annotated::new("mydescription".to_string()),
            help_link: Annotated::new(
                "https://developer.apple.com/library/content/qa/qa1367/_index.html".to_string(),
            ),
            handled: Annotated::new(false),
            source: Annotated::new("errors[0]".to_string()),
            is_exception_group: Annotated::new(false),
            exception_id: Annotated::new(1),
            parent_id: Annotated::new(0),
            data: {
                let mut map = Map::new();
                map.insert(
                    "relevant_address".to_string(),
                    Annotated::new(Value::String("0x1".to_string())),
                );
                Annotated::new(map)
            },
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::new(CError {
                    number: Annotated::new(2),
                    name: Annotated::new("ENOENT".to_string()),
                }),
                mach_exception: Annotated::new(MachException {
                    ty: Annotated::new(1),
                    code: Annotated::new(1),
                    subcode: Annotated::new(8),
                    name: Annotated::new("EXC_BAD_ACCESS".to_string()),
                }),
                signal: Annotated::new(PosixSignal {
                    number: Annotated::new(11),
                    code: Annotated::new(0),
                    name: Annotated::new("SIGSEGV".to_string()),
                    code_name: Annotated::new("SEGV_NOOP".to_string()),
                }),
                ns_error: Annotated::new(NsError {
                    code: Annotated::new(-42),
                    domain: Annotated::new("SqlException".to_string()),
                }),
                other: {
                    let mut map = Object::new();
                    map.insert(
                        "other".to_string(),
                        Annotated::new(Value::String("value".to_string())),
                    );
                    map
                },
            }),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        });

        assert_eq!(mechanism, Annotated::from_json(json).unwrap());
        assert_eq!(json, mechanism.to_json_pretty().unwrap());
    }

    #[test]
    fn test_mechanism_default_values() {
        let json = r#"{"type":"mytype"}"#;
        let mechanism = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_string()),
            ..Default::default()
        });

        assert_eq!(mechanism, Annotated::from_json(json).unwrap());
        assert_eq!(json, mechanism.to_json().unwrap());
    }

    #[test]
    fn test_mechanism_empty() {
        let mechanism = Annotated::<Mechanism>::empty();
        assert_eq!(mechanism, Annotated::from_json("{}").unwrap());
    }

    #[test]
    fn test_mechanism_legacy_conversion() {
        let input = r#"{
  "posix_signal": {
    "name": "SIGSEGV",
    "code_name": "SEGV_NOOP",
    "signal": 11,
    "code": 0
  },
  "relevant_address": "0x1",
  "mach_exception": {
    "exception": 1,
    "exception_name": "EXC_BAD_ACCESS",
    "subcode": 8,
    "code": 1
  }
}"#;

        let output = r#"{
  "type": "generic",
  "data": {
    "relevant_address": "0x1"
  },
  "meta": {
    "signal": {
      "number": 11,
      "code": 0,
      "name": "SIGSEGV",
      "code_name": "SEGV_NOOP"
    },
    "mach_exception": {
      "exception": 1,
      "code": 1,
      "subcode": 8,
      "name": "EXC_BAD_ACCESS"
    }
  }
}"#;
        let mechanism = Annotated::new(Mechanism {
            ty: Annotated::new("generic".to_string()),
            synthetic: Annotated::empty(),
            description: Annotated::empty(),
            help_link: Annotated::empty(),
            handled: Annotated::empty(),
            source: Annotated::empty(),
            is_exception_group: Annotated::empty(),
            exception_id: Annotated::empty(),
            parent_id: Annotated::empty(),
            data: {
                let mut map = Map::new();
                map.insert(
                    "relevant_address".to_string(),
                    Annotated::new(Value::String("0x1".to_string())),
                );
                Annotated::new(map)
            },
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::empty(),
                mach_exception: Annotated::new(MachException {
                    ty: Annotated::new(1),
                    code: Annotated::new(1),
                    subcode: Annotated::new(8),
                    name: Annotated::new("EXC_BAD_ACCESS".to_string()),
                }),
                signal: Annotated::new(PosixSignal {
                    number: Annotated::new(11),
                    code: Annotated::new(0),
                    name: Annotated::new("SIGSEGV".to_string()),
                    code_name: Annotated::new("SEGV_NOOP".to_string()),
                }),
                ns_error: Annotated::empty(),
                other: Object::default(),
            }),
            other: Object::default(),
        });

        assert_eq!(mechanism, Annotated::from_json(input).unwrap());
        assert_eq!(output, mechanism.to_json_pretty().unwrap());
    }
}
