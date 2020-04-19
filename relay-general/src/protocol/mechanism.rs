use crate::types::{Annotated, Error, FromValue, Object, Value};

/// POSIX signal with optional extended data.
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
pub struct CError {
    /// The error code as specified by ISO C99, POSIX.1-2001 or POSIX.1-2008.
    pub number: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,
}

/// Mach exception information.
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
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

/// POSIX signal with optional extended data.
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
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
#[derive(
    Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue, DocumentValue,
)]
pub struct MechanismMeta {
    /// Optional ISO C standard error code.
    pub errno: Annotated<CError>,

    /// Optional POSIX signal number.
    pub signal: Annotated<PosixSignal>,

    /// Optional mach exception information.
    pub mach_exception: Annotated<MachException>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// The mechanism by which an exception was generated and handled.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue, DocumentValue)]
pub struct Mechanism {
    /// Mechanism type (required).
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

    /// Human readable detail description.
    #[metastructure(pii = "true", max_chars = "message")]
    pub description: Annotated<String>,

    /// Link to online resources describing this error.
    #[metastructure(required = "false", nonempty = "true", max_chars = "path")]
    pub help_link: Annotated<String>,

    /// Flag indicating whether this exception was handled.
    pub handled: Annotated<bool>,

    /// Additional attributes depending on the mechanism type.
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

#[test]
fn test_mechanism_roundtrip() {
    use crate::types::Map;
    let json = r#"{
  "type": "mytype",
  "description": "mydescription",
  "help_link": "https://developer.apple.com/library/content/qa/qa1367/_index.html",
  "handled": false,
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

    assert_eq_dbg!(mechanism, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, mechanism.to_json_pretty().unwrap());
}

#[test]
fn test_mechanism_default_values() {
    let json = r#"{"type":"mytype"}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("mytype".to_string()),
        ..Default::default()
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, mechanism.to_json().unwrap());
}

#[test]
fn test_mechanism_empty() {
    let mechanism = Annotated::<Mechanism>::empty();
    assert_eq_dbg!(mechanism, Annotated::from_json("{}").unwrap());
}

#[test]
fn test_mechanism_legacy_conversion() {
    use crate::types::Map;

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
            other: Object::default(),
        }),
        other: Object::default(),
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, mechanism.to_json_pretty().unwrap());
}
