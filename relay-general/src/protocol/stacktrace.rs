use std::ops::{Deref, DerefMut};

use crate::protocol::{Addr, NativeImagePath, RegVal};
use crate::types::{Annotated, Array, FromValue, Object, Value};

/// Holds information about a single stacktrace frame.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_frame", value_type = "Frame")]
pub struct Frame {
    /// Name of the frame's function. This might include the name of a class.
    #[metastructure(max_chars = "symbol")]
    #[metastructure(skip_serialization = "empty")]
    pub function: Annotated<String>,

    /// A raw (but potentially truncated) function value.
    ///
    /// If this has the same value as `function` it's best to be omitted.  This
    /// exists because on many platforms the function itself contains additional
    /// information like overload specifies or a lot of generics which can make
    /// it exceed the maximum limit we provide for the field.  In those cases
    /// then we cannot reliably trim down the function any more at a later point
    /// because the more valuable information has been removed.
    ///
    /// The logic to be applied is that an intelligently trimmed function name
    /// should be stored in `function` and the value before trimming is stored
    /// in this field instead.  However also this field will be capped at 256
    /// characters at the moment which often means that not the entire original
    /// value can be stored.
    #[metastructure(max_chars = "symbol")]
    #[metastructure(skip_serialization = "empty")]
    pub raw_function: Annotated<String>,

    /// Potentially mangled name of the symbol as it appears in an executable.
    ///
    /// This is different from a function name by generally being the mangled
    /// name that appears natively in the binary.  This is relevant for languages
    /// like Swift, C++ or Rust.
    #[metastructure(max_chars = "symbol")]
    pub symbol: Annotated<String>,

    /// Name of the module the frame is contained in.
    ///
    /// Note that this might also include a class name if that is something the
    /// language natively considers to be part of the stack (for instance in Java).
    #[metastructure(skip_serialization = "empty")]
    // TODO: Cap? This can be a FS path or a dotted path
    pub module: Annotated<String>,

    /// Name of the package that contains the frame.
    ///
    /// For instance this can be a dylib for native languages, the name of the jar
    /// or .NET assembly.
    #[metastructure(skip_serialization = "empty")]
    // TODO: Cap? This can be a FS path or a dotted path
    pub package: Annotated<String>,

    /// The source file name (basename only).
    #[metastructure(max_chars = "path")]
    #[metastructure(skip_serialization = "empty", pii = "maybe")]
    pub filename: Annotated<NativeImagePath>,

    /// Absolute path to the source file.
    #[metastructure(max_chars = "path")]
    #[metastructure(skip_serialization = "empty", pii = "maybe")]
    pub abs_path: Annotated<NativeImagePath>,

    /// Line number within the source file.
    pub lineno: Annotated<u64>,

    /// Column number within the source file.
    pub colno: Annotated<u64>,

    /// Which platform this frame is from.
    #[metastructure(skip_serialization = "empty")]
    pub platform: Annotated<String>,

    /// Source code leading up to the current line.
    #[metastructure(skip_serialization = "empty")]
    pub pre_context: Annotated<Array<String>>,

    /// Source code of the current line.
    pub context_line: Annotated<String>,

    /// Source code of the lines after the current line.
    #[metastructure(skip_serialization = "empty")]
    pub post_context: Annotated<Array<String>>,

    /// Override whether this frame should be considered in-app.
    pub in_app: Annotated<bool>,

    /// Local variables in a convenient format.
    // XXX: Probably want to trim per-var => new bag size?
    #[metastructure(pii = "true", bag_size = "medium")]
    pub vars: Annotated<FrameVars>,

    /// Auxiliary information about the frame that is platform specific.
    pub data: Annotated<FrameData>,

    /// Start address of the containing code module (image).
    pub image_addr: Annotated<Addr>,

    /// Absolute address of the frame's CPU instruction.
    pub instruction_addr: Annotated<Addr>,

    /// Start address of the frame's function.
    pub symbol_addr: Annotated<Addr>,

    /// Used for native crashes to indicate how much we can "trust" the instruction_addr
    #[metastructure(max_chars = "enumlike")]
    pub trust: Annotated<String>,

    /// The language of the frame if it overrides the stacktrace language.
    #[metastructure(max_chars = "enumlike")]
    pub lang: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Frame local variables.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
pub struct FrameVars(#[metastructure(skip_serialization = "empty")] pub Object<Value>);

/// Additional frame data information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct FrameData {
    /// A reference to the sourcemap used.
    #[metastructure(max_chars = "path")]
    sourcemap: Annotated<String>,
    /// The original function name before it was resolved.
    #[metastructure(max_chars = "symbol")]
    orig_function: Annotated<String>,
    /// The original minified filename.
    #[metastructure(max_chars = "path")]
    orig_filename: Annotated<String>,
    /// The original line number.
    orig_lineno: Annotated<u64>,
    /// The original column number.
    orig_colno: Annotated<u64>,
    /// The original value of the in_app flag before grouping enhancers ran.
    ///
    /// Because we need to handle more cases the following values are used:
    ///
    /// - missing / `null`: information not available
    /// - `-1`: in_app was set to `null`
    /// - `0`: in_app was set to `false`
    /// - `1`: in_app was set to `true`
    orig_in_app: Annotated<i64>,
    /// Additional keys not handled by this protocol.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl From<Object<Value>> for FrameVars {
    fn from(value: Object<Value>) -> Self {
        FrameVars(value)
    }
}

impl FromValue for FrameVars {
    fn from_value(mut value: Annotated<Value>) -> Annotated<FrameVars> {
        value = value.map_value(|value| {
            if let Value::Array(value) = value {
                Value::Object(
                    value
                        .into_iter()
                        .enumerate()
                        .map(|(i, v)| (i.to_string(), v))
                        .collect(),
                )
            } else {
                value
            }
        });

        FromValue::from_value(value).map_value(FrameVars)
    }
}

/// Holds information about an entirey stacktrace.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_raw_stacktrace", value_type = "Stacktrace")]
pub struct RawStacktrace {
    #[metastructure(required = "true", nonempty = "true", skip_serialization = "empty")]
    pub frames: Annotated<Array<Frame>>,

    /// Register values of the thread (top frame).
    pub registers: Annotated<Object<RegVal>>,

    /// The language of the stacktrace.
    #[metastructure(max_chars = "enumlike")]
    pub lang: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Newtype to distinguish `raw_stacktrace` attributes from the rest.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_stacktrace")]
pub struct Stacktrace(pub RawStacktrace);

impl Deref for Stacktrace {
    type Target = RawStacktrace;

    fn deref(&self) -> &RawStacktrace {
        &self.0
    }
}

impl DerefMut for Stacktrace {
    fn deref_mut(&mut self) -> &mut RawStacktrace {
        &mut self.0
    }
}

impl From<RawStacktrace> for Stacktrace {
    fn from(stacktrace: RawStacktrace) -> Stacktrace {
        Stacktrace(stacktrace)
    }
}

impl From<Stacktrace> for RawStacktrace {
    fn from(stacktrace: Stacktrace) -> RawStacktrace {
        stacktrace.0
    }
}

#[test]
fn test_frame_roundtrip() {
    let json = r#"{
  "function": "main@8",
  "raw_function": "main",
  "symbol": "_main@8",
  "module": "app",
  "package": "/my/app",
  "filename": "myfile.rs",
  "abs_path": "/path/to",
  "lineno": 2,
  "colno": 42,
  "platform": "rust",
  "pre_context": [
    "fn main() {"
  ],
  "context_line": "unimplemented!()",
  "post_context": [
    "}"
  ],
  "in_app": true,
  "vars": {
    "variable": "value"
  },
  "data": {
    "sourcemap": "http://example.com/invalid.map"
  },
  "image_addr": "0x400",
  "instruction_addr": "0x404",
  "symbol_addr": "0x404",
  "trust": "69",
  "lang": "rust",
  "other": "value"
}"#;
    let frame = Annotated::new(Frame {
        function: Annotated::new("main@8".to_string()),
        raw_function: Annotated::new("main".to_string()),
        symbol: Annotated::new("_main@8".to_string()),
        module: Annotated::new("app".to_string()),
        package: Annotated::new("/my/app".to_string()),
        filename: Annotated::new("myfile.rs".into()),
        abs_path: Annotated::new("/path/to".into()),
        lineno: Annotated::new(2),
        colno: Annotated::new(42),
        platform: Annotated::new("rust".to_string()),
        pre_context: Annotated::new(vec![Annotated::new("fn main() {".to_string())]),
        context_line: Annotated::new("unimplemented!()".to_string()),
        post_context: Annotated::new(vec![Annotated::new("}".to_string())]),
        in_app: Annotated::new(true),
        vars: {
            let mut vars = Object::new();
            vars.insert(
                "variable".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            Annotated::new(vars.into())
        },
        data: Annotated::new(FrameData {
            sourcemap: Annotated::new("http://example.com/invalid.map".to_string()),
            ..Default::default()
        }),
        image_addr: Annotated::new(Addr(0x400)),
        instruction_addr: Annotated::new(Addr(0x404)),
        symbol_addr: Annotated::new(Addr(0x404)),
        trust: Annotated::new("69".into()),
        lang: Annotated::new("rust".into()),
        other: {
            let mut vars = Object::new();
            vars.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            vars
        },
    });

    assert_eq_dbg!(frame, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, frame.to_json_pretty().unwrap());
}

#[test]
fn test_frame_default_values() {
    let json = "{}";
    let frame = Annotated::new(Frame::default());

    assert_eq_dbg!(frame, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, frame.to_json_pretty().unwrap());
}

#[test]
fn test_stacktrace_roundtrip() {
    let json = r#"{
  "frames": [
    {
      "function": "foobar"
    }
  ],
  "registers": {
    "cspr": "0x20000000",
    "lr": "0x18a31aadc",
    "pc": "0x18a310ea4",
    "sp": "0x16fd75060"
  },
  "lang": "rust",
  "other": "value"
}"#;
    let stack = Annotated::new(RawStacktrace {
        frames: Annotated::new(vec![Annotated::new(Frame {
            function: Annotated::new("foobar".to_string()),
            ..Default::default()
        })]),
        registers: {
            let mut registers = Object::new();
            registers.insert("cspr".to_string(), Annotated::new(RegVal(0x2000_0000)));
            registers.insert("lr".to_string(), Annotated::new(RegVal(0x1_8a31_aadc)));
            registers.insert("pc".to_string(), Annotated::new(RegVal(0x1_8a31_0ea4)));
            registers.insert("sp".to_string(), Annotated::new(RegVal(0x1_6fd7_5060)));
            Annotated::new(registers)
        },
        lang: Annotated::new("rust".into()),
        other: {
            let mut other = Object::new();
            other.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            other
        },
    });

    assert_eq_dbg!(stack, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, stack.to_json_pretty().unwrap());
}

#[test]
fn test_stacktrace_default_values() {
    // This needs an empty frame because "frames" is required
    let json = r#"{
  "frames": [
    {}
  ]
}"#;

    let stack = Annotated::new(RawStacktrace {
        frames: Annotated::new(vec![Annotated::new(Frame::default())]),
        ..Default::default()
    });

    assert_eq_dbg!(stack, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, stack.to_json_pretty().unwrap());
}

#[test]
fn test_frame_vars_null_preserved() {
    let json = r#"{
  "vars": {
    "despacito": null
  }
}"#;
    let frame = Annotated::new(Frame {
        vars: Annotated::new({
            let mut vars = Object::new();
            vars.insert("despacito".to_string(), Annotated::empty());
            vars.into()
        }),
        ..Default::default()
    });

    assert_eq_dbg!(Annotated::from_json(json).unwrap(), frame);
    assert_eq_str!(json, frame.to_json_pretty().unwrap());
}

#[test]
fn test_frame_vars_empty_annotated_is_serialized() {
    let output = r#"{
  "vars": {
    "despacito": null,
    "despacito2": null
  }
}"#;
    let frame = Annotated::new(Frame {
        vars: Annotated::new({
            let mut vars = Object::new();
            vars.insert("despacito".to_string(), Annotated::empty());
            vars.insert("despacito2".to_string(), Annotated::empty());
            vars.into()
        }),
        ..Default::default()
    });

    assert_eq_str!(output, frame.to_json_pretty().unwrap());
}

#[test]
fn test_frame_empty_context_lines() {
    let json = r#"{
  "pre_context": [
    ""
  ],
  "context_line": "",
  "post_context": [
    ""
  ]
}"#;

    let frame = Annotated::new(Frame {
        pre_context: Annotated::new(vec![Annotated::new("".to_string())]),
        context_line: Annotated::new("".to_string()),
        post_context: Annotated::new(vec![Annotated::new("".to_string())]),
        ..Frame::default()
    });

    assert_eq_dbg!(frame, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, frame.to_json_pretty().unwrap());
}

#[test]
fn test_php_frame_vars() {
    // Buggy PHP SDKs send us this stuff
    //
    // Port of https://github.com/getsentry/sentry/commit/73d9a061dcac3ab8c318a09735601a12e81085dd

    let input = r#"{
  "vars": ["foo", "bar", "baz", null]
}"#;

    let output = r#"{
  "vars": {
    "0": "foo",
    "1": "bar",
    "2": "baz",
    "3": null
  }
}"#;

    let frame = Annotated::new(Frame {
        vars: Annotated::new({
            let mut vars = Object::new();
            vars.insert("0".to_string(), Annotated::new("foo".to_string().into()));
            vars.insert("1".to_string(), Annotated::new("bar".to_string().into()));
            vars.insert("2".to_string(), Annotated::new("baz".to_string().into()));
            vars.insert("3".to_string(), Annotated::empty());
            vars.into()
        }),
        ..Default::default()
    });

    assert_eq_dbg!(frame, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, frame.to_json_pretty().unwrap());
}
