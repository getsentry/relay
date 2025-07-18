use relay_protocol::{Annotated, Array, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Template debug information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_template_info")]
pub struct TemplateInfo {
    /// The file name (basename only).
    #[metastructure(pii = "true", max_chars = 128, max_chars_allowance = 20)]
    pub filename: Annotated<String>,

    /// Absolute path to the file.
    #[metastructure(pii = "true", max_chars = 256, max_chars_allowance = 40)]
    pub abs_path: Annotated<String>,

    /// Line number within the source file.
    pub lineno: Annotated<u64>,

    /// Column number within the source file.
    pub colno: Annotated<u64>,

    /// Source code leading up to the current line.
    pub pre_context: Annotated<Array<String>>,

    /// Source code of the current line.
    pub context_line: Annotated<String>,

    /// Source code of the lines after the current line.
    pub post_context: Annotated<Array<String>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use relay_protocol::Map;
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_template_roundtrip() {
        let json = r#"{
  "filename": "myfile.rs",
  "abs_path": "/path/to",
  "lineno": 2,
  "colno": 42,
  "pre_context": [
    "fn main() {"
  ],
  "context_line": "unimplemented!()",
  "post_context": [
    "}"
  ],
  "other": "value"
}"#;
        let template_info = Annotated::new(TemplateInfo {
            filename: Annotated::new("myfile.rs".to_owned()),
            abs_path: Annotated::new("/path/to".to_owned()),
            lineno: Annotated::new(2),
            colno: Annotated::new(42),
            pre_context: Annotated::new(vec![Annotated::new("fn main() {".to_owned())]),
            context_line: Annotated::new("unimplemented!()".to_owned()),
            post_context: Annotated::new(vec![Annotated::new("}".to_owned())]),
            other: {
                let mut map = Map::new();
                map.insert(
                    "other".to_owned(),
                    Annotated::new(Value::String("value".to_owned())),
                );
                map
            },
        });

        assert_eq!(template_info, Annotated::from_json(json).unwrap());
        assert_eq!(json, template_info.to_json_pretty().unwrap());
    }

    #[test]
    fn test_template_default_values() {
        let json = "{}";
        let template_info = Annotated::new(TemplateInfo {
            filename: Annotated::empty(),
            abs_path: Annotated::empty(),
            lineno: Annotated::empty(),
            colno: Annotated::empty(),
            pre_context: Annotated::empty(),
            context_line: Annotated::empty(),
            post_context: Annotated::empty(),
            other: Object::default(),
        });

        assert_eq!(template_info, Annotated::from_json(json).unwrap());
        assert_eq!(json, template_info.to_json().unwrap());
    }
}
