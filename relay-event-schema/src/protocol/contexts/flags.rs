use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Flags context.
///
/// The flags context is a collection of flag evaluations performed during the lifetime
/// of a process. The flags are submitted in the order they were evaluated to preserve
/// the state transformations taking place in the application.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct FlagsContext {
    /// An list of flag evaluation results in the order they were evaluated.
    pub values: Annotated<Vec<Annotated<FlagsContextItem>>>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

/// Flags context item.
///
/// A flag context item represents an individual flag evaluation result. It contains
/// the name of the flag and its evaluation result.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct FlagsContextItem {
    /// The name of the evaluated flag.
    #[metastructure(max_chars = 200, allow_chars = "a-zA-Z0-9_.:-")]
    pub flag: Annotated<String>,
    /// The result of the flag evaluation. Evaluation results can be any valid JSON
    /// type.
    #[metastructure(max_chars = 200, deny_chars = "\n")]
    pub result: Annotated<Value>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for FlagsContext {
    fn default_key() -> &'static str {
        "flags"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Flags(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Flags(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Flags(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Flags(Box::new(self))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_deserializing_flag_context() {
        let json = r#"{
  "values": [
    {
      "flag": "abc",
      "result": true
    },
    {
      "flag": "def",
      "result": false
    }
  ],
  "type": "flags"
}"#;

        let flags = vec![
            Annotated::new(FlagsContextItem {
                flag: Annotated::new("abc".to_string()),
                result: Annotated::new(Value::Bool(true)),
                other: Object::default(),
            }),
            Annotated::new(FlagsContextItem {
                flag: Annotated::new("def".to_string()),
                result: Annotated::new(Value::Bool(false)),
                other: Object::default(),
            }),
        ];

        let context = Annotated::new(Context::Flags(Box::new(FlagsContext {
            values: Annotated::new(flags),
            other: Object::default(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
