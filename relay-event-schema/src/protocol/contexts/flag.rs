use crate::processor::ProcessValue;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Value};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct FlagContext {
    pub values: Annotated<Vec<Annotated<FlagContextItem>>>,
}

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct FlagContextItem {
    #[metastructure(max_chars = 200, allow_chars = "a-zA-Z0-9_.:-")]
    pub flag: Annotated<String>,
    #[metastructure(max_chars = 200, deny_chars = "\n")]
    pub result: Annotated<Value>,
}

impl super::DefaultContext for FlagContext {
    fn default_key() -> &'static str {
        "replay"
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
    fn test_deserializing_feature_context() {
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
            Annotated::new(FlagContextItem {
                flag: Annotated::new("abc".to_string()),
                result: Annotated::new(Value::Bool(true)),
            }),
            Annotated::new(FlagContextItem {
                flag: Annotated::new("def".to_string()),
                result: Annotated::new(Value::Bool(false)),
            }),
        ];

        let context = Annotated::new(Context::Flags(Box::new(FlagContext {
            values: Annotated::new(flags),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
