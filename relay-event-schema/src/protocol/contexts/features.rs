use crate::processor::ProcessValue;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Value};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct FeatureContext {
    pub values: Annotated<Vec<Annotated<FeatureContextItem>>>,
}

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct FeatureContextItem {
    #[metastructure(max_chars = 200, allow_chars = "a-zA-Z0-9_.:-")]
    pub flag: Annotated<String>,
    #[metastructure(max_chars = 200, deny_chars = "\n")]
    pub result: Annotated<Value>,
}

impl super::DefaultContext for FeatureContext {
    fn default_key() -> &'static str {
        "replay"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Feature(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Feature(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Feature(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Feature(Box::new(self))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_deserializing_feature_context() {
        let json = r#"{
  "flags": {
    "values": [
      {
        "flag": "abc",
        "result": true
      },
      {
        "flag": "def",
        "result": false
      }
    ]
  }
}"#;

        let flags = vec![
            Annotated::new(FeatureContextItem {
                flag: Annotated::new("abc".to_string()),
                result: Annotated::new(Value::Bool(true)),
            }),
            Annotated::new(FeatureContextItem {
                flag: Annotated::new("def".to_string()),
                result: Annotated::new(Value::Bool(false)),
            }),
        ];

        let context = Annotated::new(Context::Feature(Box::new(FeatureContext {
            values: Annotated::new(flags),
        })));

        let transformed = r#"{
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
  "type": "feature"
}"#;

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(transformed, context.to_json_pretty().unwrap());
    }
}
