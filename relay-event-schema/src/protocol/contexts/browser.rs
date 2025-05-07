use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Web browser information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct BrowserContext {
    /// Computed field from `name` and `version`. Needed by the metrics extraction.
    pub browser: Annotated<String>,

    /// Display name of the browser application.
    /// This field is optional and can be inferred from user_agent if not provided.
    pub name: Annotated<String>,

    /// Version string of the browser.
    pub version: Annotated<String>,

    /// Original user agent string.
    pub user_agent: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for BrowserContext {
    fn default_key() -> &'static str {
        "browser"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Browser(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Browser(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Browser(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Browser(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_browser_context_roundtrip() {
        let json = r#"{
  "browser": "Google Chrome 67.0.3396.99",
  "name": "Google Chrome",
  "version": "67.0.3396.99",
  "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
  "other": "value",
  "type": "browser"
}"#;
        let context = Annotated::new(Context::Browser(Box::new(BrowserContext {
            browser: Annotated::new(String::from("Google Chrome 67.0.3396.99")),
            name: Annotated::new("Google Chrome".to_string()),
            version: Annotated::new("67.0.3396.99".to_string()),
            user_agent: Annotated::new("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36".to_string()),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
