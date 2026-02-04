use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Culture information.
///
/// Culture context describes the cultural properties relevant to how software is used
/// in specific regions or locales.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct CultureContext {
    /// The calendar system in use.
    ///
    /// For example, `GregorianCalendar`.
    pub calendar: Annotated<String>,

    /// Human-readable name of the culture.
    ///
    /// For example, `English (United States)`.
    pub display_name: Annotated<String>,

    /// The name identifier, usually following the RFC 4646.
    ///
    /// For example, `en-US` or `pt-BR`.
    pub locale: Annotated<String>,

    /// Whether the locale uses 24-hour time format.
    pub is_24_hour_format: Annotated<bool>,

    /// The timezone of the locale.
    ///
    /// For example, `Europe/Vienna`.
    pub timezone: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for CultureContext {
    fn default_key() -> &'static str {
        "culture"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Culture(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Culture(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Culture(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Culture(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_culture_context_roundtrip() {
        let json = r#"{
  "calendar": "GregorianCalendar",
  "display_name": "English (United States)",
  "locale": "en-US",
  "is_24_hour_format": false,
  "timezone": "Europe/Vienna",
  "other": "value",
  "type": "culture"
}"#;
        let context = Annotated::new(Context::Culture(Box::new(CultureContext {
            calendar: Annotated::new("GregorianCalendar".to_owned()),
            display_name: Annotated::new("English (United States)".to_owned()),
            locale: Annotated::new("en-US".to_owned()),
            is_24_hour_format: Annotated::new(false),
            timezone: Annotated::new("Europe/Vienna".to_owned()),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_owned(),
                    Annotated::new(Value::String("value".to_owned())),
                );
                map
            },
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
