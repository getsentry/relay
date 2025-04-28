use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// OTA (Expo) Updates context.
///
/// Contains the OTA Updates constants present when the event was created.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct OTAUpdatesContext {
    /// The channel name of the current build, if configured for use with EAS Update.
    pub channel: Annotated<String>,

    /// The runtime version of the current build.
    pub runtime_version: Annotated<String>,

    /// The UUID that uniquely identifies the currently running update.
    pub update_id: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for OTAUpdatesContext {
    fn default_key() -> &'static str {
        "ota_updates"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::OTAUpdates(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::OTAUpdates(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::OTAUpdates(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::OTAUpdates(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_ota_updates_context_roundtrip() {
        let json: &str = r#"{
  "channel": "production",
  "runtime_version": "1.0.0",
  "update_id": "12345678-1234-1234-1234-1234567890ab",
  "type": "otaupdates"
}"#;
        let context = Annotated::new(Context::OTAUpdates(Box::new(OTAUpdatesContext {
            channel: Annotated::new("production".to_string()),
            runtime_version: Annotated::new("1.0.0".to_string()),
            update_id: Annotated::new("12345678-1234-1234-1234-1234567890ab".to_string()),
            other: Object::default(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }

    #[test]
    fn test_ota_updates_context_with_extra_keys() {
        let json: &str = r#"{
  "channel": "production",
  "runtime_version": "1.0.0",
  "update_id": "12345678-1234-1234-1234-1234567890ab",
  "created_at": "2023-01-01T00:00:00.000Z",
  "emergency_launch_reason": "some reason",
  "is_embedded_launch": false,
  "is_emergency_launch": true,
  "is_enabled": true,
  "launch_duration": 1000,
  "type": "otaupdates"
}"#;
        let mut other = Object::new();
        other.insert("is_enabled".to_string(), Annotated::new(Value::Bool(true)));
        other.insert(
            "is_embedded_launch".to_string(),
            Annotated::new(Value::Bool(false)),
        );
        other.insert(
            "is_emergency_launch".to_string(),
            Annotated::new(Value::Bool(true)),
        );
        other.insert(
            "emergency_launch_reason".to_string(),
            Annotated::new(Value::String("some reason".to_string())),
        );
        other.insert(
            "launch_duration".to_string(),
            Annotated::new(Value::I64(1000)),
        );
        other.insert(
            "created_at".to_string(),
            Annotated::new(Value::String("2023-01-01T00:00:00.000Z".to_string())),
        );

        let context = Annotated::new(Context::OTAUpdates(Box::new(OTAUpdatesContext {
            channel: Annotated::new("production".to_string()),
            runtime_version: Annotated::new("1.0.0".to_string()),
            update_id: Annotated::new("12345678-1234-1234-1234-1234567890ab".to_string()),
            other,
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }

    #[test]
    fn test_ota_updates_context_with_is_enabled_false() {
        let json: &str = r#"{
  "is_enabled": false,
  "type": "otaupdates"
}"#;
        let mut other = Object::new();
        other.insert("is_enabled".to_string(), Annotated::new(Value::Bool(false)));

        let context = Annotated::new(Context::OTAUpdates(Box::new(OTAUpdatesContext {
            channel: Annotated::empty(),
            runtime_version: Annotated::empty(),
            update_id: Annotated::empty(),
            other,
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
