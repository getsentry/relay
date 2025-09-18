use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Unity context.
///
/// The Unity context contains attributes that are specific to Unity applications.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct UnityContext {
    /// Graphics texture copying capabilities.
    pub copy_texture_support: Annotated<String>,

    /// Unity Editor version.
    pub editor_version: Annotated<String>,

    /// Distribution method.
    pub install_mode: Annotated<String>,

    /// Rendering pipeline configuration.
    pub rendering_threading_mode: Annotated<String>,

    /// Target FPS setting.
    pub target_frame_rate: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for UnityContext {
    fn default_key() -> &'static str {
        "unity"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Unity(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Unity(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Unity(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Unity(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;
    use crate::protocol::contexts::DefaultContext;

    #[test]
    fn test_unity_context_roundtrip() {
        let json = r#"{
  "copy_texture_support": "Basic, Copy3D, DifferentTypes, TextureToRT, RTToTexture",
  "editor_version": "2022.1.23f1",
  "install_mode": "Store",
  "rendering_threading_mode": "LegacyJobified",
  "target_frame_rate": "-1",
  "other": "value",
  "type": "unity"
}"#;
        let context = Annotated::new(Context::Unity(Box::new(UnityContext {
            copy_texture_support: Annotated::new(
                "Basic, Copy3D, DifferentTypes, TextureToRT, RTToTexture".to_owned(),
            ),
            editor_version: Annotated::new("2022.1.23f1".to_owned()),
            install_mode: Annotated::new("Store".to_owned()),
            rendering_threading_mode: Annotated::new("LegacyJobified".to_owned()),
            target_frame_rate: Annotated::new("-1".to_owned()),
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

    #[test]
    fn test_unity_context_default_key() {
        assert_eq!(UnityContext::default_key(), "unity");
    }

    #[test]
    fn test_unity_context_minimal() {
        let json = r#"{
  "type": "unity"
}"#;
        let context = Annotated::new(Context::Unity(Box::new(UnityContext::default())));
        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
