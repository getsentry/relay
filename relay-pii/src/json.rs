use crate::transform::Transform;
use crate::{CompiledPiiConfig, PiiAttachmentsProcessor, PiiProcessor};
use relay_event_schema::processor::{FieldAttrs, Pii, ProcessingState, Processor, ValueType};
use relay_protocol::Meta;
use serde_json::Deserializer;
use std::borrow::Cow;

const FIELD_ATTRS_PII_TRUE: FieldAttrs = FieldAttrs::new().pii(Pii::True);

/// Describes the error cases that can happen during ViewHierarchy scrubbing.
#[derive(Debug, thiserror::Error)]
pub enum ScrubViewHierarchyError {
    /// If the transcoding process fails. This will most likely happen if a JSON document
    /// is invalid.
    #[error("transcoding view hierarchy json failed")]
    TranscodeFailed,
}

impl PiiAttachmentsProcessor<'_> {
    /// Applies PII rules to the given JSON.
    ///
    /// This function will perform PII scrubbing using `serde_transcode`, which means that it
    /// does not have to lead the entire document in memory but will rather perform in on a
    /// per-item basis using a streaming approach.
    ///
    /// Returns a scrubbed copy of the JSON document.
    pub fn scrub_json(&self, payload: &[u8]) -> Result<Vec<u8>, ScrubViewHierarchyError> {
        let output = Vec::new();

        let visitor = JsonScrubVisitor::new(self.compiled_config);

        let mut deserializer_inner = Deserializer::from_slice(payload);
        let deserializer = crate::transform::Deserializer::new(&mut deserializer_inner, visitor);

        let mut serializer = serde_json::Serializer::new(output);
        serde_transcode::transcode(deserializer, &mut serializer)
            .map_err(|_| ScrubViewHierarchyError::TranscodeFailed)?;
        Ok(serializer.into_inner())
    }
}

/// Visitor for JSON file scrubbing. It will be used to walk through the structure and scrub
/// PII based on the config defined in the processor.
pub struct JsonScrubVisitor<'a> {
    processor: PiiProcessor<'a>,
    /// The state encoding the current path, which is fed by `push_path` and `pop_path`.
    state: ProcessingState<'a>,
    /// The current path. This is redundant with `state`, which also contains the full path,
    /// but easier to match on.
    path: Vec<String>,
}

impl<'a> JsonScrubVisitor<'a> {
    /// Creates a new [`JsonScrubVisitor`] using the  supplied config.
    pub fn new(config: &'a CompiledPiiConfig) -> Self {
        let processor = PiiProcessor::new(config);
        Self {
            processor,
            state: ProcessingState::new_root(None, None),
            path: Vec::new(),
        }
    }
}

impl<'de> Transform<'de> for JsonScrubVisitor<'de> {
    fn push_path(&mut self, key: &'de str) {
        self.path.push(key.to_owned());

        self.state = std::mem::take(&mut self.state).enter_owned(
            key.to_owned(),
            Some(Cow::Borrowed(&FIELD_ATTRS_PII_TRUE)),
            Some(ValueType::String), // Pretend everything is a string.
        );
    }

    fn pop_path(&mut self) {
        if let Ok(Some(parent)) = std::mem::take(&mut self.state).try_into_parent() {
            self.state = parent;
        }
        let popped = self.path.pop();
        debug_assert!(popped.is_some()); // pop_path should never be called on an empty state.
    }

    fn transform_str<'a>(&mut self, v: &'a str) -> Cow<'a, str> {
        self.transform_string(v.to_owned())
    }

    fn transform_string(&mut self, mut v: String) -> Cow<'static, str> {
        let mut meta = Meta::default();
        if self
            .processor
            .process_string(&mut v, &mut meta, &self.state)
            .is_err()
        {
            return Cow::Borrowed("");
        }
        Cow::Owned(v)
    }
}

mod test {
    use crate::{PiiAttachmentsProcessor, PiiConfig};
    use bytes::Bytes;
    use serde_json::Value;

    #[test]
    pub fn test_vh() {
        let payload = Bytes::from(
            r#"
        {
          "rendering_system": "UIKIT",
          "identifier": "192.45.128.54",
          "windows": [
            {
              "type": "UIWindow",
              "identifier": "123.123.123.123",
              "width": 414,
              "height": 896,
              "x": 0,
              "y": 0,
              "alpha": 1,
              "visible": true,
              "children": []
            }
          ]
        }
        "#,
        );
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "$string": ["@ip"]
                }
            }
            "#,
        )
        .unwrap();
        let processor = PiiAttachmentsProcessor::new(config.compiled());
        let result = processor.scrub_json(&payload).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!("[ip]", parsed["identifier"].as_str().unwrap());
    }

    #[test]
    pub fn test_vh_nested() {
        let payload = Bytes::from(
            r#"
           {
               "nested": {
                    "stuff": {
                        "ident": "10.0.0.1"
                    }
               }
           }
        "#,
        );
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "nested.stuff.ident": ["@ip"]
                }
            }
        "#,
        )
        .unwrap();

        let processor = PiiAttachmentsProcessor::new(config.compiled());
        let result = processor.scrub_json(&payload).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!("[ip]", parsed["nested"]["stuff"]["ident"].as_str().unwrap());
    }

    #[test]
    pub fn test_vh_not_existing_path() {
        let payload = Bytes::from(
            r#"
           {
               "nested": {
                    "stuff": {
                        "ident": "10.0.0.1"
                    }
               }
           }
        "#,
        );
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "non.existent.path": ["@ip"]
                }
            }
        "#,
        )
        .unwrap();

        let processor = PiiAttachmentsProcessor::new(config.compiled());
        let result = processor.scrub_json(&payload).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(
            "10.0.0.1",
            parsed["nested"]["stuff"]["ident"].as_str().unwrap()
        );
    }

    #[test]
    pub fn test_vh_password() {
        let payload = Bytes::from(
            r#"
                {
                    "rendering_system": "UIKIT",
                    "password": "hunter42"
                }
            "#,
        );
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "$string": ["@password:remove"]
                }
            }
        "#,
        )
        .unwrap();

        let processor = PiiAttachmentsProcessor::new(config.compiled());
        let result = processor.scrub_json(&payload).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!("", parsed["password"]);
        assert_eq!("UIKIT", parsed["rendering_system"]);
    }
}
