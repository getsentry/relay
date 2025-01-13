use crate::transform::Transform;
use crate::{PiiAttachmentsProcessor, PiiProcessor};
use bytes::Bytes;
use relay_event_schema::processor::{FieldAttrs, Pii, ProcessingState, Processor, ValueType};
use relay_protocol::Meta;
use serde::de;
use serde::de::Error;
use serde_json::Deserializer;
use std::borrow::BorrowMut;
use std::borrow::Cow;
use std::fmt::Formatter;

const FIELD_ATTRS_PII_TRUE: FieldAttrs = FieldAttrs::new().pii(Pii::True);

impl PiiAttachmentsProcessor<'_> {
    pub fn scrub_json(&self, payload: Bytes) -> Vec<u8> {
        let slice = payload.as_ref();
        let output = Vec::new();

        let visitor = JsonScrubVisitor::new(Some(PiiProcessor::new(self.compiled_config)));

        let mut deserializer_inner = Deserializer::from_slice(slice);
        let deserializer = crate::transform::Deserializer::new(&mut deserializer_inner, visitor);

        let mut serializer = serde_json::Serializer::new(output);
        serde_transcode::transcode(deserializer, &mut serializer).unwrap();
        serializer.into_inner()
    }
}

pub struct JsonScrubVisitor<'a> {
    processor: Option<PiiProcessor<'a>>,
    /// The state encoding the current path, which is fed by `push_path` and `pop_path`.
    state: ProcessingState<'a>,
    /// The current path. This is redundant with `state`, which also contains the full path,
    /// but easier to match on.
    path: Vec<String>,
}

impl<'a> JsonScrubVisitor<'a> {
    pub fn new(processor: Option<PiiProcessor<'a>>) -> Self {
        Self {
            processor,
            state: ProcessingState::new_root(None, None),
            path: Vec::new(),
        }
    }
}

// impl<'de> de::Visitor<'de> for JsonScrubVisitor<'_> {
//     type Value = serde_json::Value;
//
//     fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//         formatter.write_str("a JSON document")
//     }
//
//     fn visit_str<E>(mut self, v: &str) -> Result<Self::Value, E>
//     where
//         E: Error
//     {
//         let mut owned = v.to_owned();
//         let mut meta = Meta::default();
//         if let Some(mut processor) = self.processor {
//             processor.process_string(&mut owned, &mut meta, ProcessingState::root()).map_err(E::custom)?;
//         }
//         Ok(serde_json::from_str(&owned).map_err(E::custom)?)
//     }
//
//     fn visit_string<E>(mut self, v: String) -> Result<Self::Value, E>
//     where
//         E: Error
//     {
//         let mut v = v;
//         let mut meta = Meta::default();
//
//         if let Some(ref mut processor) = self.processor {
//             let state  = ProcessingState::root();
//             state.enter_nothing(Some(Cow::Owned(FieldAttrs::new().pii(Pii::True))));
//             processor.process_string(&mut v, &mut meta, state).map_err(E::custom)?;
//         }
//         Ok(serde_json::from_str(&v).map_err(E::custom)?)
//     }
// }

impl<'de> Transform<'de> for JsonScrubVisitor<'de> {
    fn push_path(&mut self, key: &'de str) {
        dbg!(&key);
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
        dbg!(&self.path);
        let popped = self.path.pop();
        dbg!(&popped);
        debug_assert!(popped.is_some()); // pop_path should never be called on an empty state.
    }

    fn transform_str<'a>(&mut self, v: &'a str) -> Cow<'a, str> {
        let mut owned = v.to_owned();
        let mut meta = Meta::default();
        if let Some(ref mut processor) = self.processor.borrow_mut() {
            if let Ok(_) = processor.process_string(&mut owned, &mut meta, &self.state) {
                dbg!(&owned);
                return Cow::Owned(owned);
            }
        }
        Cow::Borrowed("")
    }

    fn transform_string(&mut self, mut v: String) -> Cow<'static, str> {
        let mut meta = Meta::default();
        if let Some(ref mut processor) = self.processor.borrow_mut() {
            if let Ok(_) = processor.process_string(&mut v, &mut meta, &self.state) {
                dbg!(&v);
                return Cow::Owned(v);
            }
        }
        Cow::Borrowed("")
    }
}

mod test {
    use crate::{PiiAttachmentsProcessor, PiiConfig};
    use bytes::Bytes;
    use serde_json::Value;
    use std::collections::HashMap;

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
        let result = processor.scrub_json(payload);
        let parsed: Result<HashMap<String, Value>, _> = serde_json::from_slice(&result);
        let map = parsed.expect("failed to parse scrubbed");
    }
}
