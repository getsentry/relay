use crate::processor::{estimate_size, process_chunked_value, Chunk, MaxChars};
use crate::processor::{process_value, BagSize, ProcessValue, ProcessingState, Processor};
use crate::types::{Array, Meta, Object, Remark, RemarkType, ValueAction};

#[derive(Clone, Copy, Debug)]
struct BagSizeState {
    size_remaining: usize,
    depth_remaining: usize,
}

impl From<BagSize> for BagSizeState {
    fn from(bag_size: BagSize) -> Self {
        BagSizeState {
            size_remaining: bag_size.max_size(),
            depth_remaining: bag_size.max_depth() + 1,
        }
    }
}

#[derive(Default)]
pub struct TrimmingProcessor {
    bag_size_state: Option<BagSizeState>,
}

impl TrimmingProcessor {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Processor for TrimmingProcessor {
    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: ProcessingState<'_>,
    ) -> ValueAction {
        if let Some(ref mut bag_size_state) = self.bag_size_state {
            trim_string(value, meta, MaxChars::Hard(bag_size_state.size_remaining));

            bag_size_state.size_remaining = bag_size_state
                .size_remaining
                .saturating_sub(value.chars().count());
        } else if let Some(max_chars) = state.attrs().max_chars {
            trim_string(value, meta, max_chars);
        }

        ValueAction::Keep
    }

    fn process_array<T>(
        &mut self,
        value: &mut Array<T>,
        meta: &mut Meta,
        state: ProcessingState<'_>,
    ) -> ValueAction
    where
        T: ProcessValue,
    {
        let old_bag_size_state = self.bag_size_state;
        let mut result = ValueAction::Keep;

        // If we encounter a bag size attribute it resets the depth and size
        // that is permitted below it.
        if let Some(bag_size) = state.attrs().bag_size {
            self.bag_size_state = Some(BagSizeState {
                size_remaining: bag_size.max_size(),
                depth_remaining: bag_size.max_depth() + 1,
            });
        }

        // If we need to check the bag size, then we go down a different path
        if let Some(mut bag_size_state) = self.bag_size_state {
            bag_size_state.depth_remaining = bag_size_state.depth_remaining.saturating_sub(1);
            bag_size_state.size_remaining = bag_size_state.size_remaining.saturating_sub(2);
            self.bag_size_state = Some(bag_size_state);

            if bag_size_state.depth_remaining == 0 {
                meta.add_remark(Remark {
                    ty: RemarkType::Removed,
                    rule_id: "!limit".to_string(),
                    range: None,
                });
                result = ValueAction::DeleteHard;
            } else {
                let original_length = value.len();

                let mut split_index = None;
                for (index, item) in value.iter_mut().enumerate() {
                    if bag_size_state.size_remaining == 0 {
                        split_index = Some(index);
                        break;
                    }

                    let item_state = state.enter_index(index, None);
                    process_value(item, self, item_state);

                    let item_length = estimate_size(&item) + 1;
                    bag_size_state.size_remaining =
                        bag_size_state.size_remaining.saturating_sub(item_length);
                    self.bag_size_state = Some(bag_size_state);
                }

                if let Some(split_index) = split_index {
                    value.split_off(split_index);
                }

                if value.len() != original_length {
                    meta.set_original_length(Some(original_length));
                }
            }
        } else {
            value.process_child_values(self, state);
        }

        self.bag_size_state = old_bag_size_state;
        result
    }

    fn process_object<T>(
        &mut self,
        value: &mut Object<T>,
        meta: &mut Meta,
        state: ProcessingState<'_>,
    ) -> ValueAction
    where
        T: ProcessValue,
    {
        let old_bag_size_state = self.bag_size_state;
        let mut result = ValueAction::Keep;

        // If we encounter a bag size attribute it resets the depth and size
        // that is permitted below it.
        if let Some(bag_size) = state.attrs().bag_size {
            self.bag_size_state = Some(BagSizeState {
                size_remaining: bag_size.max_size(),
                depth_remaining: bag_size.max_depth() + 1,
            });
        }

        // If we need to check the bag size, then we go down a different path
        if let Some(mut bag_size_state) = self.bag_size_state {
            bag_size_state.depth_remaining = bag_size_state.depth_remaining.saturating_sub(1);
            bag_size_state.size_remaining = bag_size_state.size_remaining.saturating_sub(2);
            self.bag_size_state = Some(bag_size_state);

            if bag_size_state.depth_remaining == 0 {
                meta.add_remark(Remark {
                    ty: RemarkType::Removed,
                    rule_id: "!limit".to_string(),
                    range: None,
                });
                result = ValueAction::DeleteHard;
            } else {
                let original_length = value.len();

                let mut split_key = None;
                for (key, item) in value.iter_mut() {
                    if bag_size_state.size_remaining == 0 {
                        split_key = Some(key.to_owned());
                        break;
                    }

                    let item_state = state.enter_borrowed(key, None);
                    process_value(item, self, item_state);

                    let item_length = estimate_size(&item) + 1;
                    bag_size_state.size_remaining =
                        bag_size_state.size_remaining.saturating_sub(item_length);
                    self.bag_size_state = Some(bag_size_state);
                }

                if let Some(split_key) = split_key {
                    value.split_off(&split_key);
                }

                if value.len() != original_length {
                    meta.set_original_length(Some(original_length));
                }
            }
        } else {
            value.process_child_values(self, state);
        }

        self.bag_size_state = old_bag_size_state;
        result
    }
}

/// Trims the string to the given maximum length and updates meta data.
fn trim_string(value: &mut String, meta: &mut Meta, max_chars: MaxChars) {
    let soft_limit = max_chars.limit();
    let hard_limit = soft_limit + max_chars.allowance();

    if value.chars().count() <= hard_limit {
        return;
    }

    process_chunked_value(value, meta, |chunks| {
        let mut length = 0;
        let mut new_chunks = vec![];

        for chunk in chunks {
            let chunk_chars = chunk.chars();

            // if the entire chunk fits, just put it in
            if length + chunk_chars < soft_limit {
                new_chunks.push(chunk);
                length += chunk_chars;
                continue;
            }

            match chunk {
                // if there is enough space for this chunk and the 3 character
                // ellipsis marker we can push the remaining chunk
                Chunk::Redaction { .. } => {
                    if length + chunk_chars + 3 < hard_limit {
                        new_chunks.push(chunk);
                    }
                }

                // if this is a text chunk, we can put the remaining characters in.
                Chunk::Text { text } => {
                    let mut remaining = String::new();
                    for c in text.chars() {
                        if length + 3 < soft_limit {
                            remaining.push(c);
                        } else {
                            break;
                        }
                        length += 1;
                    }
                    new_chunks.push(Chunk::Text { text: remaining });
                }
            }

            new_chunks.push(Chunk::Redaction {
                text: "...".to_string(),
                rule_id: "!limit".to_string(),
                ty: RemarkType::Substituted,
            });
            break;
        }

        new_chunks
    });
}

#[cfg(test)]
use crate::types::Annotated;

#[test]
fn test_string_trimming() {
    use crate::processor::MaxChars;
    use crate::types::{Annotated, Meta, Remark, RemarkType};

    let mut value = Annotated::new("This is my long string I want to have trimmed!".to_string());
    value.apply(|v, m| trim_string(v, m, MaxChars::Hard(20)));

    assert_eq_dbg!(
        value,
        Annotated(Some("This is my long s...".into()), {
            let mut meta = Meta::default();
            meta.add_remark(Remark {
                ty: RemarkType::Substituted,
                rule_id: "!limit".to_string(),
                range: Some((17, 20)),
            });
            meta.set_original_length(Some(46));
            meta
        })
    );
}

#[test]
fn test_basic_trimming() {
    use crate::protocol::Event;
    use crate::types::Annotated;

    use crate::processor::MaxChars;
    use std::iter::repeat;

    let mut processor = TrimmingProcessor::new();

    let mut event = Annotated::new(Event {
        culprit: Annotated::new(repeat("x").take(300).collect::<String>()),
        ..Default::default()
    });

    process_value(&mut event, &mut processor, Default::default());

    let mut expected = Annotated::new(repeat("x").take(300).collect::<String>());
    expected.apply(|v, m| trim_string(v, m, MaxChars::Symbol));
    assert_eq_dbg!(event.value().unwrap().culprit, expected);
}

#[test]
fn test_databag_stripping() {
    use crate::protocol::Event;
    use crate::types::{Annotated, Value};

    let mut processor = TrimmingProcessor::new();

    fn make_nested_object(depth: usize) -> Annotated<Value> {
        if depth == 0 {
            return Annotated::new(Value::String("max depth".to_string()));
        }
        let mut rv = Object::new();
        rv.insert(format!("key{}", depth), make_nested_object(depth - 1));
        Annotated::new(Value::Object(rv))
    }

    let databag = Annotated::new({
        let mut map = Object::new();
        map.insert(
            "key_1".to_string(),
            Annotated::new(Value::String("value 1".to_string())),
        );
        map.insert("key_2".to_string(), make_nested_object(5));
        map
    });
    let mut event = Annotated::new(Event {
        extra: databag,
        ..Default::default()
    });

    process_value(&mut event, &mut processor, Default::default());
    let stripped_extra = &event.value().unwrap().extra;
    let json = stripped_extra.to_json().unwrap();

    assert_eq_str!(json, r#"{"key_1":"value 1","key_2":{"key5":{"key4":{"key3":{"key2":null}}}},"_meta":{"key_2":{"key5":{"key4":{"key3":{"key2":{"":{"rem":[["!limit","x"]]}}}}}}}}"#);
}

#[test]
fn test_databag_array_stripping() {
    use crate::protocol::Event;
    use crate::types::{Annotated, Value};
    use std::iter::repeat;

    let mut processor = TrimmingProcessor::new();

    let databag = Annotated::new({
        let mut map = Object::new();
        for idx in 0..100 {
            map.insert(
                format!("key_{}", idx),
                Annotated::new(Value::String(repeat("x").take(100).collect::<String>())),
            );
        }
        map
    });
    let mut event = Annotated::new(Event {
        extra: databag,
        ..Default::default()
    });

    process_value(&mut event, &mut processor, Default::default());
    let stripped_extra = &event.value().unwrap().extra;
    let json = stripped_extra.to_json_pretty().unwrap();

    assert_eq_str!(json, r#"{
  "key_0": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_1": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_10": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_11": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_12": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_13": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_14": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_15": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_16": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_17": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_18": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_19": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_2": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_20": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_21": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_22": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_23": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_24": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_25": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_26": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_27": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_28": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_29": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_3": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_30": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_31": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_32": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_33": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_34": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_35": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_36": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_37": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_38": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_39": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_4": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_40": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_41": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_42": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_43": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_44": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_45": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_46": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_47": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_48": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_49": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_5": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_50": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_51": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_52": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_53": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_54": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_55": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_56": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_57": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_58": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_59": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_6": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_60": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_61": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_62": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_63": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_64": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_65": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_66": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_67": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_68": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_69": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_7": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_70": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_71": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_72": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_73": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_74": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_75": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_76": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_77": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_78": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_79": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_8": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key_80": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...",
  "_meta": {
    "": {
      "len": 100
    },
    "key_80": {
      "": {
        "rem": [
          [
            "!limit",
            "s",
            50,
            53
          ]
        ],
        "len": 100
      }
    }
  }
}"#);
}

#[test]
fn test_tags_stripping() {
    use crate::protocol::{Event, TagEntry, Tags};
    use crate::types::Annotated;
    use std::iter::repeat;

    let mut processor = TrimmingProcessor::new();

    let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(vec![Annotated::new(TagEntry(
            Annotated::new(repeat("x").take(200).collect()),
            Annotated::new(repeat("x").take(300).collect()),
        ))])),
        ..Default::default()
    });

    process_value(&mut event, &mut processor, Default::default());
    let json = event
        .value()
        .unwrap()
        .tags
        .payload_to_json_pretty()
        .unwrap();

    assert_eq_str!(json, r#"[
  [
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx...",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx..."
  ]
]"#);
}

#[test]
fn test_databag_state_leak() {
    use std::iter::repeat;

    use crate::protocol::{Breadcrumb, Event, Exception, Frame, Stacktrace, Values};
    use crate::types::{Map, Value};

    let event = Annotated::new(Event {
        breadcrumbs: Annotated::new(Values::new(
            repeat(Annotated::new(Breadcrumb {
                data: {
                    let mut map = Map::new();
                    map.insert(
                        "spamspamspam".to_string(),
                        Annotated::new(Value::String("blablabla".to_string())),
                    );
                    Annotated::new(map)
                },
                ..Default::default()
            }))
            .take(200)
            .collect(),
        )),
        exceptions: Annotated::new(Values::new(vec![Annotated::new(Exception {
            ty: Annotated::new("TypeError".to_string()),
            value: Annotated::new("important error message".to_string().into()),
            stacktrace: Annotated::new(Stacktrace {
                frames: Annotated::new(
                    repeat(Annotated::new(Frame {
                        function: Annotated::new("importantFunctionName".to_string()),
                        symbol: Annotated::new("important_symbol".to_string()),
                        ..Default::default()
                    }))
                    .take(200)
                    .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        })])),
        ..Default::default()
    });

    let mut processor = TrimmingProcessor::new();
    let mut stripped_event = event.clone();
    process_value(&mut stripped_event, &mut processor, Default::default());

    assert_eq_str!(
        event.to_json_pretty().unwrap(),
        stripped_event.to_json_pretty().unwrap()
    );
}
