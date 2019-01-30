use std::borrow::Cow;

use crate::processor::{estimate_size_flat, process_chunked_value, BagSize, Chunk, MaxChars};
use crate::processor::{process_value, ProcessValue, ProcessingState, Processor, ValueType};
use crate::types::{Array, Empty, Meta, Object, RemarkType, ValueAction};

#[derive(Clone, Debug)]
struct BagSizeState {
    bag_size: BagSize,
    encountered_at_depth: usize,
    size_remaining: usize,
}

#[derive(Default)]
pub struct TrimmingProcessor {
    bag_size_state: Vec<BagSizeState>,
}

impl TrimmingProcessor {
    pub fn new() -> Self {
        Self::default()
    }

    fn should_remove_container<T: Empty>(&self, value: &T, state: &ProcessingState<'_>) -> bool {
        // Heuristic to avoid trimming a value like `[1, 1, 1, 1, ...]` into `[null, null, null,
        // null, ...]`, making it take up more space.
        let bag_size_state = match self.bag_size_state.last() {
            Some(x) => x,
            None => return false,
        };

        let max_depth = bag_size_state.bag_size.max_depth();
        let databag_depth = state.depth() - bag_size_state.encountered_at_depth;

        // The next level will reach max depth
        let reaching_max_depth = databag_depth + 2 > max_depth;

        reaching_max_depth && !value.is_empty()
    }
}

impl Processor for TrimmingProcessor {
    fn before_process<T: ProcessValue>(
        &mut self,
        _: Option<&T>,
        _: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        // If we encounter a bag size attribute it resets the depth and size
        // that is permitted below it.
        if let Some(bag_size) = state.attrs().bag_size {
            self.bag_size_state.push(BagSizeState {
                size_remaining: bag_size.max_size(),
                encountered_at_depth: state.depth(),
                bag_size,
            });
        }

        if let Some(ref mut bag_size_state) = self.bag_size_state.last_mut() {
            // The current depth in the entire event payload minus the depth at which we found the
            // bag_size attribute is the depth where we are at in the databag.
            let databag_depth = state.depth() - bag_size_state.encountered_at_depth;

            let max_depth_reached = databag_depth + 1 > bag_size_state.bag_size.max_depth();
            let max_bag_size_reached = bag_size_state.size_remaining == 0;

            if max_depth_reached || max_bag_size_reached {
                // TODO: Create remarks (ensure they do not bloat event)
                return ValueAction::DeleteHard;
            }
        }

        ValueAction::Keep
    }

    fn after_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        _: &mut Meta,
        state: &ProcessingState<'_>,
    ) {
        if let Some(ref mut bag_size_state) = self.bag_size_state.last_mut() {
            // If our current depth is the one where we found a bag_size attribute, this means we
            // are done processing a databag. Pop the bag size state.
            if state.depth() == bag_size_state.encountered_at_depth {
                self.bag_size_state.pop().unwrap();
            }
        }

        if let Some(ref mut bag_size_state) = self.bag_size_state.last_mut() {
            // After processing a value, update the remaining bag size. We have a separate if-let
            // here in case somebody defines nested databags (a struct with bag_size that contains
            // another struct with a different bag_size), in case we just exited a databag we want
            // to update the bag_size_state of the outer databag with the remaining size.
            //
            // This also has to happen after string trimming, which is why it's running in
            // after_process.
            let item_length = estimate_size_flat(value) + 1;
            bag_size_state.size_remaining =
                bag_size_state.size_remaining.saturating_sub(item_length);
        }
    }

    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        if let Some(max_chars) = state.attrs().max_chars {
            trim_string(value, meta, max_chars);
        }

        if let Some(ref mut bag_size_state) = self.bag_size_state.last_mut() {
            trim_string(value, meta, MaxChars::Hard(bag_size_state.size_remaining));
        }

        ValueAction::Keep
    }

    fn process_array<T>(
        &mut self,
        value: &mut Array<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction
    where
        T: ProcessValue,
    {
        // If we need to check the bag size, then we go down a different path
        if !self.bag_size_state.is_empty() {
            let original_length = value.len();

            if self.should_remove_container(value, state) {
                return ValueAction::DeleteHard;
            }

            let mut split_index = None;
            for (index, item) in value.iter_mut().enumerate() {
                if self.bag_size_state.last().unwrap().size_remaining == 0 {
                    split_index = Some(index);
                    break;
                }

                let item_state = state.enter_index(index, None, ValueType::for_field(item));
                process_value(item, self, &item_state);
            }

            if let Some(split_index) = split_index {
                value.split_off(split_index);
            }

            if value.len() != original_length {
                meta.set_original_length(Some(original_length));
            }
        } else {
            value.process_child_values(self, state);
        }

        ValueAction::Keep
    }

    fn process_object<T>(
        &mut self,
        value: &mut Object<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction
    where
        T: ProcessValue,
    {
        // If we need to check the bag size, then we go down a different path
        if !self.bag_size_state.is_empty() {
            let original_length = value.len();

            if self.should_remove_container(value, state) {
                return ValueAction::DeleteHard;
            }

            let mut split_key = None;
            for (key, item) in value.iter_mut() {
                if self.bag_size_state.last().unwrap().size_remaining == 0 {
                    split_key = Some(key.to_owned());
                    break;
                }

                let item_state = state.enter_borrowed(key, None, ValueType::for_field(item));
                process_value(item, self, &item_state);
            }

            if let Some(split_key) = split_key {
                value.split_off(&split_key);
            }

            if value.len() != original_length {
                meta.set_original_length(Some(original_length));
            }
        } else {
            value.process_child_values(self, state);
        }

        ValueAction::Keep
    }
}

/// Trims the string to the given maximum length and updates meta data.
fn trim_string(value: &mut String, meta: &mut Meta, max_chars: MaxChars) {
    let soft_limit = max_chars.limit();
    let hard_limit = soft_limit + max_chars.allowance();

    if bytecount::num_chars(value.as_bytes()) <= hard_limit {
        return;
    }

    process_chunked_value(value, meta, |chunks| {
        let mut length = 0;
        let mut new_chunks = vec![];

        for chunk in chunks {
            let chunk_chars = chunk.count();

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

                    new_chunks.push(Chunk::Text {
                        text: Cow::Owned(remaining),
                    });
                }
            }

            new_chunks.push(Chunk::Redaction {
                text: Cow::Borrowed("..."),
                rule_id: Cow::Borrowed("!limit"),
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

    process_value(&mut event, &mut processor, ProcessingState::root());

    let mut expected = Annotated::new(repeat("x").take(300).collect::<String>());
    expected.apply(|v, m| trim_string(v, m, MaxChars::Culprit));
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

    process_value(&mut event, &mut processor, ProcessingState::root());
    let stripped_extra = &event.value().unwrap().extra;
    let json = stripped_extra.to_json().unwrap();

    assert_eq_str!(
        json,
        r#"{"key_1":"value 1","key_2":{"key5":{"key4":{"key3":null}}}}"#
    );
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

    process_value(&mut event, &mut processor, ProcessingState::root());
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
  "key_80": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...",
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
            52,
            55
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
        tags: Annotated::new(Tags(
            vec![Annotated::new(TagEntry(
                Annotated::new(repeat("x").take(200).collect()),
                Annotated::new(repeat("x").take(300).collect()),
            ))]
            .into(),
        )),
        ..Default::default()
    });

    process_value(&mut event, &mut processor, ProcessingState::root());
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
    process_value(&mut stripped_event, &mut processor, ProcessingState::root());

    assert_eq_str!(
        event.to_json_pretty().unwrap(),
        stripped_event.to_json_pretty().unwrap()
    );
}

#[test]
fn test_custom_context_trimming() {
    use std::iter::repeat;

    use crate::protocol::{Context, ContextInner, Contexts};
    use crate::types::{Annotated, Object, Value};

    let mut contexts = Object::new();
    for i in 1..2 {
        contexts.insert(format!("despacito{}", i), {
            let mut context = Object::new();
            context.insert(
                "foo".to_string(),
                Annotated::new(Value::String(repeat('a').take(4000).collect())),
            );
            context.insert(
                "bar".to_string(),
                Annotated::new(Value::String(repeat('a').take(5000).collect())),
            );
            Annotated::new(ContextInner(Context::Other(context)))
        });
    }

    let mut contexts = Annotated::new(Contexts(contexts));
    let mut processor = TrimmingProcessor::new();
    process_value(&mut contexts, &mut processor, ProcessingState::root());

    for i in 1..2 {
        let other = match contexts
            .value()
            .unwrap()
            .get(&format!("despacito{}", i))
            .unwrap()
            .value()
            .unwrap()
            .0
        {
            Context::Other(ref x) => x,
            _ => panic!("Context has changed type!"),
        };

        assert_eq!(
            other
                .get("bar")
                .unwrap()
                .value()
                .unwrap()
                .as_str()
                .unwrap()
                .len(),
            5000
        );
        assert_eq!(
            other
                .get("foo")
                .unwrap()
                .value()
                .unwrap()
                .as_str()
                .unwrap()
                .len(),
            3189
        );
    }
}

#[test]
fn test_extra_trimming_long_arrays() {
    use std::iter::repeat;

    use crate::protocol::Event;
    use crate::types::{Annotated, Object, Value};

    let mut extra = Object::new();
    extra.insert("foo".to_string(), {
        Annotated::new(Value::Array(
            repeat(Annotated::new(Value::U64(1)))
                .take(200_000)
                .collect(),
        ))
    });

    let mut event = Annotated::new(Event {
        extra: Annotated::new(extra),
        ..Default::default()
    });

    let mut processor = TrimmingProcessor::new();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let arr = match event
        .value()
        .unwrap()
        .extra
        .value()
        .unwrap()
        .get("foo")
        .unwrap()
        .value()
        .unwrap()
    {
        Value::Array(x) => x,
        x => panic!("Wrong type: {:?}", x),
    };

    assert_eq!(arr.len(), 4096);
}

#[test]
fn test_newtypes_do_not_add_to_depth() {
    #[derive(Debug, Clone, FromValue, ToValue, ProcessValue, Empty)]
    struct WrappedString(String);

    #[derive(Debug, Clone, FromValue, ToValue, ProcessValue, Empty)]
    struct StructChild2 {
        inner: Annotated<WrappedString>,
    }

    #[derive(Debug, Clone, FromValue, ToValue, ProcessValue, Empty)]
    struct StructChild {
        inner: Annotated<StructChild2>,
    }

    #[derive(Debug, Clone, FromValue, ToValue, ProcessValue, Empty)]
    struct Struct {
        #[metastructure(bag_size = "small")]
        inner: Annotated<StructChild>,
    }

    let mut value = Annotated::new(Struct {
        inner: Annotated::new(StructChild {
            inner: Annotated::new(StructChild2 {
                inner: Annotated::new(WrappedString("hi".to_string())),
            }),
        }),
    });

    let mut processor = TrimmingProcessor::new();
    process_value(&mut value, &mut processor, ProcessingState::root());

    // Ensure stack does not leak with newtypes
    assert!(processor.bag_size_state.is_empty());

    assert_eq_str!(
        value.to_json().unwrap(),
        r#"{"inner":{"inner":{"inner":"hi"}}}"#
    );
}
