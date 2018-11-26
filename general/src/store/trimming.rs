use crate::processor::{MaxChars, ProcessValue, ProcessingState, Processor};
use crate::types::{Annotated, Array, Object, Remark, RemarkType};

#[derive(Clone, Copy, Debug)]
struct BagSizeState {
    size_remaining: usize,
    depth_remaining: usize,
}

#[derive(Default)]
pub struct TrimmingProcessor {
    bag_size_state: Option<BagSizeState>,
}

impl TrimmingProcessor {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Processor for TrimmingProcessor {
    fn process_string(
        &mut self,
        mut value: Annotated<String>,
        state: ProcessingState,
    ) -> Annotated<String> {
        // field local max chars
        if let Some(max_chars) = state.attrs().max_chars {
            value = value.trim_string(max_chars);
        }

        // the remaining size in bytes is used when we're in bag stripping mode.
        if let Some(mut bag_size_state) = self.bag_size_state {
            let chars_left = bag_size_state.size_remaining;
            value = value.trim_string(MaxChars::Hard(chars_left));
            if let Some(ref value) = value.0 {
                bag_size_state.size_remaining = bag_size_state
                    .size_remaining
                    .saturating_sub(value.chars().count());
            }
            self.bag_size_state = Some(bag_size_state);
        }

        value
    }

    fn process_object<T: ProcessValue>(
        &mut self,
        mut value: Annotated<Object<T>>,
        state: ProcessingState,
    ) -> Annotated<Object<T>>
    where
        Self: Sized,
    {
        let old_bag_size_state = self.bag_size_state;

        // if we encounter a bag size attribute it resets the depth and size
        // that is permitted below it.
        if let Some(bag_size) = state.attrs().bag_size {
            self.bag_size_state = Some(BagSizeState {
                size_remaining: bag_size.max_size(),
                depth_remaining: bag_size.max_depth() + 1,
            });
        }

        // if we need to check the bag size, then we go down a different path
        if let Some(mut bag_size_state) = self.bag_size_state {
            bag_size_state.depth_remaining = bag_size_state.depth_remaining.saturating_sub(1);
            bag_size_state.size_remaining = bag_size_state.size_remaining.saturating_sub(2);
            self.bag_size_state = Some(bag_size_state);

            if let Annotated(Some(items), mut meta) = value {
                if bag_size_state.depth_remaining == 0 {
                    self.bag_size_state = old_bag_size_state;
                    meta.add_remark(Remark {
                        ty: RemarkType::Removed,
                        rule_id: "!limit".to_string(),
                        range: None,
                    });
                    value = Annotated(None, meta);
                } else {
                    let original_length = items.len();
                    let mut rv = Object::new();
                    for (key, value) in items.into_iter() {
                        let trimmed_value = {
                            let inner_state = state.enter_borrowed(&key, None);
                            ProcessValue::process_value(value, self, inner_state)
                        };

                        // update sizes
                        let value_len = trimmed_value.estimate_size() + 1;
                        bag_size_state.size_remaining =
                            bag_size_state.size_remaining.saturating_sub(value_len);
                        self.bag_size_state = Some(bag_size_state);

                        rv.insert(key, trimmed_value);
                        if bag_size_state.size_remaining == 0 {
                            break;
                        }
                    }
                    if rv.len() != original_length {
                        meta.original_length = Some(original_length as u32);
                    }
                    self.bag_size_state = old_bag_size_state;
                    value = Annotated(Some(rv), meta);
                }
            }
        } else {
            value = ProcessValue::process_child_values(value, self, state);
        }

        self.bag_size_state = old_bag_size_state;
        value
    }

    fn process_array<T: ProcessValue>(
        &mut self,
        mut value: Annotated<Array<T>>,
        state: ProcessingState,
    ) -> Annotated<Array<T>>
    where
        Self: Sized,
    {
        let old_bag_size_state = self.bag_size_state;

        // if we encounter a bag size attribute it resets the depth and size
        // that is permitted below it.
        if let Some(bag_size) = state.attrs().bag_size {
            self.bag_size_state = Some(BagSizeState {
                size_remaining: bag_size.max_size(),
                depth_remaining: bag_size.max_depth() + 1,
            });
        }

        // if we need to check the bag size, then we go down a different path
        if let Some(mut bag_size_state) = self.bag_size_state {
            bag_size_state.depth_remaining = bag_size_state.depth_remaining.saturating_sub(1);
            bag_size_state.size_remaining = bag_size_state.size_remaining.saturating_sub(2);
            self.bag_size_state = Some(bag_size_state);

            if let Annotated(Some(items), mut meta) = value {
                if bag_size_state.depth_remaining == 0 {
                    meta.add_remark(Remark {
                        ty: RemarkType::Removed,
                        rule_id: "!limit".to_string(),
                        range: None,
                    });
                    value = Annotated(None, meta);
                } else {
                    let original_length = items.len();
                    let mut rv = Array::new();
                    for (idx, value) in items.into_iter().enumerate() {
                        let inner_state = state.enter_index(idx, None);
                        let trimmed_value = ProcessValue::process_value(value, self, inner_state);

                        // update sizes
                        let value_len = trimmed_value.estimate_size();
                        bag_size_state.size_remaining =
                            bag_size_state.size_remaining.saturating_sub(value_len);
                        self.bag_size_state = Some(bag_size_state);

                        rv.push(trimmed_value);
                        if bag_size_state.size_remaining == 0 {
                            break;
                        }
                    }
                    if rv.len() != original_length {
                        meta.original_length = Some(original_length as u32);
                    }
                    value = Annotated(Some(rv), meta);
                }
            }
        } else {
            value = ProcessValue::process_child_values(value, self, state);
        }

        self.bag_size_state = old_bag_size_state;
        value
    }
}

#[test]
fn test_basic_trimming() {
    use crate::protocol::Event;
    use crate::types::Annotated;

    use crate::processor::MaxChars;
    use std::iter::repeat;

    let mut processor = TrimmingProcessor::new();

    let event = Annotated::new(Event {
        culprit: Annotated::new(repeat("x").take(300).collect::<String>()),
        ..Default::default()
    });

    let event = event.process(&mut processor);

    assert_eq_dbg!(
        event.0.unwrap().culprit,
        Annotated::new(repeat("x").take(300).collect::<String>()).trim_string(MaxChars::Symbol)
    );
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
    let event = Annotated::new(Event {
        extra: databag,
        ..Default::default()
    });

    let stripped_event = event.process(&mut processor);
    let stripped_extra = stripped_event.0.unwrap().extra;

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
    let event = Annotated::new(Event {
        extra: databag,
        ..Default::default()
    });

    let stripped_event = event.process(&mut processor);
    let stripped_extra = stripped_event.0.unwrap().extra;

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
    use crate::protocol::{Event, Tags};
    use crate::types::Annotated;
    use std::iter::repeat;

    let mut processor = TrimmingProcessor::new();

    let event = Annotated::new(Event {
        tags: Annotated::new(Tags(vec![Annotated::new((
            Annotated::new(repeat("x").take(200).collect()),
            Annotated::new(repeat("x").take(300).collect()),
        ))])),
        ..Default::default()
    });

    let stripped_event = event.process(&mut processor);
    let json = stripped_event
        .0
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
            })).take(200)
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
                    })).take(200)
                    .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        })])),
        ..Default::default()
    });

    let mut processor = TrimmingProcessor::new();
    let stripped_event = event.clone().process(&mut processor);

    assert_eq_str!(
        event.to_json_pretty().unwrap(),
        stripped_event.to_json_pretty().unwrap()
    );
}
