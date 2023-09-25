use std::borrow::Cow;

use relay_event_schema::processor::{
    self, BagSize, Chunk, MaxChars, ProcessValue, ProcessingAction, ProcessingResult,
    ProcessingState, Processor, ValueType,
};
use relay_event_schema::protocol::{Frame, RawStacktrace};
use relay_protocol::{Annotated, Array, Empty, Meta, Object, RemarkType, Value};

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
        self.remaining_bag_depth(state) == Some(1) && !value.is_empty()
    }

    #[inline]
    fn remaining_bag_depth(&self, state: &ProcessingState<'_>) -> Option<usize> {
        self.bag_size_state
            .iter()
            .map(|bag_size_state| {
                // The current depth in the entire event payload minus the depth at which we found the
                // bag_size attribute is the depth where we are at in the databag.
                let databag_depth = state.depth() - bag_size_state.encountered_at_depth;
                bag_size_state
                    .bag_size
                    .max_depth()
                    .saturating_sub(databag_depth)
            })
            .min()
    }

    #[inline]
    fn remaining_bag_size(&self) -> Option<usize> {
        self.bag_size_state.iter().map(|x| x.size_remaining).min()
    }
}

impl Processor for TrimmingProcessor {
    fn before_process<T: ProcessValue>(
        &mut self,
        _: Option<&T>,
        _: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // If we encounter a bag size attribute it resets the depth and size
        // that is permitted below it.
        if let Some(bag_size) = state.attrs().bag_size {
            self.bag_size_state.push(BagSizeState {
                size_remaining: bag_size.max_size(),
                encountered_at_depth: state.depth(),
                bag_size,
            });
        }

        if self.remaining_bag_size() == Some(0) {
            // TODO: Create remarks (ensure they do not bloat event)
            return Err(ProcessingAction::DeleteValueHard);
        }

        if self.remaining_bag_depth(state) == Some(0) {
            // TODO: Create remarks (ensure they do not bloat event)
            return Err(ProcessingAction::DeleteValueHard);
        }

        Ok(())
    }

    fn after_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        _: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let Some(ref mut bag_size_state) = self.bag_size_state.last_mut() {
            // If our current depth is the one where we found a bag_size attribute, this means we
            // are done processing a databag. Pop the bag size state.
            if state.depth() == bag_size_state.encountered_at_depth {
                self.bag_size_state.pop().unwrap();
            }
        }

        for bag_size_state in self.bag_size_state.iter_mut() {
            // After processing a value, update the remaining bag sizes. We have a separate if-let
            // here in case somebody defines nested databags (a struct with bag_size that contains
            // another struct with a different bag_size), in case we just exited a databag we want
            // to update the bag_size_state of the outer databag with the remaining size.
            //
            // This also has to happen after string trimming, which is why it's running in
            // after_process.

            if state.entered_anything() {
                // Do not subtract if state is from newtype struct.
                let item_length = relay_protocol::estimate_size_flat(value) + 1;
                bag_size_state.size_remaining =
                    bag_size_state.size_remaining.saturating_sub(item_length);
            }
        }

        Ok(())
    }

    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let Some(max_chars) = state.attrs().max_chars {
            trim_string(value, meta, max_chars);
        }

        if let Some(ref mut bag_size_state) = self.bag_size_state.last_mut() {
            trim_string(value, meta, MaxChars::Hard(bag_size_state.size_remaining));
        }

        Ok(())
    }

    fn process_array<T>(
        &mut self,
        value: &mut Array<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        T: ProcessValue,
    {
        // If we need to check the bag size, then we go down a different path
        if !self.bag_size_state.is_empty() {
            let original_length = value.len();

            if self.should_remove_container(value, state) {
                return Err(ProcessingAction::DeleteValueHard);
            }

            let mut split_index = None;
            for (index, item) in value.iter_mut().enumerate() {
                if self.remaining_bag_size().unwrap() == 0 {
                    split_index = Some(index);
                    break;
                }

                let item_state = state.enter_index(index, None, ValueType::for_field(item));
                processor::process_value(item, self, &item_state)?;
            }

            if let Some(split_index) = split_index {
                let _ = value.split_off(split_index);
            }

            if value.len() != original_length {
                meta.set_original_length(Some(original_length));
            }
        } else {
            value.process_child_values(self, state)?;
        }

        Ok(())
    }

    fn process_object<T>(
        &mut self,
        value: &mut Object<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        T: ProcessValue,
    {
        // If we need to check the bag size, then we go down a different path
        if !self.bag_size_state.is_empty() {
            let original_length = value.len();

            if self.should_remove_container(value, state) {
                return Err(ProcessingAction::DeleteValueHard);
            }

            let mut split_key = None;
            for (key, item) in value.iter_mut() {
                if self.remaining_bag_size().unwrap() == 0 {
                    split_key = Some(key.to_owned());
                    break;
                }

                let item_state = state.enter_borrowed(key, None, ValueType::for_field(item));
                processor::process_value(item, self, &item_state)?;
            }

            if let Some(split_key) = split_key {
                let _ = value.split_off(&split_key);
            }

            if value.len() != original_length {
                meta.set_original_length(Some(original_length));
            }
        } else {
            value.process_child_values(self, state)?;
        }

        Ok(())
    }

    fn process_value(
        &mut self,
        value: &mut Value,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        match value {
            Value::Array(_) | Value::Object(_) => {
                if self.remaining_bag_depth(state) == Some(1) {
                    if let Ok(x) = serde_json::to_string(&value) {
                        // Error case should not be possible
                        *value = Value::String(x);
                    }
                }
            }
            _ => (),
        }

        value.process_child_values(self, state)?;
        Ok(())
    }

    fn process_raw_stacktrace(
        &mut self,
        stacktrace: &mut RawStacktrace,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        processor::apply(&mut stacktrace.frames, |frames, meta| {
            enforce_frame_hard_limit(frames, meta, 250);
            Ok(())
        })?;

        stacktrace.process_child_values(self, state)?;

        processor::apply(&mut stacktrace.frames, |frames, _meta| {
            slim_frame_data(frames, 50);
            Ok(())
        })?;

        Ok(())
    }
}

/// Trims the string to the given maximum length and updates meta data.
fn trim_string(value: &mut String, meta: &mut Meta, max_chars: MaxChars) {
    let soft_limit = max_chars.limit();
    let hard_limit = soft_limit + max_chars.allowance();

    if bytecount::num_chars(value.as_bytes()) <= hard_limit {
        return;
    }

    processor::process_chunked_value(value, meta, |chunks| {
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

fn enforce_frame_hard_limit(frames: &mut Array<Frame>, meta: &mut Meta, limit: usize) {
    // Trim down the frame list to a hard limit. Prioritize the last frames.
    let original_length = frames.len();
    if original_length > limit {
        meta.set_original_length(Some(original_length));
        let _ = frames.drain(0..original_length - limit);
    }
}

/// Remove excess metadata for middle frames which go beyond `frame_allowance`.
///
/// This is supposed to be equivalent to `slim_frame_data` in Sentry.
fn slim_frame_data(frames: &mut Array<Frame>, frame_allowance: usize) {
    let frames_len = frames.len();

    if frames_len <= frame_allowance {
        return;
    }

    // Avoid ownership issues by only storing indices
    let mut app_frame_indices = Vec::with_capacity(frames_len);
    let mut system_frame_indices = Vec::with_capacity(frames_len);

    for (i, frame) in frames.iter().enumerate() {
        if let Some(frame) = frame.value() {
            match frame.in_app.value() {
                Some(true) => app_frame_indices.push(i),
                _ => system_frame_indices.push(i),
            }
        }
    }

    let app_count = app_frame_indices.len();
    let system_allowance_half = frame_allowance.saturating_sub(app_count) / 2;
    let system_frames_to_remove = system_frame_indices
        .get(system_allowance_half..system_frame_indices.len() - system_allowance_half)
        .unwrap_or(&[]);

    let remaining = frames_len
        .saturating_sub(frame_allowance)
        .saturating_sub(system_frames_to_remove.len());
    let app_allowance_half = app_count.saturating_sub(remaining) / 2;
    let app_frames_to_remove = app_frame_indices
        .get(app_allowance_half..app_frame_indices.len() - app_allowance_half)
        .unwrap_or(&[]);

    // TODO: Which annotation to set?

    for i in system_frames_to_remove.iter().chain(app_frames_to_remove) {
        if let Some(frame) = frames.get_mut(*i) {
            if let Some(ref mut frame) = frame.value_mut().as_mut() {
                frame.vars = Annotated::empty();
                frame.pre_context = Annotated::empty();
                frame.post_context = Annotated::empty();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat;

    use relay_event_schema::processor::MaxChars;
    use relay_event_schema::protocol::{
        Breadcrumb, Context, Contexts, Event, Exception, ExtraValue, Frame, RawStacktrace,
        TagEntry, Tags, Values,
    };
    use relay_protocol::{
        Annotated, Map, Meta, Object, Remark, RemarkType, SerializableAnnotated, Value,
    };
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_string_trimming() {
        let mut value =
            Annotated::new("This is my long string I want to have trimmed!".to_string());
        processor::apply(&mut value, |v, m| {
            trim_string(v, m, MaxChars::Hard(20));
            Ok(())
        })
        .unwrap();

        assert_eq!(
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
        let mut processor = TrimmingProcessor::new();

        let mut event = Annotated::new(Event {
            culprit: Annotated::new("x".repeat(300)),
            ..Default::default()
        });

        processor::process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let mut expected = Annotated::new("x".repeat(300));
        processor::apply(&mut expected, |v, m| {
            trim_string(v, m, MaxChars::Culprit);
            Ok(())
        })
        .unwrap();

        assert_eq!(event.value().unwrap().culprit, expected);
    }

    #[test]
    fn test_databag_stripping() {
        let mut processor = TrimmingProcessor::new();

        fn make_nested_object(depth: usize) -> Annotated<Value> {
            if depth == 0 {
                return Annotated::new(Value::String("max depth".to_string()));
            }
            let mut rv = Object::new();
            rv.insert(format!("key{depth}"), make_nested_object(depth - 1));
            Annotated::new(Value::Object(rv))
        }

        let databag = Annotated::new({
            let mut map = Object::new();
            map.insert(
                "key_1".to_string(),
                Annotated::new(ExtraValue(Value::String("value 1".to_string()))),
            );
            map.insert(
                "key_2".to_string(),
                make_nested_object(8).map_value(ExtraValue),
            );
            map.insert(
                "key_3".to_string(),
                // innermost key (string) is entering json stringify codepath
                make_nested_object(5).map_value(ExtraValue),
            );
            map
        });
        let mut event = Annotated::new(Event {
            extra: databag,
            ..Default::default()
        });

        processor::process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        let stripped_extra = &event.value().unwrap().extra;
        let json = stripped_extra.to_json_pretty().unwrap();

        assert_eq!(
            json,
            r#"{
  "key_1": "value 1",
  "key_2": {
    "key8": {
      "key7": {
        "key6": {
          "key5": {
            "key4": "{\"key3\":{\"key2\":{\"key1\":\"max depth\"}}}"
          }
        }
      }
    }
  },
  "key_3": {
    "key5": {
      "key4": {
        "key3": {
          "key2": {
            "key1": "max depth"
          }
        }
      }
    }
  }
}"#
        );
    }

    #[test]
    fn test_databag_array_stripping() {
        let mut processor = TrimmingProcessor::new();

        let databag = Annotated::new({
            let mut map = Object::new();
            for idx in 0..100 {
                map.insert(
                    format!("key_{idx}"),
                    Annotated::new(ExtraValue(Value::String("x".repeat(50000)))),
                );
            }
            map
        });
        let mut event = Annotated::new(Event {
            extra: databag,
            ..Default::default()
        });

        processor::process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        let stripped_extra = SerializableAnnotated(&event.value().unwrap().extra);

        insta::assert_ron_snapshot!(stripped_extra);
    }

    #[test]
    fn test_tags_stripping() {
        let mut processor = TrimmingProcessor::new();

        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(
                vec![Annotated::new(TagEntry(
                    Annotated::new("x".repeat(300)),
                    Annotated::new("x".repeat(300)),
                ))]
                .into(),
            )),
            ..Default::default()
        });

        processor::process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        let json = event
            .value()
            .unwrap()
            .tags
            .payload_to_json_pretty()
            .unwrap();

        assert_eq!(
            json,
            r#"[
  [
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx..."
  ]
]"#
        );
    }

    #[test]
    fn test_databag_state_leak() {
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
                stacktrace: Annotated::new(
                    RawStacktrace {
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
                    }
                    .into(),
                ),
                ..Default::default()
            })])),
            ..Default::default()
        });

        let mut processor = TrimmingProcessor::new();
        let mut stripped_event = event.clone();
        processor::process_value(&mut stripped_event, &mut processor, ProcessingState::root())
            .unwrap();

        assert_eq!(
            event.to_json_pretty().unwrap(),
            stripped_event.to_json_pretty().unwrap()
        );
    }

    #[test]
    fn test_custom_context_trimming() {
        let mut contexts = Contexts::new();
        for i in 1..2 {
            contexts.insert(format!("despacito{i}"), {
                let mut context = Object::new();
                context.insert(
                    "foo".to_string(),
                    Annotated::new(Value::String("a".repeat(4000))),
                );
                context.insert(
                    "bar".to_string(),
                    Annotated::new(Value::String("a".repeat(5000))),
                );
                Context::Other(context)
            });
        }

        let mut contexts = Annotated::new(contexts);
        let mut processor = TrimmingProcessor::new();
        processor::process_value(&mut contexts, &mut processor, ProcessingState::root()).unwrap();

        let contexts = contexts.value().unwrap();
        for i in 1..2 {
            let other = match contexts.get_key(&format!("despacito{i}")).unwrap() {
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
        let mut extra = Object::new();
        extra.insert("foo".to_string(), {
            Annotated::new(ExtraValue(Value::Array(
                repeat(Annotated::new(Value::U64(1)))
                    .take(200_000)
                    .collect(),
            )))
        });

        let mut event = Annotated::new(Event {
            extra: Annotated::new(extra),
            ..Default::default()
        });

        let mut processor = TrimmingProcessor::new();
        processor::process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

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
            ExtraValue(Value::Array(x)) => x,
            x => panic!("Wrong type: {x:?}"),
        };

        // this is larger / 2 for the extra value
        assert_eq!(arr.len(), 8192);
    }

    // TODO(ja): Enable this test
    // #[test]
    // fn test_newtypes_do_not_add_to_depth() {
    //     #[derive(Debug, Clone, FromValue, IntoValue, ProcessValue, Empty)]
    //     struct WrappedString(String);

    //     #[derive(Debug, Clone, FromValue, IntoValue, ProcessValue, Empty)]
    //     struct StructChild2 {
    //         inner: Annotated<WrappedString>,
    //     }

    //     #[derive(Debug, Clone, FromValue, IntoValue, ProcessValue, Empty)]
    //     struct StructChild {
    //         inner: Annotated<StructChild2>,
    //     }

    //     #[derive(Debug, Clone, FromValue, IntoValue, ProcessValue, Empty)]
    //     struct Struct {
    //         #[metastructure(bag_size = "small")]
    //         inner: Annotated<StructChild>,
    //     }

    //     let mut value = Annotated::new(Struct {
    //         inner: Annotated::new(StructChild {
    //             inner: Annotated::new(StructChild2 {
    //                 inner: Annotated::new(WrappedString("hi".to_string())),
    //             }),
    //         }),
    //     });

    //     let mut processor = TrimmingProcessor::new();
    //     process_value(&mut value, &mut processor, ProcessingState::root()).unwrap();

    //     // Ensure stack does not leak with newtypes
    //     assert!(processor.bag_size_state.is_empty());

    //     assert_eq!(
    //         value.to_json().unwrap(),
    //         r#"{"inner":{"inner":{"inner":"hi"}}}"#
    //     );
    // }

    #[test]
    fn test_frameqty_equals_limit() {
        fn create_frame(filename: &str) -> Annotated<Frame> {
            Annotated::new(Frame {
                filename: Annotated::new(filename.into()),
                ..Default::default()
            })
        }

        let mut frames = Annotated::new(vec![
            create_frame("foo3.py"),
            create_frame("foo4.py"),
            create_frame("foo5.py"),
        ]);

        processor::apply(&mut frames, |f, m| {
            enforce_frame_hard_limit(f, m, 3);
            Ok(())
        })
        .unwrap();

        // original_length isn't set, when limit is equal to length, as no trimming took place.
        assert!(frames.meta().original_length().is_none());
    }

    #[test]
    fn test_frame_hard_limit() {
        fn create_frame(filename: &str) -> Annotated<Frame> {
            Annotated::new(Frame {
                filename: Annotated::new(filename.into()),
                ..Default::default()
            })
        }

        let mut frames = Annotated::new(vec![
            create_frame("foo1.py"),
            create_frame("foo2.py"),
            create_frame("foo3.py"),
            create_frame("foo4.py"),
            create_frame("foo5.py"),
        ]);

        processor::apply(&mut frames, |f, m| {
            enforce_frame_hard_limit(f, m, 3);
            Ok(())
        })
        .unwrap();

        let mut expected_meta = Meta::default();
        expected_meta.set_original_length(Some(5));

        assert_eq!(
            frames,
            Annotated(
                Some(vec![
                    create_frame("foo3.py"),
                    create_frame("foo4.py"),
                    create_frame("foo5.py"),
                ]),
                expected_meta
            )
        );
    }

    #[test]
    fn test_slim_frame_data_under_max() {
        let mut frames = vec![Annotated::new(Frame {
            filename: Annotated::new("foo".into()),
            pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
            context_line: Annotated::new("b".to_string()),
            post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
            ..Default::default()
        })];

        let old_frames = frames.clone();
        slim_frame_data(&mut frames, 4);

        assert_eq!(frames, old_frames);
    }

    #[test]
    fn test_slim_frame_data_over_max() {
        let mut frames = vec![];

        for n in 0..5 {
            frames.push(Annotated::new(Frame {
                filename: Annotated::new(format!("foo {n}").into()),
                pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
                context_line: Annotated::new("b".to_string()),
                post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
                ..Default::default()
            }));
        }

        slim_frame_data(&mut frames, 4);

        let expected = vec![
            Annotated::new(Frame {
                filename: Annotated::new("foo 0".into()),
                pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
                context_line: Annotated::new("b".to_string()),
                post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
                ..Default::default()
            }),
            Annotated::new(Frame {
                filename: Annotated::new("foo 1".into()),
                pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
                context_line: Annotated::new("b".to_string()),
                post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
                ..Default::default()
            }),
            Annotated::new(Frame {
                filename: Annotated::new("foo 2".into()),
                context_line: Annotated::new("b".to_string()),
                ..Default::default()
            }),
            Annotated::new(Frame {
                filename: Annotated::new("foo 3".into()),
                pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
                context_line: Annotated::new("b".to_string()),
                post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
                ..Default::default()
            }),
            Annotated::new(Frame {
                filename: Annotated::new("foo 4".into()),
                pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
                context_line: Annotated::new("b".to_string()),
                post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
                ..Default::default()
            }),
        ];

        assert_eq!(frames, expected);
    }
}
