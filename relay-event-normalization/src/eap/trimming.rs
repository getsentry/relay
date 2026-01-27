use std::ops::Bound;

use relay_event_schema::processor::{
    self, ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor, ValueType,
};
use relay_event_schema::protocol::Attributes;
use relay_protocol::{Array, Empty, Meta, Object};

use crate::eap::size;

#[derive(Clone, Debug)]
struct SizeState {
    max_depth: Option<usize>,
    encountered_at_depth: usize,
    size_remaining: Option<usize>,
}

/// Processor for trimming EAP items (logs, V2 spans).
///
/// This primarily differs from the regular [`TrimmingProcessor`](crate::trimming::TrimmingProcessor)
/// in the handling of [`Attributes`]. This processor handles attributes as follows:
/// 1. Sort them by combined key and value size, where the key size is just the string length
///    and the value size is given by [`size::attribute_size`].
/// 2. Trim attributes one by one. Key lengths are counted for the attribute's size, but keys
///    aren't trimmed—if a key is too long, the attribute is simply discarded.
/// 3. If we run out of space, all subsequent attributes are discarded.
///
/// This means that large attributes will be trimmed or discarded before small ones.
#[derive(Default)]
pub struct TrimmingProcessor {
    size_state: Vec<SizeState>,
    removed_key_byte_budget: usize,
}

impl TrimmingProcessor {
    /// Creates a new trimming processor.
    pub fn new(removed_key_byte_budget: usize) -> Self {
        Self {
            size_state: Default::default(),
            removed_key_byte_budget,
        }
    }

    fn should_remove_container<T: Empty>(&self, value: &T, state: &ProcessingState<'_>) -> bool {
        // Heuristic to avoid trimming a value like `[1, 1, 1, 1, ...]` into `[null, null, null,
        // null, ...]`, making it take up more space.
        self.remaining_depth(state) == Some(1) && !value.is_empty()
    }

    #[inline]
    fn remaining_size(&self) -> Option<usize> {
        self.size_state
            .iter()
            .filter_map(|x| x.size_remaining)
            .min()
    }

    #[inline]
    fn remaining_depth(&self, state: &ProcessingState<'_>) -> Option<usize> {
        self.size_state
            .iter()
            .filter_map(|size_state| {
                // The current depth in the entire event payload minus the depth at which we found the
                // max_depth attribute is the depth where we are at in the property.
                let current_depth = state.depth() - size_state.encountered_at_depth;
                size_state
                    .max_depth
                    .map(|max_depth| max_depth.saturating_sub(current_depth))
            })
            .min()
    }

    fn consume_size(&mut self, state: Option<&ProcessingState>, default: usize) {
        let size = state.and_then(|s| s.bytes_size()).unwrap_or(default);
        for remaining in self
            .size_state
            .iter_mut()
            .filter_map(|state| state.size_remaining.as_mut())
        {
            *remaining = remaining.saturating_sub(size);
        }
    }

    /// Returns a `ProcessingAction` for removing the given key.
    ///
    /// If there is enough `removed_key_byte_budget` left to accomodate the key,
    /// this will be `ProcessingAction::DeleteValueWithRemark` (which causes a remark to be left).
    /// Otherwise, it will be `ProcessingAction::DeleteValueHard` (the key is removed without a trace).
    fn delete_value(&mut self, key: Option<&str>) -> ProcessingAction {
        let len = key.map_or(0, |key| key.len());
        if len <= self.removed_key_byte_budget {
            self.removed_key_byte_budget -= len;
            ProcessingAction::DeleteValueWithRemark("trimmed")
        } else {
            ProcessingAction::DeleteValueHard
        }
    }
}

impl Processor for TrimmingProcessor {
    fn before_process<T: ProcessValue>(
        &mut self,
        _: Option<&T>,
        _: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // If we encounter a max_bytes or max_depth attribute it
        // resets the size and depth that is permitted below it.
        if state.max_bytes().is_some() || state.attrs().max_depth.is_some() {
            self.size_state.push(SizeState {
                size_remaining: state.max_bytes(),
                encountered_at_depth: state.depth(),
                max_depth: state.attrs().max_depth,
            });
        }

        if state.attrs().trim {
            let key = state.keys().next();
            if self.remaining_size() == Some(0) {
                return Err(self.delete_value(key));
            }
            if self.remaining_depth(state) == Some(0) {
                return Err(self.delete_value(key));
            }
        }
        Ok(())
    }

    fn after_process<T: ProcessValue>(
        &mut self,
        _value: Option<&T>,
        _: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // If our current depth is the one where we found a bag_size attribute, this means we
        // are done processing a databag. Pop the bag size state.
        self.size_state
            .pop_if(|size_state| state.depth() == size_state.encountered_at_depth);

        // The general `TrimmingProcessor` counts consumed sizes at this point. We can't do this generically
        // because we want to count sizes using `size::attribute_size` for attribute values. Therefore, the
        // size accounting needs to happen in the processing functions themselves.

        Ok(())
    }
    fn process_u64(
        &mut self,
        _value: &mut u64,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.consume_size(Some(state), 8);
        Ok(())
    }

    fn process_i64(
        &mut self,
        _value: &mut i64,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.consume_size(Some(state), 8);
        Ok(())
    }

    fn process_f64(
        &mut self,
        _value: &mut f64,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.consume_size(Some(state), 8);
        Ok(())
    }

    fn process_bool(
        &mut self,
        _value: &mut bool,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.consume_size(Some(state), 1);
        Ok(())
    }

    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let Some(max_chars) = state.max_chars() {
            crate::trimming::trim_string(value, meta, max_chars, state.attrs().max_chars_allowance);
        }

        if !state.attrs().trim {
            self.consume_size(Some(state), value.len());
            return Ok(());
        }

        if let Some(size_remaining) = self.remaining_size() {
            crate::trimming::trim_string(value, meta, size_remaining, 0);
        }

        self.consume_size(Some(state), value.len());

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
        if !state.attrs().trim {
            return Ok(());
        }

        // If we need to check the bag size, then we go down a different path
        if !self.size_state.is_empty() {
            let original_length = value.len();

            if self.should_remove_container(value, state) {
                return Err(ProcessingAction::DeleteValueHard);
            }

            let mut split_index = None;
            for (index, item) in value.iter_mut().enumerate() {
                if self.remaining_size() == Some(0) {
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
        if !state.attrs().trim {
            return Ok(());
        }

        // If we need to check the bag size, then we go down a different path
        if !self.size_state.is_empty() {
            let original_length = value.len();

            if self.should_remove_container(value, state) {
                return Err(ProcessingAction::DeleteValueHard);
            }

            let mut split_key = None;
            for (key, item) in value.iter_mut() {
                if self.remaining_size() == Some(0) {
                    split_key = Some(key.to_owned());
                    break;
                }

                let item_state = state.enter_borrowed(key, None, ValueType::for_field(item));
                processor::process_value(item, self, &item_state)?;
            }

            if let Some(split_key) = split_key {
                let mut i = split_key.as_str();

                // Morally this is just `range_mut(split_key.as_str()..)`, but that doesn't work for type
                // inference reasons.
                for (key, value) in value
                    .range_mut::<str, _>((Bound::Included(split_key.as_str()), Bound::Unbounded))
                {
                    i = key.as_str();

                    match self.delete_value(Some(key.as_ref())) {
                        ProcessingAction::DeleteValueHard => break,
                        ProcessingAction::DeleteValueWithRemark(rule_id) => {
                            value.delete_with_remark(rule_id)
                        }
                        _ => unreachable!(),
                    }
                }

                let split_key = i.to_owned();
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

    fn process_attributes(
        &mut self,
        attributes: &mut Attributes,
        meta: &mut Meta,
        state: &ProcessingState,
    ) -> ProcessingResult {
        if !state.attrs().trim {
            return Ok(());
        }

        let original_length = size::attributes_size(attributes);

        // Sort attributes by key + value size so small attributes are more likely to be preserved
        let inner = std::mem::take(&mut attributes.0);
        let mut sorted: Vec<_> = inner.into_iter().collect();
        sorted.sort_by_cached_key(|(k, v)| k.len() + size::attribute_size(v));

        let mut split_idx = None;
        for (idx, (key, value)) in sorted.iter_mut().enumerate() {
            if let Some(remaining) = self.remaining_size()
                && remaining < key.len()
            {
                split_idx = Some(idx);
                break;
            }

            self.consume_size(None, key.len());

            let value_state = state.enter_borrowed(key, None, ValueType::for_field(value));
            processor::process_value(value, self, &value_state)?;
        }

        if let Some(split_idx) = split_idx {
            let mut i = split_idx;

            for (key, value) in &mut sorted[split_idx..] {
                match self.delete_value(Some(key.as_ref())) {
                    ProcessingAction::DeleteValueHard => break,
                    ProcessingAction::DeleteValueWithRemark(rule_id) => {
                        value.delete_with_remark(rule_id)
                    }
                    _ => unreachable!(),
                }

                i += 1;
            }

            let _ = sorted.split_off(i);
        }

        attributes.0 = sorted.into_iter().collect();

        let new_size = size::attributes_size(attributes);
        if new_size != original_length {
            meta.set_original_length(Some(original_length));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use relay_event_schema::{
        processor::FieldAttrs,
        protocol::{AttributeType, AttributeValue},
    };
    use relay_protocol::{Annotated, FromValue, IntoValue, SerializableAnnotated, Value};

    use super::*;

    #[derive(Debug, Clone, Empty, IntoValue, FromValue, ProcessValue)]
    struct TestObject {
        #[metastructure(max_chars = 10, trim = true)]
        body: Annotated<String>,
        // This should neither be trimmed nor factor into size calculations.
        #[metastructure(trim = false, bytes_size = 0)]
        number: Annotated<u64>,
        // This should count as 10B.
        #[metastructure(trim = false, bytes_size = 10)]
        other_number: Annotated<u64>,
        #[metastructure(max_bytes = 40, trim = true)]
        attributes: Annotated<Attributes>,
        #[metastructure(trim = true)]
        footer: Annotated<String>,
    }

    #[test]
    fn test_split_on_string() {
        let mut attributes = Attributes::new();

        attributes.insert("small", 17); // 13B
        attributes.insert("medium string", "This string should be trimmed"); // 42B
        attributes.insert("attribute is very large and should be removed", true); // 47B

        let mut value = Annotated::new(TestObject {
            attributes: Annotated::new(attributes),
            number: Annotated::empty(),
            other_number: Annotated::empty(),
            body: Annotated::new("This is longer than allowed".to_owned()),
            footer: Annotated::empty(),
        });

        let mut processor = TrimmingProcessor::new(100);

        let state = ProcessingState::new_root(Default::default(), []);
        processor::process_value(&mut value, &mut processor, &state).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&value), @r###"
        {
          "body": "This is...",
          "attributes": {
            "attribute is very large and should be removed": null,
            "medium string": {
              "type": "string",
              "value": "This string..."
            },
            "small": {
              "type": "integer",
              "value": 17
            }
          },
          "_meta": {
            "attributes": {
              "": {
                "len": 101
              },
              "attribute is very large and should be removed": {
                "": {
                  "rem": [
                    [
                      "trimmed",
                      "x"
                    ]
                  ]
                }
              },
              "medium string": {
                "value": {
                  "": {
                    "rem": [
                      [
                        "!limit",
                        "s",
                        11,
                        14
                      ]
                    ],
                    "len": 29
                  }
                }
              }
            },
            "body": {
              "": {
                "rem": [
                  [
                    "!limit",
                    "s",
                    7,
                    10
                  ]
                ],
                "len": 27
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_one_byte_left() {
        let mut attributes = Attributes::new();

        // First attribute + key of second attribute is 39B, leaving exactly one
        // byte for the second attribute's value.
        attributes.insert("small attribute", 17); // 23B
        attributes.insert("medium attribute", "This string should be trimmed"); // 45B

        let mut value = Annotated::new(TestObject {
            attributes: Annotated::new(attributes),
            number: Annotated::empty(),
            other_number: Annotated::empty(),
            body: Annotated::new("This is longer than allowed".to_owned()),
            footer: Annotated::empty(),
        });

        let mut processor = TrimmingProcessor::new(100);

        let state = ProcessingState::new_root(Default::default(), []);
        processor::process_value(&mut value, &mut processor, &state).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&value), @r###"
        {
          "body": "This is...",
          "attributes": {
            "medium attribute": {
              "type": "string",
              "value": "..."
            },
            "small attribute": {
              "type": "integer",
              "value": 17
            }
          },
          "_meta": {
            "attributes": {
              "": {
                "len": 68
              },
              "medium attribute": {
                "value": {
                  "": {
                    "rem": [
                      [
                        "!limit",
                        "s",
                        0,
                        3
                      ]
                    ],
                    "len": 29
                  }
                }
              }
            },
            "body": {
              "": {
                "rem": [
                  [
                    "!limit",
                    "s",
                    7,
                    10
                  ]
                ],
                "len": 27
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_overaccept_number() {
        let mut attributes = Attributes::new();

        // The attribute size would get used up by the value of "attribute with long name".
        // Nevertheless, we accept this attribute, thereby overaccepting 5B.
        attributes.insert("small", "abcdefgh"); // 5 + 8 = 13B
        attributes.insert("attribute with long name", 71); // 24 + 8 = 32B
        attributes.insert("attribute is very large and should be removed", true); // 46 + 1 = 47B

        let mut value = Annotated::new(TestObject {
            attributes: Annotated::new(attributes),
            number: Annotated::empty(),
            other_number: Annotated::empty(),
            body: Annotated::new("This is longer than allowed".to_owned()),
            footer: Annotated::empty(),
        });

        let mut processor = TrimmingProcessor::new(100);

        let state = ProcessingState::new_root(Default::default(), []);
        processor::process_value(&mut value, &mut processor, &state).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&value), @r###"
        {
          "body": "This is...",
          "attributes": {
            "attribute is very large and should be removed": null,
            "attribute with long name": {
              "type": "integer",
              "value": 71
            },
            "small": {
              "type": "string",
              "value": "abcdefgh"
            }
          },
          "_meta": {
            "attributes": {
              "": {
                "len": 91
              },
              "attribute is very large and should be removed": {
                "": {
                  "rem": [
                    [
                      "trimmed",
                      "x"
                    ]
                  ]
                }
              }
            },
            "body": {
              "": {
                "rem": [
                  [
                    "!limit",
                    "s",
                    7,
                    10
                  ]
                ],
                "len": 27
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_max_item_size() {
        let mut attributes = Attributes::new();

        attributes.insert("small", 17); // 13B
        attributes.insert("medium string", "This string should be trimmed"); // 42B
        attributes.insert("attribute is very large and should be removed", true); // 47B

        let mut value = Annotated::new(TestObject {
            attributes: Annotated::new(attributes),
            number: Annotated::new(0),
            other_number: Annotated::new(0),
            body: Annotated::new("Short".to_owned()),
            footer: Annotated::new("Hello World".to_owned()),
        });

        let mut processor = TrimmingProcessor::new(100);

        // The `body` takes up 5B, `other_number` 10B, the `"small"` attribute 13B, and the key "medium string" another 13B.
        // That leaves 9B for the string's value.
        // Note that the `number` field doesn't take up any size.
        // The `"footer"` is removed because it comes after the attributes and there's no space left.
        let state =
            ProcessingState::new_root(Some(Cow::Owned(FieldAttrs::default().max_bytes(50))), []);
        processor::process_value(&mut value, &mut processor, &state).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&value), @r###"
        {
          "body": "Short",
          "number": 0,
          "other_number": 0,
          "attributes": {
            "attribute is very large and should be removed": null,
            "medium string": {
              "type": "string",
              "value": "This s..."
            },
            "small": {
              "type": "integer",
              "value": 17
            }
          },
          "footer": null,
          "_meta": {
            "attributes": {
              "": {
                "len": 101
              },
              "attribute is very large and should be removed": {
                "": {
                  "rem": [
                    [
                      "trimmed",
                      "x"
                    ]
                  ]
                }
              },
              "medium string": {
                "value": {
                  "": {
                    "rem": [
                      [
                        "!limit",
                        "s",
                        6,
                        9
                      ]
                    ],
                    "len": 29
                  }
                }
              }
            },
            "footer": {
              "": {
                "rem": [
                  [
                    "trimmed",
                    "x"
                  ]
                ]
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_array_attribute() {
        let mut attributes = Attributes::new();

        let array = vec![
            Annotated::new("first string".into()),
            Annotated::new("second string".into()),
            Annotated::new("another string".into()),
            Annotated::new("last string".into()),
        ];

        attributes.insert(
            "array",
            AttributeValue {
                ty: Annotated::new(AttributeType::Array),
                value: Annotated::new(Value::Array(array)),
            },
        );

        let mut value = Annotated::new(TestObject {
            attributes: Annotated::new(attributes),
            number: Annotated::empty(),
            other_number: Annotated::empty(),
            body: Annotated::new("Short".to_owned()),
            footer: Annotated::empty(),
        });

        let mut processor = TrimmingProcessor::new(100);
        let state = ProcessingState::new_root(Default::default(), []);
        processor::process_value(&mut value, &mut processor, &state).unwrap();

        // The key `"array"` and the first and second array value take up 5 + 12 + 13 = 30B in total,
        // leaving 10B for the third array value and nothing for the last.
        insta::assert_json_snapshot!(SerializableAnnotated(&value), @r###"
        {
          "body": "Short",
          "attributes": {
            "array": {
              "type": "array",
              "value": [
                "first string",
                "second string",
                "another..."
              ]
            }
          },
          "_meta": {
            "attributes": {
              "": {
                "len": 55
              },
              "array": {
                "value": {
                  "": {
                    "len": 4
                  },
                  "2": {
                    "": {
                      "rem": [
                        [
                          "!limit",
                          "s",
                          7,
                          10
                        ]
                      ],
                      "len": 14
                    }
                  }
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_oversized_key_does_not_consume_global_limit() {
        let mut attributes = Attributes::new();
        attributes.insert("a", 1); // 9B 
        attributes.insert("this_key_is_exactly_35_chars_long!!", true); // 35B key + 1B = 36B

        let mut value = Annotated::new(TestObject {
            body: Annotated::new("Hi".to_owned()), // 2B
            number: Annotated::new(0),
            other_number: Annotated::empty(),
            attributes: Annotated::new(attributes),
            footer: Annotated::new("Hello World".to_owned()), // 11B
        });

        let mut processor = TrimmingProcessor::new(100);
        let state =
            ProcessingState::new_root(Some(Cow::Owned(FieldAttrs::default().max_bytes(30))), []);
        processor::process_value(&mut value, &mut processor, &state).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&value), @r###"
        {
          "body": "Hi",
          "number": 0,
          "attributes": {
            "a": {
              "type": "integer",
              "value": 1
            },
            "this_key_is_exactly_35_chars_long!!": null
          },
          "footer": "Hello World",
          "_meta": {
            "attributes": {
              "": {
                "len": 45
              },
              "this_key_is_exactly_35_chars_long!!": {
                "": {
                  "rem": [
                    [
                      "trimmed",
                      "x"
                    ]
                  ]
                }
              }
            }
          }
        }
        "###);
    }
}
