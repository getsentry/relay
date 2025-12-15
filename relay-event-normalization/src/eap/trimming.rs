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
    /// Whether we are currently trimming a collection of attributes.
    /// This case needs to be distinguished for the purpose of accounting
    /// for string lengths.
    in_attributes: bool,
}

impl TrimmingProcessor {
    /// Creates a new trimming processor.
    pub fn new() -> Self {
        Self::default()
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

    fn consume_size(&mut self, size: usize) {
        for remaining in self
            .size_state
            .iter_mut()
            .filter_map(|state| state.size_remaining.as_mut())
        {
            *remaining = remaining.saturating_sub(size);
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
        if state.attrs().max_bytes.is_some() || state.attrs().max_depth.is_some() {
            self.size_state.push(SizeState {
                size_remaining: state.attrs().max_bytes,
                encountered_at_depth: state.depth(),
                max_depth: state.attrs().max_depth,
            });
        }

        if state.attrs().trim {
            if self.remaining_size() == Some(0) {
                return Err(ProcessingAction::DeleteValueHard);
            }
            if self.remaining_depth(state) == Some(0) {
                return Err(ProcessingAction::DeleteValueHard);
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

    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let Some(max_chars) = state.attrs().max_chars {
            crate::trimming::trim_string(value, meta, max_chars, state.attrs().max_chars_allowance);
        }

        if !state.attrs().trim {
            return Ok(());
        }

        if let Some(size_state) = self.size_state.last()
            && let Some(size_remaining) = size_state.size_remaining
        {
            crate::trimming::trim_string(value, meta, size_remaining, 0);
        }

        // Only count string size here if we're _not_ currently trimming attributes.
        // In that case, the size accounting is already handled by `process_attributes`.
        if !self.in_attributes {
            self.consume_size(value.len());
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

        // Mark `self.in_attributes` so we don't double-count string sizes
        self.in_attributes = true;

        let original_length = size::attributes_size(attributes);

        // Sort attributes by key + value size so small attributes are more likely to be preserved
        let inner = std::mem::take(&mut attributes.0);
        let mut sorted: Vec<_> = inner.into_iter().collect();
        sorted.sort_by_cached_key(|(k, v)| k.len() + size::attribute_size(v));

        let mut split_idx = None;
        for (idx, (key, value)) in sorted.iter_mut().enumerate() {
            self.consume_size(key.len());

            if self.remaining_size() == Some(0) {
                split_idx = Some(idx);
                break;
            }

            let value_state = state.enter_borrowed(key, None, ValueType::for_field(value));
            processor::process_value(value, self, &value_state)?;
            self.consume_size(size::attribute_size(value))
        }

        if let Some(split_idx) = split_idx {
            let _ = sorted.split_off(split_idx);
        }

        attributes.0 = sorted.into_iter().collect();

        let new_size = size::attributes_size(attributes);
        if new_size != original_length {
            meta.set_original_length(Some(original_length));
        }

        self.in_attributes = false;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::{Annotated, FromValue, IntoValue, SerializableAnnotated};

    use super::*;

    #[derive(Debug, Clone, Empty, IntoValue, FromValue, ProcessValue)]
    struct TestObject {
        #[metastructure(max_bytes = 40, trim = true)]
        attributes: Annotated<Attributes>,
        #[metastructure(max_chars = 10, trim = true)]
        body: Annotated<String>,
    }

    #[test]
    fn test_total_size() {
        let mut attributes = Attributes::new();

        attributes.insert("small", 17); // 13B
        attributes.insert("medium string", "This string should be trimmed"); // 29B
        attributes.insert("attribute is very large and should be removed", true); // 47B

        let mut value = Annotated::new(TestObject {
            attributes: Annotated::new(attributes),
            body: Annotated::new("This is longer than allowed".to_owned()),
        });

        let mut processor = TrimmingProcessor::new();

        let state = ProcessingState::new_root(Default::default(), []);
        processor::process_value(&mut value, &mut processor, &state).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&value), @r###"
        {
          "attributes": {
            "medium string": {
              "type": "string",
              "value": "This string..."
            },
            "small": {
              "type": "integer",
              "value": 17
            }
          },
          "body": "This is...",
          "_meta": {
            "attributes": {
              "": {
                "len": 101
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
}
