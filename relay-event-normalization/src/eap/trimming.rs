use relay_event_schema::processor::{
    self, ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor, ValueType,
};
use relay_event_schema::protocol::Attributes;
use relay_protocol::{Array, Empty, Meta, Object, Value};

use crate::eap::size;

#[derive(Clone, Debug)]
struct SizeState {
    max_depth: Option<usize>,
    encountered_at_depth: usize,
    size_remaining: Option<usize>,
}

/// Limits properties to a maximum size and depth.
#[derive(Default)]
pub struct TrimmingProcessor {
    size_state: Vec<SizeState>,
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
        // XXX(iker): test setting only one of the two attributes.
        if state.attrs().max_bytes.is_some() || state.attrs().max_depth.is_some() {
            self.size_state.push(SizeState {
                size_remaining: state.attrs().max_bytes,
                encountered_at_depth: state.depth(),
                max_depth: state.attrs().max_depth,
            });
        }

        if state.attrs().trim {
            if self.remaining_size() == Some(0) {
                // TODO: Create remarks (ensure they do not bloat event)
                return Err(ProcessingAction::DeleteValueHard);
            }
            if self.remaining_depth(state) == Some(0) {
                // TODO: Create remarks (ensure they do not bloat event)
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

        Ok(())
    }

    fn process_u64(
        &mut self,
        _value: &mut u64,
        _: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.consume_size(size::U64_SIZE);
        Ok(())
    }

    fn process_i64(
        &mut self,
        _value: &mut i64,
        _: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.consume_size(size::I64_SIZE);
        Ok(())
    }

    fn process_f64(
        &mut self,
        _value: &mut f64,
        _: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.consume_size(size::F64_SIZE);
        Ok(())
    }

    fn process_bool(
        &mut self,
        _value: &mut bool,
        _: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.consume_size(size::BOOL_SIZE);
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

        if state.entered_anything() {
            self.consume_size(size::string_size(value));
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

        if state.entered_anything() {
            self.consume_size(size::array_size(value.as_ref()));
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

        if state.entered_anything() {
            self.consume_size(
                value
                    .iter()
                    .map(|(k, v)| k.len() + size::value_size(v))
                    .sum(),
            );
        }

        Ok(())
    }

    fn process_value(
        &mut self,
        value: &mut Value,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if !state.attrs().trim {
            return Ok(());
        }

        value.process_child_values(self, state)?;
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

        // If we need to check the bag size, then we go down a different path
        let original_length = size::attributes_size(attributes);

        let inner = std::mem::take(&mut attributes.0);
        let mut sorted: Vec<_> = inner.into_iter().collect();
        sorted.sort_by_cached_key(|(k, v)| k.len() + size::attribute_size(v));

        let mut split_idx = Some(0);
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

        Ok(())
    }
}
