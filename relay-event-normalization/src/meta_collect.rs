use std::collections::HashMap;

use relay_event_schema::processor::{
    ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::Attribute;
use relay_protocol::{Meta, MetaTree};

/// A processor which collects and removes all metadata from an `Annotated` tree.
#[derive(Debug, Default)]
pub struct MetaCollectProcessor {
    foo: HashMap<String, Meta>,
    max_depth: usize,
}

impl MetaCollectProcessor {
    /// Creates a new [`Self`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns all collected metadata keyed, by the path of the metadata within the original object.
    pub fn into_inner(self) -> HashMap<String, Meta> {
        self.foo
    }
}

impl Processor for MetaCollectProcessor {
    fn before_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        println!("before: {}", state.path());

        Ok(())
    }

    fn process_attribute(
        &mut self,
        value: &mut Attribute,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        println!("attribute: {}", state.path());
        value.process_child_values(self, state)
    }

    fn after_process<T: ProcessValue>(
        &mut self,
        _value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        println!("after: {}", state.path());

        let meta = std::mem::take(meta);

        if !meta.is_empty() {
            self.foo.insert(state.path().to_string(), meta);
        }

        Ok(())
    }
}

struct MetaTreeProcessor<'a> {
    current: &'a mut MetaTree,
}

impl Processor for MetaTreeProcessor<'_> {
    fn before_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let Some(value) = value {
            value.process_child_values(processor, state);
        }

        Err(ProcessingAction::DeleteValueHard)
    }

    fn after_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
    }
}
