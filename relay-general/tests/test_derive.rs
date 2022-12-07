use relay_general::processor::{process_value, ProcessValue, ProcessingState, Processor};
use relay_general::protocol::HeaderName;
use relay_general::types::{Annotated, Meta, ProcessingResult, Value};

struct RecordingProcessor(Vec<String>);

impl Processor for RecordingProcessor {
    fn process_value(
        &mut self,
        value: &mut Value,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.0.push(format!("process_value({})", state.path()));
        self.0.push("before_process_child_values".to_string());
        value.process_child_values(self, state)?;
        self.0.push("after_process_child_values".to_string());
        Ok(())
    }

    fn process_string(
        &mut self,
        _value: &mut String,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.0.push(format!("process_string({})", state.path()));
        Ok(())
    }

    fn process_header_name(
        &mut self,
        value: &mut HeaderName,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        self.0
            .push(format!("process_header_name({})", state.path()));
        self.0.push("before_process_child_values".to_string());
        value.process_child_values(self, state)?;
        self.0.push("after_process_child_values".to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enums_processor_calls() {
        let mut processor = RecordingProcessor(vec![]);

        let mut value = Annotated::new(Value::String("hi".to_string()));
        process_value(
            &mut value,
            &mut processor,
            &ProcessingState::root().enter_static("foo", None, None),
        )
        .unwrap();

        // Assert that calling `process_child_values` does not recurse. This is surprising and slightly
        // undesirable for processors, but not a big deal and easy to implement.
        assert_eq!(
            processor.0,
            &[
                "process_value(foo)",
                "before_process_child_values",
                "after_process_child_values",
                "process_string(foo)"
            ]
        );
    }

    #[test]
    fn test_simple_newtype() {
        let mut processor = RecordingProcessor(vec![]);

        let mut value = Annotated::new(HeaderName::new("hi"));
        process_value(
            &mut value,
            &mut processor,
            &ProcessingState::root().enter_static("foo", None, None),
        )
        .unwrap();

        // Assert that calling `process_child_values` does not recurse. This is surprising and slightly
        // undesirable for processors, but not a big deal and easy to implement.
        assert_eq!(
            processor.0,
            &[
                "process_header_name(foo)",
                "before_process_child_values",
                "after_process_child_values",
                "process_string(foo)"
            ]
        );
    }
}
