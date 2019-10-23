use semaphore_general::processor::process_value;
use semaphore_general::processor::ProcessValue;
use semaphore_general::processor::ProcessingState;
use semaphore_general::processor::Processor;
use semaphore_general::protocol::HeaderName;
use semaphore_general::types::Annotated;
use semaphore_general::types::Meta;
use semaphore_general::types::Value;
use semaphore_general::types::ValueAction;

struct RecordingProcessor(Vec<String>);

impl Processor for RecordingProcessor {
    fn process_value(
        &mut self,
        value: &mut Value,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
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
    ) -> ValueAction {
        self.0.push(format!("process_string({})", state.path()));
        Ok(())
    }

    fn process_header_name(
        &mut self,
        value: &mut HeaderName,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        self.0
            .push(format!("process_header_name({})", state.path()));
        self.0.push("before_process_child_values".to_string());
        value.process_child_values(self, state)?;
        self.0.push("after_process_child_values".to_string());
        Ok(())
    }
}

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

    let mut value = Annotated::new(HeaderName("hi".to_string()));
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
