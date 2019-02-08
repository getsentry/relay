use semaphore_general::processor::process_value;
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
        _value: &mut Value,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ValueAction {
        self.0.push("process_value".to_string());
        ValueAction::Keep
    }

    fn process_string(
        &mut self,
        _value: &mut String,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ValueAction {
        self.0.push("process_string".to_string());
        ValueAction::Keep
    }

    fn process_header_name(
        &mut self,
        _value: &mut HeaderName,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ValueAction {
        self.0.push("process_header_name".to_string());
        ValueAction::Keep
    }
}

#[test]
fn test_enums_processor_calls() {
    let mut processor = RecordingProcessor(vec![]);

    let mut value = Annotated::new(Value::String("hi".to_string()));
    process_value(&mut value, &mut processor, &ProcessingState::root());

    assert_eq!(processor.0, &["process_value", "process_string"]);
}

#[test]
fn test_simple_newtype() {
    let mut processor = RecordingProcessor(vec![]);

    let mut value = Annotated::new(HeaderName("hi".to_string()));
    process_value(&mut value, &mut processor, &ProcessingState::root());

    assert_eq!(processor.0, &["process_header_name", "process_string"]);
}
