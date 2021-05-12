use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::{Meta, ProcessingResult};

#[derive(Default)]
pub struct UnprintableFieldsProcessor {
    has_unprintable_fields: bool,
}

impl UnprintableFieldsProcessor {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Processor for UnprintableFieldsProcessor {
    fn process_string(
        &mut self,
        value: &mut String,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if !self.has_unprintable_fields {
            self.has_unprintable_fields = value.chars().any(|c| {
                c == '\u{fffd}' // unicode replacement character
                || (c.is_control() && !c.is_whitespace()) // non-whitespace control characters
            });
        }
        Ok(())
    }

    fn process_event(
        &mut self,
        value: &mut crate::protocol::Event,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        value.process_child_values(self, state)?;

        if self.has_unprintable_fields {
            // TODO: capture metric
        }

        Ok(())
    }
}

#[test]
fn test_unprintable_environments() {
    use crate::processor::process_value;
    use crate::protocol::Event;
    use crate::types::Annotated;

    let failures = [
        "�9�~YY���)�����9�~YY���)�����9�~YY���)�����9�~YY���)�����",
        "���7��#1G����7��#1G����7��#1G����7��#1G����7��#",
    ];
    let successes = ["production", "make with\t some\n normal\r\nwhitespace"];

    for fail in &failures {
        let mut event = Annotated::new(Event {
            environment: Annotated::new(fail.to_string()),
            ..Default::default()
        });

        let mut processor = UnprintableFieldsProcessor::new();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert!(processor.has_unprintable_fields);
    }

    for success in &successes {
        let mut event = Annotated::new(Event {
            environment: Annotated::new(success.to_string()),
            ..Default::default()
        });

        let mut processor = UnprintableFieldsProcessor::new();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert!(!processor.has_unprintable_fields);
    }
}
