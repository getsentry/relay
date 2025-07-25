use relay_event_schema::processor::{ProcessValue, ProcessingResult, ProcessingState, Processor};
use relay_event_schema::protocol::{Event, EventProcessingError};
use relay_protocol::{Annotated, Meta};

pub struct EmitEventErrors {
    errors: Vec<EventProcessingError>,
}

impl EmitEventErrors {
    pub fn new() -> Self {
        EmitEventErrors { errors: Vec::new() }
    }
}

impl Processor for EmitEventErrors {
    fn before_process<T: ProcessValue>(
        &mut self,
        _: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if !meta.has_errors() {
            return Ok(());
        }

        // Only append the original value to the first error if there are multiple.
        let mut original_value = meta.original_value().cloned();

        for error in meta.iter_errors() {
            self.errors.push(EventProcessingError {
                ty: Annotated::from(error.kind().to_string()),
                name: Annotated::from(state.path().to_string()),
                value: Annotated::from(original_value.take()),
                other: error
                    .data()
                    .map(|(k, v)| (k.to_owned(), Annotated::from(v.clone())))
                    .collect(),
            });
        }

        Ok(())
    }

    fn process_event(
        &mut self,
        event: &mut Event,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        event.process_child_values(self, state)?;

        if !self.errors.is_empty() {
            event
                .errors
                .get_or_insert_with(Vec::new)
                .extend(self.errors.drain(..).map(Annotated::from));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::processor::process_value;
    use relay_event_schema::protocol::{Breadcrumb, Values};
    use relay_protocol::{ErrorKind, Object, Value};
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_no_errors() {
        let mut event = Annotated::from(Event::default());

        process_value(
            &mut event,
            &mut EmitEventErrors::new(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(event.value().unwrap().errors.value(), None);
    }

    #[test]
    fn test_top_level_errors() {
        let mut event = Annotated::from(Event {
            id: Annotated::from_error(ErrorKind::InvalidData, None),
            ..Event::default()
        });

        process_value(
            &mut event,
            &mut EmitEventErrors::new(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(
            *event.value().unwrap().errors.value().unwrap(),
            vec![Annotated::from(EventProcessingError {
                ty: Annotated::from("invalid_data".to_owned()),
                name: Annotated::from("event_id".to_owned()),
                value: Annotated::empty(),
                other: Object::default(),
            })]
        );
    }

    #[test]
    fn test_errors_in_other() {
        let mut event = Annotated::from(Event {
            other: {
                let mut other = Object::new();
                other.insert(
                    "foo".to_owned(),
                    Annotated::from_error(ErrorKind::InvalidData, None),
                );
                other
            },
            ..Event::default()
        });

        process_value(
            &mut event,
            &mut EmitEventErrors::new(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(
            *event.value().unwrap().errors.value().unwrap(),
            vec![Annotated::from(EventProcessingError {
                ty: Annotated::from("invalid_data".to_owned()),
                name: Annotated::from("foo".to_owned()),
                value: Annotated::empty(),
                other: Object::default(),
            })]
        );
    }

    #[test]
    fn test_nested_errors() {
        let mut event = Annotated::from(Event {
            breadcrumbs: Annotated::from(Values::new(vec![Annotated::from(Breadcrumb {
                ty: Annotated::from_error(ErrorKind::InvalidData, None),
                ..Breadcrumb::default()
            })])),
            ..Event::default()
        });

        process_value(
            &mut event,
            &mut EmitEventErrors::new(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(
            *event.value().unwrap().errors.value().unwrap(),
            vec![Annotated::from(EventProcessingError {
                ty: Annotated::from("invalid_data".to_owned()),
                name: Annotated::from("breadcrumbs.values.0.type".to_owned()),
                value: Annotated::empty(),
                other: Object::default(),
            })]
        );
    }

    #[test]
    fn test_multiple_errors() {
        let mut meta = Meta::from_error(ErrorKind::InvalidData);
        meta.add_error(ErrorKind::MissingAttribute);

        let mut event = Annotated::from(Event {
            id: Annotated(None, meta),
            ..Event::default()
        });

        process_value(
            &mut event,
            &mut EmitEventErrors::new(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(
            *event.value().unwrap().errors.value().unwrap(),
            vec![
                Annotated::from(EventProcessingError {
                    ty: Annotated::from("invalid_data".to_owned()),
                    name: Annotated::from("event_id".to_owned()),
                    value: Annotated::empty(),
                    other: Object::default(),
                }),
                Annotated::from(EventProcessingError {
                    ty: Annotated::from("missing_attribute".to_owned()),
                    name: Annotated::from("event_id".to_owned()),
                    value: Annotated::empty(),
                    other: Object::default(),
                })
            ]
        );
    }

    #[test]
    fn test_original_value() {
        let mut meta = Meta::from_error(ErrorKind::InvalidData);
        meta.add_error(ErrorKind::MissingAttribute);
        meta.set_original_value(Some(Value::I64(42)));

        let mut event = Annotated::from(Event {
            id: Annotated(None, meta),
            ..Event::default()
        });

        process_value(
            &mut event,
            &mut EmitEventErrors::new(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(
            *event.value().unwrap().errors.value().unwrap(),
            vec![
                Annotated::from(EventProcessingError {
                    ty: Annotated::from("invalid_data".to_owned()),
                    name: Annotated::from("event_id".to_owned()),
                    value: Annotated::from(Value::I64(42)),
                    other: Object::default(),
                }),
                Annotated::from(EventProcessingError {
                    ty: Annotated::from("missing_attribute".to_owned()),
                    name: Annotated::from("event_id".to_owned()),
                    value: Annotated::empty(),
                    other: Object::default(),
                })
            ]
        );
    }
}
