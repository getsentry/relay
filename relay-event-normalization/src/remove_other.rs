use relay_event_schema::processor::{ProcessValue, ProcessingResult, ProcessingState, Processor};
use relay_event_schema::protocol::{Breadcrumb, Event};
use relay_protocol::{Annotated, ErrorKind, Meta, Object, Value};

/// Replace remaining values and all existing meta with an errors.
fn create_errors(other: &mut Object<Value>) {
    for value in other.values_mut() {
        *value = Annotated::from_error(ErrorKind::InvalidAttribute, None);
    }
}

pub struct RemoveOtherProcessor;

impl Processor for RemoveOtherProcessor {
    fn process_other(
        &mut self,
        other: &mut Object<Value>,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // Drop unknown attributes at all levels without error messages, unless `retain = "true"`
        // was specified explicitly on the field.
        if !state.attrs().retain {
            other.clear();
        }

        Ok(())
    }

    fn process_breadcrumb(
        &mut self,
        breadcrumb: &mut Breadcrumb,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // Move the current map out so we don't clear it in `process_other`
        let mut other = std::mem::take(&mut breadcrumb.other);
        create_errors(&mut other);

        // Recursively clean all `other`s now. Note that this won't touch the event's other
        breadcrumb.process_child_values(self, state)?;
        breadcrumb.other = other;
        Ok(())
    }

    fn process_event(
        &mut self,
        event: &mut Event,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // Move the current map out so we don't clear it in `process_other`
        let mut other = std::mem::take(&mut event.other);

        // Drop Sentry internal attributes
        other.remove("metadata");
        other.remove("hashes");

        // Drop known legacy attributes at top-level without errors
        other.remove("applecrashreport");
        other.remove("device");
        other.remove("repos");
        other.remove("query");

        // Replace remaining values and all existing meta with an errors
        create_errors(&mut other);

        // Recursively clean all `other`s now. Note that this won't touch the event's other
        event.process_child_values(self, state)?;

        event.other = other;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::processor::process_value;
    use relay_event_schema::protocol::{Context, Contexts, OsContext, User, Values};
    use relay_protocol::get_value;
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_remove_legacy_attributes() {
        let mut event = Annotated::new(Event {
            other: {
                let mut other = Object::new();
                other.insert("applecrashreport".to_string(), Value::U64(42).into());
                other.insert("device".to_string(), Value::U64(42).into());
                other.insert("repos".to_string(), Value::U64(42).into());
                other.insert("query".to_string(), Value::U64(42).into());
                other
            },
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut RemoveOtherProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert!(event.value().unwrap().other.is_empty());
    }

    #[test]
    fn test_remove_unknown_attributes() {
        let mut event = Annotated::new(Event {
            other: {
                let mut other = Object::new();
                other.insert("foo".to_string(), Value::U64(42).into());
                other.insert("bar".to_string(), Value::U64(42).into());
                other
            },
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut RemoveOtherProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        let other = &event.value().unwrap().other;
        assert_eq!(
            *other.get("foo").unwrap(),
            Annotated::from_error(ErrorKind::InvalidAttribute, None)
        );
        assert_eq!(
            *other.get("bar").unwrap(),
            Annotated::from_error(ErrorKind::InvalidAttribute, None)
        );
    }

    #[test]
    fn test_remove_nested_other() {
        let mut event = Annotated::new(Event {
            user: Annotated::from(User {
                other: {
                    let mut other = Object::new();
                    other.insert("foo".to_string(), Value::U64(42).into());
                    other.insert("bar".to_string(), Value::U64(42).into());
                    other
                },
                ..Default::default()
            }),
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut RemoveOtherProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert!(get_value!(event.user!).other.is_empty());
    }

    #[test]
    fn test_retain_context_other() {
        let mut os = OsContext::default();
        os.other
            .insert("foo".to_string(), Annotated::from(Value::U64(42)));

        let mut contexts = Contexts::new();
        contexts.insert("renamed".to_string(), Context::Os(Box::new(os)));

        let mut event = Annotated::new(Event {
            contexts: Annotated::new(contexts.clone()),
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut RemoveOtherProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.contexts!).0, contexts.0);
    }

    #[test]
    fn test_breadcrumb_errors() {
        let mut event = Annotated::new(Event {
            breadcrumbs: Annotated::new(Values::new(vec![Annotated::new(Breadcrumb {
                other: {
                    let mut other = Object::new();
                    other.insert("foo".to_string(), Value::U64(42).into());
                    other.insert("bar".to_string(), Value::U64(42).into());
                    other
                },
                ..Breadcrumb::default()
            })])),
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut RemoveOtherProcessor,
            ProcessingState::root(),
        )
        .unwrap();

        let other = &event
            .value()
            .unwrap()
            .breadcrumbs
            .value()
            .unwrap()
            .values
            .value()
            .unwrap()[0]
            .value()
            .unwrap()
            .other;

        assert_eq!(
            *other.get("foo").unwrap(),
            Annotated::from_error(ErrorKind::InvalidAttribute, None)
        );
        assert_eq!(
            *other.get("bar").unwrap(),
            Annotated::from_error(ErrorKind::InvalidAttribute, None)
        );
    }
}
