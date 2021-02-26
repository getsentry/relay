use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{Breadcrumb, Event};
use crate::types::{Annotated, ErrorKind, Meta, Object, ProcessingResult, Value};

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
use {crate::processor::process_value, crate::protocol::ContextInner};

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
    assert_eq_dbg!(
        *other.get("foo").unwrap(),
        Annotated::from_error(ErrorKind::InvalidAttribute, None)
    );
    assert_eq_dbg!(
        *other.get("bar").unwrap(),
        Annotated::from_error(ErrorKind::InvalidAttribute, None)
    );
}

#[test]
fn test_remove_nested_other() {
    use crate::protocol::User;

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
    use crate::protocol::{Context, Contexts, OsContext};

    let mut os = OsContext::default();
    os.other
        .insert("foo".to_string(), Annotated::from(Value::U64(42)));

    let mut contexts = Object::new();
    contexts.insert(
        "renamed".to_string(),
        Annotated::from(ContextInner(Context::Os(Box::new(os)))),
    );

    let mut event = Annotated::new(Event {
        contexts: Annotated::from(Contexts(contexts.clone())),
        ..Default::default()
    });

    process_value(
        &mut event,
        &mut RemoveOtherProcessor,
        ProcessingState::root(),
    )
    .unwrap();

    assert_eq_dbg!(get_value!(event.contexts!).0, contexts);
}

#[test]
fn test_breadcrumb_errors() {
    use crate::protocol::Values;

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

    assert_eq_dbg!(
        *other.get("foo").unwrap(),
        Annotated::from_error(ErrorKind::InvalidAttribute, None)
    );
    assert_eq_dbg!(
        *other.get("bar").unwrap(),
        Annotated::from_error(ErrorKind::InvalidAttribute, None)
    );
}
