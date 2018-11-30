use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::{Annotated, Array, Map, Object, Value};

pub struct SchemaProcessor;

impl Processor for SchemaProcessor {
    fn process_string(
        &mut self,
        mut value: Annotated<String>,
        state: ProcessingState,
    ) -> Annotated<String> {
        value = check_nonempty_value(value, &state);
        value = check_match_regex_value(value, &state);
        value
    }

    fn process_object<T: ProcessValue>(
        &mut self,
        value: Annotated<Object<T>>,
        state: ProcessingState,
    ) -> Annotated<Object<T>> {
        ProcessValue::process_child_values(check_nonempty_value(value, &state), self, state)
    }

    fn process_array<T: ProcessValue>(
        &mut self,
        value: Annotated<Array<T>>,
        state: ProcessingState,
    ) -> Annotated<Array<T>> {
        ProcessValue::process_child_values(check_nonempty_value(value, &state), self, state)
    }
}

/// Utility trait to find out if an object is empty.
trait IsEmpty {
    /// A generic check if the object is considered empty.
    fn generic_is_empty(&self) -> bool;
}

impl<T> IsEmpty for Vec<T> {
    fn generic_is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<K, V> IsEmpty for Map<K, V> {
    fn generic_is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl IsEmpty for String {
    fn generic_is_empty(&self) -> bool {
        self.is_empty()
    }
}

fn check_nonempty_value<T>(mut annotated: Annotated<T>, state: &ProcessingState) -> Annotated<T>
where
    T: IsEmpty,
{
    if state.attrs().nonempty && !annotated.1.has_errors() && annotated
        .0
        .as_ref()
        .map(|x| x.generic_is_empty())
        .unwrap_or(true)
    {
        annotated.0 = None;
        annotated.1.add_error("non-empty value required", None);
    }

    annotated
}

fn check_match_regex_value(
    mut annotated: Annotated<String>,
    state: &ProcessingState,
) -> Annotated<String> {
    if let Some(ref regex) = state.attrs().match_regex {
        if !annotated.1.has_errors() && annotated
            .0
            .as_ref()
            .map(|x| !regex.is_match(&x))
            .unwrap_or(false)
        {
            annotated.1.add_error(
                "Invalid characters in string",
                annotated.0.take().map(Value::String),
            );
        }
    }

    annotated
}

#[cfg(test)]
fn test_nonempty_base<T>()
where
    T: Default
        + PartialEq
        + crate::types::FromValue
        + crate::types::ToValue
        + crate::processor::ProcessValue,
{
    #[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
    struct Foo<T> {
        #[metastructure(required = "true", nonempty = "true")]
        bar: Annotated<T>,
        bar2: Annotated<T>,
    }

    let wrapper = Annotated::new(Foo {
        bar: Annotated::new(T::default()),
        bar2: Annotated::new(T::default()),
    });
    let wrapper = wrapper.process(&mut SchemaProcessor);

    assert_eq!(
        wrapper,
        Annotated::new(Foo {
            bar: Annotated::from_error("non-empty value required", None),
            bar2: Annotated::new(T::default())
        })
    );
}

#[test]
fn test_nonempty_string() {
    test_nonempty_base::<String>();
}

#[test]
fn test_nonempty_array() {
    test_nonempty_base::<Array<u64>>();
}

#[test]
fn test_nonempty_object() {
    test_nonempty_base::<Object<u64>>();
}

#[test]
fn test_release_newlines() {
    use crate::protocol::Event;

    let event = Annotated::new(Event {
        release: Annotated::new("a\nb".to_string()),
        ..Default::default()
    });

    let event = event.process(&mut SchemaProcessor);

    assert_eq_dbg!(
        event,
        Annotated::new(Event {
            release: Annotated::from_error(
                "Invalid characters in string",
                Some(Value::String("a\nb".into())),
            ),
            ..Default::default()
        })
    );
}

#[test]
fn test_invalid_email() {
    use crate::protocol::User;

    let user = Annotated::new(User {
        email: Annotated::new("bananabread".to_owned()),
        ..Default::default()
    });

    let user = user.process(&mut SchemaProcessor);

    assert_eq_dbg!(
        user,
        Annotated::new(User {
            email: Annotated::from_error(
                "Invalid characters in string",
                Some(Value::String("bananabread".to_string()))
            ),
            ..Default::default()
        })
    );
}
