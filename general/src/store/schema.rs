use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::{Annotated, Array, Map, Object};

pub struct SchemaProcessor;

impl Processor for SchemaProcessor {
    fn process_string(
        &mut self,
        value: Annotated<String>,
        state: ProcessingState,
    ) -> Annotated<String> {
        check_nonempty_value(value, &state)
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

#[cfg(test)]
fn test_nonempty_base<T>()
where
    T: Default
        + PartialEq
        + crate::processor::FromValue
        + crate::processor::ToValue
        + crate::processor::ProcessValue,
{
    #[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
    struct Foo<T> {
        #[metastructure(required = "true", nonempty = "true")]
        bar: Annotated<T>,
        bar2: Annotated<T>,
    }

    let foo = Annotated::new(Foo {
        bar: Annotated::new(T::default()),
        bar2: Annotated::new(T::default()),
    });
    let foo = foo.process(&mut SchemaProcessor);

    assert_eq!(
        foo,
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
