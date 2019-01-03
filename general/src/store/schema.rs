use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::{Array, Empty, Error, ErrorKind, Meta, Object, ValueAction};

pub struct SchemaProcessor;

impl Processor for SchemaProcessor {
    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        verify_value_nonempty(value, meta, &state)
            .and_then(|| verify_value_pattern(value, meta, &state))
    }

    fn process_array<T>(
        &mut self,
        value: &mut Array<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction
    where
        T: ProcessValue,
    {
        value.process_child_values(self, state);
        verify_value_nonempty(value, meta, state)
    }

    fn process_object<T>(
        &mut self,
        value: &mut Object<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction
    where
        T: ProcessValue,
    {
        value.process_child_values(self, state);
        verify_value_nonempty(value, meta, state)
    }

    fn process_none(&mut self, meta: &mut Meta, state: &ProcessingState<'_>) {
        if state.attrs().required && !meta.has_errors() {
            meta.add_error(ErrorKind::MissingAttribute);
        }
    }
}

fn verify_value_nonempty<T>(
    value: &mut T,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ValueAction
where
    T: Empty,
{
    if state.attrs().nonempty && value.is_empty() {
        meta.add_error(Error::expected("non-empty value"));
        ValueAction::DeleteHard
    } else {
        ValueAction::Keep
    }
}

fn verify_value_pattern(
    value: &mut String,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ValueAction {
    if let Some(ref regex) = state.attrs().match_regex {
        if !regex.is_match(value) {
            meta.add_error(Error::invalid("invalid characters in string"));
            return ValueAction::DeleteSoft;
        }
    }

    ValueAction::Keep
}

#[cfg(test)]
mod tests {
    use super::SchemaProcessor;
    use crate::processor::{process_value, ProcessingState};
    use crate::types::{Annotated, Array, Error, Object, Value};

    fn assert_nonempty_base<T>()
    where
        T: Default + PartialEq + crate::processor::ProcessValue,
    {
        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
        struct Foo<T> {
            #[metastructure(required = "true", nonempty = "true")]
            bar: Annotated<T>,
            bar2: Annotated<T>,
        }

        let mut wrapper = Annotated::new(Foo {
            bar: Annotated::new(T::default()),
            bar2: Annotated::new(T::default()),
        });
        process_value(&mut wrapper, &mut SchemaProcessor, ProcessingState::root());

        assert_eq_dbg!(
            wrapper,
            Annotated::new(Foo {
                bar: Annotated::from_error(Error::expected("non-empty value"), None),
                bar2: Annotated::new(T::default())
            })
        );
    }

    #[test]
    fn test_nonempty_string() {
        assert_nonempty_base::<String>();
    }

    #[test]
    fn test_nonempty_array() {
        assert_nonempty_base::<Array<u64>>();
    }

    #[test]
    fn test_nonempty_object() {
        assert_nonempty_base::<Object<u64>>();
    }

    #[test]
    fn test_release_newlines() {
        use crate::protocol::Event;

        let mut event = Annotated::new(Event {
            release: Annotated::new("a\nb".to_string()),
            ..Default::default()
        });

        process_value(&mut event, &mut SchemaProcessor, ProcessingState::root());

        assert_eq_dbg!(
            event,
            Annotated::new(Event {
                release: Annotated::from_error(
                    Error::invalid("invalid characters in string"),
                    Some(Value::String("a\nb".into())),
                ),
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_invalid_email() {
        use crate::protocol::User;

        let mut user = Annotated::new(User {
            email: Annotated::new("bananabread".to_owned()),
            ..Default::default()
        });

        process_value(&mut user, &mut SchemaProcessor, ProcessingState::root());

        assert_eq_dbg!(
            user,
            Annotated::new(User {
                email: Annotated::from_error(
                    Error::invalid("invalid characters in string"),
                    Some(Value::String("bananabread".to_string()))
                ),
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_breadcrumb_missing_attribute() {
        use crate::protocol::Breadcrumb;
        use crate::types::ErrorKind;

        let mut crumb = Annotated::new(Breadcrumb::default());

        process_value(&mut crumb, &mut SchemaProcessor, ProcessingState::root());

        let expected = Annotated::new(Breadcrumb {
            timestamp: Annotated::from_error(ErrorKind::MissingAttribute, None),
            ..Default::default()
        });

        assert_eq_dbg!(crumb, expected);
    }

    #[test]
    fn test_client_sdk_missing_attribute() {
        use crate::protocol::ClientSdkInfo;
        use crate::types::ErrorKind;

        let mut info = Annotated::new(ClientSdkInfo {
            name: Annotated::new("sentry.rust".to_string()),
            ..Default::default()
        });

        process_value(&mut info, &mut SchemaProcessor, ProcessingState::root());

        let expected = Annotated::new(ClientSdkInfo {
            name: Annotated::new("sentry.rust".to_string()),
            version: Annotated::from_error(ErrorKind::MissingAttribute, None),
            ..Default::default()
        });

        assert_eq_dbg!(info, expected);
    }

    #[test]
    fn test_mechanism_missing_attributes() {
        use crate::protocol::{CError, MachException, Mechanism, MechanismMeta, PosixSignal};
        use crate::types::ErrorKind;

        let mut mechanism = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_string()),
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::new(CError {
                    name: Annotated::new("ENOENT".to_string()),
                    ..Default::default()
                }),
                mach_exception: Annotated::new(MachException {
                    name: Annotated::new("EXC_BAD_ACCESS".to_string()),
                    ..Default::default()
                }),
                signal: Annotated::new(PosixSignal {
                    name: Annotated::new("SIGSEGV".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        process_value(
            &mut mechanism,
            &mut SchemaProcessor,
            ProcessingState::root(),
        );

        let expected = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_string()),
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::new(CError {
                    number: Annotated::from_error(ErrorKind::MissingAttribute, None),
                    name: Annotated::new("ENOENT".to_string()),
                }),
                mach_exception: Annotated::new(MachException {
                    ty: Annotated::from_error(ErrorKind::MissingAttribute, None),
                    code: Annotated::from_error(ErrorKind::MissingAttribute, None),
                    subcode: Annotated::from_error(ErrorKind::MissingAttribute, None),
                    name: Annotated::new("EXC_BAD_ACCESS".to_string()),
                }),
                signal: Annotated::new(PosixSignal {
                    number: Annotated::from_error(ErrorKind::MissingAttribute, None),
                    code: Annotated::empty(),
                    name: Annotated::new("SIGSEGV".to_string()),
                    code_name: Annotated::empty(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        assert_eq_dbg!(mechanism, expected);
    }

    #[test]
    fn test_stacktrace_missing_attribute() {
        use crate::protocol::Stacktrace;
        use crate::types::ErrorKind;

        let mut stack = Annotated::new(Stacktrace::default());

        process_value(&mut stack, &mut SchemaProcessor, ProcessingState::root());

        let expected = Annotated::new(Stacktrace {
            frames: Annotated::from_error(ErrorKind::MissingAttribute, None),
            ..Default::default()
        });

        assert_eq_dbg!(stack, expected);
    }
}
