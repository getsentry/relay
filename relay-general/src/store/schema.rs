use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::types::{
    Array, Empty, Error, ErrorKind, Meta, Object, ProcessingAction, ProcessingResult,
};

pub struct SchemaProcessor;

impl Processor for SchemaProcessor {
    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        value_trim_whitespace(value, meta, &state);
        verify_value_nonempty(value, meta, &state)?;
        verify_value_characters(value, meta, &state)?;
        Ok(())
    }

    fn process_array<T>(
        &mut self,
        value: &mut Array<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        T: ProcessValue,
    {
        value.process_child_values(self, state)?;
        verify_value_nonempty(value, meta, state)?;
        Ok(())
    }

    fn process_object<T>(
        &mut self,
        value: &mut Object<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        T: ProcessValue,
    {
        value.process_child_values(self, state)?;
        verify_value_nonempty(value, meta, state)?;
        Ok(())
    }

    fn before_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if value.is_none() && state.attrs().required && !meta.has_errors() {
            meta.add_error(ErrorKind::MissingAttribute);
        }

        Ok(())
    }
}

fn value_trim_whitespace(value: &mut String, _meta: &mut Meta, state: &ProcessingState<'_>) {
    if state.attrs().trim_whitespace {
        let new_value = value.trim().to_owned();
        value.clear();
        value.push_str(&new_value);
    }
}

fn verify_value_nonempty<T>(
    value: &mut T,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ProcessingResult
where
    T: Empty,
{
    if state.attrs().nonempty && value.is_empty() {
        meta.add_error(Error::nonempty());
        Err(ProcessingAction::DeleteValueHard)
    } else {
        Ok(())
    }
}

fn verify_value_characters(
    value: &mut String,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ProcessingResult {
    if let Some(ref character_set) = state.attrs().characters {
        for c in value.chars() {
            if !(character_set.char_is_valid)(c) {
                meta.add_error(Error::invalid(format!("invalid character {:?}", c)));
                return Err(ProcessingAction::DeleteValueSoft);
            }
        }
    }

    Ok(())
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
        process_value(&mut wrapper, &mut SchemaProcessor, ProcessingState::root()).unwrap();

        assert_eq_dbg!(
            wrapper,
            Annotated::new(Foo {
                bar: Annotated::from_error(Error::expected("a non-empty value"), None),
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
    fn test_release_invalid_newlines() {
        use crate::protocol::Event;

        let mut event = Annotated::new(Event {
            release: Annotated::new("a\nb".to_string().into()),
            ..Default::default()
        });

        process_value(&mut event, &mut SchemaProcessor, ProcessingState::root()).unwrap();

        assert_eq_dbg!(
            event,
            Annotated::new(Event {
                release: Annotated::from_error(
                    Error::invalid("invalid character \'\\n\'"),
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

        let expected = user.clone();
        process_value(&mut user, &mut SchemaProcessor, ProcessingState::root()).unwrap();

        assert_eq_dbg!(user, expected);
    }

    #[test]
    fn test_client_sdk_missing_attribute() {
        use crate::protocol::ClientSdkInfo;
        use crate::types::ErrorKind;

        let mut info = Annotated::new(ClientSdkInfo {
            name: Annotated::new("sentry.rust".to_string()),
            ..Default::default()
        });

        process_value(&mut info, &mut SchemaProcessor, ProcessingState::root()).unwrap();

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
        )
        .unwrap();

        let expected = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_string()),
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::new(CError {
                    number: Annotated::empty(),
                    name: Annotated::new("ENOENT".to_string()),
                }),
                mach_exception: Annotated::new(MachException {
                    ty: Annotated::empty(),
                    code: Annotated::empty(),
                    subcode: Annotated::empty(),
                    name: Annotated::new("EXC_BAD_ACCESS".to_string()),
                }),
                signal: Annotated::new(PosixSignal {
                    number: Annotated::empty(),
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
        use crate::protocol::RawStacktrace;
        use crate::types::ErrorKind;

        let mut stack = Annotated::new(RawStacktrace::default());

        process_value(&mut stack, &mut SchemaProcessor, ProcessingState::root()).unwrap();

        let expected = Annotated::new(RawStacktrace {
            frames: Annotated::from_error(ErrorKind::MissingAttribute, None),
            ..Default::default()
        });

        assert_eq_dbg!(stack, expected);
    }

    #[test]
    fn test_newlines_release() {
        use crate::protocol::Event;
        let mut event = Annotated::new(Event {
            release: Annotated::new("42\n".to_string().into()),
            ..Default::default()
        });

        process_value(&mut event, &mut SchemaProcessor, ProcessingState::root()).unwrap();

        let expected = Annotated::new(Event {
            release: Annotated::new("42".to_string().into()),
            ..Default::default()
        });

        assert_eq_dbg!(expected, event);
    }
}
