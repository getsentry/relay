use relay_event_schema::processor::{
    ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_protocol::{Annotated, Array, Empty, Error, ErrorKind, Meta, Object, Value};
use smallvec::SmallVec;

/// Mode how `required` values should be validated in a [`SchemaProcessor`].
#[derive(Debug, Default)]
pub enum RequiredMode {
    /// The default mode, which only deletes the value and leaves a remark.
    ///
    /// # Examples:
    ///
    /// ```
    /// # use relay_event_schema::processor::{self, ProcessingState, ProcessValue};
    /// # use relay_protocol::{Annotated, FromValue, IntoValue, Empty};
    /// # use relay_event_normalization::{RequiredMode, SchemaProcessor};
    /// #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
    /// struct Item {
    ///     #[metastructure(required = true)]
    ///     name: Annotated<String>,
    /// }
    ///
    /// let mut item = Annotated::new(Item {
    ///     name: Annotated::empty(),
    /// });
    ///
    /// let mut processor = SchemaProcessor::new().with_required(Default::default());
    /// processor::process_value(&mut item, &mut processor, ProcessingState::root()).unwrap();
    ///
    /// let name = &item.value().unwrap().name;
    /// assert!(name.value().is_none());
    /// assert!(name.meta().has_errors());
    /// ```
    #[default]
    DeleteValue,
    /// Instead of removing the value, the entire container containing the value is removed.
    ///
    /// # Examples:
    ///
    /// ```
    /// # use relay_event_schema::processor::{self, ProcessingState, ProcessValue};
    /// # use relay_protocol::{Annotated, FromValue, IntoValue, Empty};
    /// # use relay_event_normalization::{RequiredMode, SchemaProcessor};
    /// #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
    /// struct Item {
    ///     #[metastructure(required = true)]
    ///     name: Annotated<String>,
    /// }
    ///
    /// let mut item = Annotated::new(Item {
    ///     name: Annotated::empty(),
    /// });
    ///
    /// let mut processor = SchemaProcessor::new().with_required(RequiredMode::DeleteParent);
    /// processor::process_value(&mut item, &mut processor, ProcessingState::root()).unwrap();
    ///
    /// assert!(item.meta().has_errors());
    /// assert!(item.value().is_none());
    /// ```
    DeleteParent,
}

/// Validates constraints such as empty strings or arrays and invalid characters.
#[derive(Debug, Default)]
pub struct SchemaProcessor {
    required: RequiredMode,
    verbose_errors: bool,
    stack: SmallVec<[SchemaState; 10]>,
}

impl SchemaProcessor {
    /// Creates a new [`SchemaProcessor`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Configures how `required` values should be validated.
    pub fn with_required(mut self, mode: RequiredMode) -> Self {
        self.required = mode;
        self
    }

    /// If enabled the processor adds additional metadata to errors.
    pub fn with_verbose_errors(mut self, verbose: bool) -> Self {
        self.verbose_errors = verbose;
        self
    }
}

impl Processor for SchemaProcessor {
    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        value_trim_whitespace(value, meta, state);
        verify_value_nonempty_string(value, meta, state)?;
        verify_value_characters(value, meta, state)?;
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
        match self.required {
            RequiredMode::DeleteParent => {
                self.stack.push(SchemaState::default());
            }
            RequiredMode::DeleteValue => {
                if value.is_none() && state.attrs().required && !meta.has_errors() {
                    meta.add_error(ErrorKind::MissingAttribute);
                }
            }
        }

        Ok(())
    }

    fn after_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if matches!(self.required, RequiredMode::DeleteValue) {
            return Ok(());
        }

        let Some(mut current) = self.stack.pop() else {
            debug_assert!(false, "processing stack should always have a value");
            return Ok(());
        };

        // A local violation indicates that the current field violates a required requirement.
        // In such a case the parent must be deleted.
        let mut local_violation = None;
        if state.attrs().required {
            local_violation = current.required_violation.take();
            if value.is_none() {
                let violation = local_violation.get_or_insert_default();
                if self.verbose_errors {
                    violation.add(state);
                }
            }
        }

        if let Some(violation) = local_violation {
            if let Some(parent) = self.stack.last_mut() {
                // Just attaching the violation to the parent is enough,
                // as the parent will delete itself and annotate the error.
                match &mut parent.required_violation {
                    p @ None => *p = Some(violation),
                    Some(p) => p.merge_with(violation),
                }
            } else {
                // There is no parent we can attach the error to, this must be the root element and
                // we have to attach the violation to this node as an error.
                meta.add_error(violation)
            };
            Err(ProcessingAction::DeleteValueHard)
        } else if let Some(violation) = current.required_violation {
            // A child violated a required requirement, but this node itself is not required,
            // the parent does not need to be deleted and we can attach the violation information
            // to the current node.
            meta.add_error(violation);
            Err(ProcessingAction::DeleteValueHard)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default)]
struct SchemaState {
    required_violation: Option<RequiredViolation>,
}

#[derive(Debug, Default, Clone)]
struct RequiredViolation {
    path: Vec<Annotated<Value>>,
}

impl RequiredViolation {
    fn add(&mut self, state: &ProcessingState<'_>) {
        self.path
            .push(Annotated::new(state.path().to_string().into()));
    }

    fn merge_with(&mut self, other: Self) {
        let Self { path } = other;
        self.path.extend(path);
    }
}

impl From<RequiredViolation> for Error {
    fn from(value: RequiredViolation) -> Self {
        let mut error: Error = ErrorKind::MissingAttribute.into();
        if !value.path.is_empty() {
            error.insert("path", value.path);
        }
        error
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
    value: &T,
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

fn verify_value_nonempty_string<T>(
    value: &T,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ProcessingResult
where
    T: Empty,
{
    if state.attrs().nonempty && value.is_empty() {
        meta.add_error(Error::nonempty_string());
        Err(ProcessingAction::DeleteValueHard)
    } else {
        Ok(())
    }
}

fn verify_value_characters(
    value: &str,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ProcessingResult {
    if let Some(ref character_set) = state.attrs().characters {
        for c in value.chars() {
            if !(character_set.char_is_valid)(c) {
                meta.add_error(Error::invalid(format!("invalid character {c:?}")));
                return Err(ProcessingAction::DeleteValueSoft);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_event_schema::processor;
    use relay_event_schema::protocol::{
        CError, ClientSdkInfo, Event, MachException, Mechanism, MechanismMeta, PosixSignal,
        RawStacktrace, User,
    };
    use relay_protocol::{Annotated, FromValue, IntoValue, assert_annotated_snapshot};
    use similar_asserts::assert_eq;

    use super::*;

    fn assert_nonempty_base<T>(expected_error: &str)
    where
        T: Default + PartialEq + ProcessValue,
    {
        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
        struct Foo<T> {
            #[metastructure(required = true, nonempty = true)]
            bar: Annotated<T>,
            bar2: Annotated<T>,
        }

        let mut wrapper = Annotated::new(Foo {
            bar: Annotated::new(T::default()),
            bar2: Annotated::new(T::default()),
        });
        processor::process_value(
            &mut wrapper,
            &mut SchemaProcessor::new(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(
            wrapper,
            Annotated::new(Foo {
                bar: Annotated::from_error(Error::expected(expected_error), None),
                bar2: Annotated::new(T::default())
            })
        );
    }

    #[test]
    fn test_nonempty_string() {
        assert_nonempty_base::<String>("a non-empty string");
    }

    #[test]
    fn test_nonempty_array() {
        assert_nonempty_base::<Array<u64>>("a non-empty value");
    }

    #[test]
    fn test_nonempty_object() {
        assert_nonempty_base::<Object<u64>>("a non-empty value");
    }

    #[test]
    fn test_invalid_email() {
        let mut user = Annotated::new(User {
            email: Annotated::new("bananabread".to_owned()),
            ..Default::default()
        });

        let expected = user.clone();
        processor::process_value(
            &mut user,
            &mut SchemaProcessor::new(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(user, expected);
    }

    #[test]
    fn test_client_sdk_missing_attribute() {
        let mut info = Annotated::new(ClientSdkInfo {
            name: Annotated::new("sentry.rust".to_owned()),
            ..Default::default()
        });

        processor::process_value(
            &mut info,
            &mut SchemaProcessor::new(),
            ProcessingState::root(),
        )
        .unwrap();

        let expected = Annotated::new(ClientSdkInfo {
            name: Annotated::new("sentry.rust".to_owned()),
            version: Annotated::from_error(ErrorKind::MissingAttribute, None),
            ..Default::default()
        });

        assert_eq!(info, expected);
    }

    #[test]
    fn test_mechanism_missing_attributes() {
        let mut mechanism = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_owned()),
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::new(CError {
                    name: Annotated::new("ENOENT".to_owned()),
                    ..Default::default()
                }),
                mach_exception: Annotated::new(MachException {
                    name: Annotated::new("EXC_BAD_ACCESS".to_owned()),
                    ..Default::default()
                }),
                signal: Annotated::new(PosixSignal {
                    name: Annotated::new("SIGSEGV".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        processor::process_value(
            &mut mechanism,
            &mut SchemaProcessor::new(),
            ProcessingState::root(),
        )
        .unwrap();

        let expected = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_owned()),
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::new(CError {
                    number: Annotated::empty(),
                    name: Annotated::new("ENOENT".to_owned()),
                }),
                mach_exception: Annotated::new(MachException {
                    ty: Annotated::empty(),
                    code: Annotated::empty(),
                    subcode: Annotated::empty(),
                    name: Annotated::new("EXC_BAD_ACCESS".to_owned()),
                }),
                signal: Annotated::new(PosixSignal {
                    number: Annotated::empty(),
                    code: Annotated::empty(),
                    name: Annotated::new("SIGSEGV".to_owned()),
                    code_name: Annotated::empty(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        assert_eq!(mechanism, expected);
    }

    #[test]
    fn test_stacktrace_missing_attribute() {
        let mut stack = Annotated::new(RawStacktrace::default());

        processor::process_value(
            &mut stack,
            &mut SchemaProcessor::new(),
            ProcessingState::root(),
        )
        .unwrap();

        let expected = Annotated::new(RawStacktrace {
            frames: Annotated::from_error(ErrorKind::MissingAttribute, None),
            ..Default::default()
        });

        assert_eq!(stack, expected);
    }

    #[test]
    fn test_newlines_release() {
        let mut event = Annotated::new(Event {
            release: Annotated::new("42\n".to_owned().into()),
            ..Default::default()
        });

        processor::process_value(
            &mut event,
            &mut SchemaProcessor::new(),
            ProcessingState::root(),
        )
        .unwrap();

        let expected = Annotated::new(Event {
            release: Annotated::new("42".to_owned().into()),
            ..Default::default()
        });

        assert_eq!(expected, event);
    }

    #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
    struct TestItem<T> {
        #[metastructure(required = true, nonempty = true)]
        req_non_empty: Annotated<T>,
        #[metastructure(required = true)]
        req: Annotated<T>,
        other: Annotated<T>,
    }

    #[test]
    fn test_required_delete_parent_top_level_nonempty() {
        let mut item = Annotated::new(TestItem {
            req_non_empty: Annotated::new("".to_owned()),
            req: Annotated::new("something".to_owned()),
            other: Annotated::new("something".to_owned()),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new()
                .with_required(RequiredMode::DeleteParent)
                .with_verbose_errors(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "_meta": {
            "": {
              "err": [
                [
                  "missing_attribute",
                  {
                    "path": [
                      "req_non_empty"
                    ]
                  }
                ]
              ]
            }
          }
        }
        "#);
    }

    #[test]
    fn test_required_delete_parent_top_level_nonempty_not_verbose() {
        let mut item = Annotated::new(TestItem {
            req_non_empty: Annotated::new("".to_owned()),
            req: Annotated::new("something".to_owned()),
            other: Annotated::new("something".to_owned()),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new().with_required(RequiredMode::DeleteParent),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "_meta": {
            "": {
              "err": [
                "missing_attribute"
              ]
            }
          }
        }
        "#);
    }

    #[test]
    fn test_required_delete_parent_top_level_req() {
        let mut item = Annotated::new(TestItem {
            req_non_empty: Annotated::new("something".to_owned()),
            req: Annotated::empty(),
            other: Annotated::new("something".to_owned()),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new()
                .with_required(RequiredMode::DeleteParent)
                .with_verbose_errors(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "_meta": {
            "": {
              "err": [
                [
                  "missing_attribute",
                  {
                    "path": [
                      "req"
                    ]
                  }
                ]
              ]
            }
          }
        }
        "#);
    }

    #[test]
    fn test_required_delete_parent_top_level_req_error() {
        let mut item = Annotated::new(TestItem {
            req_non_empty: Annotated::new("something".to_owned()),
            req: Annotated(None, Meta::from_error(Error::expected("something"))),
            other: Annotated::new("something".to_owned()),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new()
                .with_required(RequiredMode::DeleteParent)
                .with_verbose_errors(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "_meta": {
            "": {
              "err": [
                [
                  "missing_attribute",
                  {
                    "path": [
                      "req"
                    ]
                  }
                ]
              ]
            }
          }
        }
        "#);
    }

    #[test]
    fn test_required_delete_parent_top_multiple_missing() {
        let mut item = Annotated::new(TestItem {
            req_non_empty: Annotated::new("".to_owned()),
            req: Annotated::empty(),
            other: Annotated::empty(),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new()
                .with_required(RequiredMode::DeleteParent)
                .with_verbose_errors(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "_meta": {
            "": {
              "err": [
                [
                  "missing_attribute",
                  {
                    "path": [
                      "req_non_empty",
                      "req"
                    ]
                  }
                ]
              ]
            }
          }
        }
        "#);
    }

    #[test]
    fn test_required_delete_parent_top_level_okay() {
        let mut item = Annotated::new(TestItem {
            req_non_empty: Annotated::new("something".to_owned()),
            req: Annotated::new("something".to_owned()),
            other: Annotated::empty(),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new()
                .with_required(RequiredMode::DeleteParent)
                .with_verbose_errors(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "req_non_empty": "something",
          "req": "something"
        }
        "#);
    }

    #[test]
    fn test_required_delete_parent_nested_propagated() {
        let mut item = Annotated::new(TestItem {
            req_non_empty: Annotated::new(TestItem {
                req_non_empty: Annotated::new("".to_owned()),
                req: Annotated::new("something".to_owned()),
                other: Annotated::new("something".to_owned()),
            }),
            req: Annotated::new(TestItem {
                req_non_empty: Annotated::new("something".to_owned()),
                req: Annotated::new("something".to_owned()),
                other: Annotated::new("something".to_owned()),
            }),
            other: Annotated::empty(),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new()
                .with_required(RequiredMode::DeleteParent)
                .with_verbose_errors(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "_meta": {
            "": {
              "err": [
                [
                  "missing_attribute",
                  {
                    "path": [
                      "req_non_empty.req_non_empty"
                    ]
                  }
                ]
              ]
            }
          }
        }
        "#);
    }

    #[test]
    fn test_required_delete_nested_simple_all_the_way() {
        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
        struct Foo {
            #[metastructure(required = true)]
            bar: Annotated<Bar>,
            other: Annotated<String>,
        }

        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
        struct Bar {
            #[metastructure(required = true, nonempty = true)]
            value: Annotated<String>,
        }

        let mut item = Annotated::new(Foo {
            bar: Annotated::new(Bar {
                value: Annotated::new("".to_owned()),
            }),
            other: Annotated::new("something".to_owned()),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new()
                .with_required(RequiredMode::DeleteParent)
                .with_verbose_errors(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "_meta": {
            "": {
              "err": [
                [
                  "missing_attribute",
                  {
                    "path": [
                      "bar.value"
                    ]
                  }
                ]
              ]
            }
          }
        }
        "#);
    }

    #[test]
    fn test_required_delete_nested_simple_one_layer() {
        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
        struct Foo {
            bar: Annotated<Bar>,
            other: Annotated<String>,
        }

        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
        struct Bar {
            #[metastructure(required = true, nonempty = true)]
            value: Annotated<String>,
        }

        let mut item = Annotated::new(Foo {
            bar: Annotated::new(Bar {
                value: Annotated::new("".to_owned()),
            }),
            other: Annotated::new("something".to_owned()),
        });

        processor::process_value(
            &mut item,
            &mut SchemaProcessor::new()
                .with_required(RequiredMode::DeleteParent)
                .with_verbose_errors(true),
            ProcessingState::root(),
        )
        .unwrap();

        assert_annotated_snapshot!(item, @r#"
        {
          "bar": null,
          "other": "something",
          "_meta": {
            "bar": {
              "": {
                "err": [
                  [
                    "missing_attribute",
                    {
                      "path": [
                        "bar.value"
                      ]
                    }
                  ]
                ]
              }
            }
          }
        }
        "#);
    }
}
