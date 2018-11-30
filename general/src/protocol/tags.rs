use crate::processor::{process_value, ProcessValue, ProcessingState, Processor};
use crate::protocol::LenientString;
use crate::types::{Annotated, Array, FromValue, Value};

/// Manual key/value tag pairs.
#[derive(Debug, Clone, PartialEq, ToValue)]
pub struct Tags(pub Array<(Annotated<String>, Annotated<String>)>);

impl std::ops::Deref for Tags {
    type Target = Array<(Annotated<String>, Annotated<String>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Tags {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromValue for Tags {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        type TagTuple = (Annotated<LenientString>, Annotated<LenientString>);
        fn check_chars(x: String) -> Annotated<String> {
            if x.contains('\n') {
                Annotated::from_error("invalid character in tag", Some(Value::String(x)))
            } else {
                Annotated::new(x)
            }
        }

        match value {
            Annotated(Some(Value::Array(items)), meta) => {
                let mut rv = Vec::new();
                for item in items.into_iter() {
                    rv.push(TagTuple::from_value(item).map_value(|tuple| {
                        (
                            tuple
                                .0
                                .and_then(|k| check_chars(k.0))
                                .map_value(|x: String| x.trim().replace(" ", "-")),
                            tuple.1.and_then(|v| check_chars(v.0)),
                        )
                    }));
                }
                Annotated(Some(Tags(rv)), meta)
            }
            Annotated(Some(Value::Object(items)), meta) => {
                let mut rv = Vec::new();
                for (key, value) in items.into_iter() {
                    rv.push((
                        key.trim().replace(" ", "-"),
                        LenientString::from_value(value),
                    ));
                }
                rv.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                Annotated(
                    Some(Tags(
                        rv.into_iter()
                            .map(|(k, v)| Annotated::new((Annotated::new(k), v.map_value(|x| x.0))))
                            .collect(),
                    )),
                    meta,
                )
            }
            other => FromValue::from_value(other).map_value(Tags),
        }
    }
}

impl ProcessValue for Tags {
    #[inline]
    fn process_child_values<P>(value: &mut Self, processor: &mut P, state: ProcessingState)
    where
        P: Processor,
    {
        for (index, annotated_tuple) in value.iter_mut().enumerate() {
            process_value(annotated_tuple, processor, state.enter_index(index, None));
        }
    }
}

#[test]
fn test_tags_from_object() {
    let json = r#"{
  "blah": "blub",
  "bool": true,
  "foo bar": "baz",
  "non string": 42
}"#;

    let mut arr = Array::new();
    arr.push(Annotated::new((
        Annotated::new("blah".to_string()),
        Annotated::new("blub".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("bool".to_string()),
        Annotated::new("True".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("foo-bar".to_string()),
        Annotated::new("baz".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("non-string".to_string()),
        Annotated::new("42".to_string()),
    )));

    let tags = Annotated::new(Tags(arr));
    assert_eq_dbg!(tags, Annotated::from_json(json).unwrap());
}

#[test]
fn test_tags_from_array() {
    let json = r#"[
  ["bool", true],
  ["foo bar", "baz"],
  [23, 42],
  ["blah", "blub"]
]"#;

    let mut arr = Array::new();
    arr.push(Annotated::new((
        Annotated::new("bool".to_string()),
        Annotated::new("True".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("foo-bar".to_string()),
        Annotated::new("baz".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("23".to_string()),
        Annotated::new("42".to_string()),
    )));
    arr.push(Annotated::new((
        Annotated::new("blah".to_string()),
        Annotated::new("blub".to_string()),
    )));

    let tags = Annotated::new(Tags(arr));
    assert_eq_dbg!(tags, Annotated::from_json(json).unwrap());
}
