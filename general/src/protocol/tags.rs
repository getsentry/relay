use crate::processor::{FromValue, ProcessValue, ProcessingState, Processor};
use crate::protocol::LenientString;
use crate::types::{Annotated, Array, Value};

/// Manual key/value tag pairs.
#[derive(Debug, Clone, PartialEq, ToValue)]
pub struct Tags(pub Array<(Annotated<String>, Annotated<String>)>);

impl std::ops::Deref for Tags {
    type Target = Array<(Annotated<String>, Annotated<String>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
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
    #[inline(always)]
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self>
    where
        Self: Sized,
    {
        #[derive(Debug, PartialEq, FromValue, ToValue, ProcessValue)]
        struct RealTags {
            #[metastructure(max_chars = "tag_key")]
            key: Annotated<String>,
            #[metastructure(max_chars = "tag_value")]
            value: Annotated<String>,
        }

        let v: Annotated<Array<RealTags>> = ProcessValue::process_value(
            value.map_value(|tags| {
                tags.0
                    .into_iter()
                    .map(|value| value.map_value(|(key, value)| RealTags { key, value }))
                    .collect()
            }),
            processor,
            state,
        );

        v.map_value(|tags| {
            Tags(
                tags.into_iter()
                    .map(|value| value.map_value(|RealTags { key, value }| (key, value)))
                    .collect(),
            )
        })
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
