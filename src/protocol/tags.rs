use general_derive::{ProcessValue, ToValue};

use super::*;
use crate::processor::FromValue;

/// Manual key/value tag pairs.
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Tags(pub Array<(Annotated<String>, Annotated<String>)>);

impl FromValue for Tags {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        type TagTuple = (Annotated<LenientString>, Annotated<LenientString>);
        match value {
            Annotated(Some(Value::Array(items)), meta) => {
                let mut rv = Vec::new();
                for item in items.into_iter() {
                    rv.push(TagTuple::from_value(item).map_value(|tuple| {
                        (
                            tuple.0.map_value(|key| key.0.trim().replace(" ", "-")),
                            tuple.1.map_value(|v| v.0),
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
