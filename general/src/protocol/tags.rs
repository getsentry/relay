use crate::protocol::{AsPair, LenientString, PairList};
use crate::types::{Annotated, Array, Error, FromValue, Value};

fn check_chars(string: String) -> Annotated<String> {
    if string.contains('\n') {
        let mut annotated = Annotated::empty();
        annotated
            .meta_mut()
            .add_error(Error::invalid("invalid characters in tag"));
        annotated.meta_mut().set_original_value(Some(string));
        annotated
    } else {
        Annotated::new(string)
    }
}

#[derive(Debug, Default, Clone, PartialEq, ToValue, ProcessValue)]
pub struct TagEntry(
    #[metastructure(pii = "true", max_chars = "tag_key")] pub Annotated<String>,
    #[metastructure(pii = "true", max_chars = "tag_value")] pub Annotated<String>,
);

impl AsPair for TagEntry {
    type Value = String;

    fn as_pair(&self) -> (&Annotated<String>, &Annotated<Self::Value>) {
        (&self.0, &self.1)
    }

    fn as_pair_mut(&mut self) -> (&mut Annotated<String>, &mut Annotated<Self::Value>) {
        (&mut self.0, &mut self.1)
    }
}

impl TagEntry {
    fn from_entry(entry: (String, Annotated<Value>)) -> Annotated<Self> {
        let (key, value) = entry;
        Annotated::from(TagEntry(
            check_chars(key).map_value(|k| k.trim().replace(" ", "-")),
            LenientString::from_value(value).and_then(|v| check_chars(v.into_inner())),
        ))
    }

    pub fn key(&self) -> Option<&str> {
        self.0.as_str()
    }

    pub fn key_mut(&mut self) -> &mut Option<String> {
        self.0.value_mut()
    }

    pub fn value(&self) -> Option<&str> {
        self.1.as_str()
    }

    pub fn value_mut(&mut self) -> &mut Option<String> {
        self.1.value_mut()
    }
}

impl FromValue for TagEntry {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        type TagTuple = (Annotated<LenientString>, Annotated<LenientString>);

        TagTuple::from_value(value).map_value(|(key, value)| {
            TagEntry(
                key.and_then(|k| check_chars(k.into_inner()))
                    .map_value(|k: String| k.trim().replace(" ", "-")),
                value.and_then(|v| check_chars(v.into_inner())),
            )
        })
    }
}

/// Manual key/value tag pairs.
#[derive(Debug, Default, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Tags(#[metastructure(pii = "true")] pub PairList<TagEntry>);

impl std::ops::Deref for Tags {
    type Target = Array<TagEntry>;

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
        match value {
            Annotated(Some(Value::Array(items)), meta) => {
                let entries = items.into_iter().map(TagEntry::from_value).collect();
                Annotated(Some(Tags(entries)), meta)
            }
            Annotated(Some(Value::Object(items)), meta) => {
                let entries = items.into_iter().map(TagEntry::from_entry).collect();
                Annotated(Some(Tags(entries)), meta)
            }
            Annotated(Some(other), mut meta) => {
                meta.add_error(Error::expected("tags"));
                meta.set_original_value(Some(other));
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
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
    arr.push(Annotated::new(TagEntry(
        Annotated::new("blah".to_string()),
        Annotated::new("blub".to_string()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("bool".to_string()),
        Annotated::new("True".to_string()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("foo-bar".to_string()),
        Annotated::new("baz".to_string()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("non-string".to_string()),
        Annotated::new("42".to_string()),
    )));

    let tags = Annotated::new(Tags(arr.into()));
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
    arr.push(Annotated::new(TagEntry(
        Annotated::new("bool".to_string()),
        Annotated::new("True".to_string()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("foo-bar".to_string()),
        Annotated::new("baz".to_string()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("23".to_string()),
        Annotated::new("42".to_string()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("blah".to_string()),
        Annotated::new("blub".to_string()),
    )));

    let tags = Annotated::new(Tags(arr.into()));
    assert_eq_dbg!(tags, Annotated::from_json(json).unwrap());
}
