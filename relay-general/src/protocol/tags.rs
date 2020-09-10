use smartstring::alias::String;
use crate::protocol::{AsPair, LenientString, PairList};
use crate::types::{Annotated, Array, FromValue, Value};

#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct TagEntry(
    #[metastructure(max_chars = "tag_key", match_regex = r"^[a-zA-Z0-9_\.:-]+\z")]
    pub  Annotated<String>,
    #[metastructure(max_chars = "tag_value", match_regex = r"^[^\n]+\z")] pub Annotated<String>,
);

impl AsPair for TagEntry {
    type Key = String;
    type Value = String;

    fn from_pair(pair: (Annotated<Self::Key>, Annotated<Self::Value>)) -> Self {
        TagEntry(pair.0, pair.1)
    }

    fn into_pair(self) -> (Annotated<Self::Key>, Annotated<Self::Value>) {
        (self.0, self.1)
    }

    fn as_pair(&self) -> (&Annotated<Self::Key>, &Annotated<Self::Value>) {
        (&self.0, &self.1)
    }

    fn as_pair_mut(&mut self) -> (&mut Annotated<Self::Key>, &mut Annotated<Self::Value>) {
        (&mut self.0, &mut self.1)
    }
}

impl FromValue for TagEntry {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        type TagTuple = (Annotated<LenientString>, Annotated<LenientString>);
        TagTuple::from_value(value).map_value(|(key, value)| {
            TagEntry(
                key.map_value(|x| x.into_inner().replace(" ", "-").trim().into()),
                value.map_value(|x| x.into_inner().trim().into()),
            )
        })
    }
}

/// Manual key/value tag pairs.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Tags(pub PairList<TagEntry>);

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

#[test]
fn test_tags_from_object() {
    let json = r#"{
  "blah": "blub",
  "bool": true,
  "foo bar": "baz",
  "non string": 42,
  "bam": null
}"#;

    let mut arr = Array::new();
    arr.push(Annotated::new(TagEntry(
        Annotated::new("bam".into()),
        Annotated::empty(),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("blah".into()),
        Annotated::new("blub".into()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("bool".into()),
        Annotated::new("True".into()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("foo-bar".into()),
        Annotated::new("baz".into()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("non-string".into()),
        Annotated::new("42".into()),
    )));

    let tags = Annotated::new(Tags(arr.into()));
    assert_eq_dbg!(tags, Annotated::from_json(json).unwrap());
}

#[test]
fn test_tags_from_array() {
    use crate::protocol::Event;

    let input = r#"{
  "tags": [
    [
      "bool",
      true
    ],
    [
      "foo bar",
      "baz"
    ],
    [
      23,
      42
    ],
    [
      "blah",
      "blub"
    ],
    [
      "bam",
      null
    ]
  ]
}"#;

    let output = r#"{
  "tags": [
    [
      "bool",
      "True"
    ],
    [
      "foo-bar",
      "baz"
    ],
    [
      "23",
      "42"
    ],
    [
      "blah",
      "blub"
    ],
    [
      "bam",
      null
    ]
  ]
}"#;

    let mut arr = Array::new();
    arr.push(Annotated::new(TagEntry(
        Annotated::new("bool".into()),
        Annotated::new("True".into()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("foo-bar".into()),
        Annotated::new("baz".into()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("23".into()),
        Annotated::new("42".into()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("blah".into()),
        Annotated::new("blub".into()),
    )));
    arr.push(Annotated::new(TagEntry(
        Annotated::new("bam".into()),
        Annotated::empty(),
    )));

    let tags = Annotated::new(Tags(arr.into()));
    let event = Annotated::new(Event {
        tags,
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::from_json(input).unwrap());
    assert_eq_str!(event.to_json_pretty().unwrap(), output);
}
