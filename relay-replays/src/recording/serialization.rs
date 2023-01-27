use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::recording::*;

/// Implementation tweaked from serde's `derive(Deserialize)` for internally tagged enums,
/// in order to work with integer tags.
impl<'de> Deserialize<'de> for Event {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = match Deserializer::deserialize_any(
            d,
            // NOTE: Use of this private API is discouraged by serde, but we need it for
            // efficient deserialization of these large, recursive structures into
            // internally tagged enums with integer tags.
            // Ideally, we would write our own `derive` for this, or contribute to serde
            // to support integer tags out of the box.
            serde::__private::de::TaggedContentVisitor::<u8>::new(
                "type",
                "internally tagged enum Event",
            ),
        ) {
            Ok(val) => val,
            Err(err) => {
                return Err(dbg!(err));
            }
        };
        let content_deserializer =
            serde::__private::de::ContentDeserializer::<D::Error>::new(tagged.content);
        match tagged.tag {
            0 => Result::map(Value::deserialize(content_deserializer), Event::T0),
            1 => Result::map(Value::deserialize(content_deserializer), Event::T1),
            2 => Result::map(
                Box::<FullSnapshotEvent>::deserialize(content_deserializer),
                Event::T2,
            ),
            3 => Result::map(
                Box::<IncrementalSnapshotEvent>::deserialize(content_deserializer),
                Event::T3,
            ),
            4 => Result::map(
                Box::<MetaEvent>::deserialize(content_deserializer),
                Event::T4,
            ),
            5 => Result::map(
                Box::<CustomEvent>::deserialize(content_deserializer),
                Event::T5,
            ),
            6 => Result::map(Value::deserialize(content_deserializer), Event::T6),
            value => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(value as u64),
                &"type id 0 <= i < 7",
            )),
        }
    }
}

/// Helper for [`Event`] serialization.
#[derive(Serialize)]
#[serde(untagged)]
enum InnerEvent<'a> {
    T0(&'a Value), // 0: DOMContentLoadedEvent,
    T1(&'a Value), // 1: LoadEvent,
    T2(&'a FullSnapshotEvent),
    T3(&'a IncrementalSnapshotEvent),
    T4(&'a MetaEvent),
    T5(&'a CustomEvent),
    T6(&'a Value), // 6: PluginEvent,
}

/// Helper for [`Event`] serialization.
#[derive(Serialize)]
struct OuterEvent<'a> {
    #[serde(rename = "type")]
    ty: u8,
    #[serde(flatten)]
    _helper: InnerEvent<'a>,
}

impl<'a> OuterEvent<'a> {
    fn new(event: &'a Event) -> Self {
        match event {
            Event::T0(c) => Self {
                ty: 0,
                _helper: InnerEvent::T0(c),
            },
            Event::T1(c) => Self {
                ty: 1,
                _helper: InnerEvent::T1(c),
            },
            Event::T2(c) => Self {
                ty: 2,
                _helper: InnerEvent::T2(c),
            },
            Event::T3(c) => Self {
                ty: 3,
                _helper: InnerEvent::T3(c),
            },
            Event::T4(c) => Self {
                ty: 4,
                _helper: InnerEvent::T4(c),
            },
            Event::T5(c) => Self {
                ty: 5,
                _helper: InnerEvent::T5(c),
            },
            Event::T6(c) => Self {
                ty: 6,
                _helper: InnerEvent::T6(c),
            },
        }
    }
}

impl Serialize for Event {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        OuterEvent::new(self).serialize(s)
    }
}

/// Implementation tweaked from serde's `derive(Deserialize)` for internally tagged enums,
/// in order to work with integer tags.
impl<'de> Deserialize<'de> for NodeVariant {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = match Deserializer::deserialize_any(
            d,
            serde::__private::de::TaggedContentVisitor::<u8>::new(
                "type",
                "internally tagged enum NodeVariant",
            ),
        ) {
            Ok(val) => val,
            Err(err) => {
                return Err(dbg!(err));
            }
        };

        let content_deserializer =
            serde::__private::de::ContentDeserializer::<D::Error>::new(tagged.content);
        match tagged.tag {
            0 => Result::map(
                Box::<DocumentNode>::deserialize(content_deserializer),
                NodeVariant::T0,
            ),
            1 => Result::map(
                Box::<DocumentTypeNode>::deserialize(content_deserializer),
                NodeVariant::T1,
            ),
            2 => Result::map(
                Box::<ElementNode>::deserialize(content_deserializer),
                NodeVariant::T2,
            ),
            3 => Result::map(
                Box::<TextNode>::deserialize(content_deserializer),
                NodeVariant::T3,
            ),
            4 => Result::map(
                Box::<TextNode>::deserialize(content_deserializer),
                NodeVariant::T4,
            ),
            5 => Result::map(
                Box::<TextNode>::deserialize(content_deserializer),
                NodeVariant::T5,
            ),
            value => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(value as u64),
                &"type id 0 <= i < 6",
            )),
        }
    }
}

/// Helper for [`NodeVariant`] serialization.
#[derive(Serialize)]
#[serde(untagged)]
enum InnerNodeVariant<'a> {
    T0(&'a DocumentNode),
    T1(&'a DocumentTypeNode),
    T2(&'a ElementNode),
    T3(&'a TextNode), // text
    T4(&'a TextNode), // cdata
    T5(&'a TextNode), // comment
}

/// Helper for [`NodeVariant`] serialization.
#[derive(Serialize)]
struct OuterNodeVariant<'a> {
    #[serde(rename = "type")]
    ty: u8,
    #[serde(flatten)]
    _helper: InnerNodeVariant<'a>,
}

impl<'a> OuterNodeVariant<'a> {
    fn new(nv: &'a NodeVariant) -> Self {
        match nv {
            NodeVariant::T0(c) => Self {
                ty: 0,
                _helper: InnerNodeVariant::T0(c),
            },
            NodeVariant::T1(c) => Self {
                ty: 1,
                _helper: InnerNodeVariant::T1(c),
            },
            NodeVariant::T2(c) => Self {
                ty: 2,
                _helper: InnerNodeVariant::T2(c),
            },
            NodeVariant::T3(c) => Self {
                ty: 3,
                _helper: InnerNodeVariant::T3(c),
            },
            NodeVariant::T4(c) => Self {
                ty: 4,
                _helper: InnerNodeVariant::T4(c),
            },
            NodeVariant::T5(c) => Self {
                ty: 5,
                _helper: InnerNodeVariant::T5(c),
            },
        }
    }
}

impl Serialize for NodeVariant {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        OuterNodeVariant::new(self).serialize(s)
    }
}

/// Implementation tweaked from serde's `derive(Deserialize)` for internally tagged enums,
/// in order to work with integer tags.
impl<'de> Deserialize<'de> for IncrementalSourceDataVariant {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = match Deserializer::deserialize_any(
            d,
            serde::__private::de::TaggedContentVisitor::<u8>::new(
                "source",
                "internally tagged enum IncrementalSourceDataVariant",
            ),
        ) {
            Ok(val) => val,
            Err(err) => {
                return Err(dbg!(err));
            }
        };
        let content_deserializer =
            serde::__private::de::ContentDeserializer::<D::Error>::new(tagged.content);
        match tagged.tag {
            0 => Result::map(
                Box::<MutationIncrementalSourceData>::deserialize(content_deserializer),
                IncrementalSourceDataVariant::Mutation,
            ),
            5 => Result::map(
                Box::<InputIncrementalSourceData>::deserialize(content_deserializer),
                IncrementalSourceDataVariant::Input,
            ),
            source => Result::map(Value::deserialize(content_deserializer), |value| {
                IncrementalSourceDataVariant::Default(Box::new(DefaultIncrementalSourceData {
                    source,
                    value,
                }))
            }),
        }
    }
}

/// Helper for [`IncrementalSourceDataVariant`] serialization.
#[derive(Serialize)]
#[serde(untagged)]
enum InnerISDV<'a> {
    Mutation(&'a MutationIncrementalSourceData),
    Input(&'a InputIncrementalSourceData),
    Default(&'a Value),
}

/// Helper for [`IncrementalSourceDataVariant`] serialization.
#[derive(Serialize)]
struct OuterISDV<'a> {
    source: u8,
    #[serde(flatten)]
    _helper: InnerISDV<'a>,
}

impl<'a> OuterISDV<'a> {
    fn new(isdv: &'a IncrementalSourceDataVariant) -> Self {
        match isdv {
            IncrementalSourceDataVariant::Mutation(m) => Self {
                source: 0,
                _helper: InnerISDV::Mutation(m.as_ref()),
            },
            IncrementalSourceDataVariant::Input(i) => Self {
                source: 5,
                _helper: InnerISDV::Input(i.as_ref()),
            },
            IncrementalSourceDataVariant::Default(v) => Self {
                source: v.source,
                _helper: InnerISDV::Default(&v.value),
            },
        }
    }
}

impl Serialize for IncrementalSourceDataVariant {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        OuterISDV::new(self).serialize(s)
    }
}
