use serde::de::Visitor;
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
        enum EventType {
            T0,
            T1,
            T2,
            T3,
            T4,
            T5,
            T6,
        }
        struct EventTypeVisitor;
        impl<'de> Visitor<'de> for EventTypeVisitor {
            type Value = EventType;
            fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                ::std::fmt::Formatter::write_str(formatter, "type id")
            }
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use EventType::*;
                match value {
                    0 => Ok(T0),
                    1 => Ok(T1),
                    2 => Ok(T2),
                    3 => Ok(T3),
                    4 => Ok(T4),
                    5 => Ok(T5),
                    6 => Ok(T6),
                    _ => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(value),
                        &"type id 0 <= i < 7",
                    )),
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for EventType {
            #[inline]
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Deserializer::deserialize_any(deserializer, EventTypeVisitor)
            }
        }
        let tagged = match Deserializer::deserialize_any(
            d,
            serde::__private::de::TaggedContentVisitor::<EventType>::new(
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
            EventType::T0 => Result::map(
                <Value as serde::Deserialize>::deserialize(content_deserializer),
                Event::T0,
            ),
            EventType::T1 => Result::map(
                <Value as serde::Deserialize>::deserialize(content_deserializer),
                Event::T1,
            ),
            EventType::T2 => Result::map(
                <Box<FullSnapshotEvent> as serde::Deserialize>::deserialize(content_deserializer),
                Event::T2,
            ),
            EventType::T3 => Result::map(
                <Box<IncrementalSnapshotEvent> as serde::Deserialize>::deserialize(
                    content_deserializer,
                ),
                Event::T3,
            ),
            EventType::T4 => Result::map(
                <Box<MetaEvent> as serde::Deserialize>::deserialize(content_deserializer),
                Event::T4,
            ),
            EventType::T5 => Result::map(
                <Box<CustomEvent> as serde::Deserialize>::deserialize(content_deserializer),
                Event::T5,
            ),
            EventType::T6 => Result::map(
                <Value as serde::Deserialize>::deserialize(content_deserializer),
                Event::T6,
            ),
        }
    }
}

impl Serialize for Event {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(untagged)]
        enum Helper<'a> {
            T0(&'a Value), // 0: DOMContentLoadedEvent,
            T1(&'a Value), // 1: LoadEvent,
            T2(&'a FullSnapshotEvent),
            T3(&'a IncrementalSnapshotEvent),
            T4(&'a MetaEvent),
            T5(&'a CustomEvent),
            T6(&'a Value), // 6: PluginEvent,
        }

        #[derive(Serialize)]
        struct Outer<'a> {
            #[serde(rename = "type")]
            ty: u8,
            #[serde(flatten)]
            _helper: Helper<'a>,
        }

        match self {
            Event::T0(c) => Outer {
                ty: 0,
                _helper: Helper::T0(c),
            },
            Event::T1(c) => Outer {
                ty: 1,
                _helper: Helper::T1(c),
            },
            Event::T2(c) => Outer {
                ty: 2,
                _helper: Helper::T2(c),
            },
            Event::T3(c) => Outer {
                ty: 3,
                _helper: Helper::T3(c),
            },
            Event::T4(c) => Outer {
                ty: 4,
                _helper: Helper::T4(c),
            },
            Event::T5(c) => Outer {
                ty: 5,
                _helper: Helper::T5(c),
            },
            Event::T6(c) => Outer {
                ty: 6,
                _helper: Helper::T6(c),
            },
        }
        .serialize(s)
    }
}

/// Implementation tweaked from serde's `derive(Deserialize)` for internally tagged enums,
/// in order to work with integer tags.
impl<'de> Deserialize<'de> for NodeVariant {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum NodeType {
            Document,
            DocumentType,
            Element,
            Text,
            CData,
            Comment,
        }
        struct NodeTypeVisitor;
        impl<'de> Visitor<'de> for NodeTypeVisitor {
            type Value = NodeType;
            fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                ::std::fmt::Formatter::write_str(formatter, "type id")
            }
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    0 => Ok(NodeType::Document),
                    1 => Ok(NodeType::DocumentType),
                    2 => Ok(NodeType::Element),
                    3 => Ok(NodeType::Text),
                    4 => Ok(NodeType::CData),
                    5 => Ok(NodeType::Comment),

                    _ => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(value),
                        &"type id 0 <= i < 6",
                    )),
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for NodeType {
            #[inline]
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Deserializer::deserialize_any(deserializer, NodeTypeVisitor)
            }
        }
        let tagged = match Deserializer::deserialize_any(
            d,
            serde::__private::de::TaggedContentVisitor::<NodeType>::new(
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
            NodeType::Document => Result::map(
                <Box<DocumentNode> as serde::Deserialize>::deserialize(content_deserializer),
                NodeVariant::T0,
            ),
            NodeType::DocumentType => Result::map(
                <Box<DocumentTypeNode> as serde::Deserialize>::deserialize(content_deserializer),
                NodeVariant::T1,
            ),
            NodeType::Element => Result::map(
                <Box<ElementNode> as serde::Deserialize>::deserialize(content_deserializer),
                NodeVariant::T2,
            ),
            NodeType::Text => Result::map(
                <Box<TextNode> as serde::Deserialize>::deserialize(content_deserializer),
                NodeVariant::T3,
            ),
            NodeType::CData => Result::map(
                <Box<TextNode> as serde::Deserialize>::deserialize(content_deserializer),
                NodeVariant::T4,
            ),
            NodeType::Comment => Result::map(
                <Box<TextNode> as serde::Deserialize>::deserialize(content_deserializer),
                NodeVariant::T5,
            ),
        }
    }
}

impl Serialize for NodeVariant {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(untagged)]
        enum Helper<'a> {
            T0(&'a DocumentNode),
            T1(&'a DocumentTypeNode),
            T2(&'a ElementNode),
            T3(&'a TextNode), // text
            T4(&'a TextNode), // cdata
            T5(&'a TextNode), // comment
        }

        #[derive(Serialize)]
        struct Outer<'a> {
            #[serde(rename = "type")]
            ty: u8,
            #[serde(flatten)]
            _helper: Helper<'a>,
        }

        match self {
            NodeVariant::T0(c) => Outer {
                ty: 0,
                _helper: Helper::T0(c),
            },
            NodeVariant::T1(c) => Outer {
                ty: 1,
                _helper: Helper::T1(c),
            },
            NodeVariant::T2(c) => Outer {
                ty: 2,
                _helper: Helper::T2(c),
            },
            NodeVariant::T3(c) => Outer {
                ty: 3,
                _helper: Helper::T3(c),
            },
            NodeVariant::T4(c) => Outer {
                ty: 4,
                _helper: Helper::T4(c),
            },
            NodeVariant::T5(c) => Outer {
                ty: 5,
                _helper: Helper::T5(c),
            },
        }
        .serialize(s)
    }
}

/// Implementation tweaked from serde's `derive(Deserialize)` for internally tagged enums,
/// in order to work with integer tags.
impl<'de> Deserialize<'de> for IncrementalSourceDataVariant {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SourceVisitor;
        impl<'de> Visitor<'de> for SourceVisitor {
            type Value = u8;
            fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                ::std::fmt::Formatter::write_str(formatter, "source")
            }
            fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(value)
            }
        }

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
                <Box<MutationIncrementalSourceData> as serde::Deserialize>::deserialize(
                    content_deserializer,
                ),
                IncrementalSourceDataVariant::Mutation,
            ),
            5 => Result::map(
                <Box<InputIncrementalSourceData> as serde::Deserialize>::deserialize(
                    content_deserializer,
                ),
                IncrementalSourceDataVariant::Input,
            ),
            source => Result::map(
                <Value as serde::Deserialize>::deserialize(content_deserializer),
                |value| {
                    IncrementalSourceDataVariant::Default(Box::new(DefaultIncrementalSourceData {
                        source,
                        value,
                    }))
                },
            ),
        }
    }
}

impl Serialize for IncrementalSourceDataVariant {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(untagged)]
        enum Helper<'a> {
            Mutation(&'a MutationIncrementalSourceData),
            Input(&'a InputIncrementalSourceData),
            Default(&'a Value),
        }

        #[derive(Serialize)]
        struct Outer<'a> {
            source: u8,
            #[serde(flatten)]
            _helper: Helper<'a>,
        }

        match self {
            IncrementalSourceDataVariant::Mutation(m) => Outer {
                source: 0,
                _helper: Helper::Mutation(m.as_ref()),
            },
            IncrementalSourceDataVariant::Input(i) => Outer {
                source: 5,
                _helper: Helper::Input(i.as_ref()),
            },
            IncrementalSourceDataVariant::Default(v) => Outer {
                source: v.source,
                _helper: Helper::Default(&v.value),
            },
        }
        .serialize(s)
    }
}
