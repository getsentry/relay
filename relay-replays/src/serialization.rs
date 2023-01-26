use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};

use crate::recording::{DocumentNode, DocumentTypeNode, ElementNode, NodeVariant, TextNode};

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
