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
            Err(err) => return Err(err),
        };
        let content_deserializer =
            serde::__private::de::ContentDeserializer::<D::Error>::new(tagged.content);
        match tagged.tag {
            0 => Value::deserialize(content_deserializer).map(Event::T0),
            1 => Value::deserialize(content_deserializer).map(Event::T1),
            2 => Box::<FullSnapshotEvent>::deserialize(content_deserializer).map(Event::T2),
            3 => Box::<IncrementalSnapshotEvent>::deserialize(content_deserializer).map(Event::T3),
            4 => Box::<MetaEvent>::deserialize(content_deserializer).map(Event::T4),
            5 => Box::<CustomEvent>::deserialize(content_deserializer).map(Event::T5),
            6 => Box::<PluginEvent>::deserialize(content_deserializer).map(Event::T6),
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
    T6(&'a PluginEvent),
}

/// Helper for [`Event`] serialization.
#[derive(Serialize)]
struct OuterEvent<'a> {
    #[serde(rename = "type")]
    ty: u8,
    #[serde(flatten)]
    inner: InnerEvent<'a>,
}

impl<'a> OuterEvent<'a> {
    fn new(ty: u8, inner: InnerEvent<'a>) -> Self {
        Self { ty, inner }
    }
}

impl Serialize for Event {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Event::T0(c) => OuterEvent::new(0, InnerEvent::T0(c)),
            Event::T1(c) => OuterEvent::new(1, InnerEvent::T1(c)),
            Event::T2(c) => OuterEvent::new(2, InnerEvent::T2(c)),
            Event::T3(c) => OuterEvent::new(3, InnerEvent::T3(c)),
            Event::T4(c) => OuterEvent::new(4, InnerEvent::T4(c)),
            Event::T5(c) => OuterEvent::new(5, InnerEvent::T5(c)),
            Event::T6(c) => OuterEvent::new(6, InnerEvent::T6(c)),
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
        let tagged = match Deserializer::deserialize_any(
            d,
            serde::__private::de::TaggedContentVisitor::<u8>::new(
                "type",
                "internally tagged enum NodeVariant",
            ),
        ) {
            Ok(val) => val,
            Err(err) => return Err(err),
        };

        let content_deserializer =
            serde::__private::de::ContentDeserializer::<D::Error>::new(tagged.content);
        match tagged.tag {
            0 => Box::<DocumentNode>::deserialize(content_deserializer).map(NodeVariant::T0),
            1 => Box::<DocumentTypeNode>::deserialize(content_deserializer).map(NodeVariant::T1),
            2 => Box::<ElementNode>::deserialize(content_deserializer).map(NodeVariant::T2),
            3 => Box::<TextNode>::deserialize(content_deserializer).map(NodeVariant::T3),
            4 => Box::<TextNode>::deserialize(content_deserializer).map(NodeVariant::T4),
            5 => Box::<TextNode>::deserialize(content_deserializer).map(NodeVariant::T5),
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
    inner: InnerNodeVariant<'a>,
}

impl<'a> OuterNodeVariant<'a> {
    fn new(ty: u8, inner: InnerNodeVariant<'a>) -> Self {
        Self { ty, inner }
    }
}

impl Serialize for NodeVariant {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            NodeVariant::T0(c) => OuterNodeVariant::new(0, InnerNodeVariant::T0(c)),
            NodeVariant::T1(c) => OuterNodeVariant::new(1, InnerNodeVariant::T1(c)),
            NodeVariant::T2(c) => OuterNodeVariant::new(2, InnerNodeVariant::T2(c)),
            NodeVariant::T3(c) => OuterNodeVariant::new(3, InnerNodeVariant::T3(c)),
            NodeVariant::T4(c) => OuterNodeVariant::new(4, InnerNodeVariant::T4(c)),
            NodeVariant::T5(c) => OuterNodeVariant::new(5, InnerNodeVariant::T5(c)),
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
        let tagged = match Deserializer::deserialize_any(
            d,
            serde::__private::de::TaggedContentVisitor::<u8>::new(
                "source",
                "internally tagged enum IncrementalSourceDataVariant",
            ),
        ) {
            Ok(val) => val,
            Err(err) => return Err(err),
        };
        let content_deserializer =
            serde::__private::de::ContentDeserializer::<D::Error>::new(tagged.content);
        match tagged.tag {
            0 => Box::<MutationIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::Mutation),
            1 => Box::<MouseMoveIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::MouseMove),
            2 => Box::<MouseInteractionIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::MouseInteraction),
            3 => Box::<ScrollIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::Scroll),
            4 => Box::<ViewPortResizeIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::ViewPortResize),
            5 => Box::<InputIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::Input),
            6 => Box::<TouchMoveIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::TouchMove),
            7 => Box::<MediaInteractionIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::MediaInteraction),
            12 => Box::<DragIncrementalSourceData>::deserialize(content_deserializer)
                .map(IncrementalSourceDataVariant::Drag),
            source => Value::deserialize(content_deserializer).map(|value| {
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
    MouseMove(&'a MouseMoveIncrementalSourceData),
    MouseInteraction(&'a MouseInteractionIncrementalSourceData),
    Scroll(&'a ScrollIncrementalSourceData),
    ViewPortResize(&'a ViewPortResizeIncrementalSourceData),
    Input(&'a InputIncrementalSourceData),
    TouchMove(&'a TouchMoveIncrementalSourceData),
    MediaInteraction(&'a MediaInteractionIncrementalSourceData),
    Drag(&'a DragIncrementalSourceData),
    Default(&'a Value),
}

/// Helper for [`IncrementalSourceDataVariant`] serialization.
#[derive(Serialize)]
struct OuterISDV<'a> {
    source: u8,
    #[serde(flatten)]
    inner: InnerISDV<'a>,
}

impl<'a> OuterISDV<'a> {
    fn new(source: u8, inner: InnerISDV<'a>) -> Self {
        Self { source, inner }
    }
}

impl Serialize for IncrementalSourceDataVariant {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            IncrementalSourceDataVariant::Mutation(m) => {
                OuterISDV::new(0, InnerISDV::Mutation(m.as_ref()))
            }
            IncrementalSourceDataVariant::MouseMove(i) => {
                OuterISDV::new(1, InnerISDV::MouseMove(i.as_ref()))
            }
            IncrementalSourceDataVariant::MouseInteraction(i) => {
                OuterISDV::new(2, InnerISDV::MouseInteraction(i.as_ref()))
            }
            IncrementalSourceDataVariant::Scroll(i) => {
                OuterISDV::new(3, InnerISDV::Scroll(i.as_ref()))
            }
            IncrementalSourceDataVariant::ViewPortResize(i) => {
                OuterISDV::new(4, InnerISDV::ViewPortResize(i.as_ref()))
            }
            IncrementalSourceDataVariant::Input(i) => {
                OuterISDV::new(5, InnerISDV::Input(i.as_ref()))
            }
            IncrementalSourceDataVariant::TouchMove(i) => {
                OuterISDV::new(6, InnerISDV::TouchMove(i.as_ref()))
            }
            IncrementalSourceDataVariant::MediaInteraction(i) => {
                OuterISDV::new(7, InnerISDV::MediaInteraction(i.as_ref()))
            }
            IncrementalSourceDataVariant::Drag(i) => {
                OuterISDV::new(12, InnerISDV::Drag(i.as_ref()))
            }
            IncrementalSourceDataVariant::Default(v) => {
                OuterISDV::new(v.source, InnerISDV::Default(&v.value))
            }
        }
        .serialize(s)
    }
}
