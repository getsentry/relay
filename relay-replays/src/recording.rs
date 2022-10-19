// class IncrementalSource(IntEnum):
//     MUTATION = 0
//     MOUSEMOVE = 1
//     MOUSEINTERACTION = 2
//     SCROLL = 3
//     VIEWPORTRESIZE = 4
//     INPUT = 5
//     TOUCHMOVE = 6
//     MEDIAINTERACTION = 7
//     STYLESHEETRULE = 8
//     CANVASMUTATION = 9
//     FONT = 10
//     LOG = 11
//     DRAG = 12
//     STYLEDECLARATION = 13

use std::collections::HashMap;

use serde::de::Error as DError;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};

pub fn parse_rrweb_payload(bytes: &[u8]) -> Result<Vec<Event>, Error> {
    let node: Vec<Event> = serde_json::from_slice(bytes)?;
    return Ok(node);
}

/// Event Type Parser
///
/// Events have an internally tagged variant on their "type" field. The type must be one of seven
/// values. There are no default types for this variation. Because the "type" field's values are
/// integers we must define custom serialization and deserailization behavior.

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    #[serde(flatten)]
    variant: EventVariant,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum EventVariant {
    // DOMContentLoadedEvent,
    // LoadEvent,
    T2(FullSnapshotEvent),
    T3(IncrementalSnapshotEvent),
    T4(MetaEvent),
    T5(CustomEvent),
    // PluginEvent,  No examples :O
}

impl<'de> serde::Deserialize<'de> for EventVariant {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;

        match value.get("type") {
            Some(val) => match Value::as_u64(val) {
                Some(v) => match v {
                    2 => match FullSnapshotEvent::deserialize(value) {
                        Ok(event) => Ok(EventVariant::T2(event)),
                        Err(_) => Err(DError::custom("could not parse snapshot event")),
                    },
                    3 => match IncrementalSnapshotEvent::deserialize(value) {
                        Ok(event) => Ok(EventVariant::T3(event)),
                        Err(_) => Err(DError::custom("could not parse incremental snapshot event")),
                    },
                    4 => match MetaEvent::deserialize(value) {
                        Ok(event) => Ok(EventVariant::T4(event)),
                        Err(_) => Err(DError::custom("could not parse meta event")),
                    },
                    5 => match CustomEvent::deserialize(value) {
                        Ok(event) => Ok(EventVariant::T5(event)),
                        Err(_) => Err(DError::custom("could not parse custom event")),
                    },
                    _ => return Err(DError::custom("invalid type value")),
                },
                None => return Err(DError::custom("type field must be an integer")),
            },
            None => return Err(DError::missing_field("type")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FullSnapshotEvent {
    #[serde(rename = "type")]
    ty: u8,
    timestamp: u64,
    data: FullSnapshotEventData,
}

#[derive(Debug, Serialize, Deserialize)]
struct FullSnapshotEventData {
    node: NodeVariant,
    #[serde(rename = "initialOffset")]
    initial_offset: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct IncrementalSnapshotEvent {
    #[serde(rename = "type")]
    ty: u8,
    timestamp: u64,
    data: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetaEvent {
    #[serde(rename = "type")]
    ty: u8,
    timestamp: u64,
    data: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct CustomEvent {
    #[serde(rename = "type")]
    ty: u8,
    timestamp: f64,
    data: Value,
}

/// Node Type Parser
///
/// Nodes have an internally tagged variant on their "type" field. The type must be one of six
/// values.  There are no default types for this variation. Because the "type" field's values are
/// integers we must define custom serialization and deserailization behavior.

#[derive(Debug, Serialize, Deserialize)]
struct Node {
    #[serde(flatten)]
    variant: NodeVariant,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum NodeVariant {
    T0(DocumentNode),
    T1(DocumentTypeNode),
    T2(ElementNode),
    T3(TextNode), // types 3 (text), 4 (cdata), 5 (comment)
}

impl<'de> serde::Deserialize<'de> for NodeVariant {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;

        match value.get("type") {
            Some(val) => match Value::as_u64(val) {
                Some(v) => match v {
                    0 => match DocumentNode::deserialize(value) {
                        Ok(document) => Ok(NodeVariant::T0(document)),
                        Err(_) => Err(DError::custom("could not parse document object.")),
                    },
                    1 => match DocumentTypeNode::deserialize(value) {
                        Ok(document_type) => Ok(NodeVariant::T1(document_type)),
                        Err(_) => Err(DError::custom("could not parse document-type object")),
                    },
                    2 => match ElementNode::deserialize(value) {
                        Ok(element) => Ok(NodeVariant::T2(element)),
                        Err(_) => Err(DError::custom("could not parse element object")),
                    },
                    3 | 4 | 5 => match TextNode::deserialize(value) {
                        Ok(text) => Ok(NodeVariant::T3(text)),
                        Err(_) => Err(DError::custom("could not parse text object")),
                    },
                    _ => return Err(DError::custom("invalid type value")),
                },
                None => return Err(DError::custom("type field must be an integer")),
            },
            None => return Err(DError::missing_field("type")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DocumentNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    #[serde(rename = "childNodes")]
    child_nodes: Vec<Node>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DocumentTypeNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    #[serde(rename = "publicId")]
    public_id: String,
    #[serde(rename = "systemId")]
    system_id: String,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TextNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    #[serde(rename = "textContent")]
    text_content: String,
}

/// Element Node Type Parser.
///
/// The element type has a variant on it's "tagName" field.  "style" tags have special "childNodes"
/// which do not conform to other tags.  The default variant is a catchall for every tag other
/// than "style".

#[derive(Debug, Serialize, Deserialize)]
struct ElementNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    attributes: HashMap<String, String>,
    #[serde(flatten)]
    variant: ElementNodeVariant,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ElementNodeVariant {
    Style(StyleElementNode),
    SVG(SVGElementNode),
    Default(DefaultElementNode),
}

impl<'de> serde::Deserialize<'de> for ElementNodeVariant {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;

        match value.get("tagName") {
            Some(val) => match Value::as_str(val) {
                Some(v) => match v {
                    "style" => match StyleElementNode::deserialize(value) {
                        Ok(node) => Ok(ElementNodeVariant::Style(node)),
                        Err(_) => Err(DError::custom("could not parse style element.")),
                    },
                    "svg" | "path" => match SVGElementNode::deserialize(value) {
                        Ok(node) => Ok(ElementNodeVariant::SVG(node)),
                        Err(_) => Err(DError::custom("could not parse style element.")),
                    },
                    _ => match DefaultElementNode::deserialize(value) {
                        Ok(node) => Ok(ElementNodeVariant::Default(node)),
                        Err(_) => Err(DError::custom("could not parse element")),
                    },
                },
                None => return Err(DError::custom("type field must be a string")),
            },
            None => return Err(DError::missing_field("tagName")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DefaultElementNode {
    #[serde(rename = "tagName")]
    tag_name: String,
    #[serde(rename = "childNodes")]
    child_nodes: Vec<NodeVariant>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SVGElementNode {
    #[serde(rename = "tagName")]
    tag_name: String,
    #[serde(rename = "childNodes")]
    child_nodes: Vec<NodeVariant>,
    #[serde(rename = "isSVG")]
    is_svg: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct StyleElementNode {
    #[serde(rename = "tagName")]
    tag_name: String,
    #[serde(rename = "childNodes")]
    child_nodes: Vec<StyleTextNode>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StyleTextNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    #[serde(rename = "textContent")]
    text_content: String,
    #[serde(rename = "isStyle")]
    is_style: bool,
}

#[cfg(test)]
mod tests {
    use crate::recording;
    use assert_json_diff::assert_json_eq;
    use serde_json::Value;

    // RRWeb Payload Coverage

    #[test]
    fn test_rrweb_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb.json");

        let input_parsed = recording::parse_rrweb_payload(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    // Node coverage
    #[test]
    fn test_rrweb_node_2_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb-node-2.json");

        let input_parsed: recording::Node = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    #[test]
    fn test_rrweb_node_2_style_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb-node-2-style.json");

        let input_parsed: recording::Node = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    // Event coverage

    #[test]
    fn test_rrweb_event_3_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-3.json");

        let input_parsed: recording::Event = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    #[test]
    fn test_rrweb_event_5_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-5.json");

        let input_parsed: recording::Event = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }
}
