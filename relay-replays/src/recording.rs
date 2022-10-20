use std::collections::HashMap;

use serde::de::Error as DError;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};

pub fn parse(bytes: &[u8]) -> Result<Vec<Event>, Error> {
    let node: Vec<Event> = serde_json::from_slice(bytes)?;
    return Ok(node);
}

pub fn write(rrweb: Vec<Event>) -> Result<Vec<u8>, Error> {
    return serde_json::to_vec(&rrweb);
}

pub fn mask_pii(mut events: Vec<Event>) -> Vec<Event> {
    for event in &mut events {
        match &mut event.variant {
            EventVariant::T2(variant) => recurse_snapshot_node(&mut variant.data.node),
            EventVariant::T3(variant) => recurse_incremental_source(&mut variant.data),
            EventVariant::T5(variant) => {}
            _ => {}
        }
    }
    return events;
}

fn recurse_incremental_source(variant: &mut IncrementalSourceDataVariant) {
    match variant {
        IncrementalSourceDataVariant::Mutation(mutation) => {}
        IncrementalSourceDataVariant::Input(input) => {}
        _ => {}
    }
}

fn recurse_snapshot_node(node: &mut Node) {
    match &mut node.variant {
        NodeVariant::T0(document) => {
            for node in &mut document.child_nodes {
                recurse_snapshot_node(node)
            }
        }
        NodeVariant::T2(element) => recurse_element(element),
        NodeVariant::Rest(text) => {
            text.strip_pii();
        }
        _ => {}
    }
}

fn recurse_element(element: &mut ElementNode) {
    match element.tag_name.as_str() {
        "script" | "style" => {}
        "img" => {
            let attrs = &mut element.attributes;
            attrs.insert("src".to_string(), "#".to_string());

            recurse_element_children(element)
        }
        _ => recurse_element_children(element),
    }
}

fn recurse_element_children(element: &mut ElementNode) {
    for node in &mut element.child_nodes {
        recurse_snapshot_node(node)
    }
}

fn strip_pii(value: &str) -> &str {
    return value;
}

/// Event Type Parser
///
/// Events have an internally tagged variant on their "type" field. The type must be one of seven
/// values. There are no default types for this variation. Because the "type" field's values are
/// integers we must define custom deserailization behavior.
///
/// -> DOMCONTENTLOADED = 0
/// -> LOAD = 1
/// -> FULLSNAPSHOT = 2
/// -> INCREMENTALSNAPSHOT = 3
/// -> META = 4
/// -> CUSTOM = 5
/// -> PLUGIN = 6

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
                        Err(e) => Err(DError::custom(e.to_string())),
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
    node: Node,
    #[serde(rename = "initialOffset")]
    initial_offset: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct IncrementalSnapshotEvent {
    #[serde(rename = "type")]
    ty: u8,
    timestamp: u64,
    data: IncrementalSourceDataVariant,
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
    data: CustomEventDataVariant,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum CustomEventDataVariant {
    #[serde(rename = "breadcrumb")]
    Breadcrumb(Breadcrumb),
    #[serde(rename = "performanceSpan")]
    PerformanceSpan(PerformanceSpan),
}

#[derive(Debug, Serialize, Deserialize)]
struct Breadcrumb {
    tag: String,
    payload: BreadcrumbPayload,
}

#[derive(Debug, Serialize, Deserialize)]
struct BreadcrumbPayload {
    #[serde(rename = "type")]
    ty: String,
    timestamp: f64,
    category: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PerformanceSpan {
    tag: String,
    payload: PerformanceSpanPayload,
}

#[derive(Debug, Serialize, Deserialize)]
struct PerformanceSpanPayload {
    op: String,
    description: String, // TODO: needs to be pii stripped (uri params)
    #[serde(rename = "startTimestamp")]
    start_timestamp: f64,
    #[serde(rename = "endTimestamp")]
    end_timestamp: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

/// Node Type Parser
///
/// Nodes have an internally tagged variant on their "type" field. The type must be one of six
/// values.  There are no default types for this variation. Because the "type" field's values are
/// integers we must define custom deserailization behavior.
///
/// -> DOCUMENT = 0
/// -> DOCUMENTTYPE = 1
/// -> ELEMENT = 2
/// -> TEXT = 3
/// -> CDATA = 4
/// -> COMMENT = 5

#[derive(Debug, Deserialize, Serialize)]
struct Node {
    #[serde(rename = "rootId", skip_serializing_if = "Option::is_none")]
    root_id: Option<u32>,
    #[serde(rename = "isShadowHost", skip_serializing_if = "Option::is_none")]
    is_shadow_host: Option<bool>,
    #[serde(rename = "isShadow", skip_serializing_if = "Option::is_none")]
    is_shadow: Option<bool>,
    #[serde(rename = "compatMode", skip_serializing_if = "Option::is_none")]
    compat_mode: Option<String>,
    #[serde(flatten)]
    variant: NodeVariant,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum NodeVariant {
    T0(DocumentNode),
    T1(DocumentTypeNode),
    T2(ElementNode),
    Rest(TextNode), // types 3 (text), 4 (cdata), 5 (comment)
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
                        Ok(text) => Ok(NodeVariant::Rest(text)),
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
struct ElementNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    attributes: HashMap<String, String>,
    #[serde(rename = "tagName")]
    tag_name: String,
    #[serde(rename = "childNodes")]
    child_nodes: Vec<Node>,
    #[serde(rename = "isSVG", skip_serializing_if = "Option::is_none")]
    is_svg: Option<bool>,
    #[serde(rename = "needBlock", skip_serializing_if = "Option::is_none")]
    need_block: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TextNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    #[serde(rename = "textContent")]
    text_content: String,
    #[serde(rename = "isStyle", skip_serializing_if = "Option::is_none")]
    is_style: Option<bool>,
}

impl TextNode {
    fn strip_pii(&mut self) {
        self.text_content = strip_pii(&self.text_content).to_string()
    }
}

/// Incremental Source Parser
///
/// Sources have an internally tagged variant on their "source" field. The type must be one of
/// fourteen values.  Because the "type" field's values are integers we must define custom
/// deserailization behavior.
///
/// -> MUTATION = 0
/// -> MOUSEMOVE = 1
/// -> MOUSEINTERACTION = 2
/// -> SCROLL = 3
/// -> VIEWPORTRESIZE = 4
/// -> INPUT = 5
/// -> TOUCHMOVE = 6
/// -> MEDIAINTERACTION = 7
/// -> STYLESHEETRULE = 8
/// -> CANVASMUTATION = 9
/// -> FONT = 10
/// -> LOG = 11
/// -> DRAG = 12
/// -> STYLEDECLARATION = 13

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum IncrementalSourceDataVariant {
    Mutation(MutationIncrementalSourceData),
    Input(InputIncrementalSourceData),
    Default(Value),
}

impl<'de> serde::Deserialize<'de> for IncrementalSourceDataVariant {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;

        match value.get("source") {
            Some(val) => match Value::as_u64(val) {
                Some(v) => match v {
                    0 => match MutationIncrementalSourceData::deserialize(value) {
                        Ok(document) => Ok(IncrementalSourceDataVariant::Mutation(document)),
                        Err(_) => Err(DError::custom("could not parse mutation object.")),
                    },
                    5 => match InputIncrementalSourceData::deserialize(value) {
                        Ok(document_type) => Ok(IncrementalSourceDataVariant::Input(document_type)),
                        Err(_) => Err(DError::custom("could not parse input object")),
                    },
                    _ => Ok(IncrementalSourceDataVariant::Default(value)),
                },
                None => return Err(DError::custom("type field must be an integer")),
            },
            None => return Err(DError::missing_field("type")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct InputIncrementalSourceData {
    source: u8,
    id: u32,
    text: String,
    #[serde(rename = "isChecked")]
    is_checked: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct MutationIncrementalSourceData {
    source: u8,
    texts: Vec<Value>,
    attributes: Vec<Value>,
    removes: Vec<Value>,
    adds: Vec<MutationAdditionIncrementalSourceData>,
    #[serde(rename = "isAttachIframe", skip_serializing_if = "Option::is_none")]
    is_attach_iframe: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MutationAdditionIncrementalSourceData {
    #[serde(rename = "parentId")]
    parent_id: u32,
    #[serde(rename = "nextId")]
    next_id: Option<u32>,
    node: Node,
}

#[cfg(test)]
mod tests {
    use crate::recording;
    use assert_json_diff::assert_json_eq;
    use serde_json::Value;

    // RRWeb Payload Coverage

    #[test]
    fn test_rrweb_snapshot_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb.json");

        let input_parsed = recording::parse(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    #[test]
    fn test_rrweb_incremental_source_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb-diff.json");

        let input_parsed = recording::parse(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    // Node coverage
    #[test]
    fn test_rrweb_node_2_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb-node-2.json");

        let input_parsed: recording::NodeVariant = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    #[test]
    fn test_rrweb_node_2_style_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb-node-2-style.json");

        let input_parsed: recording::NodeVariant = serde_json::from_slice(payload).unwrap();
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
