use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{self, BufRead, Read, Write};

use relay_general::pii::{DataScrubbingConfig, PiiProcessor};
use relay_general::processor::{FieldAttrs, Pii, ProcessingState, Processor, ValueType};
use relay_general::types::{Meta, ProcessingAction};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde::de::Error as DError;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};

pub fn process_recording(bytes: &[u8]) -> Result<Vec<u8>, RecordingParseError> {
    // Split recording headers and body.
    let cursor = io::Cursor::new(bytes);
    let mut split_iter = cursor
        .split(b'\n')
        .map(|r| r.map_err(|e| RecordingParseError::Message(e.to_string())));
    let header = split_iter.next().ok_or_else(|| {
        RecordingParseError::Message("no headers found. was data provided?".to_string())
    })??;
    let body = split_iter.next().ok_or_else(|| {
        RecordingParseError::Message("no data found. are the headers missing?".to_string())
    })??;

    // Deserialization.
    let mut events = loads(body)?;

    // Processing.
    strip_pii(&mut events).map_err(RecordingParseError::ProcessingAction)?;

    // Serialization.
    let out_bytes = dumps(events)?;
    Ok([header, vec![b'\n'], out_bytes].concat())
}

fn loads(zipped_input: Vec<u8>) -> Result<Vec<Event>, RecordingParseError> {
    let mut decoder = ZlibDecoder::new(zipped_input.as_slice());
    let mut buffer = String::new();
    decoder.read_to_string(&mut buffer)?;

    let events: Vec<Event> = serde_json::from_str(&buffer)?;
    Ok(events)
}

fn dumps(rrweb: Vec<Event>) -> Result<Vec<u8>, RecordingParseError> {
    let buffer = serde_json::to_vec(&rrweb)?;

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&buffer)?;
    let result = encoder.finish()?;
    Ok(result)
}

fn strip_pii(events: &mut Vec<Event>) -> Result<(), ProcessingAction> {
    let mut scrub_config = DataScrubbingConfig::default();
    scrub_config.scrub_data = true;
    scrub_config.scrub_defaults = true;

    let pii_config = scrub_config.pii_config().unwrap().as_ref().unwrap().clone();
    let pii_processor = PiiProcessor::new(pii_config.compiled());
    let mut processor = RecordingProcessor::new(pii_processor);
    processor.mask_pii(events)?;

    Ok(())
}

// Error

#[derive(Debug)]
pub enum RecordingParseError {
    SerdeError(Error),
    IoError(std::io::Error),
    Message(String),
    ProcessingAction(ProcessingAction),
}

impl Display for RecordingParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordingParseError::SerdeError(serde_error) => write!(f, "{}", serde_error),
            RecordingParseError::IoError(io_error) => write!(f, "{}", io_error),
            RecordingParseError::Message(message) => write!(f, "{}", message),
            RecordingParseError::ProcessingAction(action) => write!(f, "{}", action),
        }
    }
}

impl std::error::Error for RecordingParseError {}

impl From<Error> for RecordingParseError {
    fn from(err: Error) -> Self {
        RecordingParseError::SerdeError(err)
    }
}

impl From<std::io::Error> for RecordingParseError {
    fn from(err: std::io::Error) -> Self {
        RecordingParseError::IoError(err)
    }
}

// Recording Processor

struct RecordingProcessor<'a> {
    pii_processor: PiiProcessor<'a>,
}

impl RecordingProcessor<'_> {
    fn new(pii_processor: PiiProcessor) -> RecordingProcessor {
        RecordingProcessor { pii_processor }
    }

    fn mask_pii(&mut self, events: &mut Vec<Event>) -> Result<(), ProcessingAction> {
        for event in events {
            match event {
                Event::T2(variant) => self.recurse_snapshot_node(&mut variant.data.node)?,
                Event::T3(variant) => self.recurse_incremental_source(&mut variant.data)?,
                Event::T5(variant) => self.recurse_custom_event(variant)?,
                _ => {}
            };
        }
        Ok(())
    }

    fn recurse_incremental_source(
        &mut self,
        variant: &mut IncrementalSourceDataVariant,
    ) -> Result<(), ProcessingAction> {
        match variant {
            IncrementalSourceDataVariant::Mutation(mutation) => {
                for addition in &mut mutation.adds {
                    match self.recurse_snapshot_node(&mut addition.node) {
                        Ok(_) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
            IncrementalSourceDataVariant::Input(input) => self.strip_pii(&mut input.text)?,
            _ => {}
        }

        Ok(())
    }

    fn recurse_snapshot_node(&mut self, node: &mut Node) -> Result<(), ProcessingAction> {
        match &mut node.variant {
            NodeVariant::T0(document) => {
                for node in &mut document.child_nodes {
                    self.recurse_snapshot_node(node)?
                }
            }
            NodeVariant::T2(element) => self.recurse_element(element)?,
            NodeVariant::Rest(text) => {
                self.strip_pii(&mut text.text_content)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn recurse_custom_event(&mut self, event: &mut CustomEvent) -> Result<(), ProcessingAction> {
        match &mut event.data {
            CustomEventDataVariant::Breadcrumb(breadcrumb) => match &mut breadcrumb.payload.message
            {
                Some(message) => self.strip_pii(message)?,
                None => {}
            },
            CustomEventDataVariant::PerformanceSpan(_) => {}
        }

        Ok(())
    }

    fn recurse_element(&mut self, element: &mut ElementNode) -> Result<(), ProcessingAction> {
        match element.tag_name.as_str() {
            "script" | "style" => {}
            "img" | "source" => {
                let attrs = &mut element.attributes;
                attrs.insert("src".to_string(), "#".to_string());
                self.recurse_element_children(element)?
            }
            _ => self.recurse_element_children(element)?,
        }

        Ok(())
    }

    fn recurse_element_children(
        &mut self,
        element: &mut ElementNode,
    ) -> Result<(), ProcessingAction> {
        for node in &mut element.child_nodes {
            self.recurse_snapshot_node(node)?
        }

        Ok(())
    }

    fn strip_pii(&mut self, value: &mut String) -> Result<(), ProcessingAction> {
        let field_attrs = Cow::Owned(FieldAttrs::new().pii(Pii::True));
        let processing_state =
            ProcessingState::new_root(Some(field_attrs), Some(ValueType::String));
        self.pii_processor
            .process_string(value, &mut Meta::default(), &processing_state)?;

        Ok(())
    }
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

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Event {
    T2(FullSnapshotEvent),
    T3(IncrementalSnapshotEvent),
    T4(MetaEvent),
    T5(CustomEvent),
    Default(Value),
    // 0: DOMContentLoadedEvent,
    // 1: LoadEvent,
    // 6: PluginEvent,
}

impl<'de> serde::Deserialize<'de> for Event {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;

        match value.get("type") {
            Some(val) => match Value::as_u64(val) {
                Some(v) => match v {
                    2 => match FullSnapshotEvent::deserialize(value) {
                        Ok(event) => Ok(Event::T2(event)),
                        Err(_) => Err(DError::custom("could not parse snapshot event")),
                    },
                    3 => match IncrementalSnapshotEvent::deserialize(value) {
                        Ok(event) => Ok(Event::T3(event)),
                        Err(_) => Err(DError::custom("could not parse incremental snapshot event")),
                    },
                    4 => match MetaEvent::deserialize(value) {
                        Ok(event) => Ok(Event::T4(event)),
                        Err(_) => Err(DError::custom("could not parse meta event")),
                    },
                    5 => match CustomEvent::deserialize(value) {
                        Ok(event) => Ok(Event::T5(event)),
                        Err(e) => Err(DError::custom(e.to_string())),
                    },
                    0 | 1 | 6 => Ok(Event::Default(value)),
                    _ => Err(DError::custom("invalid type value")),
                },
                None => Err(DError::custom("type field must be an integer")),
            },
            None => Err(DError::missing_field("type")),
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
#[serde(rename_all = "camelCase")]
struct PerformanceSpanPayload {
    op: String,
    description: String,
    start_timestamp: f64,
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
#[serde(rename_all = "camelCase")]
struct Node {
    #[serde(skip_serializing_if = "Option::is_none")]
    root_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_shadow_host: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_shadow: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
                    _ => Err(DError::custom("invalid type value")),
                },
                None => Err(DError::custom("type field must be an integer")),
            },
            None => Err(DError::missing_field("type")),
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
#[serde(rename_all = "camelCase")]
struct DocumentTypeNode {
    #[serde(rename = "type")]
    ty: u8,
    id: u32,
    public_id: String,
    system_id: String,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ElementNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    attributes: HashMap<String, String>,
    tag_name: String,
    child_nodes: Vec<Node>,
    #[serde(rename = "isSVG", skip_serializing_if = "Option::is_none")]
    is_svg: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    need_block: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TextNode {
    id: u32,
    #[serde(rename = "type")]
    ty: u8,
    text_content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_style: Option<bool>,
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
                None => Err(DError::custom("type field must be an integer")),
            },
            None => Err(DError::missing_field("type")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InputIncrementalSourceData {
    source: u8,
    id: u32,
    text: String,
    is_checked: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MutationIncrementalSourceData {
    source: u8,
    texts: Vec<Value>,
    attributes: Vec<Value>,
    removes: Vec<Value>,
    adds: Vec<MutationAdditionIncrementalSourceData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_attach_iframe: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MutationAdditionIncrementalSourceData {
    parent_id: u32,
    next_id: Option<u32>,
    node: Node,
}

#[cfg(test)]
mod tests {
    use crate::recording;
    use crate::recording::Event;
    use assert_json_diff::assert_json_eq;
    use serde_json::{Error, Value};

    fn loads(bytes: &[u8]) -> Result<Vec<Event>, Error> {
        serde_json::from_slice(bytes)
    }

    // RRWeb Payload Coverage

    #[test]
    fn test_process_recording_no_config() {
        let payload = include_bytes!("../tests/fixtures/rrweb-binary.txt");
        recording::process_recording(payload).unwrap();
    }

    #[test]
    fn test_process_recording_has_config() {
        let payload = include_bytes!("../tests/fixtures/rrweb-binary.txt");
        recording::process_recording(payload).unwrap();
    }

    #[test]
    fn test_pii_removal() {
        let payload = include_bytes!("../tests/fixtures/rrweb-pii.json");
        let mut events: Vec<Event> = serde_json::from_slice(payload).unwrap();

        recording::strip_pii(&mut events).unwrap();

        let aa = events.pop().unwrap();
        if let recording::Event::T3(bb) = aa {
            if let recording::IncrementalSourceDataVariant::Mutation(mut cc) = bb.data {
                let dd = cc.adds.pop().unwrap();
                if let recording::NodeVariant::T2(mut ee) = dd.node.variant {
                    let ff = ee.child_nodes.pop().unwrap();
                    if let recording::NodeVariant::Rest(gg) = ff.variant {
                        assert!(gg.text_content != "4917-4845-8989-7107".to_string());
                        return;
                    }
                }
            }
        }
        assert!(false);
    }

    #[test]
    fn test_rrweb_snapshot_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb.json");

        let input_parsed = loads(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    #[test]
    fn test_rrweb_incremental_source_parsing() {
        let payload = include_bytes!("../tests/fixtures/rrweb-diff.json");

        let input_parsed = loads(payload).unwrap();
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
