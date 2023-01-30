use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::io::{Read, Write};

use relay_general::pii::{PiiConfig, PiiProcessor};
use relay_general::processor::{
    FieldAttrs, Pii, ProcessingState, Processor, SelectorSpec, ValueType,
};
use relay_general::types::{Meta, ProcessingAction};

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod serialization;

/// Parses compressed replay recording payloads and applies data scrubbers.
///
/// `limit` controls the maximum size in bytes during decompression. This function returns an `Err`
/// if decompressed contents exceed the limit.
pub fn process_recording(bytes: &[u8], limit: usize) -> Result<Vec<u8>, RecordingParseError> {
    let (headers, mut body) = deserialize(bytes, limit)?;

    // Walk recording scrubbing PII as we go.
    strip_pii(&mut body).map_err(RecordingParseError::ProcessingAction)?;

    let out_bytes = serialize(headers, body)?;
    Ok([headers.into(), vec![b'\n'], out_bytes].concat())
}

fn strip_pii(events: &mut Vec<Event>) -> Result<(), ProcessingAction> {
    let mut pii_config = PiiConfig::default();
    pii_config.applications =
        BTreeMap::from([(SelectorSpec::And(vec![]), vec!["@common".to_string()])]);

    let pii_processor = PiiProcessor::new(pii_config.compiled());
    let mut processor = RecordingProcessor::new(pii_processor);
    processor.mask_pii(events)?;

    Ok(())
}

// Protocol.
//
// The following section includes the behaviors responsible for deserializing a recording from
// its HTTP transport schema and serializing a recording to its Kafka transport schema.
//
// We expect recordings to come in the format of plaintext headers (JSON encoded), then a new
// line character, then an optionally compressed RRWeb recording data.  Failure to inclue any
// component of this schema results in an error.

fn deserialize(bytes: &[u8], limit: usize) -> Result<(&[u8], Vec<Event>), RecordingParseError> {
    let (headers, body) = read(bytes)?;

    // We always attempt to decompress the body value. If decompression fails we try to JSON
    // deserialize the body bytes as is.
    match decompress(body, limit) {
        Ok(buf) => {
            let events: Vec<Event> =
                serde_json::from_slice(&buf).map_err(RecordingParseError::InvalidBody)?;
            Ok((headers, events))
        }
        Err(_) => {
            let events: Vec<Event> =
                serde_json::from_slice(body).map_err(RecordingParseError::InvalidBody)?;
            Ok((headers, events))
        }
    }
}

fn serialize(headers: &[u8], body: Vec<Event>) -> Result<Vec<u8>, RecordingParseError> {
    let output = serde_json::to_vec(&body).map_err(RecordingParseError::InvalidBody)?;
    let output_bytes = compress(output)?;
    Ok([headers.into(), vec![b'\n'], output_bytes].concat())
}

fn compress(output: Vec<u8>) -> Result<Vec<u8>, RecordingParseError> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&output)
        .map_err(RecordingParseError::Compression)?;
    encoder.finish().map_err(RecordingParseError::Compression)
}

fn decompress(zipped_bytes: &[u8], limit: usize) -> Result<Vec<u8>, RecordingParseError> {
    let mut buffer = Vec::new();

    let decoder = ZlibDecoder::new(zipped_bytes);
    decoder
        .take(limit as u64)
        .read_to_end(&mut buffer)
        .map_err(RecordingParseError::Compression)?;

    Ok(buffer)
}

fn read(bytes: &[u8]) -> Result<(&[u8], &[u8]), RecordingParseError> {
    match bytes.is_empty() {
        true => Err(RecordingParseError::MissingData),
        false => {
            let mut split = bytes.splitn(2, |b| b == &b'\n');
            let header = split.next().ok_or(RecordingParseError::MissingHeaders)?;

            // Try to parse the headers to determine if they are valid JSON. This is a good sanity
            // check to determine if our headers extraction is working properly.
            serde_json::from_slice::<Value>(header).map_err(RecordingParseError::InvalidHeaders)?;

            let body = match split.next() {
                Some(b"") | None => return Err(RecordingParseError::MissingBody),
                Some(body) => body,
            };

            Ok((header, body))
        }
    }
}

// Error

#[derive(Debug)]
pub enum RecordingParseError {
    MissingData,
    MissingHeaders,
    MissingBody,
    InvalidBody(serde_json::Error),
    InvalidHeaders(serde_json::Error),
    Compression(std::io::Error),
    Message(&'static str),
    ProcessingAction(ProcessingAction),
}

impl Display for RecordingParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordingParseError::MissingData => write!(f, "no recording found"),
            RecordingParseError::MissingHeaders => write!(f, "no recording headers found"),
            RecordingParseError::MissingBody => write!(f, "no recording body found"),
            RecordingParseError::InvalidHeaders(serde_error) => write!(f, "{serde_error}"),
            RecordingParseError::InvalidBody(serde_error) => write!(f, "{serde_error}"),
            RecordingParseError::Compression(io_error) => write!(f, "{io_error}"),
            RecordingParseError::Message(message) => write!(f, "{message}"),
            RecordingParseError::ProcessingAction(action) => write!(f, "{action}"),
        }
    }
}

impl std::error::Error for RecordingParseError {}

impl From<serde_json::Error> for RecordingParseError {
    fn from(err: serde_json::Error) -> Self {
        RecordingParseError::InvalidBody(err)
    }
}

impl From<std::io::Error> for RecordingParseError {
    fn from(err: std::io::Error) -> Self {
        RecordingParseError::Compression(err)
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
            NodeVariant::T3(text) | NodeVariant::T4(text) | NodeVariant::T5(text) => {
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
            CustomEventDataVariant::PerformanceSpan(span) => {
                self.strip_pii(&mut span.payload.description)?;
            }
        }

        Ok(())
    }

    fn recurse_element(&mut self, element: &mut ElementNode) -> Result<(), ProcessingAction> {
        match element.tag_name.as_str() {
            "script" | "style" => {}
            "img" | "source" => {
                let attrs = &mut element.attributes;
                attrs.insert("src".to_string(), Value::String("#".to_string()));
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
            ProcessingState::root().enter_static("", Some(field_attrs), Some(ValueType::String));
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

#[derive(Debug)]
enum Event {
    T0(Value), // 0: DOMContentLoadedEvent,
    T1(Value), // 1: LoadEvent,
    T2(Box<FullSnapshotEvent>),
    T3(Box<IncrementalSnapshotEvent>),
    T4(Box<MetaEvent>),
    T5(Box<CustomEvent>),
    T6(Value), // 6: PluginEvent,
}

#[derive(Debug, Serialize, Deserialize)]
struct FullSnapshotEvent {
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
    timestamp: u64,
    data: IncrementalSourceDataVariant,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetaEvent {
    timestamp: u64,
    data: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct CustomEvent {
    timestamp: f64,
    data: CustomEventDataVariant,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum CustomEventDataVariant {
    #[serde(rename = "breadcrumb")]
    Breadcrumb(Box<Breadcrumb>),
    #[serde(rename = "performanceSpan")]
    PerformanceSpan(Box<PerformanceSpan>),
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
    root_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_shadow_host: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_shadow: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compat_mode: Option<String>,
    #[serde(flatten)]
    variant: NodeVariant,
}

#[derive(Debug)]

enum NodeVariant {
    T0(Box<DocumentNode>),
    T1(Box<DocumentTypeNode>),
    T2(Box<ElementNode>),
    T3(Box<TextNode>), // text
    T4(Box<TextNode>), // cdata
    T5(Box<TextNode>), // comment
}

#[derive(Debug, Serialize, Deserialize)]
struct DocumentNode {
    id: i32,
    #[serde(rename = "childNodes")]
    child_nodes: Vec<Node>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DocumentTypeNode {
    id: Value,
    public_id: Value,
    system_id: Value,
    name: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ElementNode {
    id: Value,
    attributes: HashMap<String, Value>,
    tag_name: String,
    child_nodes: Vec<Node>,
    #[serde(rename = "isSVG", skip_serializing_if = "Option::is_none")]
    is_svg: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    need_block: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TextNode {
    id: Value,
    text_content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_style: Option<Value>,
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

#[derive(Debug)]
enum IncrementalSourceDataVariant {
    Mutation(Box<MutationIncrementalSourceData>),
    Input(Box<InputIncrementalSourceData>),
    Default(Box<DefaultIncrementalSourceData>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InputIncrementalSourceData {
    id: i32,
    text: String,
    is_checked: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    user_triggered: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MutationIncrementalSourceData {
    texts: Vec<Value>,
    attributes: Vec<Value>,
    removes: Vec<Value>,
    adds: Vec<MutationAdditionIncrementalSourceData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_attach_iframe: Option<Value>,
}

#[derive(Debug)]
struct DefaultIncrementalSourceData {
    pub source: u8,
    pub value: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MutationAdditionIncrementalSourceData {
    parent_id: Value,
    next_id: Value,
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

    // End to end test coverage.

    #[test]
    fn test_process_recording_end_to_end() {
        // Valid compressed rrweb payload.  Contains a 16 byte header followed by a new line
        // character and concludes with a gzipped rrweb payload.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10, 120,
            156, 149, 144, 91, 106, 196, 32, 20, 64, 247, 114, 191, 237, 160, 241, 145, 234, 38,
            102, 1, 195, 124, 152, 104, 6, 33, 169, 193, 40, 52, 4, 247, 94, 91, 103, 40, 20, 108,
            59, 191, 247, 30, 207, 225, 122, 57, 32, 238, 171, 5, 69, 17, 24, 29, 53, 168, 3, 54,
            159, 194, 88, 70, 4, 193, 234, 55, 23, 157, 127, 219, 64, 93, 14, 120, 7, 37, 100, 1,
            119, 80, 29, 102, 8, 156, 1, 213, 11, 4, 209, 45, 246, 60, 77, 155, 141, 160, 94, 232,
            43, 206, 232, 206, 118, 127, 176, 132, 177, 7, 203, 42, 75, 36, 175, 44, 231, 63, 88,
            217, 229, 107, 174, 179, 45, 234, 101, 45, 172, 232, 49, 163, 84, 22, 191, 232, 63, 61,
            207, 93, 130, 229, 189, 216, 53, 138, 84, 182, 139, 178, 199, 191, 22, 139, 179, 238,
            196, 227, 244, 134, 137, 240, 158, 60, 101, 34, 255, 18, 241, 6, 116, 42, 212, 119, 35,
            234, 27, 40, 24, 130, 213, 102, 12, 105, 25, 160, 252, 147, 222, 103, 175, 205, 215,
            182, 45, 168, 17, 48, 118, 210, 105, 142, 229, 217, 168, 163, 189, 249, 80, 254, 19,
            146, 59, 13, 115, 10, 144, 115, 190, 126, 0, 2, 68, 180, 16,
        ];

        let result = recording::process_recording(payload, 1000);
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn test_process_recording_no_body_data() {
        // Empty bodies can not be decompressed and fail.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10,
        ];

        let result = recording::process_recording(payload, 1000);
        assert!(matches!(
            result.unwrap_err(),
            recording::RecordingParseError::MissingBody,
        ));
    }

    #[test]
    fn test_process_recording_bad_body_data() {
        // Invalid gzip body contents.  Can not deflate.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10, 22,
        ];

        let result = recording::process_recording(payload, 1000);
        assert!(matches!(
            result.unwrap_err(),
            // Compression errors on deserialize are suppressed and appear as InvalidBody types.
            recording::RecordingParseError::InvalidBody(_),
        ));
    }

    #[test]
    fn test_process_recording_invalid_headers() {
        // Invalid headers.  Validation should fail.
        let payload: &[u8] = &[101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125];

        let result = recording::process_recording(payload, 1000);
        assert!(matches!(
            result.unwrap_err(),
            recording::RecordingParseError::InvalidHeaders(_),
        ));
    }

    #[test]
    fn test_process_recording_no_headers() {
        // No header delimiter.  Entire payload is consumed as headers.  The empty body fails.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125,
        ];

        let result = recording::process_recording(payload, 1000);
        assert!(matches!(
            result.unwrap_err(),
            recording::RecordingParseError::MissingBody,
        ));
    }

    #[test]
    fn test_process_recording_no_contents() {
        // Empty payload can not be decompressed.  Header check never fails.
        let payload: &[u8] = &[];

        let result = recording::process_recording(payload, 1000);
        assert!(matches!(
            result.unwrap_err(),
            recording::RecordingParseError::MissingData,
        ));
    }

    // RRWeb Payload Coverage

    #[test]
    fn test_pii_credit_card_removal() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-pii.json");
        let mut events: Vec<Event> = serde_json::from_slice(payload).unwrap();

        recording::strip_pii(&mut events).unwrap();

        let aa = events.pop().unwrap();
        if let recording::Event::T3(bb) = aa {
            if let recording::IncrementalSourceDataVariant::Mutation(mut cc) = bb.data {
                let dd = cc.adds.pop().unwrap();
                if let recording::NodeVariant::T2(mut ee) = dd.node.variant {
                    let ff = ee.child_nodes.pop().unwrap();
                    if let recording::NodeVariant::T3(gg) = ff.variant {
                        assert_eq!(gg.text_content, "[creditcard]");
                        return;
                    }
                }
            }
        }
        unreachable!();
    }

    #[test]
    fn test_scrub_pii_navigation() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-performance-navigation.json");
        let mut events: Vec<Event> = serde_json::from_slice(payload).unwrap();

        recording::strip_pii(&mut events).unwrap();

        let event = events.pop().unwrap();
        if let recording::Event::T5(custom) = &event {
            if let recording::CustomEventDataVariant::PerformanceSpan(span) = &custom.data {
                assert_eq!(
                    &span.payload.description,
                    "https://sentry.io?credit-card=[creditcard]"
                );
                return;
            }
        }

        unreachable!();
    }

    #[test]
    fn test_scrub_pii_resource() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-performance-resource.json");
        let mut events: Vec<Event> = serde_json::from_slice(payload).unwrap();

        recording::strip_pii(&mut events).unwrap();

        let event = events.pop().unwrap();
        if let recording::Event::T5(custom) = &event {
            if let recording::CustomEventDataVariant::PerformanceSpan(span) = &custom.data {
                assert_eq!(
                    &span.payload.description,
                    "https://sentry.io?credit-card=[creditcard]"
                );
                return;
            }
        }

        unreachable!();
    }

    #[test]
    fn test_pii_ip_address_removal() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-pii-ip-address.json");
        let mut events: Vec<Event> = serde_json::from_slice(payload).unwrap();

        recording::strip_pii(&mut events).unwrap();

        let aa = events.pop().unwrap();
        if let recording::Event::T3(bb) = aa {
            if let recording::IncrementalSourceDataVariant::Mutation(mut cc) = bb.data {
                let dd = cc.adds.pop().unwrap();
                if let recording::NodeVariant::T2(mut ee) = dd.node.variant {
                    let ff = ee.child_nodes.pop().unwrap();
                    if let recording::NodeVariant::T3(gg) = ff.variant {
                        assert_eq!(gg.text_content, "[ip]");
                        return;
                    }
                }
            }
        }
        unreachable!();
    }

    #[test]
    fn test_rrweb_snapshot_parsing() {
        let payload = include_bytes!("../../tests/fixtures/rrweb.json");

        let input_parsed = loads(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    #[test]
    fn test_rrweb_incremental_source_parsing() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-diff.json");

        let input_parsed = loads(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    // Node coverage
    #[test]
    fn test_rrweb_node_2_parsing() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-node-2.json");

        let input_parsed: recording::NodeVariant = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    #[test]
    fn test_rrweb_node_2_style_parsing() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-node-2-style.json");

        let input_parsed: recording::NodeVariant = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        serde_json::to_string_pretty(&input_parsed).unwrap();
        assert_json_eq!(input_parsed, input_raw);
    }

    // Event coverage

    #[test]
    fn test_rrweb_event_3_parsing() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-event-3.json");

        let input_parsed: recording::Event = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw)
    }

    #[test]
    fn test_rrweb_event_5_parsing() {
        let payload = include_bytes!("../../tests/fixtures/rrweb-event-5.json");

        let input_parsed: Vec<recording::Event> = serde_json::from_slice(payload).unwrap();
        let input_raw: Value = serde_json::from_slice(payload).unwrap();
        assert_json_eq!(input_parsed, input_raw);
    }
}

#[doc(hidden)]
/// Only used in benchmarks.
pub fn _deserialize_event(payload: &[u8]) {
    let _: Vec<Event> = serde_json::from_slice(payload).unwrap();
}
