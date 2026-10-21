//! Validation of version 1 [flame graph attachments].
//!
//! [flame graph attachments]: https://develop.sentry.dev/sdk/telemetry/attachments/#flame-graph

use std::collections::HashSet;

use bytes::Bytes;
use relay_event_schema::protocol::{DebugId, VALID_PLATFORMS};
use serde::{Deserialize, Serialize};

use crate::sample;

/// Maximum serialized UTF-8 JSON size required by the flame graph attachment contract:
/// <https://develop.sentry.dev/sdk/telemetry/attachments/#flame-graph>.
pub const MAX_FLAMEGRAPH_SIZE: usize = 10 * 1024 * 1024;
// JavaScript's Number.MAX_SAFE_INTEGER (2^53 - 1), so JSON consumers retain exact counts.
const MAX_SAMPLE_COUNT: u64 = 9_007_199_254_740_991;

/// A flame graph which does not conform to the attachment contract.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid JSON")]
    InvalidJson(#[from] serde_json::Error),
    #[error("attachment too large")]
    AttachmentTooLarge,
    #[error("unsupported version")]
    UnsupportedVersion,
    #[error("unknown platform")]
    UnknownPlatform,
    #[error("empty frames or nodes")]
    EmptyFramesOrNodes,
    #[error("frame has no filename, function or address")]
    MissingFrameLocation,
    #[error("unknown frame platform")]
    UnknownFramePlatform,
    #[error("native frame has no instruction address")]
    MissingNativeInstructionAddress,
    #[error("invalid addressing mode")]
    InvalidAddressingMode,
    #[error("relative address has no debug images")]
    MissingDebugImages,
    #[error("relative address has no matching debug image")]
    MissingDebugImage,
    #[error("empty roots")]
    EmptyRoots,
    #[error("node index out of bounds: {0}")]
    NodeIndex(usize),
    #[error("frame index out of bounds: {0}")]
    FrameIndex(usize),
    #[error("sample count out of bounds: {0}")]
    SampleCount(u64),
    #[error("children exceed parent sample count")]
    ChildSampleCount,
}

#[derive(Deserialize, Serialize)]
struct Graph {
    version: String,
    platform: String,
    frames: Vec<Frame>,
    nodes: Vec<Node>,
    trees: Vec<Tree>,
    #[serde(skip_serializing_if = "Option::is_none")]
    debug_meta: Option<sample::DebugMeta>,
}

impl Graph {
    fn node(&self, index: usize) -> Result<&Node, Error> {
        self.nodes.get(index).ok_or(Error::NodeIndex(index))
    }
}

#[derive(Deserialize, Serialize)]
struct Frame {
    #[serde(flatten)]
    frame: sample::Frame,
    #[serde(skip_serializing_if = "Option::is_none")]
    addr_mode: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct Tree {
    roots: Vec<usize>,
    #[serde(rename = "thread_id", skip_serializing_if = "Option::is_none")]
    _thread_id: Option<String>,
    #[serde(rename = "thread_attributed", skip_serializing_if = "Option::is_none")]
    _thread_attributed: Option<bool>,
}

#[derive(Deserialize, Serialize)]
struct Node {
    frame_id: usize,
    sample_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    children: Vec<usize>,
}

/// Validate an attachment using the shared V2 profiling frame and debug metadata schemas.
pub fn validate(payload: &[u8]) -> Result<Bytes, Error> {
    let graph = parse(payload)?;
    let serialized = serde_json::to_vec(&graph)?;

    Ok(serialized.into())
}

fn parse(payload: &[u8]) -> Result<Graph, Error> {
    if payload.len() > MAX_FLAMEGRAPH_SIZE {
        return Err(Error::AttachmentTooLarge);
    }
    let graph: Graph = serde_json::from_slice(payload)?;

    if graph.version != "1" {
        return Err(Error::UnsupportedVersion);
    }
    if !VALID_PLATFORMS.contains(&graph.platform.as_str()) {
        return Err(Error::UnknownPlatform);
    }
    if graph.frames.is_empty() || graph.trees.is_empty() || graph.nodes.is_empty() {
        return Err(Error::EmptyFramesOrNodes);
    }
    let image_debug_ids: HashSet<_> = graph
        .debug_meta
        .iter()
        .flat_map(|meta| &meta.images)
        .filter_map(|image| image.debug_id.map(|id| id.0))
        .collect();
    for entry in &graph.frames {
        let frame = &entry.frame;
        if frame.filename.is_none() && frame.function.is_none() && frame.instruction_addr.is_none()
        {
            return Err(Error::MissingFrameLocation);
        }
        let platform = frame.platform.as_deref().unwrap_or(&graph.platform);
        if !VALID_PLATFORMS.contains(&platform) {
            return Err(Error::UnknownFramePlatform);
        }
        if matches!(platform, "cocoa" | "objc" | "native" | "c") && frame.instruction_addr.is_none()
        {
            return Err(Error::MissingNativeInstructionAddress);
        }
        if let Some(mode) = entry.addr_mode.as_deref().filter(|mode| *mode != "abs") {
            let reference = mode
                .strip_prefix("rel:")
                .ok_or(Error::InvalidAddressingMode)?;
            let images = &graph
                .debug_meta
                .as_ref()
                .ok_or(Error::MissingDebugImages)?
                .images;
            let matches_image = if let Ok(index) = reference.parse::<usize>() {
                index < images.len()
            } else {
                reference
                    .parse::<DebugId>()
                    .ok()
                    .is_some_and(|id| image_debug_ids.contains(&id.0))
            };
            if !matches_image {
                return Err(Error::MissingDebugImage);
            }
        }
    }
    for node in &graph.nodes {
        if node.frame_id >= graph.frames.len() {
            return Err(Error::FrameIndex(node.frame_id));
        }
        if node.sample_count == 0 || node.sample_count > MAX_SAMPLE_COUNT {
            return Err(Error::SampleCount(node.sample_count));
        }
        let mut remaining = node.sample_count;
        for child_index in &node.children {
            let child = graph.node(*child_index)?;
            remaining = remaining
                .checked_sub(child.sample_count)
                .ok_or(Error::ChildSampleCount)?;
        }
    }
    for tree in &graph.trees {
        if tree.roots.is_empty() {
            return Err(Error::EmptyRoots);
        }
        for node_index in &tree.roots {
            graph.node(*node_index)?;
        }
    }
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_flamegraph_deep_call_tree() {
        let depth = 1000;
        let nodes: Vec<_> = (0..depth)
            .map(|index| {
                json!({
                    "frame_id": 0,
                    "sample_count": 1,
                    "children": if index + 1 < depth { vec![index + 1] } else { vec![] }
                })
            })
            .collect();
        let mut graph = json!({
            "version": "1", "platform": "cocoa",
            "frames": [{"instruction_addr": "0x1000"}],
            "nodes": nodes,
            "trees": [{"roots": [0]}]
        });
        validate(&serde_json::to_vec(&graph).unwrap()).unwrap();

        graph["nodes"][depth - 1]["sample_count"] = json!(0);
        assert!(matches!(
            validate(&serde_json::to_vec(&graph).unwrap()),
            Err(Error::SampleCount(0))
        ));
    }

    #[test]
    fn test_flamegraph_node_index_out_of_bounds() {
        let mut graph = json!({
            "version": "1", "platform": "cocoa",
            "frames": [{"instruction_addr": "0x1000"}],
            "nodes": [{"frame_id": 0, "sample_count": 1}],
            "trees": [{"roots": [42]}]
        });
        assert!(matches!(
            validate(&serde_json::to_vec(&graph).unwrap()),
            Err(Error::NodeIndex(42))
        ));

        graph["trees"][0]["roots"] = json!([0]);
        graph["nodes"][0]["children"] = json!([43]);
        assert!(matches!(
            validate(&serde_json::to_vec(&graph).unwrap()),
            Err(Error::NodeIndex(43))
        ));
    }

    #[test]
    fn test_flamegraph_frame_index_out_of_bounds() {
        let graph = json!({
            "version": "1", "platform": "cocoa",
            "frames": [{"instruction_addr": "0x1000"}],
            "nodes": [{"frame_id": 42, "sample_count": 1}],
            "trees": [{"roots": [0]}]
        });
        assert!(matches!(
            validate(&serde_json::to_vec(&graph).unwrap()),
            Err(Error::FrameIndex(42))
        ));
    }

    #[test]
    fn test_flamegraph_sample_count_out_of_bounds() {
        for sample_count in [0, MAX_SAMPLE_COUNT + 1] {
            let graph = json!({
                "version": "1", "platform": "cocoa",
                "frames": [{"instruction_addr": "0x1000"}],
                "nodes": [{"frame_id": 0, "sample_count": sample_count}],
                "trees": [{"roots": [0]}]
            });
            assert!(matches!(
                validate(&serde_json::to_vec(&graph).unwrap()),
                Err(Error::SampleCount(count)) if count == sample_count
            ));
        }
    }

    #[test]
    fn test_flamegraph_native_debug_image_types() {
        let debug_id = "84a04d24-0e60-3810-a8c0-90a65e2df61a";
        for image_type in ["macho", "symbolic", "elf", "pe", "pe_dotnet", "wasm"] {
            for addr_mode in ["abs", "rel:0", &format!("rel:{debug_id}")] {
                let mut graph = json!({
                    "version": "1", "platform": "native",
                    "frames": [{"instruction_addr": "0x1000", "addr_mode": addr_mode}],
                    "debug_meta": {"images": [{
                        "type": image_type, "debug_id": debug_id,
                        "code_file": "app", "image_addr": "0x1000", "image_size": 4096,
                        "future": {"preserve": true}
                    }]},
                    "nodes": [{"frame_id": 0, "sample_count": 1}],
                    "trees": [{"roots": [0]}]
                });
                let payload = serde_json::to_vec(&graph).unwrap();
                validate(&payload)
                    .unwrap_or_else(|error| panic!("{image_type} with {addr_mode}: {error}"));

                graph["debug_meta"]["images"][0]["debug_id"] =
                    json!("94a04d24-0e60-3810-a8c0-90a65e2df61a");
                let result = validate(&serde_json::to_vec(&graph).unwrap());
                if addr_mode.starts_with("rel:") && addr_mode != "rel:0" {
                    assert!(matches!(result, Err(Error::MissingDebugImage)));
                } else {
                    result.unwrap();
                }
            }
        }
    }

    #[test]
    fn test_flamegraph_large_relative_address_tables() {
        let debug_id = "84a04d24-0e60-3810-a8c0-90a65e2df61a";
        let frame = json!({"instruction_addr": "0x1000", "addr_mode": format!("rel:{debug_id}")});
        let mut images = vec![
            json!({
                "type": "macho", "debug_id": "94a04d24-0e60-3810-a8c0-90a65e2df61a"
            });
            10_000
        ];
        images.push(json!({"type": "macho", "debug_id": debug_id}));
        let mut graph = json!({
            "version": "1", "platform": "cocoa",
            "frames": vec![frame; 10_000],
            "debug_meta": {"images": images},
            "nodes": [{"frame_id": 9999, "sample_count": 1}],
            "trees": [{"roots": [0]}]
        });
        let payload = serde_json::to_vec(&graph).unwrap();
        assert!(payload.len() < MAX_FLAMEGRAPH_SIZE);
        validate(&payload).unwrap();

        graph["debug_meta"]["images"].as_array_mut().unwrap().pop();
        assert!(matches!(
            validate(&serde_json::to_vec(&graph).unwrap()),
            Err(Error::MissingDebugImage)
        ));
    }
}
