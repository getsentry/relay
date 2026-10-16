//! Validation and representative call-path selection for version 1 [flame graph attachments].
//!
//! [flame graph attachments]: https://develop.sentry.dev/sdk/telemetry/attachments/#flame-graph

use std::collections::{HashMap, HashSet};

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
    #[error("cyclic call path")]
    CyclicCallPath,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_attributed: Option<bool>,
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

/// Identify a representative caller-to-callee path, expressed as frame-table indices.
///
/// Like the `AnrCulpritIdentifier` in the Android SDK, this scores identical root-to-frame prefixes by
/// `sample_count * (depth + application_frames)`. Node counts are inclusive, so they already
/// include the observations ending in descendants. Explicit `in_app: true` supplies the app boost.
///
/// Sample populations from different trees are never added together. Prefer trees attributed to
/// the diagnostic when present; otherwise choose the strongest path across all trees. Ties retain
/// the first path in input order. The attachment itself remains unchanged.
pub fn identify_culprit(payload: &[u8]) -> Result<Vec<usize>, Error> {
    let graph = parse(payload)?;

    // A producer may emit duplicate frame-table entries. Intern their known frame data once,
    // rather than copying or hashing entire paths at every node.
    let mut frames = HashMap::new();
    let mut frame_ids = Vec::with_capacity(graph.frames.len());
    for (index, frame) in graph.frames.iter().enumerate() {
        let key = (&frame.frame, frame.addr_mode.as_deref().unwrap_or("abs"));
        frame_ids.push(*frames.entry(key).or_insert(index));
    }

    struct Prefix {
        parent: Option<usize>,
        frame_id: usize,
        depth: usize,
        app_frames: usize,
        samples: u128,
    }

    let has_attributed_tree = graph
        .trees
        .iter()
        .any(|tree| tree.thread_attributed == Some(true));
    let mut best_score = 0;
    let mut best_path = Vec::new();
    for tree in &graph.trees {
        if has_attributed_tree && tree.thread_attributed != Some(true) {
            continue;
        }
        let mut prefixes = HashMap::new();
        let mut candidates: Vec<Prefix> = Vec::new();
        let mut pending: Vec<_> = tree.roots.iter().rev().map(|node| (node, None)).collect();
        while let Some((node_index, parent)) = pending.pop() {
            let node = graph.node(*node_index)?;
            let frame_id = frame_ids[node.frame_id];
            let prefix = *prefixes.entry((parent, frame_id)).or_insert_with(|| {
                let (depth, app_frames) = parent
                    .map(|parent: usize| {
                        let prefix = &candidates[parent];
                        (prefix.depth, prefix.app_frames)
                    })
                    .unwrap_or_default();
                let index = candidates.len();
                candidates.push(Prefix {
                    parent,
                    frame_id,
                    depth: depth + 1,
                    app_frames: app_frames
                        + usize::from(graph.frames[frame_id].frame.in_app == Some(true)),
                    samples: 0,
                });
                index
            });
            if candidates[prefix].depth > graph.nodes.len() {
                return Err(Error::CyclicCallPath);
            }
            candidates[prefix].samples += u128::from(node.sample_count);
            pending.extend(
                node.children
                    .iter()
                    .rev()
                    .map(|child| (child, Some(prefix))),
            );
        }

        // Integer scores retain differences near the maximum safe JSON sample count. The input
        // byte limit bounds the number of nodes and path depth, so u128 also bounds aggregation.
        let mut best_prefix = None;
        for (index, prefix) in candidates.iter().enumerate() {
            let score = prefix.samples * (prefix.depth + prefix.app_frames) as u128;
            if score > best_score {
                best_score = score;
                best_prefix = Some(index);
            }
        }
        if let Some(index) = best_prefix {
            let mut path = Vec::new();
            let mut current = Some(index);
            while let Some(index) = current {
                let prefix = &candidates[index];
                path.push(prefix.frame_id);
                current = prefix.parent;
            }
            path.reverse();
            best_path = path;
        }
    }
    Ok(best_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn identify(graph: serde_json::Value) -> Result<Vec<usize>, Error> {
        identify_culprit(&serde_json::to_vec(&graph).unwrap())
    }

    fn culprit_graph() -> serde_json::Value {
        json!({
            "version": "1", "platform": "cocoa",
            "frames": [
                {"instruction_addr": "0x1000", "in_app": false},
                {"instruction_addr": "0x2000", "in_app": true},
                {"instruction_addr": "0x3000", "in_app": false},
                {"instruction_addr": "0x4000", "in_app": false}
            ],
            "nodes": [
                {"frame_id": 0, "sample_count": 100, "children": [1]},
                {"frame_id": 1, "sample_count": 100, "children": [2, 3]},
                {"frame_id": 2, "sample_count": 60},
                {"frame_id": 3, "sample_count": 40}
            ],
            "trees": [{"roots": [0]}]
        })
    }

    #[test]
    fn test_culprit_shared_application_prefix() {
        // Shared caller: 100 * (2 + 1) = 300, versus 60 * (3 + 1) = 240.
        assert_eq!(identify(culprit_graph()).unwrap(), vec![0, 1]);
    }

    #[test]
    fn test_culprit_duplicate_prefixes_and_frame_entries() {
        let mut graph = culprit_graph();
        let duplicate = graph["frames"][0].clone();
        graph["frames"].as_array_mut().unwrap().push(duplicate);
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 60, "children": [1]},
            {"frame_id": 1, "sample_count": 60, "children": [2]},
            {"frame_id": 2, "sample_count": 60},
            {"frame_id": 4, "sample_count": 40, "children": [4]},
            {"frame_id": 1, "sample_count": 40, "children": [5]},
            {"frame_id": 3, "sample_count": 40}
        ]);
        graph["trees"][0]["roots"] = json!([0, 3]);
        assert_eq!(identify(graph).unwrap(), vec![0, 1]);
    }

    #[test]
    fn test_culprit_application_weight_and_stable_ties() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 2, "sample_count": 10},
            {"frame_id": 1, "sample_count": 10},
            {"frame_id": 3, "sample_count": 10}
        ]);
        graph["trees"][0]["roots"] = json!([0, 1, 2]);
        assert_eq!(identify(graph.clone()).unwrap(), vec![1]);
        graph["frames"][1]["in_app"] = json!(false);
        assert_eq!(identify(graph.clone()).unwrap(), vec![2]);
        graph["frames"][1].as_object_mut().unwrap().remove("in_app");
        assert_eq!(identify(graph).unwrap(), vec![2]);
    }

    #[test]
    fn test_culprit_inclusive_counts_and_recursion() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 1, "sample_count": 100, "children": [1]},
            {"frame_id": 1, "sample_count": 60, "children": [2]},
            {"frame_id": 1, "sample_count": 30}
        ]);
        // Inclusive counts must not be counted again from descendant samples.
        assert_eq!(identify(graph).unwrap(), vec![1, 1]);
    }

    #[test]
    fn test_culprit_does_not_merge_independent_tree_populations() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 10},
            {"frame_id": 0, "sample_count": 10},
            {"frame_id": 1, "sample_count": 9}
        ]);
        graph["trees"] = json!([
            {"roots": [0]}, {"roots": [1]}, {"roots": [2]}
        ]);
        assert_eq!(identify(graph.clone()).unwrap(), vec![1]);
        graph["trees"][0]["thread_attributed"] = json!(true);
        assert_eq!(identify(graph).unwrap(), vec![0]);
    }

    #[test]
    fn test_culprit_exact_large_sample_counts() {
        let mut graph = culprit_graph();
        graph["frames"][0]["in_app"] = json!(true);
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": MAX_SAMPLE_COUNT - 1},
            {"frame_id": 1, "sample_count": MAX_SAMPLE_COUNT}
        ]);
        graph["trees"][0]["roots"] = json!([0, 1]);
        assert_eq!(identify(graph).unwrap(), vec![1]);
    }

    #[test]
    fn test_culprit_rejects_invalid_graph() {
        let mut graph = culprit_graph();
        graph["nodes"][0]["sample_count"] = json!(1);
        assert!(identify(graph).is_err());
        assert!(identify(json!({"version": "2"})).is_err());
    }

    #[test]
    fn test_culprit_rejects_cyclic_call_path() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 1, "children": [0]}
        ]);
        assert!(matches!(identify(graph), Err(Error::CyclicCallPath)));
    }

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
        assert_eq!(
            identify_culprit(&serde_json::to_vec(&graph).unwrap()).unwrap(),
            vec![0; depth]
        );

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
