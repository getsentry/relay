//! Validation and representative call-path selection for version 1 [flame graph attachments].
//!
//! [flame graph attachments]: https://develop.sentry.dev/sdk/telemetry/attachments/#flame-graph

use std::collections::{HashMap, HashSet};

use relay_event_schema::protocol::{DebugId, VALID_PLATFORMS};
use serde::Deserialize;

use crate::sample::{DebugMeta, Frame};

/// Maximum serialized UTF-8 JSON size required by the flame graph attachment contract:
/// <https://develop.sentry.dev/sdk/telemetry/attachments/#flame-graph>.
pub const MAX_FLAMEGRAPH_SIZE: usize = 10 * 1024 * 1024;
// JavaScript's Number.MAX_SAFE_INTEGER (2^53 - 1), so JSON consumers retain exact counts.
const MAX_SAMPLE_COUNT: u64 = 9_007_199_254_740_991;

/// A flame graph which does not conform to the attachment contract.
#[derive(Debug, thiserror::Error)]
pub enum FlamegraphError {
    #[error("invalid flame graph JSON")]
    InvalidJson(#[from] serde_json::Error),
    #[error("invalid flame graph: {0}")]
    InvalidPayload(&'static str),
}

#[derive(Deserialize)]
struct Flamegraph<F = FlamegraphFrame> {
    version: String,
    platform: String,
    frames: Vec<F>,
    trees: Vec<Tree>,
    debug_meta: Option<DebugMeta>,
}

#[derive(Deserialize)]
struct FlamegraphFrame {
    #[serde(flatten)]
    frame: Frame,
    addr_mode: Option<String>,
}

#[derive(Deserialize)]
struct Tree {
    roots: Vec<Node>,
    #[serde(rename = "thread_id")]
    _thread_id: Option<String>,
    thread_attributed: Option<bool>,
}

#[derive(Deserialize)]
struct Node {
    frame_id: usize,
    sample_count: u64,
    #[serde(default)]
    children: Vec<Node>,
}

impl Drop for Node {
    fn drop(&mut self) {
        // Deserialization can grow the stack, but destruction must not recurse through call paths.
        let mut pending = std::mem::take(&mut self.children);
        while let Some(mut node) = pending.pop() {
            pending.append(&mut node.children);
        }
    }
}

/// Validate an attachment using the shared V2 profiling frame and debug metadata schemas.
///
/// The original bytes, including unknown fields and symbolication inputs, remain unchanged.
/// Symbolication and its frame-processing budget belong to the downstream consumer.
pub fn validate(payload: &[u8]) -> Result<(), FlamegraphError> {
    parse(payload).map(|_| ())
}

fn parse(payload: &[u8]) -> Result<Flamegraph, FlamegraphError> {
    use FlamegraphError::InvalidPayload;

    if payload.len() > MAX_FLAMEGRAPH_SIZE {
        return Err(InvalidPayload("attachment too large"));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    deserializer.disable_recursion_limit();
    let raw: Flamegraph<&serde_json::value::RawValue> =
        Flamegraph::deserialize(serde_stacker::Deserializer::new(&mut deserializer))?;
    deserializer.end()?;
    // Grow the stack only for call trees. Parse flattened frame data separately with JSON's
    // normal nesting limit, including unknown fields which serde temporarily buffers.
    let graph = Flamegraph {
        version: raw.version,
        platform: raw.platform,
        frames: raw
            .frames
            .into_iter()
            .map(|frame| serde_json::from_str(frame.get()))
            .collect::<Result<Vec<FlamegraphFrame>, _>>()?,
        trees: raw.trees,
        debug_meta: raw.debug_meta,
    };
    if graph.version != "1" {
        return Err(InvalidPayload("unsupported version"));
    }
    if !VALID_PLATFORMS.contains(&graph.platform.as_str()) {
        return Err(InvalidPayload("unknown platform"));
    }
    if graph.frames.is_empty() || graph.trees.is_empty() {
        return Err(InvalidPayload("empty frames or trees"));
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
            return Err(InvalidPayload("frame has no filename, function or address"));
        }
        let platform = frame.platform.as_deref().unwrap_or(&graph.platform);
        if !VALID_PLATFORMS.contains(&platform) {
            return Err(InvalidPayload("unknown frame platform"));
        }
        if matches!(platform, "cocoa" | "objc" | "native" | "c") && frame.instruction_addr.is_none()
        {
            return Err(InvalidPayload("native frame has no instruction address"));
        }
        if let Some(mode) = entry.addr_mode.as_deref().filter(|mode| *mode != "abs") {
            let reference = mode
                .strip_prefix("rel:")
                .ok_or(InvalidPayload("invalid addressing mode"))?;
            let images = &graph
                .debug_meta
                .as_ref()
                .ok_or(InvalidPayload("relative address has no debug images"))?
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
                return Err(InvalidPayload(
                    "relative address has no matching debug image",
                ));
            }
        }
    }
    for tree in &graph.trees {
        if tree.roots.is_empty() {
            return Err(InvalidPayload("empty roots"));
        }
        let mut nodes: Vec<_> = tree.roots.iter().collect();
        while let Some(node) = nodes.pop() {
            if node.frame_id >= graph.frames.len() {
                return Err(InvalidPayload("frame index out of bounds"));
            }
            if node.sample_count == 0 || node.sample_count > MAX_SAMPLE_COUNT {
                return Err(InvalidPayload("sample count out of bounds"));
            }
            let mut remaining = node.sample_count;
            for child in &node.children {
                remaining = remaining
                    .checked_sub(child.sample_count)
                    .ok_or(InvalidPayload("children exceed parent sample count"))?;
                nodes.push(child);
            }
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
pub fn identify_culprit(payload: &[u8]) -> Result<Vec<usize>, FlamegraphError> {
    if payload.len() > MAX_FLAMEGRAPH_SIZE {
        return Err(FlamegraphError::InvalidPayload("attachment too large"));
    }
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
        while let Some((node, parent)) = pending.pop() {
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

    fn identify(graph: serde_json::Value) -> Result<Vec<usize>, FlamegraphError> {
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
            "trees": [{"roots": [{"frame_id": 0, "sample_count": 100, "children": [
                {"frame_id": 1, "sample_count": 100, "children": [
                    {"frame_id": 2, "sample_count": 60},
                    {"frame_id": 3, "sample_count": 40}
                ]}
            ]}]}]
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
        graph["trees"][0]["roots"] = json!([
            {"frame_id": 0, "sample_count": 60, "children": [
                {"frame_id": 1, "sample_count": 60, "children": [
                    {"frame_id": 2, "sample_count": 60}
                ]}
            ]},
            {"frame_id": 4, "sample_count": 40, "children": [
                {"frame_id": 1, "sample_count": 40, "children": [
                    {"frame_id": 3, "sample_count": 40}
                ]}
            ]}
        ]);
        assert_eq!(identify(graph).unwrap(), vec![0, 1]);
    }

    #[test]
    fn test_culprit_application_weight_and_stable_ties() {
        let mut graph = culprit_graph();
        graph["trees"][0]["roots"] = json!([
            {"frame_id": 2, "sample_count": 10},
            {"frame_id": 1, "sample_count": 10},
            {"frame_id": 3, "sample_count": 10}
        ]);
        assert_eq!(identify(graph.clone()).unwrap(), vec![1]);
        graph["frames"][1]["in_app"] = json!(false);
        assert_eq!(identify(graph.clone()).unwrap(), vec![2]);
        graph["frames"][1].as_object_mut().unwrap().remove("in_app");
        assert_eq!(identify(graph).unwrap(), vec![2]);
    }

    #[test]
    fn test_culprit_inclusive_counts_and_recursion() {
        let mut graph = culprit_graph();
        graph["trees"][0]["roots"] = json!([
            {"frame_id": 1, "sample_count": 100, "children": [
                {"frame_id": 1, "sample_count": 60, "children": [
                    {"frame_id": 1, "sample_count": 30}
                ]}
            ]}
        ]);
        // Inclusive counts must not be counted again from descendant samples.
        assert_eq!(identify(graph).unwrap(), vec![1, 1]);
    }

    #[test]
    fn test_culprit_does_not_merge_independent_tree_populations() {
        let mut graph = culprit_graph();
        graph["trees"] = json!([
            {"roots": [{"frame_id": 0, "sample_count": 10}]},
            {"roots": [{"frame_id": 0, "sample_count": 10}]},
            {"roots": [{"frame_id": 1, "sample_count": 9}]}
        ]);
        assert_eq!(identify(graph.clone()).unwrap(), vec![1]);
        graph["trees"][0]["thread_attributed"] = json!(true);
        assert_eq!(identify(graph).unwrap(), vec![0]);
    }

    #[test]
    fn test_culprit_exact_large_sample_counts() {
        let mut graph = culprit_graph();
        graph["frames"][0]["in_app"] = json!(true);
        graph["trees"][0]["roots"] = json!([
            {"frame_id": 0, "sample_count": MAX_SAMPLE_COUNT - 1},
            {"frame_id": 1, "sample_count": MAX_SAMPLE_COUNT}
        ]);
        assert_eq!(identify(graph).unwrap(), vec![1]);
    }

    #[test]
    fn test_culprit_rejects_invalid_graph() {
        let mut graph = culprit_graph();
        graph["trees"][0]["roots"][0]["sample_count"] = json!(1);
        assert!(identify(graph).is_err());
        assert!(identify(json!({"version": "2"})).is_err());
    }

    #[test]
    fn test_flamegraph_deep_call_tree() {
        let depth = 1000;
        let payload = format!(
            r#"{{"version":"1","platform":"cocoa","frames":[{{"instruction_addr":"0x1000"}}],"trees":[{{"roots":[{}{{"frame_id":0,"sample_count":1}}{}]}}]}}"#,
            r#"{"frame_id":0,"sample_count":1,"children":["#.repeat(depth - 1),
            "]}".repeat(depth - 1),
        );
        validate(payload.as_bytes()).unwrap();
        let invalid = payload.replace(r#""sample_count":1}"#, r#""sample_count":0}"#);
        assert!(validate(invalid.as_bytes()).is_err());
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
                    "trees": [{"roots": [{"frame_id": 0, "sample_count": 1}]}]
                });
                let payload = serde_json::to_vec(&graph).unwrap();
                validate(&payload)
                    .unwrap_or_else(|error| panic!("{image_type} with {addr_mode}: {error}"));

                graph["debug_meta"]["images"][0]["debug_id"] =
                    json!("94a04d24-0e60-3810-a8c0-90a65e2df61a");
                let result = validate(&serde_json::to_vec(&graph).unwrap());
                if addr_mode.starts_with("rel:") && addr_mode != "rel:0" {
                    assert!(matches!(
                        result,
                        Err(FlamegraphError::InvalidPayload(
                            "relative address has no matching debug image"
                        ))
                    ));
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
            "trees": [{"roots": [{"frame_id": 9999, "sample_count": 1}]}]
        });
        let payload = serde_json::to_vec(&graph).unwrap();
        assert!(payload.len() < MAX_FLAMEGRAPH_SIZE);
        validate(&payload).unwrap();

        graph["debug_meta"]["images"].as_array_mut().unwrap().pop();
        assert!(matches!(
            validate(&serde_json::to_vec(&graph).unwrap()),
            Err(FlamegraphError::InvalidPayload(
                "relative address has no matching debug image"
            ))
        ));
    }
}
