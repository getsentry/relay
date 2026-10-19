//! Validation of version 1 [flame graph attachments] without rewriting captured data.
//!
//! [flame graph attachments]: https://develop.sentry.dev/sdk/telemetry/attachments/#flame-graph

use std::collections::HashSet;

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
struct Flamegraph {
    version: String,
    platform: String,
    frames: Vec<FlamegraphFrame>,
    nodes: Vec<Node>,
    debug_meta: Option<DebugMeta>,
}

#[derive(Deserialize)]
struct FlamegraphFrame {
    #[serde(flatten)]
    frame: Frame,
    addr_mode: Option<String>,
}

#[derive(Deserialize)]
struct Node {
    frame_id: usize,
    sample_count: u64,
    #[serde(default)]
    children: Vec<usize>,
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

    let graph: Flamegraph = serde_json::from_slice(payload)?;

    if graph.version != "1" {
        return Err(InvalidPayload("unsupported version"));
    }
    if !VALID_PLATFORMS.contains(&graph.platform.as_str()) {
        return Err(InvalidPayload("unknown platform"));
    }
    if graph.frames.is_empty() || graph.nodes.is_empty() {
        return Err(InvalidPayload("empty frames or nodes"));
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

    for node in &graph.nodes {
        if node.frame_id >= graph.frames.len() {
            return Err(InvalidPayload("frame index out of bounds"));
        }
        if node.sample_count == 0 || node.sample_count > MAX_SAMPLE_COUNT {
            return Err(InvalidPayload("sample count out of bounds"));
        }

        let mut remaining_sample_count = node.sample_count;
        for child in &node.children {
            let Some(child) = graph.nodes.get(*child) else {
                return Err(InvalidPayload("child index out of bounds"));
            };
            remaining_sample_count = remaining_sample_count
                .checked_sub(child.sample_count)
                .ok_or(InvalidPayload("children exceed parent sample count"))?;
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
