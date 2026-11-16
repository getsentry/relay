//! Validation and representative call-path selection for version 1 [flame graph attachments].
//!
//! [flame graph attachments]: https://develop.sentry.dev/sdk/telemetry/attachments/#flame-graph

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;

use bytes::Bytes;
use relay_event_schema::protocol::{DebugId, RawStacktrace, VALID_PLATFORMS};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

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
    #[error("prefix index out of bounds: {0}")]
    PrefixIndex(usize),
    #[error("sample count out of bounds: {0}")]
    SampleCount(u64),
    #[error("children exceed parent sample count")]
    ChildSampleCount,
    #[error("cyclic call path")]
    CyclicCallPath,
    #[error("call-path traversal limit exceeded")]
    TraversalLimit,
}

/// A validated flame graph with borrowed source data for selected frames and debug images.
///
/// Construct graphs through [`parse`] so validation cannot be bypassed by deserialization.
///
/// ```compile_fail
/// use relay_profiling::flamegraph::Graph;
///
/// let graph: Graph<'_> = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Serialize)]
#[serde(transparent)]
pub struct Graph<'a>(RawGraph<'a>);

#[derive(Deserialize, Serialize)]
struct RawGraph<'a> {
    version: String,
    platform: String,
    #[serde(borrow)]
    frames: Vec<Source<'a, Frame>>,
    nodes: Vec<Node>,
    trees: Vec<Tree>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(borrow)]
    debug_meta: Option<Source<'a, sample::DebugMeta>>,
}

/// Retain source fields for event enrichment while validating the profiling schema.
struct Source<'a, T> {
    raw: &'a RawValue,
    parsed: T,
}

impl<'de: 'a, 'a, T: Deserialize<'de>> Deserialize<'de> for Source<'a, T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <&'de RawValue>::deserialize(deserializer)?;
        let parsed = serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
        Ok(Self { raw, parsed })
    }
}

impl<T: Serialize> Serialize for Source<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.parsed.serialize(serializer)
    }
}

impl<T> std::ops::Deref for Source<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.parsed
    }
}

impl Graph<'_> {
    /// Original debug metadata, including event-specific image fields.
    pub fn debug_meta(&self) -> Option<&RawValue> {
        self.0.debug_meta.as_ref().map(|meta| meta.raw)
    }
}

impl RawGraph<'_> {
    /// Count nodes, child edges, and roots to bound queued references, including shared
    /// nodes under distinct prefixes, without allowing exponential path expansion.
    fn graph_cardinality(&self) -> usize {
        self.nodes.len()
            + self
                .nodes
                .iter()
                .map(|node| node.children.len())
                .sum::<usize>()
            + self
                .trees
                .iter()
                .map(|tree| tree.roots.len())
                .sum::<usize>()
    }

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

/// Parse and validate an attachment for reuse during normalization and enrichment.
pub fn parse(payload: &[u8]) -> Result<Graph<'_>, Error> {
    if payload.len() > MAX_FLAMEGRAPH_SIZE {
        return Err(Error::AttachmentTooLarge);
    }
    let graph: RawGraph = serde_json::from_slice(payload)?;

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
    Ok(Graph(graph))
}

/// A representative caller-to-callee path, borrowing frame JSON from the attachment.
///
/// Retention follows the frame limits on [`RawStacktrace`], matching event stacktrace trimming.
///
/// Like the `AnrCulpritIdentifier` in the Android SDK, this scores identical root-to-frame prefixes by
/// `sample_count * (depth + application_frames)`. Prefixes use the producer's frame-table indices
/// as frame identity; entries at different indices remain distinct even if their values match.
/// Node counts are inclusive, so they already include the observations ending in descendants.
/// Explicit `in_app: true` supplies the app boost.
///
/// Sample populations from different trees are never added together. Prefer trees attributed to
/// the diagnostic when present; otherwise choose the strongest path across all trees. Ties retain
/// the first path in input order. The attachment itself remains unchanged. Graphs whose expanded
/// paths exceed a traversal budget linear in nodes, child edges, and roots are rejected.
pub struct Culprit<'payload> {
    /// Retained source frames in caller-to-callee order.
    pub frames: Vec<&'payload RawValue>,
    /// Number of frames before middle-frame trimming.
    pub original_len: usize,
    /// Whether every sample at the selected endpoint continues into an omitted callee.
    /// An internal node can also have samples that end there; those endpoints return false.
    pub ends_at_caller: bool,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct PrefixId(NonZeroUsize);

impl PrefixId {
    fn from_index(index: usize) -> Option<Self> {
        let value = index.checked_add(1)?;
        NonZeroUsize::new(value).map(Self)
    }

    fn index(self) -> usize {
        self.0.get() - 1
    }
}

struct Prefix {
    parent: Option<PrefixId>,
    frame_id: usize,
    depth: usize,
    app_frames: usize,
    samples: u128,
}

#[derive(Default)]
struct Workspace<'graph> {
    prefixes: HashMap<(Option<PrefixId>, usize), PrefixId>,
    candidates: Vec<Prefix>,
    pending: Vec<(&'graph usize, Option<PrefixId>)>,
    visited: HashSet<(usize, Option<PrefixId>)>,
}

impl Workspace<'_> {
    fn clear(&mut self) {
        let Self {
            prefixes,
            candidates,
            pending,
            visited,
        } = self;
        prefixes.clear();
        candidates.clear();
        pending.clear();
        visited.clear();
    }
}

fn get<T>(values: &[T], index: usize) -> Result<&T, Error> {
    values.get(index).ok_or(Error::PrefixIndex(index))
}

impl<'payload> Graph<'payload> {
    /// Select the bounded culprit path from this validated graph without reparsing it.
    pub fn identify_culprit(&self) -> Result<Culprit<'payload>, Error> {
        let graph = &self.0;

        let has_attributed_tree = graph
            .trees
            .iter()
            .any(|tree| tree.thread_attributed == Some(true));
        let mut remaining_visits = graph.graph_cardinality();
        let mut best_score = 0;
        let mut best_path = Culprit {
            frames: Vec::new(),
            original_len: 0,
            ends_at_caller: false,
        };
        let mut workspace = Workspace::default();
        for tree in &graph.trees {
            if has_attributed_tree && tree.thread_attributed != Some(true) {
                continue;
            }
            remaining_visits = remaining_visits
                .checked_sub(tree.roots.len())
                .ok_or(Error::TraversalLimit)?;
            workspace.clear();
            let Workspace {
                prefixes,
                candidates,
                pending,
                visited,
            } = &mut workspace;
            pending.extend(tree.roots.iter().rev().map(|node| (node, None)));
            while let Some((node_index, parent)) = pending.pop() {
                // A node's inclusive count belongs to this prefix only once, even when
                // repeated edges or equivalent parents lead to the same node.
                if !visited.insert((*node_index, parent)) {
                    continue;
                }
                let node = graph.node(*node_index)?;
                let frame_id = node.frame_id;
                let prefix = match prefixes.entry((parent, frame_id)) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        let (depth, app_frames) = parent
                            .map(|parent: PrefixId| {
                                let prefix = get(candidates, parent.index())?;
                                Ok::<_, Error>((prefix.depth, prefix.app_frames))
                            })
                            .transpose()?
                            .unwrap_or_default();
                        let index =
                            PrefixId::from_index(candidates.len()).ok_or(Error::TraversalLimit)?;
                        candidates.push(Prefix {
                            parent,
                            frame_id,
                            depth: depth + 1,
                            app_frames: app_frames
                                + usize::from(
                                    get(&graph.frames, frame_id)?.frame.in_app == Some(true),
                                ),
                            samples: 0,
                        });
                        *entry.insert(index)
                    }
                };
                let candidate = candidates
                    .get_mut(prefix.index())
                    .ok_or(Error::PrefixIndex(prefix.index()))?;
                if candidate.depth > graph.nodes.len() {
                    return Err(Error::CyclicCallPath);
                }
                candidate.samples += u128::from(node.sample_count);
                remaining_visits = remaining_visits
                    .checked_sub(node.children.len())
                    .ok_or(Error::TraversalLimit)?;
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
            let mut child_samples = 0;
            for (index, prefix) in candidates.iter().enumerate() {
                let score = prefix.samples * (prefix.depth + prefix.app_frames) as u128;
                if score > best_score {
                    best_score = score;
                    best_prefix = Some(index);
                    // Parents precede their children, so all children of the new winner
                    // are still ahead of us. Sample totals are already fully aggregated.
                    child_samples = 0;
                }
                if prefix
                    .parent
                    .is_some_and(|parent| Some(parent.index()) == best_prefix)
                {
                    child_samples += prefix.samples;
                }
            }
            if let Some(index) = best_prefix {
                let winner = PrefixId::from_index(index).ok_or(Error::TraversalLimit)?;
                // A winner with 100 samples and children with 60 + 40 samples is exclusively
                // a caller. With 60 + 20 child samples, 20 samples end at the winner itself.
                let candidate = get(candidates, index)?;
                let ends_at_caller = child_samples == candidate.samples;
                let depth = candidate.depth;
                let mut path = Vec::with_capacity(depth.min(RawStacktrace::MAX_FRAMES));
                let mut current = Some(winner);
                while let Some(index) = current {
                    let prefix = get(candidates, index.index())?;
                    if prefix.depth <= RawStacktrace::MAX_OLDEST_FRAMES
                        || prefix.depth > depth.saturating_sub(RawStacktrace::MAX_NEWEST_FRAMES)
                    {
                        path.push(get(&graph.frames, prefix.frame_id)?.raw);
                    }
                    current = prefix.parent;
                }
                path.reverse();
                best_path = Culprit {
                    frames: path,
                    original_len: depth,
                    ends_at_caller,
                };
            }
        }
        Ok(best_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn identify(graph: serde_json::Value) -> Result<Vec<usize>, Error> {
        let payload = serde_json::to_vec(&graph).unwrap();
        let graph = parse(&payload)?;
        Ok(graph
            .identify_culprit()?
            .frames
            .into_iter()
            .map(|raw| {
                graph
                    .0
                    .frames
                    .iter()
                    .position(|frame| std::ptr::eq(frame.raw, raw))
                    .unwrap()
            })
            .collect())
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
    fn test_culprit_borrows_payload_independently_of_graph() {
        fn select(payload: &[u8]) -> Vec<&RawValue> {
            parse(payload).unwrap().identify_culprit().unwrap().frames
        }

        let payload = serde_json::to_vec(&culprit_graph()).unwrap();
        let frames = select(&payload);
        assert_eq!(frames.len(), 2);
        for frame in frames {
            assert!(payload.as_ptr_range().contains(&frame.get().as_ptr()));
        }
    }

    #[test]
    fn test_prefix_id_compact_optional_handle() {
        assert_eq!(
            std::mem::size_of::<Option<PrefixId>>(),
            std::mem::size_of::<usize>()
        );
        for index in [0, 1, 50_000] {
            assert_eq!(PrefixId::from_index(index).unwrap().index(), index);
        }
    }

    #[test]
    fn test_prefix_id_rejects_overflow_without_panicking() {
        assert!(PrefixId::from_index(usize::MAX).is_none());
    }

    #[test]
    fn test_culprit_shared_application_prefix() {
        // Shared caller: 100 * (2 + 1) = 300, versus 60 * (3 + 1) = 240.
        assert_eq!(identify(culprit_graph()).unwrap(), vec![0, 1]);
    }

    #[test]
    fn test_culprit_aggregates_prefixes_with_shared_frame_ids() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 60, "children": [1]},
            {"frame_id": 1, "sample_count": 60, "children": [2]},
            {"frame_id": 2, "sample_count": 60},
            {"frame_id": 0, "sample_count": 40, "children": [4]},
            {"frame_id": 1, "sample_count": 40, "children": [5]},
            {"frame_id": 3, "sample_count": 40}
        ]);
        graph["trees"][0]["roots"] = json!([0, 3]);
        let payload = serde_json::to_vec(&graph).unwrap();
        let culprit = parse(&payload).unwrap().identify_culprit().unwrap();
        assert!(culprit.ends_at_caller);
        assert_eq!(identify(graph).unwrap(), vec![0, 1]);
    }

    #[test]
    fn test_culprit_caller_status_after_winner_changes_and_ties() {
        for child_samples in [40, 50] {
            for roots in [[0, 3, 6], [0, 6, 3]] {
                for separate_trees in [false, true] {
                    let mut graph = culprit_graph();
                    graph["frames"].as_array_mut().unwrap().push(json!({
                        "instruction_addr": "0x5000", "in_app": true
                    }));
                    graph["nodes"] = json!([
                        {"frame_id": 0, "sample_count": 50, "children": [1, 2]},
                        {"frame_id": 2, "sample_count": 25},
                        {"frame_id": 3, "sample_count": 25},
                        {"frame_id": 1, "sample_count": 100, "children": [4, 5]},
                        {"frame_id": 2, "sample_count": child_samples},
                        {"frame_id": 3, "sample_count": child_samples},
                        {"frame_id": 4, "sample_count": 100}
                    ]);
                    graph["trees"] = if separate_trees {
                        roots.iter().map(|root| json!({"roots": [root]})).collect()
                    } else {
                        json!([{"roots": roots}])
                    };
                    // Root 0 initially wins and has child samples. Roots 3 and 6 then
                    // tie at 200, so the first one wins, with its own caller status.
                    let expected_frame = if roots[1] == 3 { 1 } else { 4 };
                    let payload = serde_json::to_vec(&graph).unwrap();
                    let graph = parse(&payload).unwrap();
                    let culprit = graph.identify_culprit().unwrap();
                    assert_eq!(culprit.frames.len(), 1);
                    assert_eq!(
                        culprit.frames[0].get(),
                        graph.0.frames[expected_frame].raw.get()
                    );
                    assert_eq!(
                        culprit.ends_at_caller,
                        roots[1] == 3 && child_samples == 50,
                        "roots: {roots:?}, child samples: {child_samples}, separate trees: {separate_trees}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_culprit_preserves_distinct_root_frame_ids() {
        let mut graph = culprit_graph();
        let duplicate = graph["frames"][0].clone();
        graph["frames"].as_array_mut().unwrap().push(duplicate);
        graph["trees"][0]["roots"] = json!([0, 3]);
        for (first_samples, second_samples, expected) in
            [(60, 40, vec![0, 1, 2]), (40, 60, vec![4, 1, 3])]
        {
            graph["nodes"] = json!([
                {"frame_id": 0, "sample_count": first_samples, "children": [1]},
                {"frame_id": 1, "sample_count": first_samples, "children": [2]},
                {"frame_id": 2, "sample_count": first_samples},
                {"frame_id": 4, "sample_count": second_samples, "children": [4]},
                {"frame_id": 1, "sample_count": second_samples, "children": [5]},
                {"frame_id": 3, "sample_count": second_samples}
            ]);
            // Distinct root IDs keep the paths separate, despite identical frame values.
            // The stronger full path scores 60 * (3 + 1) = 240, not a merged caller's 300.
            let payload = serde_json::to_vec(&graph).unwrap();
            let culprit = parse(&payload).unwrap().identify_culprit().unwrap();
            assert!(!culprit.ends_at_caller);
            assert_eq!(identify(graph.clone()).unwrap(), expected);
        }
    }

    #[test]
    fn test_culprit_aggregates_children_by_frame_id() {
        let mut graph = culprit_graph();
        let duplicate = graph["frames"][1].clone();
        graph["frames"].as_array_mut().unwrap().push(duplicate);
        graph["frames"][2]["in_app"] = json!(true);
        for (second_frame_id, expected) in [(1, vec![0, 1]), (4, vec![0, 2])] {
            graph["nodes"] = json!([
                {"frame_id": 0, "sample_count": 180, "children": [1, 2, 3]},
                {"frame_id": 1, "sample_count": 60},
                {"frame_id": second_frame_id, "sample_count": 40},
                {"frame_id": 2, "sample_count": 80}
            ]);
            // Shared IDs score (60 + 40) * 3 = 300. Distinct IDs score at most 60 * 3
            // separately, so the competing child's 80 * 3 = 240 wins instead.
            assert_eq!(identify(graph.clone()).unwrap(), expected);
        }
    }

    #[test]
    fn test_culprit_counts_repeated_edges_once() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 100, "children": [1, 1]},
            {"frame_id": 2, "sample_count": 40}
        ]);
        // The root scores 100, while its child scores 40 * 2 = 80, not 160.
        assert_eq!(identify(graph).unwrap(), vec![0]);
    }

    #[test]
    fn test_culprit_counts_shared_nodes_once_per_prefix() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 50, "children": [2]},
            {"frame_id": 0, "sample_count": 50, "children": [2]},
            {"frame_id": 2, "sample_count": 40, "children": [3]},
            {"frame_id": 3, "sample_count": 40},
            {"frame_id": 1, "sample_count": 70}
        ]);
        graph["trees"][0]["roots"] = json!([0, 1, 4]);
        // The app root scores 140, versus 100 for the merged roots and 120 for their path.
        assert_eq!(identify(graph).unwrap(), vec![1]);
    }

    #[test]
    fn test_culprit_counts_repeated_roots_once() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 10},
            {"frame_id": 2, "sample_count": 15}
        ]);
        graph["trees"][0]["roots"] = json!([0, 0, 1]);
        assert_eq!(identify(graph).unwrap(), vec![2]);
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
    fn test_culprit_rebuilds_prefixes_between_branching_trees() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 100, "children": [1, 3]},
            {"frame_id": 2, "sample_count": 60, "children": [2]},
            {"frame_id": 3, "sample_count": 60},
            {"frame_id": 1, "sample_count": 40},
            {"frame_id": 0, "sample_count": 200, "children": [5, 6]},
            {"frame_id": 1, "sample_count": 110},
            {"frame_id": 2, "sample_count": 90},
            {"frame_id": 3, "sample_count": 200, "children": [8, 10]},
            {"frame_id": 1, "sample_count": 100, "children": [9]},
            {"frame_id": 1, "sample_count": 100},
            {"frame_id": 2, "sample_count": 100}
        ]);
        let roots = [0, 4, 7];
        // The best paths score 180, 330, and 500 respectively.
        let expected = [vec![0, 2, 3], vec![0, 1], vec![3, 1, 1]];
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            for count in 1..=order.len() {
                let trees = &order[..count];
                graph["trees"] = trees
                    .iter()
                    .map(|&index| json!({"roots": [roots[index]]}))
                    .collect();
                let winner = *trees.iter().max().unwrap();
                assert_eq!(
                    identify(graph.clone()).unwrap(),
                    expected[winner],
                    "tree order: {trees:?}"
                );
            }
        }
    }

    #[test]
    fn test_culprit_revisits_shared_subtree_in_independent_trees() {
        let mut graph = culprit_graph();
        graph["nodes"] = json!([
            {"frame_id": 0, "sample_count": 100, "children": [2, 4]},
            {"frame_id": 1, "sample_count": 100, "children": [2, 4]},
            {"frame_id": 2, "sample_count": 60, "children": [3]},
            {"frame_id": 3, "sample_count": 60},
            {"frame_id": 0, "sample_count": 40}
        ]);
        for roots in [[0, 1], [1, 0]] {
            graph["trees"] = roots
                .into_iter()
                .map(|root| json!({"roots": [root]}))
                .collect();
            // The shared subtree scores 180 under root 0, but 240 under the in-app root 1.
            assert_eq!(identify(graph.clone()).unwrap(), vec![1, 2, 3]);
        }
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
    fn test_culprit_bounds_repeated_child_edges() {
        let depth = 12;
        let mut graph = culprit_graph();
        graph["nodes"] = (0..depth)
            .map(|index| {
                json!({
                    "frame_id": 0,
                    "sample_count": 1_u64 << (depth - index - 1),
                    "children": if index + 1 < depth {
                        vec![index + 1, index + 1]
                    } else {
                        vec![]
                    }
                })
            })
            .collect();
        validate(&serde_json::to_vec(&graph).unwrap()).unwrap();
        assert_eq!(identify(graph).unwrap(), vec![0]);
    }

    #[test]
    fn test_culprit_bounds_distinct_shared_paths() {
        let depth = 12;
        let mut graph = culprit_graph();
        graph["nodes"] = (0..depth * 2)
            .map(|index| {
                let level = index / 2;
                json!({
                    "frame_id": if index % 2 == 0 { 0 } else { 2 },
                    "sample_count": 1_u64 << (depth - level - 1),
                    "children": if level + 1 < depth {
                        vec![(level + 1) * 2, (level + 1) * 2 + 1]
                    } else {
                        vec![]
                    }
                })
            })
            .collect();
        graph["trees"][0]["roots"] = json!([0, 1]);
        validate(&serde_json::to_vec(&graph).unwrap()).unwrap();
        assert!(matches!(identify(graph), Err(Error::TraversalLimit)));
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
    fn test_culprit_retention_boundary() {
        for depth in [
            RawStacktrace::MAX_FRAMES - 1,
            RawStacktrace::MAX_FRAMES,
            RawStacktrace::MAX_FRAMES + 1,
        ] {
            let mut graph = culprit_graph();
            graph["nodes"] = (0..depth)
                .map(|index| {
                    json!({
                        "frame_id": 1,
                        "sample_count": 1,
                        "children": if index + 1 < depth { vec![index + 1] } else { vec![] }
                    })
                })
                .collect();
            let payload = serde_json::to_vec(&graph).unwrap();
            let culprit = parse(&payload).unwrap().identify_culprit().unwrap();
            assert_eq!(culprit.frames.len(), depth.min(RawStacktrace::MAX_FRAMES));
            assert_eq!(culprit.original_len, depth);
        }
    }

    #[test]
    fn test_flamegraph_deep_call_tree() {
        let depth = 1000;
        let nodes: Vec<_> = (0..depth)
            .map(|index| {
                json!({
                    "frame_id": index,
                    "sample_count": 1,
                    "children": if index + 1 < depth { vec![index + 1] } else { vec![] }
                })
            })
            .collect();
        let mut graph = json!({
            "version": "1", "platform": "cocoa",
            "frames": (0..depth).map(|index| json!({
                "instruction_addr": format!("0x{:x}", 0x1000 + index)
            })).collect::<Vec<_>>(),
            "nodes": nodes,
            "trees": [{"roots": [0]}]
        });
        validate(&serde_json::to_vec(&graph).unwrap()).unwrap();
        assert_eq!(
            identify(graph.clone()).unwrap(),
            (0..50).chain(800..1000).collect::<Vec<_>>()
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
