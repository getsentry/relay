//! Enrichment of hang exceptions with a representative path from a flame graph attachment.

use std::collections::{BTreeMap, HashMap};

use relay_event_schema::protocol::{
    Addr, DebugId, DebugImage, DebugMeta, Event, Frame, NativeDebugImage, RawStacktrace, Stacktrace,
};
use relay_protocol::{Annotated, Array, DeserializableAnnotated};
use serde::Deserialize;

use crate::envelope::{AttachmentType, Item};

#[derive(Deserialize)]
struct FlamegraphContext<'a> {
    #[serde(borrow)]
    frames: Vec<&'a serde_json::value::RawValue>,
    debug_meta: Option<DeserializableAnnotated<DebugMeta>>,
}

/// Fill a missing hang exception stack without changing the attachment.
///
/// Existing stacks remain authoritative, including when a processing Relay receives an event
/// already enriched by another Relay. Only MetricKit hang diagnostics opt into this enrichment.
pub fn enrich(event: &mut Event, attachments: &[Item], max_event_size: usize) -> bool {
    let Some(exception) = event
        .exceptions
        .value_mut()
        .as_mut()
        .and_then(|exceptions| exceptions.values.value_mut().as_mut())
        .into_iter()
        .flatten()
        .filter_map(|value| value.value_mut().as_mut())
        .find(|exception| {
            exception.stacktrace.value().is_none()
                && exception.raw_stacktrace.value().is_none()
                && exception
                    .mechanism
                    .value()
                    .is_some_and(|mechanism| mechanism.ty.as_str() == Some("mx_hang_diagnostic"))
        })
    else {
        return false;
    };

    let Some((mut frames, debug_meta)) = attachments
        .iter()
        .find_map(|item| extract(item, max_event_size))
    else {
        return false;
    };
    if let Some(debug_meta) = debug_meta {
        merge_debug_images(&mut event.debug_meta, debug_meta, &mut frames);
    }
    exception.stacktrace = Annotated::new(Stacktrace::from(RawStacktrace {
        frames: Annotated::new(frames),
        ..Default::default()
    }));
    true
}

fn extract(item: &Item, max_event_size: usize) -> Option<(Array<Frame>, Option<DebugMeta>)> {
    if item.attachment_type() != Some(AttachmentType::Flamegraph) {
        return None;
    }
    let payload = item.payload();
    let indices = relay_profiling::flamegraph::identify_culprit(&payload).ok()?;
    let context: FlamegraphContext = serde_json::from_slice(&payload).ok()?;
    // Recursion can reference one large frame many times. Bound the derived event before
    // materializing frames, rather than amplifying a small attachment ahead of normalization.
    indices
        .iter()
        .try_fold(max_event_size, |remaining, index| {
            remaining.checked_sub(context.frames[*index].get().len())
        })?;
    let frames = indices
        .into_iter()
        .map(|index| {
            serde_json::from_str::<DeserializableAnnotated<Frame>>(context.frames[index].get())
                .map(|frame| frame.0)
        })
        .collect::<Result<Array<Frame>, _>>()
        .ok()?;
    Some((
        frames,
        context.debug_meta.and_then(|meta| meta.0.into_value()),
    ))
}

fn merge_debug_images(
    target: &mut Annotated<DebugMeta>,
    source: DebugMeta,
    frames: &mut Array<Frame>,
) {
    let source_images = source.images.into_value().unwrap_or_default();
    let images = target
        .get_or_insert_with(DebugMeta::default)
        .images
        .get_or_insert_with(Vec::new);
    let mut image_indices = HashMap::new();
    let mut images_by_address = BTreeMap::new();
    let mut images_by_debug_id = HashMap::new();
    for (index, image) in source_images.iter().enumerate() {
        let Some(image) = native_image(image) else {
            continue;
        };
        if let Some(address) = image.image_addr.value() {
            images_by_address.insert(address.0, (index, image));
        }
        if let Some(debug_id) = image.debug_id.value() {
            images_by_debug_id
                .entry(debug_id.to_string())
                .or_insert(index);
        }
    }

    for frame in frames
        .iter_mut()
        .filter_map(|frame| frame.value_mut().as_mut())
    {
        let Some((source_index, address)) =
            referenced_image(frame, &images_by_address, &images_by_debug_id)
        else {
            continue;
        };
        let index = *image_indices.entry(source_index).or_insert_with(|| {
            let index = images.len();
            images.push(source_images[source_index].clone());
            index
        });
        // Keep only images needed by the selected path. Explicit relative references also avoid
        // resolving against current-run images at the same addresses as an earlier diagnostic.
        frame.instruction_addr = Annotated::new(Addr(address));
        frame.addr_mode = Annotated::new(format!("rel:{index}"));
    }
}

fn referenced_image(
    frame: &Frame,
    images_by_address: &BTreeMap<u64, (usize, &NativeDebugImage)>,
    images_by_debug_id: &HashMap<String, usize>,
) -> Option<(usize, u64)> {
    let address = frame.instruction_addr.value()?.0;
    if let Some(reference) = frame
        .addr_mode
        .as_str()
        .and_then(|mode| mode.strip_prefix("rel:"))
    {
        let index = if let Ok(index) = reference.parse::<usize>() {
            index
        } else {
            let debug_id: DebugId = reference.parse().ok()?;
            *images_by_debug_id.get(&debug_id.to_string())?
        };
        return Some((index, address));
    }
    // Like Symbolicator, missing or zero image sizes extend to the next loaded image.
    let (start, (index, image)) = images_by_address.range(..=address).next_back()?;
    let relative = address - start;
    image
        .image_size
        .value()
        .is_none_or(|size| *size == 0 || relative < *size)
        .then_some((*index, relative))
}

fn native_image(image: &Annotated<DebugImage>) -> Option<&NativeDebugImage> {
    match image.value()? {
        DebugImage::Symbolic(image)
        | DebugImage::MachO(image)
        | DebugImage::Elf(image)
        | DebugImage::Pe(image)
        | DebugImage::PeDotnet(image)
        | DebugImage::Wasm(image) => Some(image),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::{Annotated, get_value};
    use serde_json::{Value, json};

    use super::*;
    use crate::envelope::{AttachmentType, ContentType, ItemType};

    fn enrich(event: &mut Event, attachments: &[Item]) -> bool {
        super::enrich(
            event,
            attachments,
            relay_config::Config::default().current().max_event_size(),
        )
    }

    fn event() -> Annotated<Event> {
        Annotated::from_json(
            r#"{"platform":"cocoa","exception":{"values":[{
                "type":"MXHangDiagnostic","value":"hangDuration:2s",
                "mechanism":{"type":"mx_hang_diagnostic","handled":true,"synthetic":true}
            }]}}"#,
        )
        .unwrap()
    }

    fn graph() -> Value {
        json!({
            "version": "1", "platform": "cocoa",
            "frames": [
                {"instruction_addr": "0x1010", "in_app": false},
                {"instruction_addr": "0x1020", "in_app": true, "addr_mode": "abs",
                 "package": "SampleApp", "function": "work", "filename": "App.swift", "lineno": 42},
                {"instruction_addr": "0x1030", "in_app": false},
                {"instruction_addr": "0x1040", "in_app": false}
            ],
            "debug_meta": {"images": [{
                "type": "macho", "debug_id": "84a04d24-0e60-3810-a8c0-90a65e2df61a",
                "code_file": "SampleApp", "image_addr": "0x1000", "image_size": 4096,
                "image_vmaddr": "0x100000000", "arch": "arm64", "code_id": "abcdef"
            }]},
            "nodes": [
                {"frame_id": 0, "sample_count": 100, "children": [1]},
                {"frame_id": 1, "sample_count": 100, "children": [2, 3]},
                {"frame_id": 2, "sample_count": 60},
                {"frame_id": 3, "sample_count": 40}
            ],
            "trees": [{"roots": [0]}]
        })
    }

    fn attachment(graph: Value) -> Item {
        let mut item = Item::new(ItemType::Attachment);
        item.set_attachment_type(AttachmentType::Flamegraph);
        item.set_filename("flamegraph.json");
        item.set_payload(ContentType::Json, serde_json::to_vec(&graph).unwrap());
        item
    }

    #[test]
    fn test_flamegraph_enriches_hang_without_rewriting_attachment() {
        let mut event = event();
        let attachments = [attachment(graph())];
        let original = attachments[0].payload();

        assert!(enrich(event.value_mut().as_mut().unwrap(), &attachments));

        let frames = get_value!(event.exceptions.values[0].stacktrace.frames!);
        assert_eq!(frames.len(), 2);
        assert_eq!(
            frames[0]
                .value()
                .unwrap()
                .instruction_addr
                .value()
                .unwrap()
                .0,
            0x10
        );
        let app = frames[1].value().unwrap();
        assert_eq!(app.function.as_str(), Some("work"));
        assert_eq!(app.lineno.value(), Some(&42));
        assert_eq!(app.addr_mode.as_str(), Some("rel:0"));
        assert_eq!(app.instruction_addr.value(), Some(&Addr(0x20)));
        assert_eq!(app.in_app.value(), Some(&true));
        let serialized: Value = serde_json::from_str(&event.to_json().unwrap()).unwrap();
        assert_eq!(
            serialized["debug_meta"]["images"][0],
            graph()["debug_meta"]["images"][0]
        );
        assert_eq!(
            serialized["exception"]["values"][0]["value"],
            "hangDuration:2s"
        );
        assert_eq!(
            serialized["exception"]["values"][0]["mechanism"]["synthetic"],
            true
        );
        assert_eq!(attachments[0].payload(), original);
    }

    #[test]
    fn test_flamegraph_preserves_existing_stack() {
        let mut event = event();
        let attachments = [attachment(graph())];
        assert!(enrich(event.value_mut().as_mut().unwrap(), &attachments));
        let original = event.clone();
        assert!(!enrich(event.value_mut().as_mut().unwrap(), &attachments));
        assert_eq!(event, original);
    }

    #[test]
    fn test_flamegraph_keeps_event_images_and_remaps_relative_addresses() {
        let mut value: Value = serde_json::from_str(&event().to_json().unwrap()).unwrap();
        value["debug_meta"] = json!({"images": [{
            "type": "macho", "debug_id": "94a04d24-0e60-3810-a8c0-90a65e2df61a",
            "code_file": "CurrentRun", "image_addr": "0x1000", "image_size": 4096
        }]});
        let mut event = Annotated::<Event>::from_json(&value.to_string()).unwrap();
        let mut payload = graph();
        payload["frames"][1]["addr_mode"] = json!("rel:0");
        payload["frames"][1]["instruction_addr"] = json!("0x20");

        assert!(enrich(
            event.value_mut().as_mut().unwrap(),
            &[attachment(payload)]
        ));

        let serialized: Value = serde_json::from_str(&event.to_json().unwrap()).unwrap();
        assert_eq!(
            serialized["debug_meta"]["images"][0],
            value["debug_meta"]["images"][0]
        );
        assert_eq!(
            serialized["debug_meta"]["images"][1],
            graph()["debug_meta"]["images"][0]
        );
        let frames = &serialized["exception"]["values"][0]["stacktrace"]["frames"];
        // The absolute frame must resolve against the diagnostic's image, not the current run's.
        assert_eq!(frames[0]["addr_mode"], "rel:1");
        assert_eq!(frames[0]["instruction_addr"], "0x10");
        assert_eq!(frames[1]["addr_mode"], "rel:1");
        assert_eq!(frames[1]["instruction_addr"], "0x20");
    }

    #[test]
    fn test_flamegraph_bounds_repeated_frame_data() {
        let mut payload = graph();
        payload["frames"][1]["filename"] = json!("x".repeat(400_000));
        payload["nodes"] = json!([
            {"frame_id": 1, "sample_count": 1, "children": [1]},
            {"frame_id": 1, "sample_count": 1, "children": [2]},
            {"frame_id": 1, "sample_count": 1}
        ]);
        let mut event = event();
        let original = event.clone();
        assert!(!enrich(
            event.value_mut().as_mut().unwrap(),
            &[attachment(payload)]
        ));
        assert_eq!(event, original);
    }

    #[test]
    fn test_flamegraph_images_without_sizes() {
        let mut payload = graph();
        payload["debug_meta"]["images"][0]
            .as_object_mut()
            .unwrap()
            .remove("image_size");
        // Image order is not guaranteed. Missing sizes extend to the next loaded image.
        let mut later = payload["debug_meta"]["images"][0].clone();
        later["image_addr"] = json!("0x2000");
        later["debug_id"] = json!("94a04d24-0e60-3810-a8c0-90a65e2df61a");
        payload["debug_meta"]["images"]
            .as_array_mut()
            .unwrap()
            .insert(0, later);
        let mut event = event();

        assert!(enrich(
            event.value_mut().as_mut().unwrap(),
            &[attachment(payload)]
        ));
        let serialized: Value = serde_json::from_str(&event.to_json().unwrap()).unwrap();
        assert_eq!(
            serialized["debug_meta"]["images"].as_array().unwrap().len(),
            1
        );
        let frames = &serialized["exception"]["values"][0]["stacktrace"]["frames"];
        assert_eq!(frames[0]["addr_mode"], "rel:0");
        assert_eq!(frames[0]["instruction_addr"], "0x10");
        assert_eq!(frames[1]["addr_mode"], "rel:0");
        assert_eq!(frames[1]["instruction_addr"], "0x20");
    }

    #[test]
    fn test_flamegraph_copies_only_images_used_by_selected_path() {
        let mut payload = graph();
        let mut unused = payload["debug_meta"]["images"][0].clone();
        unused["code_file"] = json!("UnusedFramework");
        unused["image_addr"] = json!("0x5000");
        unused["debug_id"] = json!("94a04d24-0e60-3810-a8c0-90a65e2df61a");
        payload["debug_meta"]["images"]
            .as_array_mut()
            .unwrap()
            .insert(0, unused);
        payload["frames"][1]["addr_mode"] = json!("rel:1");
        payload["frames"][1]["instruction_addr"] = json!("0x20");
        let mut event = event();

        assert!(enrich(
            event.value_mut().as_mut().unwrap(),
            &[attachment(payload)]
        ));

        let serialized: Value = serde_json::from_str(&event.to_json().unwrap()).unwrap();
        assert_eq!(
            serialized["debug_meta"]["images"],
            graph()["debug_meta"]["images"]
        );
        let frames = &serialized["exception"]["values"][0]["stacktrace"]["frames"];
        assert_eq!(frames[0]["addr_mode"], "rel:0");
        assert_eq!(frames[0]["instruction_addr"], "0x10");
        assert_eq!(frames[1]["addr_mode"], "rel:0");
        assert_eq!(frames[1]["instruction_addr"], "0x20");
    }
}
