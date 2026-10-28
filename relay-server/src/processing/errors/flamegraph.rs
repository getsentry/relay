//! Enrichment of exceptions with a representative path from a flame graph attachment.

use std::collections::{BTreeMap, HashMap};

use relay_event_schema::protocol::{
    Addr, DebugId, DebugImage, DebugMeta, Event, Frame, NativeDebugImage, RawStacktrace, Stacktrace,
};
use relay_protocol::{Annotated, Array, DeserializableAnnotated};

use crate::envelope::{AttachmentType, Item};

pub(super) type Culprit = (Annotated<Array<Frame>>, Option<DebugMeta>);

pub(super) fn needs_enrichment(event: &Event) -> bool {
    event
        .exceptions
        .value()
        .and_then(|exceptions| exceptions.values.value())
        .into_iter()
        .flatten()
        .filter_map(|exception| exception.value())
        .any(|exception| {
            exception.stacktrace.value().is_none() && exception.raw_stacktrace.value().is_none()
        })
}

/// Fill a missing exception stack without changing the attachment.
///
/// Existing stacks remain authoritative, including when a processing Relay receives an event
/// already enriched by another Relay. A flame graph attachment opts into this enrichment.
pub fn enrich(event: &mut Event, culprit: impl FnOnce() -> Option<Culprit>) -> bool {
    let Some(exception) = event
        .exceptions
        .value_mut()
        .as_mut()
        .and_then(|exceptions| exceptions.values.value_mut().as_mut())
        .into_iter()
        .flatten()
        .filter_map(|value| value.value_mut().as_mut())
        .find(|exception| {
            exception.stacktrace.value().is_none() && exception.raw_stacktrace.value().is_none()
        })
    else {
        return false;
    };

    let Some((mut frames, debug_meta)) = culprit() else {
        return false;
    };
    if let Some(debug_meta) = debug_meta {
        merge_debug_images(
            &mut event.debug_meta,
            debug_meta,
            frames.get_or_insert_with(Vec::new),
        );
    }
    exception.stacktrace = Annotated::new(Stacktrace::from(RawStacktrace {
        frames,
        ..Default::default()
    }));
    true
}

pub(super) fn extract_attachments(attachments: &[Item], max_event_size: usize) -> Option<Culprit> {
    attachments.iter().find_map(|item| {
        if item.attachment_type() != Some(AttachmentType::Flamegraph) {
            return None;
        }
        let payload = item.payload();
        let graph = relay_profiling::flamegraph::parse(&payload).ok()?;
        extract(&graph, max_event_size)
    })
}

pub(super) fn extract(
    graph: &relay_profiling::flamegraph::Graph<'_>,
    max_event_size: usize,
) -> Option<Culprit> {
    let culprit = graph.identify_culprit().ok()?;
    let indices = culprit.frames;
    // Recursion can reference one large frame many times. Bound the derived event before
    // materializing frames, rather than amplifying a small attachment ahead of normalization.
    indices
        .iter()
        .try_fold(max_event_size, |remaining, index| {
            remaining.checked_sub(graph.frame(*index).get().len())
        })?;
    let frames = indices
        .into_iter()
        .map(|index| {
            serde_json::from_str::<DeserializableAnnotated<Frame>>(graph.frame(index).get())
                .map(|frame| frame.0)
        })
        .collect::<Result<Array<Frame>, _>>()
        .ok()?;
    let debug_meta = graph
        .debug_meta()
        .map(|raw| serde_json::from_str::<DeserializableAnnotated<DebugMeta>>(raw.get()))
        .transpose()
        .ok()?
        .and_then(|meta| meta.0.into_value());
    let mut frames = Annotated::new(frames);
    if culprit.original_len > 250 {
        frames
            .meta_mut()
            .set_original_length(Some(culprit.original_len));
    }
    Some((frames, debug_meta))
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
    use relay_protocol::{Annotated, SerializableAnnotated};
    use serde_json::{Value, json};

    use super::*;
    use crate::envelope::{AttachmentType, ContentType, ItemType};

    fn enrich(event: &mut Event, attachments: &[Item]) -> bool {
        super::enrich(event, || {
            extract_attachments(
                attachments,
                relay_config::Config::default().current().max_event_size(),
            )
        })
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

        insta::assert_json_snapshot!(SerializableAnnotated(&event), @r#"
        {
          "platform": "cocoa",
          "exception": {
            "values": [
              {
                "type": "MXHangDiagnostic",
                "value": "hangDuration:2s",
                "stacktrace": {
                  "frames": [
                    {
                      "in_app": false,
                      "instruction_addr": "0x10",
                      "addr_mode": "rel:0"
                    },
                    {
                      "function": "work",
                      "package": "SampleApp",
                      "filename": "App.swift",
                      "lineno": 42,
                      "in_app": true,
                      "instruction_addr": "0x20",
                      "addr_mode": "rel:0"
                    }
                  ]
                },
                "mechanism": {
                  "type": "mx_hang_diagnostic",
                  "synthetic": true,
                  "handled": true
                }
              }
            ]
          },
          "debug_meta": {
            "images": [
              {
                "code_id": "abcdef",
                "code_file": "SampleApp",
                "debug_id": "84a04d24-0e60-3810-a8c0-90a65e2df61a",
                "arch": "arm64",
                "image_addr": "0x1000",
                "image_size": 4096,
                "image_vmaddr": "0x100000000",
                "type": "macho"
              }
            ]
          }
        }
        "#);
        assert_eq!(attachments[0].payload(), original);
    }

    #[test]
    fn test_flamegraph_enriches_without_metrickit_mechanism() {
        for mechanism in [json!({"type": "generic"}), Value::Null] {
            let mut value: Value = serde_json::from_str(&event().to_json().unwrap()).unwrap();
            value["exception"]["values"][0]["mechanism"] = mechanism;
            let mut event = Annotated::<Event>::from_json(&value.to_string()).unwrap();
            assert!(enrich(
                event.value_mut().as_mut().unwrap(),
                &[attachment(graph())]
            ));
        }
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
        // The absolute frame must resolve against the diagnostic's image, not the current run's.
        let mut payload = graph();
        payload["frames"][1]["addr_mode"] = json!("rel:0");
        payload["frames"][1]["instruction_addr"] = json!("0x20");

        assert!(enrich(
            event.value_mut().as_mut().unwrap(),
            &[attachment(payload)]
        ));

        insta::assert_json_snapshot!(SerializableAnnotated(&event), @r#"
        {
          "platform": "cocoa",
          "exception": {
            "values": [
              {
                "type": "MXHangDiagnostic",
                "value": "hangDuration:2s",
                "stacktrace": {
                  "frames": [
                    {
                      "in_app": false,
                      "instruction_addr": "0x10",
                      "addr_mode": "rel:1"
                    },
                    {
                      "function": "work",
                      "package": "SampleApp",
                      "filename": "App.swift",
                      "lineno": 42,
                      "in_app": true,
                      "instruction_addr": "0x20",
                      "addr_mode": "rel:1"
                    }
                  ]
                },
                "mechanism": {
                  "type": "mx_hang_diagnostic",
                  "synthetic": true,
                  "handled": true
                }
              }
            ]
          },
          "debug_meta": {
            "images": [
              {
                "code_file": "CurrentRun",
                "debug_id": "94a04d24-0e60-3810-a8c0-90a65e2df61a",
                "image_addr": "0x1000",
                "image_size": 4096,
                "type": "macho"
              },
              {
                "code_id": "abcdef",
                "code_file": "SampleApp",
                "debug_id": "84a04d24-0e60-3810-a8c0-90a65e2df61a",
                "arch": "arm64",
                "image_addr": "0x1000",
                "image_size": 4096,
                "image_vmaddr": "0x100000000",
                "type": "macho"
              }
            ]
          }
        }
        "#);
    }

    #[test]
    fn test_flamegraph_does_not_materialize_discarded_middle_frames() {
        let mut payload = graph();
        payload["frames"][2]["filename"] = json!("x".repeat(200_000));
        payload["nodes"] = (0..251)
            .map(|index| {
                json!({
                    "frame_id": if index == 50 { 2 } else { 1 },
                    "sample_count": 1,
                    "children": if index < 250 { vec![index + 1] } else { vec![] }
                })
            })
            .collect();
        let mut event = event();
        assert!(super::enrich(event.value_mut().as_mut().unwrap(), || {
            extract_attachments(&[attachment(payload)], 100_000)
        }));
        let event = event.value().unwrap();
        let exception = event.exceptions.value().unwrap().values.value().unwrap()[0]
            .value()
            .unwrap();
        let frames = exception
            .stacktrace
            .value()
            .unwrap()
            .frames
            .value()
            .unwrap();
        assert_eq!(
            exception
                .stacktrace
                .value()
                .unwrap()
                .frames
                .meta()
                .original_length(),
            Some(251)
        );
        assert_eq!(frames.len(), 250);
        assert!(frames.iter().all(|frame| {
            frame
                .value()
                .unwrap()
                .filename
                .value()
                .is_some_and(|path| path.0 == "App.swift")
        }));
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
        insta::assert_json_snapshot!(SerializableAnnotated(&event), @r#"
        {
          "platform": "cocoa",
          "exception": {
            "values": [
              {
                "type": "MXHangDiagnostic",
                "value": "hangDuration:2s",
                "stacktrace": {
                  "frames": [
                    {
                      "in_app": false,
                      "instruction_addr": "0x10",
                      "addr_mode": "rel:0"
                    },
                    {
                      "function": "work",
                      "package": "SampleApp",
                      "filename": "App.swift",
                      "lineno": 42,
                      "in_app": true,
                      "instruction_addr": "0x20",
                      "addr_mode": "rel:0"
                    }
                  ]
                },
                "mechanism": {
                  "type": "mx_hang_diagnostic",
                  "synthetic": true,
                  "handled": true
                }
              }
            ]
          },
          "debug_meta": {
            "images": [
              {
                "code_id": "abcdef",
                "code_file": "SampleApp",
                "debug_id": "84a04d24-0e60-3810-a8c0-90a65e2df61a",
                "arch": "arm64",
                "image_addr": "0x1000",
                "image_vmaddr": "0x100000000",
                "type": "macho"
              }
            ]
          }
        }
        "#);
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

        insta::assert_json_snapshot!(SerializableAnnotated(&event), @r#"
        {
          "platform": "cocoa",
          "exception": {
            "values": [
              {
                "type": "MXHangDiagnostic",
                "value": "hangDuration:2s",
                "stacktrace": {
                  "frames": [
                    {
                      "in_app": false,
                      "instruction_addr": "0x10",
                      "addr_mode": "rel:0"
                    },
                    {
                      "function": "work",
                      "package": "SampleApp",
                      "filename": "App.swift",
                      "lineno": 42,
                      "in_app": true,
                      "instruction_addr": "0x20",
                      "addr_mode": "rel:0"
                    }
                  ]
                },
                "mechanism": {
                  "type": "mx_hang_diagnostic",
                  "synthetic": true,
                  "handled": true
                }
              }
            ]
          },
          "debug_meta": {
            "images": [
              {
                "code_id": "abcdef",
                "code_file": "SampleApp",
                "debug_id": "84a04d24-0e60-3810-a8c0-90a65e2df61a",
                "arch": "arm64",
                "image_addr": "0x1000",
                "image_size": 4096,
                "image_vmaddr": "0x100000000",
                "type": "macho"
              }
            ]
          }
        }
        "#);
    }
}
