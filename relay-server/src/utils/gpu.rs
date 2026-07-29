use relay_event_schema::protocol::EventId;
use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::Managed;

/// Splits a GPU crash dump off an error envelope into its own event.
///
/// The dump and its debug info move onto a second envelope carrying a copy of the
/// scope, so it becomes its own trace-connected, billed event while the original
/// crash keeps its envelope. The GPU crash processor turns the copied scope into
/// the event.
///
/// The GPU envelope is `None` when there is no dump or no scope to copy the event
/// from.
pub fn split_crash(
    envelope: Managed<Box<Envelope>>,
) -> (Managed<Box<Envelope>>, Option<Managed<Box<Envelope>>>) {
    if !envelope.items().any(is_gpu_dump_item) {
        return (envelope, None);
    }

    // Without a scope there is nothing to build the GPU event from.
    let scope: Vec<Item> = envelope
        .items()
        .filter(|item| is_scope_item(item))
        .cloned()
        .collect();
    if scope.is_empty() {
        return (envelope, None);
    }

    // Cloning the scope duplicates the event and any attachment-shaped scope items.
    let mut duplicated: Vec<(DataCategory, isize)> = vec![(DataCategory::Error, 1)];
    for item in &scope {
        for (category, quantity) in item.quantities() {
            if category != DataCategory::Error {
                duplicated.push((category, quantity as isize));
            }
        }
    }

    let (cpu, gpu) = envelope.split_once(move |mut envelope, records| {
        let mut gpu = Envelope::from_request(Some(EventId::new()), envelope.meta().clone());
        for item in scope {
            gpu.add_item(item);
        }
        for item in envelope.take_items_by(is_gpu_crash_item) {
            gpu.add_item(item);
        }

        for (category, quantity) in duplicated {
            records.modify_by(category, quantity);
        }

        (envelope, gpu)
    });

    (cpu, Some(gpu))
}

/// Items an event is assembled from. Narrower than [`Item::creates_event`], which
/// also matches crash reports that must stay on the original event.
fn is_scope_item(item: &Item) -> bool {
    item.ty() == &ItemType::Event
        || matches!(
            item.attachment_type(),
            Some(AttachmentType::EventPayload | AttachmentType::Breadcrumbs)
        )
}

/// The GPU crash dump whose presence triggers the split.
fn is_gpu_dump_item(item: &Item) -> bool {
    item.attachment_type() == Some(AttachmentType::NvGpuDump)
}

/// The dump plus the debug info moved onto the GPU envelope alongside it.
fn is_gpu_crash_item(item: &Item) -> bool {
    matches!(
        item.attachment_type(),
        Some(AttachmentType::NvGpuDump) | Some(AttachmentType::NvShaderDebug)
    )
}

#[cfg(test)]
mod tests {
    use relay_system::Addr;

    use super::*;
    use crate::extractors::RequestMeta;

    fn attachment(ty: AttachmentType) -> Item {
        let mut item = Item::new(ItemType::Attachment);
        item.set_attachment_type(ty);
        item
    }

    fn envelope(items: impl IntoIterator<Item = Item>) -> Managed<Box<Envelope>> {
        let meta = RequestMeta::new(
            "https://a94ae32be2582e0bbd7a4cbb95971fee:@sentry.io/42"
                .parse()
                .unwrap(),
        );
        let mut envelope = Envelope::from_request(Some(EventId::new()), meta);
        for item in items {
            envelope.add_item(item);
        }
        Managed::from_envelope(envelope, Addr::dummy())
    }

    fn attachment_types(envelope: &Envelope) -> Vec<Option<AttachmentType>> {
        envelope
            .items()
            .map(|item| item.attachment_type())
            .collect()
    }

    #[test]
    fn test_is_scope_item() {
        assert!(is_scope_item(&Item::new(ItemType::Event)));
        assert!(is_scope_item(&attachment(AttachmentType::EventPayload)));
        assert!(is_scope_item(&attachment(AttachmentType::Breadcrumbs)));

        assert!(!is_scope_item(&attachment(AttachmentType::NvGpuDump)));
        assert!(!is_scope_item(&attachment(AttachmentType::Minidump)));
    }

    #[test]
    fn test_is_gpu_crash_item() {
        assert!(is_gpu_crash_item(&attachment(AttachmentType::NvGpuDump)));
        assert!(is_gpu_crash_item(&attachment(
            AttachmentType::NvShaderDebug
        )));
        assert!(!is_gpu_crash_item(&attachment(AttachmentType::Minidump)));
    }

    #[test]
    fn test_split_moves_dump_and_copies_scope() {
        let (cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::Minidump),
            attachment(AttachmentType::EventPayload),
            attachment(AttachmentType::NvGpuDump),
            attachment(AttachmentType::NvShaderDebug),
        ]));
        let gpu = gpu.expect("GPU envelope split off");

        let gpu_types = attachment_types(&gpu);
        assert!(gpu_types.contains(&Some(AttachmentType::NvGpuDump)));
        assert!(gpu_types.contains(&Some(AttachmentType::NvShaderDebug)));
        assert!(gpu_types.contains(&Some(AttachmentType::EventPayload)));
        assert!(!gpu_types.contains(&Some(AttachmentType::Minidump)));

        let cpu_types = attachment_types(&cpu);
        assert!(cpu_types.contains(&Some(AttachmentType::Minidump)));
        assert!(cpu_types.contains(&Some(AttachmentType::EventPayload)));
        assert!(!cpu_types.contains(&Some(AttachmentType::NvGpuDump)));
        assert!(!cpu_types.contains(&Some(AttachmentType::NvShaderDebug)));
    }

    #[test]
    fn test_no_split_without_scope() {
        let (_cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::Minidump),
            attachment(AttachmentType::NvGpuDump),
        ]));
        assert!(gpu.is_none());
    }

    #[test]
    fn test_no_split_with_only_shader_debug() {
        // A dump is required to split; debug info alone stays on the original event.
        let (cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::Minidump),
            attachment(AttachmentType::EventPayload),
            attachment(AttachmentType::NvShaderDebug),
        ]));
        assert!(gpu.is_none());
        assert!(attachment_types(&cpu).contains(&Some(AttachmentType::NvShaderDebug)));
    }
}
