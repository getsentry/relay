use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::Managed;

/// Splits an envelope containing an error and a GPU crash into two separate envelopes.
///
/// GPU crash attachments are moved into the GPU crash envelope, the error is cloned.
/// The error is cloned into the GPU envelope and GPU crash attachments are separated into the GPU crash.
pub fn split_crash(
    envelope: Managed<Box<Envelope>>,
) -> (Managed<Box<Envelope>>, Option<Managed<Box<Envelope>>>) {
    if !envelope.items().any(is_gpu_crash_item) {
        return (envelope, None);
    }

    let Some(event) = envelope
        .items()
        .find(|item| item.ty() == &ItemType::Event)
        .cloned()
    else {
        return (envelope, None);
    };

    let (cpu, gpu) = envelope.split_once(|mut envelope, records| {
        let mut gpu = Envelope::from_request(None, envelope.meta().clone());
        gpu.add_item(event);
        for item in envelope.take_items_by(is_gpu_crash_item) {
            gpu.add_item(item);
        }

        // We duplicate the error event into a second envelope.
        records.modify_by(DataCategory::Error, 1);

        (envelope, gpu)
    });

    (cpu, Some(gpu))
}

fn is_gpu_crash_item(item: &Item) -> bool {
    matches!(
        item.attachment_type(),
        Some(AttachmentType::NvGpuDump) | Some(AttachmentType::NvShaderDebug)
    )
}
