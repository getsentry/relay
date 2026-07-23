use relay_event_schema::protocol::EventId;
use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::Managed;

/// Splits an envelope carrying an error event and a GPU crash into two envelopes.
///
/// The GPU crash attachments (`.nv-gpudmp` / `.nvdbg`) are moved onto a second
/// envelope that also carries a copy of the scope items, so the GPU crash becomes
/// its own trace-connected, billed event while the CPU crash keeps the original.
/// The GPU crash processor (see [`crate::processing::errors`]) turns the copied
/// scope into the event.
///
/// Returns the `(cpu, gpu)` envelopes. The GPU envelope is `None` when there is no
/// GPU crash item, or no scope to copy the GPU event from.
pub fn split_crash(
    envelope: Managed<Box<Envelope>>,
) -> (Managed<Box<Envelope>>, Option<Managed<Box<Envelope>>>) {
    if !envelope.items().any(is_gpu_crash_item) {
        return (envelope, None);
    }

    // The GPU event is a copy of the CPU event's scope, so it inherits the trace,
    // release and tags. Clone the scope items — an `Event`, or the crashpad
    // `__sentry-event` / breadcrumb attachments the event is assembled from. With
    // no scope there is nothing to copy, so leave the crash on the CPU event.
    let scope: Vec<Item> = envelope
        .items()
        .filter(|item| is_scope_item(item))
        .cloned()
        .collect();
    if scope.is_empty() {
        return (envelope, None);
    }

    // The GPU event is billed as a duplicated error; cloning the scope also
    // duplicates any attachment-shaped scope items, so account for those too.
    let mut duplicated: Vec<(DataCategory, isize)> = vec![(DataCategory::Error, 1)];
    for item in &scope {
        for (category, quantity) in item.quantities() {
            if category != DataCategory::Error {
                duplicated.push((category, quantity as isize));
            }
        }
    }

    let (cpu, gpu) = envelope.split_once(move |mut envelope, records| {
        // A fresh id keeps the GPU event distinct from the CPU event it copies;
        // the envelope header id is authoritative and overwrites the cloned one.
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

/// Scope items that carry the event: an [`ItemType::Event`], or the crashpad
/// `__sentry-event` ([`AttachmentType::EventPayload`]) and breadcrumb attachments
/// the event is assembled from. Deliberately narrower than [`Item::creates_event`]
/// (which also matches minidumps, which must stay on the CPU event).
fn is_scope_item(item: &Item) -> bool {
    item.ty() == &ItemType::Event
        || matches!(
            item.attachment_type(),
            Some(AttachmentType::EventPayload | AttachmentType::Breadcrumbs)
        )
}

fn is_gpu_crash_item(item: &Item) -> bool {
    matches!(
        item.attachment_type(),
        Some(AttachmentType::NvGpuDump) | Some(AttachmentType::NvShaderDebug)
    )
}
