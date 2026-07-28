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
/// Returns the CPU [`Managed`] envelope and the raw GPU envelope. The GPU envelope is
/// `None` when there is no GPU crash dump, or no scope to copy the GPU event from.
///
/// The GPU envelope is returned unmanaged: it is a new event, so the caller wraps it
/// with [`Managed::from_envelope`] (the same way endpoints manage their primary
/// envelope) to attribute its outcomes to the GPU event id. Wrapping it here via the
/// split would instead share the CPU event's managed metadata.
pub fn split_crash(
    envelope: Managed<Box<Envelope>>,
) -> (Managed<Box<Envelope>>, Option<Box<Envelope>>) {
    // Only a dump makes a GPU crash: the GPU crash processor expands the dump into
    // the event, and the shader debug info (`.nvdbg`) merely rides along. Shader
    // debug on its own would move onto an envelope no expander can claim, so leave
    // it on the CPU event.
    if !envelope.items().any(is_gpu_dump_item) {
        return (envelope, None);
    }

    // The GPU event is a copy of the CPU event's scope, so it inherits the trace,
    // release and tags. Clone the scope items — an `Event`, or the crashpad
    // `__sentry-event` / breadcrumb attachments the event is assembled from. With no
    // scope there is nothing to copy, so leave the crash on the CPU event.
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
        // A fresh id keeps the GPU event distinct from the CPU event it copies; the
        // envelope header id is authoritative and overwrites the cloned one.
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

    // Detach the GPU envelope from the split's shared (CPU) metadata; the caller
    // re-wraps it as its own managed envelope. `accept` moves outcome responsibility
    // to the caller and emits nothing.
    (cpu, Some(gpu.accept(|envelope| envelope)))
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

/// The GPU crash dump (`.nv-gpudmp`) the GPU crash processor turns into the event.
/// Its presence is what triggers the split.
fn is_gpu_dump_item(item: &Item) -> bool {
    item.attachment_type() == Some(AttachmentType::NvGpuDump)
}

/// Items moved onto the GPU envelope: the dump plus any shader debug info
/// (`.nvdbg`) that rides along with it.
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
        // The event and the crashpad `__sentry-event` / breadcrumb attachments are
        // scope.
        assert!(is_scope_item(&Item::new(ItemType::Event)));
        assert!(is_scope_item(&attachment(AttachmentType::EventPayload)));
        assert!(is_scope_item(&attachment(AttachmentType::Breadcrumbs)));

        // The dumps ride on the GPU event but are not scope; the minidump stays on
        // the CPU event.
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
        // The dump and shader debug move onto the GPU envelope, which also carries a
        // copy of the scope. The CPU envelope keeps the minidump and scope.
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
        // Without a scope item there is nothing to copy the GPU event from, so the
        // crash stays on the CPU event rather than being dropped.
        let (_cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::Minidump),
            attachment(AttachmentType::NvGpuDump),
        ]));
        assert!(gpu.is_none());
    }

    #[test]
    fn test_no_split_with_only_shader_debug() {
        // Shader debug info alone is not a GPU crash: the GPU crash processor only
        // expands a dump, so without one there is nothing to claim the split-off
        // envelope. Leave the `.nvdbg` on the CPU event instead of dropping it.
        let (cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::Minidump),
            attachment(AttachmentType::EventPayload),
            attachment(AttachmentType::NvShaderDebug),
        ]));
        assert!(gpu.is_none());
        assert!(attachment_types(&cpu).contains(&Some(AttachmentType::NvShaderDebug)));
    }

    #[test]
    fn test_gpu_outcomes_attributed_to_gpu_event() {
        // The split shares the CPU envelope's managed metadata, whose event id is the
        // CPU event's. The GPU envelope gets a fresh event id and is returned
        // unmanaged, so wrapping it with `from_envelope` attributes its outcomes to
        // that id, not the CPU event's.
        let (outcome_aggregator, mut outcomes) = Addr::custom();
        let meta = RequestMeta::new(
            "https://a94ae32be2582e0bbd7a4cbb95971fee:@sentry.io/42"
                .parse()
                .unwrap(),
        );
        let mut inner = Envelope::from_request(Some(EventId::new()), meta);
        inner.add_item(attachment(AttachmentType::Minidump));
        inner.add_item(attachment(AttachmentType::EventPayload));
        inner.add_item(attachment(AttachmentType::NvGpuDump));
        let cpu_event_id = inner.event_id();

        let managed = Managed::from_envelope(inner, outcome_aggregator.clone());
        let (_cpu, gpu) = split_crash(managed);
        let gpu = gpu.expect("GPU envelope split off");
        let gpu_event_id = gpu.event_id();
        assert!(gpu_event_id.is_some());
        assert_ne!(gpu_event_id, cpu_event_id);

        // Rejecting the GPU envelope (here via drop) emits its outcomes; every one
        // must carry the GPU event id. `_cpu` is left alive so only the GPU envelope
        // emits into the channel.
        let gpu = Managed::from_envelope(gpu, outcome_aggregator);
        drop(gpu);

        let mut emitted = 0;
        while let Ok(outcome) = outcomes.try_recv() {
            assert_eq!(outcome.event_id, gpu_event_id);
            emitted += 1;
        }
        assert!(
            emitted > 0,
            "dropping the GPU envelope should emit outcomes"
        );
    }
}
