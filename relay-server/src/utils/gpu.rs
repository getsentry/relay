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

    // The GPU dump is itself event-creating. If it is the *only* such item, moving it
    // to the GPU envelope would leave the CPU envelope with nothing to turn into an
    // event — the error processor skips envelopes without an event-creating item
    // (see [`crate::processing::errors`]), so the original crash would be dropped as
    // orphaned attachments. This happens for Unreal crashes whose event lives in the
    // (non-event-creating) `UnrealContext` with no minidump alongside it. Keep the
    // crash whole in that case rather than splitting.
    if !envelope
        .items()
        .any(|item| item.creates_event() && !is_gpu_crash_item(item))
    {
        return (envelope, None);
    }

    // The GPU event is a copy of the CPU event's scope, so it inherits the trace,
    // release and tags. Clone the scope items — an `Event`, the crashpad
    // `__sentry-event` / breadcrumb attachments, or the Unreal context the event is
    // assembled from. With no scope there is nothing to copy, so leave the crash on
    // the CPU event.
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
        // The GPU envelope is its own crash, expanded by the GPU crash processor.
        // The Unreal endpoint marks every extracted item `unreal_expanded`; clear it
        // on the copied scope and moved dumps so the Unreal expander (ordered before
        // the GPU one) does not claim this envelope as an Unreal report.
        let dumps = envelope.take_items_by(is_gpu_crash_item);
        for mut item in scope.into_iter().chain(dumps) {
            if item.is_unreal_expanded() {
                item.set_unreal_expanded(false);
            }
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

/// Scope items that carry the event: an [`ItemType::Event`], the crashpad
/// `__sentry-event` ([`AttachmentType::EventPayload`]) and breadcrumb attachments
/// the event is assembled from, or the Unreal context
/// ([`AttachmentType::UnrealContext`]) whose `__sentry` game data holds the event
/// payload. Deliberately narrower than [`Item::creates_event`] (which also matches
/// minidumps, which must stay on the CPU event).
fn is_scope_item(item: &Item) -> bool {
    item.ty() == &ItemType::Event
        || matches!(
            item.attachment_type(),
            Some(
                AttachmentType::EventPayload
                    | AttachmentType::Breadcrumbs
                    | AttachmentType::UnrealContext
            )
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

    fn attachment(ty: AttachmentType, unreal_expanded: bool) -> Item {
        let mut item = Item::new(ItemType::Attachment);
        item.set_attachment_type(ty);
        item.set_unreal_expanded(unreal_expanded);
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
        // The Unreal context is scope: its `__sentry` game data holds the event.
        assert!(is_scope_item(&attachment(
            AttachmentType::UnrealContext,
            false
        )));
        assert!(is_scope_item(&attachment(
            AttachmentType::EventPayload,
            false
        )));
        assert!(is_scope_item(&attachment(
            AttachmentType::Breadcrumbs,
            false
        )));
        assert!(is_scope_item(&Item::new(ItemType::Event)));

        // The dumps ride on the GPU event but are not scope; the minidump and logs
        // stay on the CPU event.
        assert!(!is_scope_item(&attachment(
            AttachmentType::NvGpuDump,
            false
        )));
        assert!(!is_scope_item(&attachment(AttachmentType::Minidump, false)));
        assert!(!is_scope_item(&attachment(
            AttachmentType::UnrealLogs,
            false
        )));
    }

    #[test]
    fn test_is_gpu_crash_item() {
        assert!(is_gpu_crash_item(&attachment(
            AttachmentType::NvGpuDump,
            false
        )));
        assert!(is_gpu_crash_item(&attachment(
            AttachmentType::NvShaderDebug,
            false
        )));
        assert!(!is_gpu_crash_item(&attachment(
            AttachmentType::Minidump,
            false
        )));
        assert!(!is_gpu_crash_item(&attachment(
            AttachmentType::UnrealContext,
            false
        )));
    }

    #[test]
    fn test_split_unreal_gpu_crash_clears_unreal_expanded() {
        // An Unreal endpoint expansion marks every extracted item `unreal_expanded`.
        // The GPU dump splits off onto its own envelope with a copy of the Unreal
        // context as scope; those items must not stay `unreal_expanded`, or the
        // Unreal expander (ordered before the GPU one) would claim the GPU envelope
        // instead of letting the GPU crash processor expand it.
        let (cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::Minidump, true),
            attachment(AttachmentType::UnrealContext, true),
            attachment(AttachmentType::NvGpuDump, true),
            attachment(AttachmentType::NvShaderDebug, true),
        ]));
        let gpu = gpu.expect("GPU envelope split off using the Unreal context scope");

        // The GPU envelope carries the dumps plus a copy of the context, none of
        // which are still flagged as extracted from an Unreal report.
        let gpu_types = attachment_types(&gpu);
        assert!(gpu_types.contains(&Some(AttachmentType::NvGpuDump)));
        assert!(gpu_types.contains(&Some(AttachmentType::NvShaderDebug)));
        assert!(gpu_types.contains(&Some(AttachmentType::UnrealContext)));
        assert!(!gpu_types.contains(&Some(AttachmentType::Minidump)));
        assert!(gpu.items().all(|item| !item.is_unreal_expanded()));

        // The CPU envelope keeps the minidump and context; the dumps moved off it.
        let cpu_types = attachment_types(&cpu);
        assert!(cpu_types.contains(&Some(AttachmentType::Minidump)));
        assert!(cpu_types.contains(&Some(AttachmentType::UnrealContext)));
        assert!(!cpu_types.contains(&Some(AttachmentType::NvGpuDump)));
        assert!(!cpu_types.contains(&Some(AttachmentType::NvShaderDebug)));
    }

    #[test]
    fn test_no_split_without_scope() {
        // Without a scope item there is nothing to copy the GPU event from, so the
        // crash stays on the CPU event rather than being dropped.
        let (_cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::Minidump, true),
            attachment(AttachmentType::NvGpuDump, true),
        ]));
        assert!(gpu.is_none());
    }

    #[test]
    fn test_no_split_with_only_shader_debug() {
        // Shader debug info alone is not a GPU crash: the GPU crash processor only
        // expands a dump, so without one there is nothing to claim the split-off
        // envelope. Leave the `.nvdbg` on the CPU event instead of dropping it.
        let (cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::Minidump, true),
            attachment(AttachmentType::UnrealContext, true),
            attachment(AttachmentType::NvShaderDebug, true),
        ]));
        assert!(gpu.is_none());
        assert!(attachment_types(&cpu).contains(&Some(AttachmentType::NvShaderDebug)));
    }

    #[test]
    fn test_no_split_when_dump_is_the_only_event() {
        // An Unreal crash whose event lives in the (non-event-creating) `UnrealContext`
        // with a dump but no minidump: moving the dump would strand the CPU envelope
        // (no event-creating item left), so it never becomes an event. Keep it whole.
        let (cpu, gpu) = split_crash(envelope([
            attachment(AttachmentType::UnrealContext, true),
            attachment(AttachmentType::UnrealLogs, true),
            attachment(AttachmentType::NvGpuDump, true),
        ]));
        assert!(gpu.is_none());
        // The dump stays on the CPU event rather than being dropped.
        assert!(attachment_types(&cpu).contains(&Some(AttachmentType::NvGpuDump)));
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
        // The minidump keeps the CPU envelope an event, mirroring the real flow.
        inner.add_item(attachment(AttachmentType::Minidump, true));
        inner.add_item(attachment(AttachmentType::UnrealContext, true));
        inner.add_item(attachment(AttachmentType::NvGpuDump, true));
        let cpu_event_id = inner.event_id();

        let managed = Managed::from_envelope(inner, outcome_aggregator.clone());
        let (_cpu, gpu) = split_crash(managed);
        let gpu = gpu.expect("GPU envelope split off using the Unreal context scope");
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
