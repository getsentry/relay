use relay_cogs::{AppFeature, FeatureWeights};
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;
use relay_protocol::Empty;
use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{AttachmentType, ContentType, EnvelopeHeaders, Item, ItemType};
use crate::integrations::{ErrorsIntegration, Integration};
use crate::managed::{Counted, Managed, ManagedEnvelope, Quantities, RecordKeeper, Rejected};
use crate::processing::errors::{Error, Result, utils};
use crate::processing::{self, Context, Intermediates, Nothing, Output};
use crate::services::processor::ProcessingError;
use crate::utils::{ProcessedUnrealReport, UnrealExpansion, gpu};

/// A processor for Unreal items created by Relay's Unreal endpoint.
///
/// Also accepts legacy [`ItemType::UnrealReport`] items, which are still forwarded by older
/// Relays.
#[derive(Debug)]
pub struct UnrealErrorIntegrationProcessor;

impl processing::Processor for UnrealErrorIntegrationProcessor {
    type Input = SerializedUnrealReport;
    type Output = Nothing;
    type Error = Error;

    fn cogs() -> FeatureWeights {
        AppFeature::Errors.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let report = envelope.envelope_mut().take_item_by(|item| {
            matches!(
                item.integration(),
                Some(Integration::Errors(ErrorsIntegration::Unreal))
            ) || *item.ty() == ItemType::UnrealReport
        })?;

        let headers = envelope.envelope().headers().clone();

        let work = SerializedUnrealReport { headers, report };
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        unreal: Managed<Self::Input>,
        ctx: processing::Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let unreal = unreal.try_map(|unreal, r| convert(unreal, r, ctx))?;
        Ok(Output::intermediates(unreal))
    }
}

/// A serialized unreal report.
#[derive(Debug)]
pub struct SerializedUnrealReport {
    headers: EnvelopeHeaders,
    report: Item,
}

impl Counted for SerializedUnrealReport {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Error, 1)]
    }
}

/// Converts an Unreal crash report into the envelope protocol.
fn convert(
    unreal: SerializedUnrealReport,
    records: &mut RecordKeeper<'_>,
    ctx: Context<'_>,
) -> Result<Intermediates> {
    let event_id = unreal.headers.event_id().unwrap_or_default();

    let UnrealExpansion {
        event,
        context,
        attachments,
    } = crate::utils::expand_unreal(unreal.report.payload(), ctx.config)?;

    let mut attachments = attachments.to_vec();

    let mut metrics = Default::default();
    let mut event = match event {
        Some(event) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
        None => utils::take_event_from_attachments(&mut attachments, &mut metrics, ctx)?,
    };
    // Metrics are not actually required here, they will be calculated again by the error processor.
    drop(metrics);

    let user_header = unreal
        .headers
        .get_header(crate::constants::UNREAL_USER_HEADER)
        .and_then(|v| v.as_str());

    let report =
        crate::utils::process_unreal(context, event_id, &mut event, &attachments, user_header)
            .map_err(ProcessingError::InvalidUnrealReport)?;

    let mut intermediates = Intermediates::default();

    let mut envelope = Envelope::from_request(Some(event_id), unreal.headers.meta().clone());

    // It's possible that there still is no event and a later pass through the error processor must
    // create a dummy/placeholder event for the contained crash.
    let event = match event.is_empty() {
        true => None,
        false => {
            let mut item = Item::new(ItemType::Event);
            item.set_payload(
                ContentType::Json,
                event.to_json().map_err(ProcessingError::SerializeFailed)?,
            );
            Some(item)
        }
    };

    if ctx.project_info.has_feature(Feature::NvGpuCrashSplit)
        && let Some(gpu) = take_gpu_crash(&unreal.headers, event.as_ref(), &mut attachments)
    {
        records.add(&gpu);
        intermediates.add_envelope(gpu);
    }

    if let Some(event) = event {
        envelope.add_item(event);
    }

    for attachment in attachments {
        records.add(&attachment);
        envelope.add_item(attachment);
    }

    if let Some(ProcessedUnrealReport { user_reports }) = report {
        for user_report in user_reports {
            records.add(&user_report);
            envelope.add_item(user_report);
        }
    }

    // There must at least be an `event` item with data or a crash which can create an event.
    if !envelope.items().any(|item| item.creates_event()) {
        return Err(ProcessingError::NoEventPayload.into());
    }
    intermediates.add_envelope(envelope);

    Ok(intermediates)
}

fn take_gpu_crash(
    headers: &EnvelopeHeaders,
    event: Option<&Item>,
    items: &mut Vec<Item>,
) -> Option<Box<Envelope>> {
    // No need to fork off an envelope if there is no GPU crash at all.
    if !items.iter().any(is_cpu_dump_item) {
        return None;
    }

    if !items.iter().any(gpu::is_gpu_dump_item) {
        return None;
    }

    let mut envelope = Envelope::from_request(Some(EventId::new()), headers.meta().clone());
    if let Some(event) = event {
        envelope.add_item(event.clone());
    }
    for item in items.extract_if(.., |item| gpu::is_gpu_crash_item(item)) {
        envelope.add_item(item);
    }

    Some(envelope)
}

fn is_cpu_dump_item(item: &Item) -> bool {
    matches!(
        item.attachment_type(),
        Some(AttachmentType::Minidump | AttachmentType::AppleCrashReport)
    )
}
