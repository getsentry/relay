use relay_cogs::{AppFeature, FeatureWeights};
use relay_dynamic_config::Feature;
use relay_protocol::Empty;
use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, Quantities, RecordKeeper, Rejected};
use crate::processing::errors::{Error, Result, utils};
use crate::processing::{self, Context, Intermediates, Nothing, Output};
use crate::services::processor::ProcessingError;
use crate::utils::{ProcessedUnrealReport, UnrealExpansion};

/// A processor for Unreal items created by Relay's Unreal endpoint.
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
        let report = envelope
            .envelope_mut()
            .take_item_by(|item| *item.ty() == ItemType::UnrealReport)?;

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

/// Converts a [`ItemType::UnrealReport`] into the envelope protocol.
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

    let context_stacktrace = ctx
        .project_info
        .has_feature(Feature::UnrealCrashContextStacktrace);

    let report = crate::utils::process_unreal(
        context,
        event_id,
        &mut event,
        &attachments,
        user_header,
        context_stacktrace,
    )
    .map_err(ProcessingError::InvalidUnrealReport)?;

    let mut envelope = Envelope::from_request(Some(event_id), unreal.headers.meta().clone());

    // It's possible that there still is no event and a later pass through the error processor must
    // create a dummy/placeholder event for the contained crash.
    if !event.is_empty() {
        envelope.add_item({
            let mut item = Item::new(ItemType::Event);
            item.set_payload(
                ContentType::Json,
                event.to_json().map_err(ProcessingError::SerializeFailed)?,
            );
            item
        });
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

    Ok(Intermediates::from_envelope(envelope))
}
