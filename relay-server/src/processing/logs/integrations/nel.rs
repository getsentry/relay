use relay_cogs::AppFeature;
use relay_event_normalization::nel;
use relay_event_schema::protocol::ourlog::container::{InferIp, InferUserAgent, IngestSettings};
use relay_protocol::DeserializableAnnotated;
use relay_quotas::DataCategory;

use crate::envelope::WithHeader;
use crate::integrations::{Integration, LogsIntegration};
use crate::managed::{Managed, ManagedEnvelope, RecordKeeper, Rejected};
use crate::processing::logs::integrations::{
    Error, SerializedLogsIntegration, create_intermediates,
};
use crate::processing::{self, Intermediates, Nothing, Output};
use crate::services::outcome::DiscardReason;

/// A processor for NEL log integrations.
///
/// This processor only produces intermediates (items
/// that are put back into the processing pipeline) by
/// converting log integration items into actual logs. It
/// never passes anything on to the store or another Relay.
#[derive(Debug)]
pub struct NelLogsIntegrationProcessor;

impl processing::Processor for NelLogsIntegrationProcessor {
    type Input = SerializedLogsIntegration;
    type Output = Nothing;
    type Error = Error;

    fn cogs() -> relay_cogs::FeatureWeights {
        AppFeature::Logs.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let meta = envelope.envelope().headers().meta().clone();

        let item = envelope.envelope_mut().take_item_by(|item| {
            matches!(
                item.integration(),
                Some(Integration::Logs(LogsIntegration::Nel))
            )
        })?;

        let work = SerializedLogsIntegration { meta, item };
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::Input>,
        _ctx: processing::Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let intermediates = work.try_map(expand)?;
        Ok(Output::intermediates(intermediates))
    }
}

/// Expands a NEL log integration into an envelope of logs that can be
/// returned to the processing pipeline.
pub fn expand(
    work: SerializedLogsIntegration,
    records: &mut RecordKeeper,
) -> Result<Intermediates, Error> {
    let SerializedLogsIntegration { meta, item } = work;

    // Log byte counts will change here as we go from an estimated count based on the body, to
    // accurately counted bytes.
    records.lenient(DataCategory::LogByte);

    let payload = item.payload();
    let received_at = meta.received_at();

    let logs = serde_json::from_slice::<Vec<_>>(&payload)
        .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?
        .into_iter()
        .filter_map(|DeserializableAnnotated(nel)| nel::create_log(nel, received_at))
        .map(|log| {
            let byte_size = relay_ourlogs::calculate_size(&log);

            records.modify_by(DataCategory::LogItem, 1);
            records.modify_by(DataCategory::LogByte, byte_size as isize);

            WithHeader::just(log.into())
        })
        .collect();

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    for (category, quantity) in item.quantities() {
        records.modify_by(category, -(quantity as isize));
    }

    let settings = IngestSettings {
        infer_user_agent: Some(InferUserAgent::Auto),
        infer_ip: Some(InferIp::Never),
    };

    create_intermediates(meta, logs, settings)
}
