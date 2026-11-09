use relay_cogs::AppFeature;
use relay_config::ByteSize;
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
        let item = envelope.envelope_mut().take_item_by(|item| {
            matches!(
                item.integration(),
                Some(Integration::Logs(LogsIntegration::Nel))
            )
        })?;
        let headers = envelope.envelope().headers().clone();

        let work = SerializedLogsIntegration { headers, item };
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::Input>,
        ctx: processing::Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let intermediates = work.try_map(|work, records| {
            expand(
                work,
                records,
                ctx.config.max_expanded_log_operations(),
                ctx.config.max_expanded_logs_size(),
            )
        })?;
        Ok(Output::intermediates(intermediates))
    }
}

/// Expands a NEL log integration into an envelope of logs that can be
/// returned to the processing pipeline.
pub fn expand(
    SerializedLogsIntegration { headers, item }: SerializedLogsIntegration,
    records: &mut RecordKeeper,
    max_ops: usize,
    max_logs_size: ByteSize,
) -> Result<Intermediates, Error> {
    // Log byte counts will change here as we go from an estimated count based on the body, to
    // accurately counted bytes.
    records.lenient(DataCategory::LogByte);

    let payload = item.payload();
    let received_at = headers.meta().received_at();
    let mut total_logs_size = 0;

    let mut de = serde_json::Deserializer::from_slice(&payload);

    let reports: std::vec::IntoIter<
        DeserializableAnnotated<relay_event_schema::protocol::NetworkReportRaw>,
    > = relay_serialization::serde::deserialize::<Vec<_>, _>(&mut de, max_ops)
        .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?
        .into_iter();
    de.end()
        .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?;

    let mut logs = vec![];
    for report in reports {
        if let Some(log) = nel::create_log(report.0, received_at) {
            let byte_size = relay_ourlogs::calculate_size(&log);

            records.add([
                (DataCategory::LogItem, 1),
                (DataCategory::LogByte, byte_size as usize),
            ]);

            total_logs_size += byte_size as usize;

            if total_logs_size > max_logs_size.as_bytes() {
                return Err(Error::Invalid(DiscardReason::RequestTooLarge));
            }

            logs.push(WithHeader::just(log.into()));
        }
    }

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    records.remove(&item);

    let settings = IngestSettings {
        infer_user_agent: Some(InferUserAgent::Auto),
        infer_ip: Some(InferIp::Never),
    };

    create_intermediates(headers, logs, settings)
}
