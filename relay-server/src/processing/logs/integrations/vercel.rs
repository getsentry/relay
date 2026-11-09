use relay_cogs::AppFeature;
use relay_config::ByteSize;
use relay_event_schema::protocol::ourlog::container::{InferIp, InferUserAgent, IngestSettings};
use relay_ourlogs::VercelLog;
use relay_quotas::DataCategory;

use crate::envelope::WithHeader;
use crate::integrations::{Integration, LogsIntegration, VercelLogDrainFormat};
use crate::managed::{Counted, Managed, ManagedEnvelope, RecordKeeper, Rejected};
use crate::processing::logs::integrations::{
    Error, SerializedLogsIntegration, create_intermediates,
};
use crate::processing::{self, Intermediates, Nothing, Output};
use crate::services::outcome::DiscardReason;

#[derive(Debug)]
pub struct SerializedVercelLogsIntegration(SerializedLogsIntegration, VercelLogDrainFormat);

impl Counted for SerializedVercelLogsIntegration {
    fn quantities(&self) -> crate::managed::Quantities {
        self.0.quantities()
    }
}

/// A processor for Vercel log integrations.
///
/// This processor only produces intermediates (items
/// that are put back into the processing pipeline) by
/// converting log integration items into actual logs. It
/// never passes anything on to the store or another Relay.
#[derive(Debug)]
pub struct VercelLogsIntegrationProcessor;

impl processing::Processor for VercelLogsIntegrationProcessor {
    type Input = SerializedVercelLogsIntegration;
    type Output = Nothing;
    type Error = Error;

    fn cogs() -> relay_cogs::FeatureWeights {
        AppFeature::Logs.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let (item, format) =
            envelope
                .envelope_mut()
                .take_item_by_map(|item| match item.integration() {
                    Some(Integration::Logs(LogsIntegration::VercelDrainLog { format })) => {
                        Some(format)
                    }
                    _ => None,
                })?;
        let headers = envelope.envelope().headers().clone();

        let work =
            SerializedVercelLogsIntegration(SerializedLogsIntegration { headers, item }, format);
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

/// Expands a Vercel log integration into an envelope of logs that can be
/// returned to the processing pipeline.
pub fn expand(
    SerializedVercelLogsIntegration(SerializedLogsIntegration { headers, item }, format): SerializedVercelLogsIntegration,
    records: &mut RecordKeeper,
    max_ops: usize,
    max_logs_size: ByteSize,
) -> Result<Intermediates, Error> {
    // Log byte counts will change here as we go from an estimated count based on the body, to
    // accurately counted bytes.
    records.lenient(DataCategory::LogByte);

    let mut total_logs_size = 0;
    let mut logs = Vec::new();
    let mut produce = |log| -> Result<(), Error> {
        // https://github.com/getsentry/relay/issues/6605: This is only used to keep records in case serializing
        // the logs back into an envelope fails. Consider removing it.
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

        Ok(())
    };

    let mut count: i32 = 0;

    let payload = item.payload();
    match format {
        VercelLogDrainFormat::Json => {
            let mut de = serde_json::Deserializer::from_slice(&payload);

            let logs =
                relay_serialization::serde::deserialize::<Vec<VercelLog>, _>(&mut de, max_ops)
                    .map_err(|e| {
                        relay_log::debug!(
                            error = &e as &dyn std::error::Error,
                            "Failed to parse logs data as JSON"
                        );
                        Error::Invalid(DiscardReason::InvalidJson)
                    })?;

            de.end()
                .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?;

            for log in logs {
                count += 1;
                let ourlog = relay_ourlogs::vercel_log_to_sentry_log(log);
                produce(ourlog)?;
            }
        }
        VercelLogDrainFormat::NdJson => {
            for line in payload.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }

                if let Ok(log) = serde_json::from_slice::<VercelLog>(line) {
                    count += 1;
                    let ourlog = relay_ourlogs::vercel_log_to_sentry_log(log);
                    produce(ourlog)?;
                }
            }
        }
    }

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    records.remove(&item);

    if count == 0 {
        relay_log::debug!("Failed to parse any logs from vercel log drain payload");
        return Err(Error::Invalid(DiscardReason::InvalidJson));
    }

    let settings = IngestSettings {
        infer_user_agent: Some(InferUserAgent::Never),
        infer_ip: Some(InferIp::Never),
    };

    create_intermediates(headers, logs, settings)
}
