use relay_cogs::AppFeature;
use relay_conventions::attributes::SENTRY__RELAY__INGRESS;
use relay_event_normalization::eap::Ingress;
use relay_event_schema::protocol::ourlog::container::{InferIp, InferUserAgent, IngestSettings};
use relay_ourlogs::VercelLog;
use relay_quotas::DataCategory;

use crate::envelope::WithHeader;
use crate::integrations::{Integration, LogsIntegration, VercelLogDrainFormat};
use crate::managed::{Counted, Managed, ManagedEnvelope, RecordKeeper, Rejected};
use crate::processing::logs::integrations::{Error, SerializedLogsIntegration};
use crate::processing::{self, Forward, Intermediates, Output};
use crate::services::outcome::DiscardReason;

/// The "output" of a [`VercelLogsIntegrationProcessor`].
///
/// This type is uninhabited to enforce that the processor
/// can only produce intermediates.
#[derive(Debug, Clone, Copy)]
pub enum VercelLogsIntegrationOutput {}

impl Forward for VercelLogsIntegrationOutput {
    fn serialize_envelope(
        self,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        match self {}
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        _s: processing::StoreHandle<'_>,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        match self {}
    }
}

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
pub struct VercelLogsIntegrationProcessor {}

impl VercelLogsIntegrationProcessor {
    /// Creates a new [`Self`].
    pub fn new() -> Self {
        Self {}
    }
}

impl processing::Processor for VercelLogsIntegrationProcessor {
    type Input = SerializedVercelLogsIntegration;
    type Output = VercelLogsIntegrationOutput;
    type Error = Error;

    fn cogs() -> relay_cogs::FeatureWeights {
        AppFeature::Logs.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let meta = envelope.envelope().headers().meta().clone();

        let (item, format) =
            envelope
                .envelope_mut()
                .take_item_by_map(|item| match item.integration() {
                    Some(Integration::Logs(LogsIntegration::VercelDrainLog { format })) => {
                        Some(format)
                    }
                    _ => None,
                })?;

        let work =
            SerializedVercelLogsIntegration(SerializedLogsIntegration { meta, item }, format);
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::Input>,
        _ctx: processing::Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let intermediates = work.try_map(expand)?;

        Ok(Output {
            main: None,
            metrics: None,
            intermediates: Some(intermediates),
        })
    }
}

/// Expands a Vercel log integration into an envelope of logs that can be
/// returned to the processing pipeline.
pub fn expand(
    work: SerializedVercelLogsIntegration,
    records: &mut RecordKeeper,
) -> Result<Intermediates, Error> {
    let SerializedVercelLogsIntegration(SerializedLogsIntegration { meta, item }, format) = work;

    // Log byte counts will change here as we go from an estimated count based on the body, to
    // accurately counted bytes.
    records.lenient(DataCategory::LogByte);

    let mut logs = Vec::new();
    let mut produce = |mut log| {
        let byte_size = relay_ourlogs::calculate_size(&log);

        records.modify_by(DataCategory::LogItem, 1);
        records.modify_by(DataCategory::LogByte, byte_size as isize);

        // Set the `ingress` attribute. The logs processor will not overwrite it.
        log.attributes
            .value_mut()
            .get_or_insert_default()
            .insert(SENTRY__RELAY__INGRESS, Ingress::Integration.to_string());

        logs.push(WithHeader::just(log.into()));
    };

    let mut count: i32 = 0;

    let payload = item.payload();
    match format {
        VercelLogDrainFormat::Json => {
            let logs = serde_json::from_slice::<Vec<VercelLog>>(&payload).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse logs data as JSON"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })?;

            for log in logs {
                count += 1;
                let ourlog = relay_ourlogs::vercel_log_to_sentry_log(log);
                produce(ourlog);
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
                    produce(ourlog);
                }
            }
        }
    }

    if count == 0 {
        relay_log::debug!("Failed to parse any logs from vercel log drain payload");
        return Err(Error::Invalid(DiscardReason::InvalidJson));
    }

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    for (category, quantity) in item.quantities() {
        records.modify_by(category, -(quantity as isize));
    }

    let settings = IngestSettings {
        infer_user_agent: Some(InferUserAgent::Never),
        infer_ip: Some(InferIp::Never),
    };

    super::create_intermediates(meta, logs, settings)
}
